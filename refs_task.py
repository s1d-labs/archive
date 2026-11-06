"""The reference-scan task (scrape.json "refs"), run on GitHub runners: B2 is close and reads run in parallel there.
Each queue item is a sha256 prefix. The worker asks the archive API for presigned links to the texts under that prefix
(/refs/work), reads them, finds exact identifier and tender references with archive.refs.refs(), and reports them
(/refs/report). No B2 key on the runner; only the matches travel back to dev-vps-2."""
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from archive.refs import refs

PROJECT = os.environ.get("SCRAPE_POOL_PROJECT", "")
API = os.environ["ARCHIVE_API"]
TOKEN = os.environ["ARCHIVE_INGEST_TOKEN"]
READERS = int(os.environ.get("REFS_READERS", "48"))
DEADLINE = time.time() + float(os.environ.get("SCRAPE_MAX_HOURS", "1")) * 3600 - 300
UA = "archive-pool/1.0 (+https://archive.s1d.me)"     # Cloudflare 403s the default urllib UA (error 1010)


def api(path, body, timeout=120):
    req = urllib.request.Request(f"{API}{path}", data=json.dumps(body).encode(), headers={
        "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json", "User-Agent": UA})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception:  # noqa: BLE001  API busy or a 502 from the edge: back off and retry
            if attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))


def read(item):
    try:
        with urllib.request.urlopen(urllib.request.Request(item["url"], headers={"User-Agent": UA}), timeout=60) as r:
            return item["sha"], refs(r.read().decode("utf-8", "replace"))
    except Exception:  # noqa: BLE001  missing text: leave it unread, a later run tries again
        return item["sha"], None


def main():
    from scrape_hub_client import HubClient
    hub = HubClient(PROJECT, job_index=int(os.environ.get("SCRAPE_JOB_INDEX", "0")),
                    total_jobs=int(os.environ.get("SCRAPE_TOTAL_JOBS", "1")))
    try:
        with ThreadPoolExecutor(READERS) as ex:
            while time.time() < DEADLINE:
                items = hub.claim(1, steal=True, retry_failed=True)
                if not items:
                    break
                item = items[0]
                try:
                    work = api("/refs/work", {"prefix": item["payload"]["prefix"]})["items"]
                    batch, docs, found = [], 0, 0
                    for sha, r in ex.map(read, work):
                        hub.metric("n_http")
                        if r is None:
                            hub.metric("http_error")
                            continue
                        batch.append({"sha": sha, "refs": r})
                        docs += 1
                        found += len(r)
                        if len(batch) >= 500:
                            api("/refs/report", {"items": batch})
                            batch = []
                    if batch:
                        api("/refs/report", {"items": batch})
                    hub.metric("documents", docs)
                    hub.metric("refs", found)
                    hub.complete([item["key"]], "done", fetched=docs)
                except Exception as e:  # noqa: BLE001  one bad prefix must not end the worker
                    hub.error()
                    hub.complete([item["key"]], "failed", error=f"{type(e).__name__}: {e}"[:200])
        hub.close()
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200])
        raise


if __name__ == "__main__":
    main()
