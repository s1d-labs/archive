"""The bucket task (scrape.json "bucket"): copy files that scrape-hub families already stored in their buckets into the
archive, so every document they collected is in the one content-addressed corpus with its text indexed.

Each queue item holds up to 50 files with signed 7-day GET links (made on dev-vps-2 by scripts/bucket_seed.py; no storage
key on the runner). For each file the worker downloads it, and the archive sink hashes it, extracts text, uploads blob and
text to the archive bucket through presigned PUTs and reports the capture: url scrapehub://<target>/<project>/files/<path>,
fetched_at = when the family stored it, vantage "scrape-hub". A file the archive already holds is not uploaded again."""
import asyncio
import os
import time
import urllib.error
import urllib.request

from archive import capture

PROJECT = os.environ.get("SCRAPE_POOL_PROJECT", "")
API = os.environ["ARCHIVE_API"]
TOKEN = os.environ["ARCHIVE_INGEST_TOKEN"]
PARALLEL = int(os.environ.get("BUCKET_PARALLEL", "8"))
DEADLINE = time.time() + float(os.environ.get("SCRAPE_MAX_HOURS", "1")) * 3600 - 300


def download(url, tries=5):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url), timeout=300) as r:
                return r.status, r.headers.get("Content-Type"), r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404) or i == tries - 1:      # 403 = expired link (re-seed), 404 = gone
                return e.code, None, None
        except Exception:  # noqa: BLE001  network trouble: back off and retry
            if i == tries - 1:
                return 0, None, None
        time.sleep(5 * 2 ** i)
    return 0, None, None


def main():
    from scrape_hub_client import HubClient
    hub = HubClient(PROJECT, job_index=int(os.environ.get("SCRAPE_JOB_INDEX", "0")),
                    total_jobs=int(os.environ.get("SCRAPE_TOTAL_JOBS", "1")))
    run_id = f"pool-{hub.run_id}"
    try:
        while time.time() < DEADLINE:
            if hub.settings.get("paused"):
                time.sleep(30)
                continue
            items = hub.claim(1, steal=True, retry_failed=True)
            if not items:
                break
            item = items[0]
            files = item["payload"]["files"]

            async def go():
                ok = expired = 0
                sink = capture.Sink("direct", api=API, token=TOKEN, run_id=run_id, vantage="scrape-hub")
                sem = asyncio.Semaphore(PARALLEL)     # download + hash + text + upload of several files at once

                async def one(f):
                    nonlocal ok, expired
                    async with sem:
                        st, ctype, body = await asyncio.to_thread(download, f["get"])
                        if st == 403:
                            expired += 1
                            return
                        await sink.send(f["url"], st, None, body, ctype, fetched_at=f.get("modified"))
                        ok += 1 if body else 0
                try:
                    await asyncio.gather(*(one(f) for f in files))
                finally:
                    await sink.close()
                return ok, expired

            try:
                ok, expired = asyncio.run(go())
            except Exception as e:  # noqa: BLE001  one bad item must not kill the worker
                hub.error()
                hub.complete([item["key"]], "failed", error=f"{type(e).__name__}: {e}"[:200])
                continue
            hub.metric("documents", ok)
            if expired:
                hub.complete([item["key"]], "failed", error=f"{expired} signed links expired: re-seed", fetched=ok)
            else:
                hub.complete([item["key"]], "done", fetched=ok)
        hub.close()
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200])
        raise


if __name__ == "__main__":
    main()
