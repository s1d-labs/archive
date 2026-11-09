"""The Wayback task (scrape.json "wayback"): recover a government host's past pages and documents from the Internet
Archive's Wayback Machine into the archive. Used first for hosts whose domain no longer resolves (dead sites), where the
Wayback copy is the only copy left.

Each queue item is one host. The worker lists every distinct version of every URL of the host from the CDX API
(status 200, collapse=digest: one row per change of content), then fetches each version's original bytes
(web.archive.org/web/<timestamp>id_/<url>) and stores it under its ORIGINAL url with the ORIGINAL capture date, vantage
"wayback". Progress (the index into the CDX list) is saved in the frontier table, so a large host continues in the next job.
Polite to the Internet Archive: one request at a time per worker, a gap between requests, a long pause on 429/503."""
import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from archive import capture
from archive_task import frontier_load, frontier_save

PROJECT = os.environ.get("SCRAPE_POOL_PROJECT", "")
API = os.environ["ARCHIVE_API"]
TOKEN = os.environ["ARCHIVE_INGEST_TOKEN"]
GAP = float(os.environ.get("WAYBACK_GAP_S", "1.0"))
MAX_VERSIONS = int(os.environ.get("WAYBACK_MAX_VERSIONS", "200000"))
DEADLINE = time.time() + float(os.environ.get("SCRAPE_MAX_HOURS", "1")) * 3600 - 300
UA = "archive.s1d.me/1.0 (archive of Indian government public records; +https://archive.s1d.me)"
CDX = "https://web.archive.org/cdx/search/cdx"
# Pages and documents first: images, styles and scripts are most of the rows and add load on the Internet Archive.
SKIP_MIME = lambda m: (m or "").startswith(("image/", "text/css", "text/javascript", "application/javascript",
                                            "application/x-javascript", "font/", "warc/"))


def http_get(url, timeout=120):
    """GET with backoff: the Internet Archive answers 429/503 when busy; wait and retry, never hammer."""
    for i in range(6):
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.headers.get("Content-Type"), r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 503, 502, 504) and i < 5:
                time.sleep(min(300, 30 * 2 ** i))
                continue
            return e.code, None, None
        except Exception:  # noqa: BLE001  network trouble: back off and retry
            if i == 5:
                return 0, None, None
            time.sleep(10 * (i + 1))
    return 0, None, None


def cdx_rows(host):
    """Every distinct version of every URL of the host (and its www. twin), oldest first. Uses the CDX resume key so a
    host with many captures is read in pages."""
    out = []
    for h in sorted({host, host[4:] if host.startswith("www.") else "www." + host}):
        resume = None
        while len(out) < MAX_VERSIONS:
            q = {"url": f"{h}/*", "fl": "timestamp,original,mimetype,digest", "filter": "statuscode:200",
                 "collapse": "digest", "output": "json", "limit": "50000", "showResumeKey": "true"}
            if resume:
                q["resumeKey"] = resume
            st, _, body = http_get(f"{CDX}?{urllib.parse.urlencode(q)}", timeout=300)
            if st != 200 or not body:
                break
            rows = json.loads(body or b"[]")
            resume = None
            data = rows[1:] if rows else []
            if len(data) >= 2 and data[-2] == [] and len(data[-1]) == 1:      # [..., [], [resumeKey]]
                resume = data[-1][0]
                data = data[:-2]
            out += [r for r in data if len(r) == 4 and not SKIP_MIME(r[2])]
            time.sleep(GAP)
            if not resume:
                break
    return out[:MAX_VERSIONS]


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
            host = item["key"]
            fkey = "wayback:" + host
            try:
                state = asyncio.run(frontier_load(fkey)) or {}
                if state.get("done"):
                    hub.complete([host], "done")
                    continue
                rows = cdx_rows(host)
                i, got = int(state.get("pages", 0)), 0

                async def go():
                    nonlocal i, got
                    sink = capture.Sink("direct", api=API, token=TOKEN, run_id=run_id, vantage="wayback")
                    try:
                        while i < len(rows) and time.time() < DEADLINE:
                            ts, orig, mime, _digest = rows[i]
                            wb = f"https://web.archive.org/web/{ts}id_/{orig}"
                            st, ctype, body = await asyncio.to_thread(http_get, wb)
                            when = datetime.strptime(ts[:14].ljust(14, "0"), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
                            await sink.send(orig, st, wb, body, ctype or mime, fetched_at=when)
                            got += 1 if body else 0
                            i += 1
                            if i % 200 == 0:
                                await sink.flush()
                                await frontier_save(fkey, {"q": [], "seen": [], "seen_docs": [], "pages": i, "docs": 0}, False)
                            await asyncio.sleep(GAP)
                    finally:
                        await sink.close()
                    done = i >= len(rows)
                    await frontier_save(fkey, {"q": [], "seen": [], "seen_docs": [], "pages": i, "docs": 0}, done)
                    return done

                done = asyncio.run(go())
            except Exception as e:  # noqa: BLE001  one bad host must not kill the worker
                hub.error()
                hub.complete([host], "failed", error=f"{type(e).__name__}: {e}"[:200])
                continue
            hub.metric("pages", got)
            if not done:
                hub.complete([host], "pending", fetched=got)        # resume in the next job
                break
            hub.complete([host], "done", fetched=len(rows))
        hub.close()
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200])
        raise


if __name__ == "__main__":
    main()
