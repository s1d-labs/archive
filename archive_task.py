"""The archive capture task (see scrape.json). The pool worker runs it for one project (SCRAPE_POOL_PROJECT). Each queue
item is one host; this claims hosts from the hub, walks each host's pages + documents, and reports every capture to the
archive ingest API (archive.s1d.me/ingest), token-authenticated. It reports completion + a couple of counters to the hub
for tracking and politeness holds. Self-contained: uses the bundled archive/ package (core_fetch + capture)."""
import asyncio
import os
import time

from archive import capture

PROJECT = os.environ.get("SCRAPE_POOL_PROJECT", "")
API = os.environ["ARCHIVE_API"]                      # https://archive.s1d.me  (set in the project env)
TOKEN = os.environ["ARCHIVE_INGEST_TOKEN"]
VANTAGE = os.environ.get("ARCHIVE_VANTAGE", "pool")
MAX_PAGES = int(os.environ.get("MAX_PAGES", "250"))
DEADLINE = time.time() + float(os.environ.get("SCRAPE_MAX_HOURS", "1")) * 3600 - 120


def main():
    from scrape_hub_client import HubClient
    hub = HubClient(PROJECT, job_index=int(os.environ.get("SCRAPE_JOB_INDEX", "0")),
                    total_jobs=int(os.environ.get("SCRAPE_TOTAL_JOBS", "1")))
    run_id = f"pool-{hub.run_id}"
    try:
        while time.time() < DEADLINE:
            if hub.settings.get("paused"):
                time.sleep(30)                       # the hub holds us (target down, or the family owner paused it)
                continue
            items = hub.claim(1, steal=True, retry_failed=True)
            if not items:
                break                                # nothing left for this project: exit, the worker asks for more
            item = items[0]
            url = item["payload"]["url"]
            sink = capture.Sink("http", api=API, token=TOKEN, run_id=run_id, vantage=VANTAGE)

            async def go():
                res = await capture.run([url], sink, max_pages=MAX_PAGES)
                await sink.close()
                return res[0]

            try:
                r = asyncio.run(go())
            except Exception as e:  # noqa: BLE001  one bad host must not kill the worker
                hub.error()
                hub.complete([item["key"]], "failed", error=f"{type(e).__name__}: {e}"[:200])
                continue
            hub.metric("pages", r["pages"])
            hub.metric("documents", r["documents"])
            hub.complete([item["key"]], "done", fetched=r["pages"] + r["documents"])
        hub.close()
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200])
        raise


if __name__ == "__main__":
    main()
