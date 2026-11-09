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
# Deep pass (DEEP=1): one host per item, crawled across many jobs. The frontier is saved to the archive API every few hundred
# pages and when the job's time is up; an unfinished host goes back to the queue as pending and the next job resumes it.
DEEP = os.environ.get("DEEP") == "1"
MAX_DOCS = int(os.environ.get("MAX_DOCS", "200000" if DEEP else "20000"))
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

            async def go():
                # direct sink: upload blob + text straight to B2 (presigned), report only metadata — dev-vps-2 is off the byte path
                sink = capture.Sink("direct", api=API, token=TOKEN, run_id=run_id, vantage=VANTAGE)
                try:
                    if not DEEP:
                        res = await capture.run([url], sink, max_pages=MAX_PAGES)
                        return res[0]
                    host = item["key"]
                    state = await frontier_load(host)
                    if state and state.get("done"):
                        return {"host": host, "pages": 0, "documents": 0, "complete": True}

                    async def save(st, done):
                        await sink.flush()                   # captures before the frontier that says they are done
                        if done and st["pages"] <= 1:
                            return                           # the start page failed: no frontier, so a retry starts fresh
                        await frontier_save(host, st, done)
                    async with capture.make_session() as s:
                        return await capture.capture_host(s, url, sink, max_pages=MAX_PAGES, max_docs=MAX_DOCS, state=state,
                                                          deadline=DEADLINE - 300, checkpoint=save)
                finally:
                    await sink.close()

            try:
                r = asyncio.run(go())
            except Exception as e:  # noqa: BLE001  one bad host must not kill the worker
                hub.error()
                hub.complete([item["key"]], "failed", error=f"{type(e).__name__}: {e}"[:200])
                continue
            hub.metric("pages", r["pages"])
            hub.metric("documents", r["documents"])
            if DEEP and r.get("complete") and r["pages"] <= 1:
                hub.complete([item["key"]], "failed", error="start page gave no links (blocked or down from this vantage)")
                continue
            if DEEP and not r.get("complete", True):
                hub.complete([item["key"]], "pending", fetched=r["pages"] + r["documents"])   # resume in the next job
                break                                                                          # this job's time is used up
            hub.complete([item["key"]], "done", fetched=r["pages"] + r["documents"])
        hub.close()
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200])
        raise


async def frontier_load(host):
    """The host's saved frontier, or None when the deep pass has not started it."""
    import gzip, json
    import aiohttp
    async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {TOKEN}", "User-Agent": capture.UA}) as s:
        async with s.get(f"{API}/frontier/{host}", timeout=aiohttp.ClientTimeout(total=300)) as r:
            if r.status == 404:
                return None
            r.raise_for_status()
            st = json.loads(gzip.decompress(await r.read()))
            st["done"] = r.headers.get("X-Done") == "1"
            return st


async def frontier_save(host, st, done):
    import gzip, json
    import aiohttp
    body = gzip.compress(json.dumps(st, separators=(",", ":")).encode(), 6)
    params = {"pages": st["pages"], "docs": st["docs"], "queued": len(st["q"]), "done": int(done)}
    async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {TOKEN}", "User-Agent": capture.UA}) as s:
        for attempt in range(4):
            try:
                async with s.put(f"{API}/frontier/{host}", params=params, data=body,
                                 timeout=aiohttp.ClientTimeout(total=300)) as r:
                    r.raise_for_status()
                    return
            except Exception:  # noqa: BLE001  a lost checkpoint costs re-crawling, never data; retry a few times
                await asyncio.sleep(5 * (attempt + 1))


if __name__ == "__main__":
    main()
