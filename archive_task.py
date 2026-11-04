"""The pool task: capture one host into the archive. Run by the pool runner for each queue item (a host). It fetches the
host's pages + documents and reports each capture to the archive ingest API (archive.s1d.me/ingest), token-authenticated.
The runner handles the queue, politeness holds (the hub's rate limits + the worker's own per-host gap), and failures are
reported per host, not by re-queuing. Self-contained: uses the bundled archive/ package (core_fetch + capture)."""
import asyncio
import os

from scrape_hub_client import PoolClient

from archive import capture


def main():
    hub = PoolClient()                    # reads the assignment: family, project, target (host), env
    api = os.environ["ARCHIVE_API"]       # https://archive.s1d.me  (set in the project env)
    token = os.environ["ARCHIVE_INGEST_TOKEN"]
    vantage = os.environ.get("ARCHIVE_VANTAGE", "pool")

    for item in hub.items():              # each item is one host
        url = item.payload["url"]
        sink = capture.Sink("http", api=api, token=token, run_id=hub.run_id, vantage=vantage)

        async def go():
            res = await capture.run([url], sink, max_pages=int(os.environ.get("MAX_PAGES", "5000")))
            await sink.close()
            return res[0]
        r = asyncio.run(go())
        hub.done(item, records=r["pages"] + r["documents"], extra={"pages": r["pages"], "documents": r["documents"]})


if __name__ == "__main__":
    main()
