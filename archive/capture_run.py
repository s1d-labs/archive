"""Drive a real local capture from dev-vps-2: walk many hosts into the archive (local sink -> B2 + index), concurrently
but politely, and resumably. Top-value hosts first (the targets/ lists are pre-ranked). Resume skips any host that
already has a capture row in this run_id. Per-host politeness is the worker's own gap; host-level concurrency is capped.
NIC/geo hosts that refuse this box are captured as status-0 rows (provenance of the refusal) and move on.

  python -m archive.capture_run targets/reachable.txt --run-id archive-local-1 --concurrency 5 --max-pages 400 [--limit N]
"""
import argparse
import asyncio
import time

from . import db
from .capture import Sink, capture_host
from .core_fetch import make_session


def already(run_id):
    with db.conn() as c:
        return {r["host"] for r in c.execute("SELECT DISTINCT host FROM capture WHERE run_id=%s", (run_id,)).fetchall()}


def load(path, limit=0):
    urls = []
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls[:limit] if limit else urls


async def main_async(path, run_id, concurrency, max_pages, limit):
    urls = load(path, limit)
    done = already(run_id)
    todo = [u for u in urls if u.split("//", 1)[-1].split("/", 1)[0] not in done]
    print(f"{len(urls)} hosts, {len(done)} already captured, {len(todo)} to do", flush=True)
    sem = asyncio.Semaphore(concurrency)
    n = [0]
    t0 = time.monotonic()
    async with make_session() as s:
        sink = Sink("local", run_id=run_id, vantage="dev-vps-2")

        async def one(u):
            async with sem:
                try:
                    r = await capture_host(s, u, sink, max_pages=max_pages)
                except Exception as e:  # noqa: BLE001: one bad host never stops the sweep
                    r = {"host": u, "pages": 0, "documents": 0, "err": type(e).__name__}
            n[0] += 1
            if n[0] % 20 == 0:
                rate = n[0] / (time.monotonic() - t0)
                print(f"{n[0]}/{len(todo)} ({rate:.2f} hosts/s, eta {(len(todo)-n[0])/rate/60:.0f} min) last={r['host']} p={r['pages']} d={r['documents']}", flush=True)
        await asyncio.gather(*(one(u) for u in todo))
    print(f"done: {n[0]} hosts", flush=True)


def main():
    ap = argparse.ArgumentParser(prog="archive.capture_run")
    ap.add_argument("targets")
    ap.add_argument("--run-id", default="archive-local-1")
    ap.add_argument("--concurrency", type=int, default=5)
    ap.add_argument("--max-pages", type=int, default=400)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    asyncio.run(main_async(a.targets, a.run_id, a.concurrency, a.max_pages, a.limit))


if __name__ == "__main__":
    main()
