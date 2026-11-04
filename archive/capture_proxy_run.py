"""Capture India-only hosts from dev-vps-2 through the pool's 10 Mumbai proxies — 10 Indian egress IPs, straight into the
archive index + B2 (local sink), no India box or repo push needed. Each concurrent slot borrows one proxy (round-robin),
so each IP is one visitor at a time; concurrency is a small multiple of the proxy count. Resumable by run_id.

  python -m archive.capture_proxy_run targets/india.txt --run-id archive-proxy-1 --per-proxy 2 --max-pages 400
Proxies come from /etc/archive/proxies.txt (one http://user:pass@ip:port per line), written from the hub's proxy CSV.
"""
import argparse
import asyncio
import time
from pathlib import Path
from urllib.parse import urlsplit

from . import config, db
from .capture import Sink, capture_host
from .core_fetch import make_session


def load_proxies():
    f = Path(config.ETC) / "proxies.txt"
    return [l.strip() for l in f.read_text().splitlines() if l.strip() and not l.startswith("#")]


def already(run_id):
    with db.conn() as c:
        return {r["host"] for r in c.execute("SELECT DISTINCT host FROM capture WHERE run_id=%s", (run_id,)).fetchall()}


async def main_async(path, run_id, per_proxy, max_pages, limit):
    proxies = load_proxies()
    if not proxies:
        raise SystemExit("no proxies in /etc/archive/proxies.txt")
    conc = per_proxy * len(proxies)
    urls = [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
    if limit:
        urls = urls[:limit]
    done = already(run_id)
    todo = [u for u in urls if (urlsplit(u).hostname or "") not in done]
    print(f"{len(proxies)} proxies, concurrency {conc}; {len(urls)} hosts, {len(done)} done, {len(todo)} to do", flush=True)
    sem = asyncio.Semaphore(conc)
    n = [0]
    t0 = time.monotonic()
    async with make_session() as s:
        sink = Sink("local", run_id=run_id, vantage="IN-proxy")

        async def one(i, u):
            proxy = proxies[i % len(proxies)]
            async with sem:
                try:
                    r = await capture_host(s, u, sink, max_pages=max_pages, proxy=proxy)
                except Exception as e:  # noqa: BLE001
                    r = {"host": urlsplit(u).hostname, "pages": 0, "documents": 0, "err": type(e).__name__}
            n[0] += 1
            if n[0] % 20 == 0:
                rate = n[0] / (time.monotonic() - t0)
                print(f"{n[0]}/{len(todo)} ({rate:.2f} hosts/s, eta {(len(todo)-n[0])/rate/60:.0f} min) last={r['host']} p={r['pages']} d={r['documents']}", flush=True)
        await asyncio.gather(*(one(i, u) for i, u in enumerate(todo)))
    print(f"done: {n[0]} hosts", flush=True)


def main():
    ap = argparse.ArgumentParser(prog="archive.capture_proxy_run")
    ap.add_argument("targets")
    ap.add_argument("--run-id", default="archive-proxy-1")
    ap.add_argument("--per-proxy", type=int, default=2)
    ap.add_argument("--max-pages", type=int, default=400)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    asyncio.run(main_async(a.targets, a.run_id, a.per_proxy, a.max_pages, a.limit))


if __name__ == "__main__":
    main()
