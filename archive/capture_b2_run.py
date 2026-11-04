"""Capture India-only hosts from an India vantage (vpsin) straight into the archive B2 bucket, writing one metadata
JSONL per host (out/<host>.jsonl). Resumable (a host whose JSONL exists is skipped), concurrent but polite. The JSONLs
are rsynced back to dev-vps-2 and imported into the index with `python -m archive.import_b2`. Needs the archive B2 write
key in /etc/archive/b2-write.env (or ARCHIVE_ETC) on the box. No index/tunnel needed here.

  python -m archive.capture_b2_run india.txt out/ --run-id archive-india-1 --concurrency 5 --max-pages 400
"""
import argparse
import asyncio
import time
from pathlib import Path
from urllib.parse import urlsplit

from .capture import Sink, capture_host
from .core_fetch import make_session


async def main_async(path, outdir, run_id, concurrency, max_pages, limit):
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    urls = [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
    if limit:
        urls = urls[:limit]
    todo = [u for u in urls if not (out / (urlsplit(u).hostname + ".jsonl")).exists()]
    print(f"{len(urls)} hosts, {len(todo)} to capture", flush=True)
    sem = asyncio.Semaphore(concurrency)
    n = [0]
    t0 = time.monotonic()
    async with make_session() as s:
        async def one(u):
            async with sem:                          # cap host-level concurrency (and open files): politeness + fd safety
                host = urlsplit(u).hostname
                part = out / (host + ".jsonl.part")
                sink = Sink("b2", run_id=run_id, vantage="IN", jsonl=str(part))
                try:
                    await capture_host(s, u, sink, max_pages=max_pages)
                except Exception:  # noqa: BLE001
                    pass
                finally:
                    await sink.close()
                part.rename(out / (host + ".jsonl"))   # atomic: only a complete host gets the final name (clean resume)
            n[0] += 1
            if n[0] % 20 == 0:
                rate = n[0] / (time.monotonic() - t0)
                print(f"{n[0]}/{len(todo)} ({rate:.2f} hosts/s, eta {(len(todo)-n[0])/rate/60:.0f} min) last={host}", flush=True)
        await asyncio.gather(*(one(u) for u in todo))
    print(f"done: {n[0]} hosts", flush=True)


def main():
    ap = argparse.ArgumentParser(prog="archive.capture_b2_run")
    ap.add_argument("targets")
    ap.add_argument("outdir")
    ap.add_argument("--run-id", default="archive-india-1")
    ap.add_argument("--concurrency", type=int, default=5)
    ap.add_argument("--max-pages", type=int, default=400)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    asyncio.run(main_async(a.targets, a.outdir, a.run_id, a.concurrency, a.max_pages, a.limit))


if __name__ == "__main__":
    main()
