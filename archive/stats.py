"""Compute the archive's headline numbers once and cache them to a JSON file, so the public status page serves them
instantly instead of running full-table counts (minutes, on a multi-million-row corpus) on every request. Run by a
systemd timer every few minutes.  python -m archive.stats
"""
import json
import time

from . import db

CACHE = "/tmp/archive_stats.json"


def compute():
    with db.conn() as c:
        hosts = c.execute("SELECT count(DISTINCT host) n FROM capture").fetchone()["n"]
        cap = c.execute("SELECT count(*) n FROM capture").fetchone()["n"]
        docs = c.execute("SELECT count(*) n FROM capture WHERE is_document").fetchone()["n"]
        b = c.execute("SELECT count(*) n, coalesce(sum(size),0) b, count(*) FILTER (WHERE tsv IS NOT NULL) idx FROM blob").fetchone()
        hr = c.execute("SELECT count(*) n FROM capture WHERE fetched_at > now()-interval '1 hour'").fetchone()["n"]
    return {"hosts": hosts, "captures": cap, "documents": docs, "blobs": b["n"], "bytes": int(b["b"]),
            "indexed": b["idx"], "last_hour": hr, "at": int(time.time())}


def main():
    s = compute()
    tmp = CACHE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f)
    import os
    os.replace(tmp, CACHE)
    print("stats:", s, flush=True)


if __name__ == "__main__":
    main()
