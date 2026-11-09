"""Fold scrape-hub family output into the archive: one dated snapshot of a project's records, as content-addressed
gzip JSONL parts in the archive bucket, each registered as a capture.

The records table is 130 GB, more than the free disk, so records are NOT copied into the archive database. Each part is a
blob (raw/<sha256>), stored once; an unchanged part in a later snapshot costs nothing. The capture URL says where it came
from: scrapehub://<target-host>/<project>/records/<date>/part-NNNN.jsonl.gz.

Input on stdin: the output of
  COPY (SELECT record_id, scraped_at, data FROM records WHERE project = '<project>' ORDER BY record_id) TO STDOUT
streamed by psql as postgres (this process runs as the archive user and has no access to scrape_hub).
Usage: python -m archive.import_hub PROJECT TARGET_HOST [--part-size 200000]"""
import argparse
import gzip
import io
import json
import sys
from datetime import datetime, timezone

from . import ingest


def unescape(field: str) -> str:
    """COPY text format: undo the backslash escapes (\\\\, \\t, \\n, \\r)."""
    if "\\" not in field:
        return field
    out, i = [], 0
    while i < len(field):
        ch = field[i]
        if ch == "\\" and i + 1 < len(field):
            nxt = field[i + 1]
            out.append({"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}.get(nxt, nxt))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("project")
    ap.add_argument("target")
    ap.add_argument("--part-size", type=int, default=200000)
    a = ap.parse_args()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    part, n_part, n_total, buf, gz = 0, 0, 0, None, None
    out = []

    def open_part():
        nonlocal buf, gz
        buf = io.BytesIO()
        gz = gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6, mtime=0)   # mtime 0: same records -> same bytes -> same sha

    def close_part():
        nonlocal part, n_part
        gz.close()
        part += 1
        url = f"scrapehub://{a.target}/{a.project}/records/{day}/part-{part:04d}.jsonl.gz"
        r = ingest.ingest(url, buf.getvalue(), content_type="application/x-ndjson+gzip", vantage="scrape-hub",
                          run_id=f"hub-import-{day}", extract_text=False,
                          headers={"records": n_part, "project": a.project, "source": "scrape_hub.records"})
        out.append({"part": part, "records": n_part, "sha256": r["sha256"], "new": r["newly_stored"]})
        n_part = 0

    open_part()
    for line in sys.stdin:
        rid, at, data = line.rstrip("\n").split("\t", 2)
        rec = {"record_id": unescape(rid), "scraped_at": at, "data": json.loads(unescape(data))}
        gz.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")).encode() + b"\n")
        n_part += 1
        n_total += 1
        if n_part >= a.part_size:
            close_part()
            open_part()
    if n_part:
        close_part()
    print(json.dumps({"project": a.project, "records": n_total, "parts": out}))


if __name__ == "__main__":
    main()
