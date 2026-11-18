"""Back up the archive index database to the archive's own B2 bucket (backups/archive-db/<UTC stamp>/).

Until 2026-11-18 nothing backed this database up: scrape-hub-backup dumps only the hub database and restic takes files.
The blobs and texts are safe in B2, but which url was captured when, from where, with which bytes (capture, document,
frontier, refs) exists only here.

Two files per backup:
  archive.dump     pg_dump --format=custom of every table, without the data of `blob`
  blob.csv.gz      blob rows without `tsv` (the search vector: 71 GB of the table, rebuilt from the texts in B2)
Each upload is checked by size; the local copy is deleted only after that. The newest KEEP backups stay.

Restore (blob rows before the constraints; blob.csv.gz is written after the dump, so it holds every blob the dump uses):
  pg_restore --section=pre-data -d archive archive.dump
  \\copy blob (sha256,size,content_type,b2_key,stored_at,text_sha,text_chars,ocr) from program 'zcat blob.csv.gz' csv
  pg_restore --section=data -d archive archive.dump && pg_restore --section=post-data -d archive archive.dump
then rebuild blob.tsv from the texts in B2 (the fts backfill).

  python -m archive.backup [WORKDIR]       (ARCHIVE_DB_URL; B2 write key via archive.config)
"""
import datetime as dt
import gzip
import os
import subprocess
import sys

from . import db, store

KEEP = int(os.environ.get("ARCHIVE_BACKUP_KEEP", "4"))
PREFIX = "backups/archive-db/"
BLOB_COLS = "sha256, size, content_type, b2_key, stored_at, text_sha, text_chars, ocr"


def main():
    work = sys.argv[1] if len(sys.argv) > 1 else "/var/lib/archive-backup"
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%MZ")
    os.makedirs(work, exist_ok=True)
    dump, blobs = os.path.join(work, "archive.dump"), os.path.join(work, "blob.csv.gz")

    subprocess.run(["pg_dump", "--format=custom", "--compress=6", "--no-owner", "--exclude-table-data=blob",
                    "--file", dump, os.environ["ARCHIVE_DB_URL"]], check=True)
    rows = 0
    with db.conn() as c, gzip.open(blobs, "wb", compresslevel=6) as out:
        c.execute("SET statement_timeout = 0")
        with c.cursor().copy(f"COPY (SELECT {BLOB_COLS} FROM blob) TO STDOUT (FORMAT csv)") as cp:
            for chunk in cp:
                data = bytes(chunk)
                out.write(data)
                rows += data.count(b"\n")

    s3, bucket = store._s3(write=True)
    for path in (dump, blobs):
        key = f"{PREFIX}{stamp}/{os.path.basename(path)}"
        s3.upload_file(path, bucket, key)
        size = os.path.getsize(path)
        got = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        if got != size:
            raise SystemExit(f"upload check failed for {key}: local {size} B, bucket {got} B (local copy kept)")
        os.remove(path)
        print(f"uploaded {key} {size:,} B", flush=True)

    stamps = sorted({o["Key"][len(PREFIX):].split("/", 1)[0]
                     for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX)
                     for o in p.get("Contents", [])})
    for old in stamps[:-KEEP] if len(stamps) > KEEP else []:
        for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{PREFIX}{old}/"):
            for o in p.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=o["Key"])
        print(f"removed old backup {PREFIX}{old}/", flush=True)
    print(f"archive backup {stamp}: blob rows {rows:,}; kept {min(len(stamps), KEEP)} backups", flush=True)


if __name__ == "__main__":
    main()
