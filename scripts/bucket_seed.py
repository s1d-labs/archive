#!/usr/bin/env python3
"""Seed the archive-bucket project: files that scrape-hub families stored in their buckets, to be copied into the archive by
GitHub runners (task "bucket", bucket_task.py).

Runs as root on dev-vps-2 with the scrape-hub venv (boto3). It reads each bucket with an EXISTING read-only key (member read
keys in /etc/scrape-hub/member-keys/NAME.env; the hub's own R2 key for eprocure-docs), lists the objects, signs a 7-day GET
link for each, and queues items of up to 50 files / 500 MB. No key leaves the box; runners only get the signed links
(the same pattern as the GeM PDF parse, 2026-11-03).
Skipped: members' own folders (from-box/), derived outputs (gem-pdf-parse/, doc-ocr-*), database dumps (backups/, dev-vps-2/).

Usage: bucket_seed.py SOURCE [SOURCE ...]   SOURCE = member:NAME (bucket s1d-labs-NAME) or r2:PREFIX (hub R2 bucket, one prefix)
"""
import json
import re
import subprocess
import sys
import zlib

import boto3
from botocore.config import Config

SKIP = re.compile(r"^(from-box/|gem-pdf-parse/|doc-ocr-|backups/|dev-vps-2/)")
PROJECT = "archive-bucket"
MAX_FILES, MAX_BYTES, MAX_FILE = 50, 500 * 1024 ** 2, 2 * 1024 ** 3
EXPIRES = 7 * 24 * 3600


def env(path):
    out = {}
    for line in open(path):
        m = re.match(r"\s*([A-Z0-9_]+)=(.*)", line)
        if m:
            out[m.group(1)] = m.group(2).strip().strip('"')
    return out


def psql(sql, stdin=None):
    return subprocess.run(["sudo", "-u", "postgres", "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-d", "scrape_hub", "-c", sql],
                          input=stdin, capture_output=True, text=True, check=True).stdout


def client(source):
    kind, name = source.split(":", 1)
    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    if kind == "member":
        e = env(f"/etc/scrape-hub/member-keys/{name}.env")
        return boto3.client("s3", endpoint_url=e["B2_ENDPOINT"], aws_access_key_id=e["AWS_ACCESS_KEY_ID"],
                            aws_secret_access_key=e["AWS_SECRET_ACCESS_KEY"], region_name=e.get("B2_REGION") or None,
                            config=cfg), e["B2_BUCKET"], ""
    e = env("/etc/scrape-hub/env")
    return boto3.client("s3", endpoint_url=e["R2_ENDPOINT"], aws_access_key_id=e["R2_ACCESS_KEY_ID"],
                        aws_secret_access_key=e["R2_SECRET_ACCESS_KEY"], region_name="auto", config=cfg), e["R2_BUCKET"], name


def main():
    targets = {}
    for line in psql("SELECT name, coalesce(settings->>'target','') FROM projects").splitlines():
        n, t = line.split("|", 1)
        targets[n] = t if "." in t else n.split("-", 1)[0] + ".scrape-hub"
    for source in sys.argv[1:]:
        s3, bucket, prefix = client(source)
        items, cur, size, n_files, n_skip = [], [], 0, 0, 0

        def flush():
            nonlocal cur, size
            if cur:
                items.append(cur)
            cur, size = [], 0

        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                k = o["Key"]
                if SKIP.match(k) or o["Size"] == 0 or o["Size"] > MAX_FILE:
                    n_skip += 1
                    continue
                proj, _, rest = k.partition("/")
                host = targets.get(proj, proj + ".scrape-hub")
                cur.append({"key": k, "size": o["Size"], "modified": o["LastModified"].isoformat(),
                            "url": f"scrapehub://{host}/{proj}/files/{rest}",
                            "get": s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": k}, ExpiresIn=EXPIRES)})
                size += o["Size"]
                n_files += 1
                if len(cur) >= MAX_FILES or size >= MAX_BYTES:
                    flush()
        flush()
        rows = []
        for i, files in enumerate(items):
            key = f"{bucket}:{i:06d}"
            rows.append("\t".join([PROJECT, key, str(zlib.crc32(key.encode()) % 1000000),
                                   json.dumps({"bucket": bucket, "files": files}).replace("\\", "\\\\")]))
        # one psql script on stdin: create, COPY the rows (terminated by \.), insert into the queue
        script = ("CREATE TEMP TABLE s (project text, key text, partition_id int, payload jsonb);\n"
                  "COPY s FROM STDIN;\n" + "\n".join(rows) + "\n\\.\n"
                  "INSERT INTO queue (project, key, partition_id, status, payload, attempts, last_error, fetched) "
                  "SELECT project, key, partition_id, 'pending', payload, 0, '', 0 FROM s "
                  "ON CONFLICT (project, key) DO UPDATE SET payload = EXCLUDED.payload, status = 'pending', attempts = 0;\n")
        subprocess.run(["sudo", "-u", "postgres", "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", "scrape_hub"],
                       input=script, text=True, check=True)
        print(json.dumps({"source": source, "bucket": bucket, "files": n_files, "items": len(items), "skipped": n_skip}), flush=True)


if __name__ == "__main__":
    main()
