"""Import India-vantage capture metadata (the out/<host>.jsonl files from capture_b2_run on vpsin, rsynced back) into the
index. The blobs + text are already in B2; this only writes the blob / capture / document rows. Idempotent.
  python -m archive.import_b2 /var/lib/archive/india_cap/
"""
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

from . import db, store

DOC_EXT = (".pdf", ".xls", ".xlsx", ".csv", ".doc", ".docx", ".ppt", ".pptx", ".zip", ".odt", ".ods", ".rtf")


def main(src):
    files = sorted(Path(src).glob("*.jsonl"))
    caps = blobs = docs = 0
    with db.conn() as c:
        for fp in files:
            for line in fp.read_text().splitlines():
                if not line.strip():
                    continue
                r = json.loads(line)
                sha = r.get("sha256")
                host = urlsplit(r.get("final_url") or r["url"]).hostname or ""
                is_doc = bool(r.get("content_type") and "html" not in (r["content_type"] or "").lower()
                              and (r["url"].lower().rsplit("?", 1)[0].endswith(DOC_EXT)
                                   or (r["content_type"] or "").startswith("application/")))
                if sha:
                    c.execute("""INSERT INTO blob (sha256, size, content_type, b2_key, text_sha, text_chars)
                                 VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (sha256) DO NOTHING""",
                              (sha, r.get("size") or 0, r.get("content_type"), store.blob_key(sha),
                               r.get("text_sha"), r.get("text_chars") or None))
                    blobs += 1
                    if is_doc:
                        c.execute("INSERT INTO document (content_sha, ext) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                                  (sha, r["url"].lower().rsplit(".", 1)[-1][:5] if "." in r["url"] else None))
                        docs += 1
                c.execute("""INSERT INTO capture (url, host, fetched_at, vantage, http_status, final_url, content_sha,
                               content_type, is_document, run_id)
                             VALUES (%s,%s, now(), %s,%s,%s,%s,%s,%s,%s)""",
                          (r["url"], host, r.get("vantage", "IN"), r.get("http_status"), r.get("final_url"),
                           sha, r.get("content_type"), is_doc, r.get("run_id")))
                caps += 1
        c.commit()
    print(f"imported {caps} captures, {blobs} blob rows, {docs} documents from {len(files)} hosts")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/var/lib/archive/india_cap/")
