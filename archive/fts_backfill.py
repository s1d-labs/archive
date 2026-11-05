"""Parallel full-text backfill: index the text blobs captured before ingest-time indexing existed. Reads each blob's
extracted text from B2 with a thread pool (the slow part is the B2 round-trip, so it parallelizes well) and builds
blob.tsv ('simple' config, multilingual). Idempotent + resumable: only touches tsv-null blobs, and the UPDATE re-checks
tsv IS NULL so it never fights the live crawl's own indexing. Run: python -m archive.fts_backfill [workers]
"""
import sys
from concurrent.futures import ThreadPoolExecutor

from . import db, store

UPDATE = "UPDATE blob SET tsv = to_tsvector('simple', left(%s, 600000)) WHERE sha256 = %s AND tsv IS NULL"


def main(workers=12):
    store._s3()                                        # warm the shared boto3 client before threads use it
    with db.conn() as c:
        shas = [r["sha256"] for r in c.execute("SELECT sha256 FROM blob WHERE text_chars > 0 AND tsv IS NULL").fetchall()]
    print(f"{len(shas)} blobs to index with {workers} readers", flush=True)

    def fetch(sha):
        try:
            return sha, store.get_text(sha)
        except Exception:  # noqa: BLE001  missing/again later
            return sha, None

    seen = idx = 0
    buf = []
    with db.conn() as c, ThreadPoolExecutor(workers) as ex:
        for sha, txt in ex.map(fetch, shas, chunksize=8):
            seen += 1
            if txt:
                buf.append((txt.replace("\x00", ""), sha))   # Postgres text can't hold NUL bytes; strip for the index
            if len(buf) >= 100:
                for args in buf:
                    c.execute(UPDATE, args)
                c.commit()
                idx += len(buf)
                buf = []
            if seen % 5000 == 0:
                print(f"{seen}/{len(shas)} indexed={idx}", flush=True)
        if buf:
            for args in buf:
                c.execute(UPDATE, args)
            c.commit()
            idx += len(buf)
    print(f"done: {seen} seen, {idx} indexed", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 12)
