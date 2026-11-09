"""Integrity check: verify captured documents are complete, flag bad ones in document.ok (null=unchecked, true=ok,
false=truncated/corrupt/missing). Cheap + scalable: for a PDF it range-fetches only the first 8 bytes and last 4 KB from
B2 (no full download) and checks for %PDF ... %%EOF — which is exactly what truncation breaks. Non-PDF documents can't be
validated this cheaply, so they are left ok=true (present). A nonzero `false` count = captures to re-crawl.

Each new document is checked once, as it is ingested: with the body in hand (ingest.ingest, e.g. import_hub) from the bytes,
otherwise (worker already uploaded to B2) api.py hands its sha to check_new, in the background.
`python -m archive.validate [limit]` is the daily safety sweep for anything that missed that (an API restart mid-check) and,
at first, the backlog from before checks ran at ingest; the partial index document_unchecked keeps it cheap.

  python -m archive.validate [limit]      # limit 0 = every unchecked document
"""
import concurrent.futures as cf
import sys

from . import db, store


def _range(sha, rng):
    s3, bucket = store._s3()
    try:
        return s3.get_object(Bucket=bucket, Key=store.blob_key(sha), Range=rng)["Body"].read()
    except Exception:  # noqa: BLE001  missing object or range error
        return None


def check_bytes(content):
    """The same test on a body already in memory (ingest.ingest sets document.ok with it, no B2 round trip)."""
    if content[:5] == b"%PDF-":
        return b"%%EOF" in content[-4096:]
    return True


def check(sha, content_type):
    head = _range(sha, "bytes=0-7")
    if head is None:
        return False                                  # blob missing from B2
    if head[:5] == b"%PDF-":
        tail = _range(sha, "bytes=-4096") or b""
        return b"%%EOF" in tail                        # a complete PDF ends with %%EOF; truncation drops it
    return True                                        # non-PDF: present, not cheaply verifiable -> accept


def _check_rows(rows, label="", workers=8):
    """Check (sha, content_type) rows in parallel and record the result; returns (good, bad)."""
    good = bad = 0
    with cf.ThreadPoolExecutor(workers) as ex, db.conn() as c:
        for i, (r, ok) in enumerate(zip(rows, ex.map(lambda r: check(r["content_sha"], r["content_type"]), rows)), 1):
            c.execute("UPDATE document SET ok=%s WHERE content_sha=%s", (ok, r["content_sha"]))
            good += ok
            bad += (not ok)
            if i % 1000 == 0:
                c.commit()
                if label:
                    print(f"{label}{i}/{len(rows)}  ok={good} bad={bad}", flush=True)
        c.commit()
    return good, bad


def check_new(shas):
    """Check the not-yet-checked documents among these just-ingested shas (called from the ingest API, in the background)."""
    shas = [s for s in shas if s]
    if not shas:
        return 0, 0
    with db.conn() as c:
        rows = c.execute("""SELECT d.content_sha, b.content_type FROM document d JOIN blob b ON d.content_sha = b.sha256
                            WHERE d.ok IS NULL AND d.content_sha = ANY(%s)""", (shas,)).fetchall()
    return _check_rows(rows, workers=4) if rows else (0, 0)


def main(limit=0):
    with db.conn() as c:
        rows = c.execute(f"""SELECT d.content_sha, b.content_type FROM document d JOIN blob b ON d.content_sha = b.sha256
                             WHERE d.ok IS NULL {"LIMIT %s" % int(limit) if limit else ""}""").fetchall()
    print(f"{len(rows)} documents to check", flush=True)
    good, bad = _check_rows(rows, label="", workers=16)
    print(f"done: {good} ok, {bad} bad of {good + bad}", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
