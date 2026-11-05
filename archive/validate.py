"""Integrity check: verify captured documents are complete, flag bad ones in document.ok (null=unchecked, true=ok,
false=truncated/corrupt/missing). Cheap + scalable: for a PDF it range-fetches only the first 8 bytes and last 4 KB from
B2 (no full download) and checks for %PDF ... %%EOF — which is exactly what truncation breaks. Non-PDF documents can't be
validated this cheaply, so they are left ok=true (present). A nonzero `false` count = captures to re-crawl.

  python -m archive.validate [limit]      # limit 0 = every unchecked document
"""
import sys

from . import db, store


def _range(sha, rng):
    s3, bucket = store._s3()
    try:
        return s3.get_object(Bucket=bucket, Key=store.blob_key(sha), Range=rng)["Body"].read()
    except Exception:  # noqa: BLE001  missing object or range error
        return None


def check(sha, content_type):
    head = _range(sha, "bytes=0-7")
    if head is None:
        return False                                  # blob missing from B2
    if head[:5] == b"%PDF-":
        tail = _range(sha, "bytes=-4096") or b""
        return b"%%EOF" in tail                        # a complete PDF ends with %%EOF; truncation drops it
    return True                                        # non-PDF: present, not cheaply verifiable -> accept


def main(limit=0):
    with db.conn() as c:
        rows = c.execute("""SELECT d.content_sha, b.content_type FROM document d JOIN blob b ON d.content_sha = b.sha256
                            WHERE d.ok IS NULL ORDER BY b.stored_at DESC""").fetchall()
    todo = rows[:limit] if limit else rows
    print(f"{len(todo)} documents to check", flush=True)
    good = bad = 0
    with db.conn() as c:
        for r in todo:
            ok = check(r["content_sha"], r["content_type"])
            c.execute("UPDATE document SET ok=%s WHERE content_sha=%s", (ok, r["content_sha"]))
            good += ok
            bad += (not ok)
            if (good + bad) % 500 == 0:
                c.commit()
                print(f"{good + bad}/{len(todo)}  ok={good} bad={bad}", flush=True)
        c.commit()
    print(f"done: {good} ok, {bad} bad of {good + bad}", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
