"""python -m archive <cmd>
  init-db                      create/upgrade the schema
  ingest-file URL PATH [CT]    ingest a local file as if captured from URL (content-type CT optional)
  ingest-url URL               fetch URL once from this box and ingest it (for smoke tests only)
  stats                        counts: captures, blobs, documents, bytes
"""
import sys

from . import db, ingest, store


def stats():
    with db.conn() as c:
        cap = c.execute("SELECT count(*) n FROM capture").fetchone()["n"]
        bl = c.execute("SELECT count(*) n, coalesce(sum(size),0) b FROM blob").fetchone()
        doc = c.execute("SELECT count(*) n FROM document").fetchone()["n"]
        hosts = c.execute("SELECT count(DISTINCT host) n FROM capture").fetchone()["n"]
        txt = c.execute("SELECT count(*) n FROM blob WHERE text_sha IS NOT NULL").fetchone()["n"]
    print(f"captures {cap:,} | blobs {bl['n']:,} ({int(bl['b'])/1e9:.2f} GB) | documents {doc:,} | "
          f"hosts {hosts:,} | blobs with text {txt:,}")


def ingest_url(url):
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": "archive.s1d.me smoke test; +https://archive.s1d.me"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
        ct = r.headers.get("Content-Type")
        final = r.geturl()
    print(ingest.ingest(url, data, content_type=ct, final_url=final, http_status=200, vantage="dev-vps-2"))


def main(argv):
    if not argv:
        print(__doc__)
        return
    cmd = argv[0]
    if cmd == "init-db":
        db.init()
        print("schema applied")
    elif cmd == "ingest-file":
        url, path = argv[1], argv[2]
        ct = argv[3] if len(argv) > 3 else None
        data = open(path, "rb").read()
        print(ingest.ingest(url, data, content_type=ct, vantage="dev-vps-2"))
    elif cmd == "ingest-url":
        ingest_url(argv[1])
    elif cmd == "stats":
        stats()
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
