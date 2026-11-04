"""The archive ingest API: the HTTP door the Phase-1 capture workers (on the pool / India servers) POST to.
A worker uploads raw bytes to B2 itself (hub-signed) OR posts the body here for small items; it then reports the
capture. Token-authenticated. Loopback / tunnel only. FastAPI, matching scrape-hub's style.

  POST /ingest        multipart or json: a captured fetch (url, content_type, status, body or content_sha) -> capture row
  GET  /healthz
  GET  /stats
Run: uvicorn archive.api:app (via the archive-index.service unit).
"""
import os

from fastapi import Depends, FastAPI, Header, HTTPException, Request

from . import db, ingest

app = FastAPI(title="archive ingest")
TOKEN = None


def _token():
    global TOKEN
    if TOKEN is None:
        TOKEN = os.environ.get("ARCHIVE_INGEST_TOKEN", "")
    return TOKEN


def auth(authorization: str = Header(default="")):
    want = _token()
    if not want or authorization != f"Bearer {want}":
        raise HTTPException(401, "bad or missing ingest token")


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/stats", dependencies=[Depends(auth)])
def stats():
    with db.conn() as c:
        cap = c.execute("SELECT count(*) n FROM capture").fetchone()["n"]
        bl = c.execute("SELECT count(*) n, coalesce(sum(size),0) b FROM blob").fetchone()
        hosts = c.execute("SELECT count(DISTINCT host) n FROM capture").fetchone()["n"]
    return {"captures": cap, "blobs": bl["n"], "bytes": int(bl["b"]), "hosts": hosts}


@app.post("/ingest", dependencies=[Depends(auth)])
async def ingest_one(request: Request):
    """JSON body: {url, content_type?, http_status?, final_url?, headers?, robots_ok?, run_id?, linked_from?,
    and either body_b64 (raw bytes, base64) for small items, or content_sha (already uploaded to B2 by the worker)}.
    For Phase 1, workers upload large blobs to B2 directly and send content_sha; small pages can inline body_b64."""
    import base64
    b = await request.json()
    content = base64.b64decode(b["body_b64"]) if b.get("body_b64") is not None else None
    if content is None and b.get("content_sha"):
        # the worker already stored the blob; record the capture against it (no re-upload, no re-extract here)
        with db.conn() as c:
            cap = c.execute("""INSERT INTO capture (url, host, fetched_at, vantage, http_status, final_url, content_sha,
                                 content_type, is_document, headers, robots_ok, run_id)
                               VALUES (%(url)s,%(host)s, now(), %(vantage)s, %(status)s, %(final)s, %(sha)s, %(ct)s,
                                 %(doc)s, %(hdr)s::jsonb, %(rok)s, %(run)s) RETURNING id""",
                            {"url": b["url"], "host": (b["url"].split("/")[2] if "//" in b["url"] else ""),
                             "vantage": b.get("vantage", "pool"), "status": b.get("http_status"), "final": b.get("final_url"),
                             "sha": b["content_sha"], "ct": b.get("content_type"), "doc": b.get("is_document", False),
                             "hdr": __import__("json").dumps(b.get("headers") or {}), "rok": b.get("robots_ok"),
                             "run": b.get("run_id")}).fetchone()
            c.commit()
        return {"capture_id": cap["id"], "sha256": b["content_sha"], "newly_stored": False}
    return ingest.ingest(b["url"], content, content_type=b.get("content_type"), fetched_at=b.get("fetched_at"),
                         vantage=b.get("vantage", "pool"), http_status=b.get("http_status", 200),
                         final_url=b.get("final_url"), headers=b.get("headers"), robots_ok=b.get("robots_ok"),
                         run_id=b.get("run_id"), linked_from=b.get("linked_from"))
