"""The archive ingest API: the HTTP door the Phase-1 capture workers (on the pool / India servers) POST to.
A worker uploads raw bytes to B2 itself (hub-signed) OR posts the body here for small items; it then reports the
capture. Token-authenticated. Loopback / tunnel only. FastAPI, matching scrape-hub's style.

  POST /ingest        multipart or json: a captured fetch (url, content_type, status, body or content_sha) -> capture row
  GET  /healthz
  GET  /stats
Run: uvicorn archive.api:app (via the archive-index.service unit).
"""
import asyncio
import os

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse

from . import db, ingest

app = FastAPI(title="archive ingest")
TOKEN = None


@app.get("/", response_class=HTMLResponse)
def home():
    """Public progress page. Aggregate counts only — the archived data itself is team-gated (token on /ingest, /stats)."""
    with db.conn() as c:
        hosts = c.execute("SELECT count(DISTINCT host) n FROM capture").fetchone()["n"]
        cap = c.execute("SELECT count(*) n FROM capture").fetchone()["n"]
        docs = c.execute("SELECT count(*) n FROM capture WHERE is_document").fetchone()["n"]
        bl = c.execute("SELECT count(*) n, coalesce(sum(size),0) b, count(*) FILTER (WHERE text_chars>0) t FROM blob").fetchone()
        hr = c.execute("SELECT count(*) n FROM capture WHERE fetched_at > now()-interval '1 hour'").fetchone()["n"]
        van = c.execute("SELECT coalesce(vantage,'?') v, count(DISTINCT host) h FROM capture GROUP BY 1 ORDER BY 2 DESC LIMIT 6").fetchall()
    gb = int(bl["b"]) / 1e9
    rows = "".join(f"<tr><td>{r['v']}</td><td>{r['h']:,}</td></tr>" for r in van)
    def big(n, lbl):
        return f'<div class=c><div class=n>{n}</div><div class=l>{lbl}</div></div>'
    return f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>archive.s1d.me</title>
<meta http-equiv=refresh content=60>
<style>:root{{color-scheme:dark}}body{{background:#0b0d10;color:#e6e8eb;font:15px/1.5 ui-sans-serif,system-ui,sans-serif;
margin:0;padding:48px 20px;max-width:860px;margin:0 auto}}h1{{font-size:22px;font-weight:650;letter-spacing:-.01em;margin:0 0 4px}}
.sub{{color:#8a9099;margin:0 0 36px;font-size:14px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin-bottom:36px}}
.c{{background:#14181d;border:1px solid #222830;border-radius:12px;padding:18px 20px}}.n{{font-size:28px;font-weight:650;letter-spacing:-.02em}}
.l{{color:#8a9099;font-size:13px;margin-top:3px}}table{{border-collapse:collapse;width:100%;font-size:14px}}td{{padding:7px 10px;border-bottom:1px solid #1c2127}}
td:last-child{{text-align:right;color:#8a9099;font-variant-numeric:tabular-nums}}.foot{{color:#555b63;font-size:12px;margin-top:32px}}a{{color:#6ea8fe}}</style></head>
<body><h1>archive.s1d.me</h1><p class=sub>A content-addressed, versioned archive of the Indian republic&rsquo;s public government web. Capture everything; understand it in layers.</p>
<div class=grid>{big(f'{hosts:,}','hosts touched')}{big(f'{cap:,}','captures')}{big(f'{gb:,.1f} GB','raw data')}{big(f'{docs:,}','documents')}{big(f'{bl["t"]:,}','with searchable text')}{big(f'{hr:,}','captures / last hour')}</div>
<table><thead><tr><td>vantage</td><td>hosts</td></tr></thead><tbody>{rows}</tbody></table>
<p class=foot>Aggregate progress only. The archived data is private (team-gated). Refreshes every 60s.</p></body></html>"""


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
    if content is None:
        # scale path: the worker already uploaded the blob + text to B2 (presigned); we only write rows + tsv here
        return await asyncio.to_thread(
            ingest.ingest_meta, b["url"], content_sha=b.get("content_sha"), size=b.get("size", 0),
            content_type=b.get("content_type"), text=b.get("text"), text_sha=b.get("text_sha"),
            text_chars=b.get("text_chars", 0), http_status=b.get("http_status", 200), final_url=b.get("final_url"),
            headers=b.get("headers"), robots_ok=b.get("robots_ok"), run_id=b.get("run_id"),
            vantage=b.get("vantage", "pool"), linked_from=b.get("linked_from"), fetched_at=b.get("fetched_at"))
    return ingest.ingest(b["url"], content, content_type=b.get("content_type"), fetched_at=b.get("fetched_at"),
                         vantage=b.get("vantage", "pool"), http_status=b.get("http_status", 200),
                         final_url=b.get("final_url"), headers=b.get("headers"), robots_ok=b.get("robots_ok"),
                         run_id=b.get("run_id"), linked_from=b.get("linked_from"))


@app.post("/blob/prepare", dependencies=[Depends(auth)])
async def blob_prepare(request: Request):
    """A pool worker asks which parts it must upload before reporting a capture. Returns presigned PUT URLs for the raw
    blob and/or its text, or null for parts already in the store (dedup by the blob index). Keeps B2 creds off the runner."""
    from . import store
    b = await request.json()
    sha = b.get("content_sha")
    out = {"raw_url": None, "txt_url": None}
    row = None
    if sha:
        with db.conn() as c:
            row = c.execute("SELECT text_sha FROM blob WHERE sha256=%s", (sha,)).fetchone()
        if not row:
            out["raw_url"] = await asyncio.to_thread(store.put_url, store.blob_key(sha))
        if b.get("text_sha") and (not row or not row.get("text_sha")):
            out["txt_url"] = await asyncio.to_thread(store.put_url, store.text_key(sha))
    return out
