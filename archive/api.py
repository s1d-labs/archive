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
from fastapi.responses import HTMLResponse, RedirectResponse

from . import db, ingest

app = FastAPI(title="archive ingest")
TOKEN = None


@app.get("/", response_class=HTMLResponse)
def home():
    """Public progress page. Serves cached aggregate numbers (archive.stats writes them via a timer) so it's instant even
    on a multi-million-row corpus — no live full-table counts here. Data itself is team-gated."""
    try:
        with db.conn() as c:
            row = c.execute("SELECT data FROM stats_cache WHERE id=1").fetchone()
        s = (row["data"] if row else {}) or {}
    except Exception:  # noqa: BLE001  cache table not created yet
        s = {}
    gb = s.get("bytes", 0) / 1e9

    def big(n, lbl):
        return f'<div class=c><div class=n>{n}</div><div class=l>{lbl}</div></div>'
    cards = (big(f'{s.get("hosts",0):,}', 'hosts') + big(f'{s.get("captures",0):,}', 'captures')
             + big(f'{gb:,.1f} GB', 'raw data') + big(f'{s.get("documents",0):,}', 'documents')
             + big(f'{s.get("indexed",0):,}', 'searchable (indexed)') + big(f'{s.get("last_hour",0):,}', 'captures / last hour')
             ) if s else '<p>warming up…</p>'
    return f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>archive.s1d.me</title>
<meta http-equiv=refresh content=120>
<style>:root{{color-scheme:dark}}body{{background:#0b0d10;color:#e6e8eb;font:15px/1.5 ui-sans-serif,system-ui,sans-serif;
margin:0;padding:48px 20px;max-width:860px;margin:0 auto}}h1{{font-size:22px;font-weight:650;letter-spacing:-.01em;margin:0 0 4px}}
.sub{{color:#8a9099;margin:0 0 36px;font-size:14px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin-bottom:36px}}
.c{{background:#14181d;border:1px solid #222830;border-radius:12px;padding:18px 20px}}.n{{font-size:28px;font-weight:650;letter-spacing:-.02em}}
.l{{color:#8a9099;font-size:13px;margin-top:3px}}.foot{{color:#555b63;font-size:12px;margin-top:32px}}a{{color:#6ea8fe}}</style></head>
<body><h1>archive.s1d.me</h1><p class=sub>A content-addressed, versioned archive of the Indian republic&rsquo;s public government web. Capture everything; understand it in layers.</p>
<div class=grid>{cards}</div>
<p class=foot>Aggregate progress only. The archived data is private (team-gated).</p></body></html>"""


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


@app.post("/ingest/batch", dependencies=[Depends(auth)])
async def ingest_batch(request: Request):
    """Many already-uploaded captures in one call (blobs already PUT to B2). One DB transaction for the whole batch — the
    scale path that lets worker count rise without dev-vps-2 write-contention. Body: {items: [ {url, content_sha, ...}, ... ]}."""
    b = await request.json()
    return await asyncio.to_thread(ingest.ingest_meta_batch, b.get("items") or [])


def _authed(request: Request, k: str) -> bool:
    want = _token()
    return bool(want) and (request.headers.get("authorization", "") == f"Bearer {want}" or k == want)


@app.get("/get")
async def get_one(request: Request, url: str, k: str = "", dl: int = 0):
    """Retrieve an archived page/document by its ORIGINAL url (structure preserved — the url is the key). Returns the
    latest capture, as a time-limited direct-from-B2 link. Token via header or ?k=. ?dl=1 forces download over inline."""
    if not _authed(request, k):
        raise HTTPException(401, "bad or missing token")
    import os.path
    from . import store
    with db.conn() as c:
        row = c.execute("""SELECT b.b2_key, b.content_type FROM capture c JOIN blob b ON c.content_sha = b.sha256
                           WHERE c.url = %s AND c.content_sha IS NOT NULL ORDER BY c.fetched_at DESC LIMIT 1""",
                        (url,)).fetchone()
    if not row:
        raise HTTPException(404, "not archived (try the exact captured url, or /host to browse)")
    fn = os.path.basename(url.split("?")[0]) or "download"
    link = await asyncio.to_thread(store.get_url, row["b2_key"], row["content_type"], fn, 3600, not dl)
    return RedirectResponse(link)


@app.get("/search")
async def search_corpus(request: Request, q: str, k: str = "", host: str = "", limit: int = 50):
    """Full-text search across the captured corpus. Results carry each hit's original url/host/path, so the site structure
    is preserved in the answer. Optional host= filter. Needs the fts backfill to have indexed the text (blob.tsv)."""
    if not _authed(request, k):
        raise HTTPException(401, "bad or missing token")
    limit = min(max(1, limit), 200)
    sql = "SELECT url, host, fetched_at, content_type, is_document FROM search WHERE tsv @@ websearch_to_tsquery('simple', %s)"
    args = [q]
    if host:
        sql += " AND host = %s"
        args.append(host)
    sql += " ORDER BY fetched_at DESC LIMIT %s"
    args.append(limit)
    with db.conn() as c:
        rows = c.execute(sql, tuple(args)).fetchall()
    return {"q": q, "count": len(rows),
            "results": [{"url": r["url"], "host": r["host"], "is_document": r["is_document"],
                         "content_type": r["content_type"], "fetched_at": r["fetched_at"].isoformat()} for r in rows]}


@app.get("/host")
async def host_browse(request: Request, host: str, k: str = "", limit: int = 200):
    """Browse what's archived for one host, newest first — the original url paths, so the site's structure is navigable."""
    if not _authed(request, k):
        raise HTTPException(401, "bad or missing token")
    limit = min(max(1, limit), 1000)
    with db.conn() as c:
        rows = c.execute("""SELECT DISTINCT ON (url) url, content_type, is_document, fetched_at FROM capture
                            WHERE host = %s AND content_sha IS NOT NULL ORDER BY url, fetched_at DESC LIMIT %s""",
                         (host, limit)).fetchall()
    return {"host": host, "count": len(rows),
            "urls": [{"url": r["url"], "is_document": r["is_document"], "content_type": r["content_type"],
                      "fetched_at": r["fetched_at"].isoformat()} for r in rows]}


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


# ---------------------------------------------------------------- Wayback-style viewer (team-gated via cookie / ?k=)
import html as _html
import urllib.parse as _url

_WB_HEAD = ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            "<meta name=viewport content='width=device-width,initial-scale=1'><title>archive · wayback</title>"
            "<style>:root{color-scheme:dark}body{background:#0b0d10;color:#e6e8eb;"
            "font:15px/1.6 ui-sans-serif,system-ui,sans-serif;margin:0 auto;padding:40px 20px;max-width:900px}"
            "h1{font-size:21px;margin:0 0 14px}a{color:#6ea8fe}input{background:#14181d;border:1px solid #2a313a;"
            "color:#e6e8eb;border-radius:8px;padding:9px 12px;font-size:14px}button{background:#1f6feb;border:0;color:#fff;"
            "border-radius:8px;padding:9px 14px;font-size:14px;margin-left:6px;cursor:pointer}form{margin:0 0 14px}"
            "ul{list-style:none;padding:0}li{padding:7px 0;border-bottom:1px solid #1c2127;font-size:14px}"
            ".m{color:#8a9099;font-size:12px}.u{color:#8a9099;word-break:break-all;margin:-6px 0 16px}"
            "code{background:#14181d;padding:2px 6px;border-radius:5px}</style></head><body>")
_WB_FOOT = "<p style='margin-top:26px'><a href='/wayback'>&larr; search</a></p></body></html>"


def _wb_ok(request, k):
    want = _token()
    return bool(want) and (k == want or request.cookies.get("ak") == want
                           or request.headers.get("authorization", "") == f"Bearer {want}")


@app.get("/wayback", response_class=HTMLResponse)
def wb_home(request: Request, k: str = ""):
    if not _wb_ok(request, k):
        return HTMLResponse(_WB_HEAD + "<h1>archive · wayback</h1><p>Append <code>?k=&lt;token&gt;</code> to open.</p></body></html>",
                            status_code=401)
    body = ("<h1>archive · wayback</h1>"
            "<form action='/wayback/url'><input name=u placeholder='https://a.gov.in/page' size=46 autofocus>"
            "<button>captures of a URL</button></form>"
            "<form action='/wayback/search'><input name=q placeholder='full-text search the corpus' size=46>"
            "<button>search</button></form>"
            "<p class=m>Enter an exact archived URL to see its snapshots over time, or search the full text.</p>")
    r = HTMLResponse(_WB_HEAD + body + "</body></html>")
    if k and k == _token():
        r.set_cookie("ak", k, httponly=True, max_age=604800, samesite="lax")
    return r


@app.get("/wayback/url", response_class=HTMLResponse)
def wb_url(request: Request, u: str, k: str = ""):
    if not _wb_ok(request, k):
        raise HTTPException(401, "add ?k=<token>")
    with db.conn() as c:
        rows = c.execute("""SELECT id, fetched_at, content_type, is_document FROM capture
                            WHERE url=%s AND content_sha IS NOT NULL ORDER BY fetched_at DESC LIMIT 500""", (u,)).fetchall()
    li = "".join("<li><a href='/wayback/view?c=%d'>%s</a> <span class=m>%s%s</span></li>"
                 % (r["id"], r["fetched_at"].strftime("%Y-%m-%d %H:%M"), _html.escape(r["content_type"] or "?"),
                    " &middot; document" if r["is_document"] else "") for r in rows)
    body = ("<h1>%d capture%s</h1><p class=u>%s</p><ul>%s</ul>"
            % (len(rows), "" if len(rows) == 1 else "s", _html.escape(u),
               li or "<li>none archived at this exact URL yet — try /wayback/search or the exact captured URL</li>"))
    return HTMLResponse(_WB_HEAD + body + _WB_FOOT)


@app.get("/wayback/search", response_class=HTMLResponse)
def wb_search(request: Request, q: str, k: str = "", limit: int = 60):
    if not _wb_ok(request, k):
        raise HTTPException(401, "add ?k=<token>")
    with db.conn() as c:
        rows = c.execute("""SELECT url, host, fetched_at, is_document FROM search
                            WHERE tsv @@ websearch_to_tsquery('simple', %s) ORDER BY fetched_at DESC LIMIT %s""",
                         (q, min(max(1, limit), 200))).fetchall()
    li = "".join("<li><a href='/wayback/url?u=%s'>%s</a> <span class=m>%s &middot; %s%s</span></li>"
                 % (_url.quote(r["url"], safe=""), _html.escape(r["url"][:120]), _html.escape(r["host"] or ""),
                    r["fetched_at"].strftime("%Y-%m-%d"), " &middot; doc" if r["is_document"] else "") for r in rows)
    body = ("<h1>%d result%s for &ldquo;%s&rdquo;</h1><ul>%s</ul>"
            % (len(rows), "" if len(rows) == 1 else "s", _html.escape(q),
               li or "<li>no matches (the search index may still be building)</li>"))
    return HTMLResponse(_WB_HEAD + body + _WB_FOOT)


@app.get("/wayback/view", response_class=HTMLResponse)
def wb_view(request: Request, c: int, k: str = ""):
    if not _wb_ok(request, k):
        raise HTTPException(401, "add ?k=<token>")
    from . import store
    with db.conn() as conn:
        row = conn.execute("""SELECT cap.url, cap.fetched_at, b.b2_key, b.content_type
                              FROM capture cap JOIN blob b ON cap.content_sha=b.sha256 WHERE cap.id=%s""", (c,)).fetchone()
    if not row:
        raise HTTPException(404, "no such capture")
    ct = row["content_type"] or "application/octet-stream"
    link = await_get_url(row["b2_key"], ct)
    is_html = "html" in ct.lower()
    sbox = " sandbox=''" if is_html else ""     # neuter scripts in archived HTML; let the browser render PDFs normally
    bar = ("<div class=bar><b>archived</b> %s <span>&middot; %s &middot; "
           "<a href='/wayback/url?u=%s'>all captures</a> &middot; <a href='%s'>raw</a></span></div>"
           % (_html.escape(row["url"][:140]), row["fetched_at"].strftime("%Y-%m-%d %H:%M"),
              _url.quote(row["url"], safe=""), _html.escape(link)))
    frame = "<iframe%s src='%s'></iframe>" % (sbox, _html.escape(link))
    return HTMLResponse("<!doctype html><html><head><meta charset=utf-8><style>"
                        "body{margin:0;font:13px ui-sans-serif,system-ui}"
                        ".bar{background:#0b0d10;color:#e6e8eb;padding:8px 12px;border-bottom:1px solid #222;word-break:break-all}"
                        ".bar a{color:#6ea8fe}.bar span{color:#8a9099}"
                        "iframe{border:0;width:100vw;height:calc(100vh - 36px)}</style></head><body>"
                        + bar + frame + "</body></html>")


def await_get_url(key, ct):
    from . import store
    return store.get_url(key, ct, None, 3600, inline=True)
