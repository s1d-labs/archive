"""Ingest one captured fetch into the archive: store the raw bytes once (content-addressed), extract text for search,
and append an immutable capture row. This is the single door everything (CLI, the Phase-1 pool workers via the HTTP API)
goes through. Idempotent: the same bytes never store twice; re-ingesting the same (url, content) is cheap and safe."""
import json
import re
from datetime import datetime, timezone
from urllib.parse import urlsplit

from . import db, store, text

DOC_EXT = re.compile(r"\.(pdf|xlsx?|csv|docx?|pptx?|zip|odt|ods|rtf)(\?|$)", re.I)


def _host(url):
    return (urlsplit(url).hostname or "").lower()


def _is_document(url, content_type):
    ct = (content_type or "").lower()
    return bool(DOC_EXT.search(url)) or ("html" not in ct and ("pdf" in ct or "excel" in ct or "spreadsheet" in ct
                                         or "msword" in ct or "officedocument" in ct or ct.startswith("application/")))


def ingest(url, content, content_type=None, fetched_at=None, vantage="dev-vps-2", http_status=200, final_url=None,
           headers=None, robots_ok=None, run_id=None, linked_from=None, extract_text=True):
    """Store one fetched body + its provenance. `content` is raw bytes (or None for a bodyless error). Returns a summary."""
    fetched_at = fetched_at or datetime.now(timezone.utc)
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    host = _host(final_url or url)
    is_doc = _is_document(final_url or url, content_type)
    sha = tkey_chars = None
    newly = txt = None
    with db.conn() as c:
        if content is not None:
            sha, key, newly = store.put_blob(content, content_type)
            row = c.execute("SELECT text_sha FROM blob WHERE sha256=%s", (sha,)).fetchone()
            if not row:
                c.execute("INSERT INTO blob (sha256, size, content_type, b2_key) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                          (sha, len(content), content_type, key))
                row = {"text_sha": None}
            if extract_text and row.get("text_sha") is None:
                txt = text.extract(content, content_type, url)
                if txt:
                    tsha = store.sha256(txt.encode("utf-8", "replace"))
                    store.put_text(sha, txt)
                    # full-text search: 'simple' config is script/language agnostic (gov text is multilingual); cap at the tsvector size limit
                    c.execute("UPDATE blob SET text_sha=%s, text_chars=%s, tsv=to_tsvector('simple', left(%s, 900000)) WHERE sha256=%s",
                              (tsha, len(txt), txt, sha))
                    tkey_chars = len(txt)
            if is_doc:
                lf = json.dumps((linked_from or [])[:20])
                c.execute("""INSERT INTO document (content_sha, ext, linked_from) VALUES (%s,%s,%s)
                             ON CONFLICT (content_sha) DO UPDATE SET linked_from =
                               (SELECT to_jsonb(array(SELECT DISTINCT jsonb_array_elements(document.linked_from || EXCLUDED.linked_from) LIMIT 50)))""",
                          (sha, (DOC_EXT.search(url).group(1).lower() if DOC_EXT.search(url) else None), lf))
        cap = c.execute("""INSERT INTO capture (url, host, fetched_at, vantage, http_status, final_url, content_sha,
                             content_type, is_document, headers, robots_ok, run_id)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (url, host, fetched_at, vantage, http_status, final_url, sha, content_type, is_doc,
                         json.dumps(headers or {}), robots_ok, run_id)).fetchone()
        c.commit()
    return {"capture_id": cap["id"], "sha256": sha, "newly_stored": bool(newly), "is_document": is_doc,
            "text_chars": tkey_chars, "host": host}


def _meta_rows(c, b):
    """Write the blob/document/capture rows for one already-uploaded capture, on cursor `c` (no commit). `b` is a dict.
    tsv is NOT computed here (heaviest per-request CPU) — fts_backfill builds it from the text in B2. Plain inserts only,
    so many pool workers can report captures without overloading dev-vps-2."""
    url = b["url"]
    fetched_at = b.get("fetched_at") or datetime.now(timezone.utc)
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    sha = b.get("content_sha")
    ct = b.get("content_type")
    host = _host(b.get("final_url") or url)
    is_doc = _is_document(b.get("final_url") or url, ct)
    if sha:
        txt = (b.get("text") or "").replace("\x00", "")   # NULs are legal in extracted text but Postgres text/tsvector rejects them
        # build the full-text index here (batched, so cheap): new captures are searchable at once, and a re-captured blob
        # whose row had a null tsv gets filled. 'simple' config = multilingual. No text -> tsv stays null.
        c.execute("""INSERT INTO blob (sha256, size, content_type, b2_key, text_sha, text_chars, tsv)
                     VALUES (%s,%s,%s,%s,%s,%s, CASE WHEN %s <> '' THEN to_tsvector('simple', left(%s, 600000)) END)
                     ON CONFLICT (sha256) DO UPDATE SET
                       text_sha = COALESCE(blob.text_sha, EXCLUDED.text_sha),
                       text_chars = COALESCE(blob.text_chars, EXCLUDED.text_chars),
                       tsv = COALESCE(blob.tsv, EXCLUDED.tsv)""",
                  (sha, b.get("size", 0), ct, store.blob_key(sha), b.get("text_sha"), b.get("text_chars", 0), txt, txt))
        if is_doc:
            m = DOC_EXT.search(url)
            c.execute("""INSERT INTO document (content_sha, ext, linked_from) VALUES (%s,%s,%s)
                         ON CONFLICT (content_sha) DO NOTHING""",
                      (sha, (m.group(1).lower() if m else None), json.dumps((b.get("linked_from") or [])[:20])))
    return c.execute("""INSERT INTO capture (url, host, fetched_at, vantage, http_status, final_url, content_sha,
                          content_type, is_document, headers, robots_ok, run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                     (url, host, fetched_at, b.get("vantage", "pool"), b.get("http_status", 200), b.get("final_url"),
                      sha, ct, is_doc, json.dumps(b.get("headers") or {}), b.get("robots_ok"), b.get("run_id"))).fetchone()


def ingest_meta(url, **kw):
    """Record one already-uploaded capture (blob+text in B2). Writes rows only — no bytes/B2/extraction here."""
    with db.conn() as c:
        cap = _meta_rows(c, {"url": url, **kw})
        c.commit()
    return {"capture_id": cap["id"], "sha256": kw.get("content_sha"), "host": _host(kw.get("final_url") or url)}


def ingest_meta_batch(items):
    """Record many already-uploaded captures in ONE transaction — the pool's scale path. One commit per batch instead of
    one per capture collapses dev-vps-2's write load, so worker count (and thus aggregate download bandwidth) can scale."""
    with db.conn() as c:
        for b in items:
            _meta_rows(c, b)
        c.commit()
    return {"ingested": len(items)}
