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


def ingest_meta(url, content_sha=None, size=0, content_type=None, text=None, text_sha=None, text_chars=0,
                http_status=200, final_url=None, headers=None, robots_ok=None, run_id=None, vantage="pool",
                linked_from=None, fetched_at=None):
    """Record a capture whose blob (and extracted text) a pool worker already uploaded to B2 via a presigned URL.
    Writes the blob/document/capture rows and the tsvector only — no bytes, no B2 upload, no text extraction here.
    This is the scale path: the heavy work (upload + extraction) happens on the worker, dev-vps-2 only indexes."""
    fetched_at = fetched_at or datetime.now(timezone.utc)
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    host = _host(final_url or url)
    is_doc = _is_document(final_url or url, content_type)
    with db.conn() as c:
        if content_sha:
            if text:
                c.execute("""INSERT INTO blob (sha256, size, content_type, b2_key, text_sha, text_chars, tsv)
                             VALUES (%s,%s,%s,%s,%s,%s, to_tsvector('simple', left(%s, 900000)))
                             ON CONFLICT (sha256) DO UPDATE SET
                               text_sha = COALESCE(blob.text_sha, EXCLUDED.text_sha),
                               text_chars = COALESCE(blob.text_chars, EXCLUDED.text_chars),
                               tsv = COALESCE(blob.tsv, EXCLUDED.tsv)""",
                          (content_sha, size, content_type, store.blob_key(content_sha), text_sha, text_chars, text))
            else:
                c.execute("""INSERT INTO blob (sha256, size, content_type, b2_key, text_sha, text_chars)
                             VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (sha256) DO NOTHING""",
                          (content_sha, size, content_type, store.blob_key(content_sha), text_sha, text_chars))
            if is_doc:
                m = DOC_EXT.search(url)
                c.execute("""INSERT INTO document (content_sha, ext, linked_from) VALUES (%s,%s,%s)
                             ON CONFLICT (content_sha) DO NOTHING""",
                          (content_sha, (m.group(1).lower() if m else None), json.dumps((linked_from or [])[:20])))
        cap = c.execute("""INSERT INTO capture (url, host, fetched_at, vantage, http_status, final_url, content_sha,
                             content_type, is_document, headers, robots_ok, run_id)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (url, host, fetched_at, vantage, http_status, final_url, content_sha, content_type, is_doc,
                         json.dumps(headers or {}), robots_ok, run_id)).fetchone()
        c.commit()
    return {"capture_id": cap["id"], "sha256": content_sha, "is_document": is_doc, "host": host}
