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
                    c.execute("UPDATE blob SET text_sha=%s, text_chars=%s WHERE sha256=%s", (tsha, len(txt), sha))
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
