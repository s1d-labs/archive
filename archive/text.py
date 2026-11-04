"""Extract plain text from captured content for full-text search. HTML via selectolax; PDF via pdftotext (poppler),
falling back to pypdf. OCR of scanned PDFs is deferred to Phase 4."""
import re
import subprocess
import tempfile


def from_html(data: bytes) -> str:
    from selectolax.lexbor import LexborHTMLParser as HTMLParser
    tree = HTMLParser(data if isinstance(data, bytes) else data.encode())
    for bad in tree.css("script, style, noscript, svg"):
        bad.decompose()
    body = tree.body or tree
    return " ".join((body.text() or "").split())


def from_pdf(data: bytes) -> str:
    with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
        f.write(data)
        f.flush()
        try:
            out = subprocess.run(["pdftotext", "-layout", "-q", f.name, "-"], capture_output=True, timeout=120)
            if out.returncode == 0 and out.stdout.strip():
                return " ".join(out.stdout.decode("utf-8", "replace").split())
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
    try:
        import io

        import pypdf
        r = pypdf.PdfReader(io.BytesIO(data))
        return " ".join(" ".join((p.extract_text() or "") for p in r.pages).split())
    except Exception:  # noqa: BLE001
        return ""


def extract(data: bytes, content_type: str, url: str = "") -> str:
    ct = (content_type or "").lower()
    if "html" in ct or (not ct and data[:200].lstrip().lower().startswith((b"<!doctype", b"<html"))):
        return from_html(data)
    if "pdf" in ct or url.lower().endswith(".pdf") or data[:5] == b"%PDF-":
        return from_pdf(data)
    if ct.startswith("text/") or "json" in ct or "xml" in ct or "csv" in ct:
        return " ".join(data.decode("utf-8", "replace").split())[:5_000_000]
    return ""
