"""Document classification and the per-object size limit. No database, no network beyond a local test server."""
import asyncio
import http.server
import threading

from archive import capture
from archive.ingest import _is_document


def test_is_document():
    assert _is_document("https://a.gov.in/x.pdf", "application/pdf", 200)
    assert _is_document("https://a.gov.in/x.pdf", None, 200)
    assert not _is_document("https://a.gov.in/x.pdf", "text/html; charset=UTF-8", 200)   # the site's error page
    assert not _is_document("https://a.gov.in/x.pdf", "application/pdf", 404)
    assert _is_document("https://a.gov.in/get?id=1", "application/vnd.ms-excel", 200)
    assert not _is_document("https://a.gov.in/page", "text/html", 200)


def test_too_large_is_not_stored(monkeypatch):
    body = b"%PDF-1.4 " + b"x" * (3 << 20)

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}/big.pdf"

    async def get(limit):
        monkeypatch.setattr(capture, "MAX_BODY", limit)
        async with capture.make_session() as s:
            return await capture._get(s, url)
    meta, data = asyncio.run(get(1 << 20))
    assert data is None and meta["status"] == 0 and meta["err"].startswith("too-large")
    meta, data = asyncio.run(get(8 << 20))
    assert meta["status"] == 200 and data == body
    srv.shutdown()
