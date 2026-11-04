"""End-to-end acceptance for Phase 0: ingest a page and a PDF into the live archive (B2 + Postgres), and prove
dedup + provenance. Runs against the deployed store/db (ARCHIVE_* env from /etc/archive/env). Skips if not configured."""
import os
from pathlib import Path

import pytest

FIX = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def mods():
    if not os.path.exists("/etc/archive/b2-read.env"):
        pytest.skip("archive not provisioned (run ~/setup/113-archive.sh)")
    from archive import db, ingest, store
    return db, ingest, store


def test_ingest_html_and_pdf_dedup_and_provenance(mods):
    db, ingest, store = mods
    html = (FIX / "sample.html").read_bytes()
    pdf = (FIX / "sample.pdf").read_bytes()
    url = "https://ranchi.nic.in/test/notices/"
    r1 = ingest.ingest(url, html, content_type="text/html", vantage="dev-vps-2", run_id="pytest")
    assert r1["sha256"] == store.sha256(html)
    assert r1["host"] == "ranchi.nic.in"
    assert not r1["is_document"]
    # re-ingest the same bytes: a new capture row (provenance), but the blob is NOT stored again (dedup)
    r2 = ingest.ingest(url, html, content_type="text/html", vantage="dev-vps-2", run_id="pytest")
    assert r2["capture_id"] != r1["capture_id"]
    assert r2["newly_stored"] is False
    # a PDF is recognised as a document and its text is extracted
    rp = ingest.ingest("https://ranchi.nic.in/doc/notice.pdf", pdf, content_type="application/pdf", vantage="dev-vps-2", run_id="pytest")
    assert rp["is_document"]
    # provenance + blob round-trip
    with db.conn() as c:
        cap = c.execute("SELECT * FROM capture WHERE id=%s", (r1["capture_id"],)).fetchone()
        assert cap["url"] == url and cap["content_sha"] == r1["sha256"] and cap["vantage"] == "dev-vps-2"
        blob = c.execute("SELECT * FROM blob WHERE sha256=%s", (r1["sha256"],)).fetchone()
        assert blob["size"] == len(html) and blob["text_chars"] and blob["text_chars"] > 0
    assert store.get_blob(r1["sha256"]) == html
    assert "Notices" in store.get_text(r1["sha256"])
