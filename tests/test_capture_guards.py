"""Crawler-trap guards in archive.capture (canon / looped / doc_key / links). No database, no network."""
from archive.capture import canon, doc_key, links

TRAP_PAGES = [
    "https://no1dehradun.kvs.ac.in/en/cleanliness/facebook.com/instagram.com/kvhbk1ddn/facebook.com/x",
    "https://www.iucaa.in/hi/research-hi/en/en/about/hi/en/about/science-highlights",
    "https://www.sac.gov.in/careers/@%7B/careers/@%7B/${ENFLAG}",
    "https://www.sac.gov.in/nisar/@%7B/srtd?lang=hi",
    "https://gecskp.ac.in/pdf/news/pdf/news/pdf/news/x",
    "https://sasnagar.punjabpolice.gov.in/pdf/pdf/pdf/x",
    "https://a.gov.in/" + "/".join("s%d" % i for i in range(20)),
]
REAL_PAGES = [
    "https://cbri.res.in/",
    "https://x.gov.in/en/about-us/organisation/divisions/finance",
    "https://x.gov.in/en/en-us/page",
    "https://x.gov.in/hi/hi-page/hi",
    "https://ecourts.gov.in/ecourts_home/static/manuals/index.php?p=home",
]
REAL_DOCS = [
    "https://egazette.gov.in/WriteReadData/2024/251234.pdf",
    "https://x.nic.in/files/2024/01/01/notice.pdf",
    "https://www.dtcp.gov.in/sites/dtcp.gov.in/files/rules.pdf",           # Drupal: a folder named after the site itself
    "https://charity.maharashtra.gov.in/LinkClick.aspx?fileticket=abc%3d&tabid=511&portalid=0&language=mr-IN",
    # a site that links its PDFs only through a relative-link loop (the server ignores the extra folders)
    "https://ifgtb.icfre.gov.in/view-gallery/gass_museum/student_programme/student_programme/student_programme/x/SH.pdf",
]


def host(u):
    from urllib.parse import urlsplit
    return urlsplit(u).hostname


def test_trap_pages_dropped():
    for u in TRAP_PAGES:
        assert canon(u, host(u)) is None, u


def test_real_pages_kept():
    for u in REAL_PAGES:
        assert canon(u, host(u)) is not None, u


def test_real_docs_kept():
    for u in REAL_DOCS:
        assert canon(u, host(u), doc=True) is not None, u


def test_loop_variants_of_one_document_share_a_key():
    a = "https://i.gov.in/g/m/s/s/s/x/SH.pdf"
    b = "https://i.gov.in/g/s/m/s/s/m/m/SH.pdf"
    assert doc_key(a, "i.gov.in") == doc_key(b, "i.gov.in") == "loop:SH.pdf"
    plain = "https://i.gov.in/g/x/SH.pdf"
    assert doc_key(plain, "i.gov.in") == plain


def test_links_unescape_and_linkclick():
    pages, docs = links("https://a.in/", b'<a href="/p?color=60&amp;fabric=291">x</a>'
                                         b'<a href="/LinkClick.aspx?fileticket=z&amp;tabid=1">f</a>')
    assert pages == ["https://a.in/p?color=60&fabric=291"]
    assert docs == ["https://a.in/LinkClick.aspx?fileticket=z&tabid=1"]


def test_shop_noise_dropped():
    for u in ["https://www.indiahandmade.com/catalog/product/view/id/7642/category/34/<?%2F%2F=+%24block",
              "https://www.indiahandmade.com/sendfriend/product/send/id/5/",
              "https://www.indiahandmade.com/stores/store/redirect/___store/hi/___from_store/en/uenc/aHR0cHM6Ly93"]:
        assert canon(u, host(u)) is None, u


def test_science_mirror_excluded():
    assert canon("https://ibdc.dbt.gov.in/isda/ftp/mmCIF/uu/3uun.cif.gz", "ibdc.dbt.gov.in") is None
    assert canon("https://ibdc.dbt.gov.in/about-us", "ibdc.dbt.gov.in") is not None
