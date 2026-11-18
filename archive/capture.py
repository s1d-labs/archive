"""The generic capture worker (Phase 1). Walk a host's same-site pages breadth-first, capturing the raw bytes of every
page and every linked document, with provenance. Source-agnostic — the same code for every host. Two sinks:
  - local:  call ingest.ingest directly (runs on dev-vps-2, which has the DB + B2 write key). Used for smoke tests.
  - http:   POST each capture to the archive ingest API (used by pool / India-server workers, which have neither).
Polite: one request at a time per host, a gap between requests, robots honoured, an honest UA, caps on pages/docs/bytes.
"""
import argparse
import asyncio
import html as _html
import base64
import json
import time
from collections import deque
from urllib.parse import urldefrag, urljoin, urlsplit

from .core_fetch import GAP_S, UA, make_session

DOC_EXT = (".pdf", ".xls", ".xlsx", ".csv", ".doc", ".docx", ".ppt", ".pptx", ".zip", ".odt", ".ods", ".rtf")
SKIP = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".css", ".js", ".woff", ".woff2", ".ttf", ".mp4", ".mp3")


def same_site(host, other):
    a, b = (host or "").lower().removeprefix("www."), (other or "").lower().removeprefix("www.")
    return bool(b) and a == b


import re as _re
from urllib.parse import parse_qsl, urlencode, urlunsplit

# Crawler-trap guards (2026-11-10: deep-pass frontiers held 8.7 M URLs, mostly filter/sort/calendar variants of one page:
# nipfp blog index ?year&month 1.2 M, indiahandmade catalogue filters 1 M, licindia tenders list 113 K).
JUNK_PARAM = _re.compile(r"^(utm_\w+|fbclid|gclid|mc_\w+|_ga|sessionid|jsessionid|phpsessid|sid|cid|sort|order|orderby|dir|"
                         r"limit|mode|view|layout|print|share|replytocom|ref|referer|return_?url|redirect|lang_switch|t|ts|_)$", _re.I)
DEAD_END = _re.compile(r"/(login|logout|signin|sign-in|signup|register|account|customer|cart|checkout|wishlist|compare|"
                       r"catalogsearch|search|calendar|print|share|feed|wp-json|xmlrpc|sendfriend|uenc)(/|$|\.)", _re.I)
# Relative-link loops (2026-11-18: four deep-pass hosts reached the 400,000-page cap with 0-51 documents):
#   /en/cleanliness/facebook.com/instagram.com/kvhbk1ddn/facebook.com/...   (social links written without https://)
#   /hi/research-hi/en/en/about/hi/en/about/...                               (language switch links relative to the page)
#   /@%7B/careers/@%7B/careers/...${ENFLAG}                                    (unrendered page templates)
TEMPLATE = _re.compile(r"(\$\{|@\{|\{\{|<%|<\?|%7B%7B|@%7B|\$%7B|%3C%3F|%3C%25)", _re.I)
DOMAIN_SEG = _re.compile(r"^(www\.)?[a-z0-9-]+(\.[a-z0-9-]+)*\.(com|org|net|in|co|me|io|gov|nic)$", _re.I)
START_RETRY_WAITS = (5, 15, 30)   # seconds before each retry of a start page that gave no answer
MAX_DEPTH = 15          # path segments; real sites stay far below
SEG_REPEAT = 3          # the same non-empty segment this many times in one path = a loop


def looped(path, host=""):
    """True for a path that a relative-link loop or an unrendered template made, not a real page. A folder named after
    another domain is a loop ("facebook.com"); one named after the site itself is not (Drupal: /sites/<host>/files/)."""
    if TEMPLATE.search(path):
        return True
    segs = [x for x in path.split("/") if x]
    if len(segs) > MAX_DEPTH:
        return True
    own = (host or "").lower().removeprefix("www.")
    if any(DOMAIN_SEG.match(x) and x.lower().removeprefix("www.") != own for x in segs[:-1]):
        return True                                    # "facebook.com" as a folder: a link written without its scheme
    seen = {}
    for x in segs:
        seen[x] = seen.get(x, 0) + 1
        if seen[x] >= SEG_REPEAT:
            return True
    return False


# Not government records: mirrors of world science databases on .gov.in hosts (2026-11-18: ibdc.dbt.gov.in /isda/ftp/ is
# a copy of the Protein Data Bank, 1.5 M structure files queued in one deep pass).
EXCLUDE = {"ibdc.dbt.gov.in": ("/isda/ftp/", "/isda/api/")}


def excluded(host, path):
    return any(path.startswith(x) for x in EXCLUDE.get((host or "").lower().removeprefix("www."), ()))


MAX_PARAMS = 3          # more query parameters than this = a filter combination, not a page
DOC_PARAMS = 6          # documents: DotNetNuke LinkClick.aspx?fileticket=&tabid=&portalid=&language= is one file


def doc_key(u, host):
    """The key a document is fetched once under: the url itself, or for a looped path the file name + query (all loop
    variants of one file share it)."""
    p = urlsplit(u)
    if looped(p.path, host):
        return "loop:" + p.path.rstrip("/").rsplit("/", 1)[-1] + ("?" + p.query if p.query else "")
    return u
PATH_CAP = 5000         # at most this many query variants of one path (real id/page lists fit; filter explosions stop)
PLAIN_CAP = 50          # a path that also exists WITHOUT a query: its query variants are mostly copies (?year=&month=)


def canon(u, host, doc=False):
    """One spelling per URL: start host's www/non-www form, no fragment, no junk parameters, sorted parameters, no '//'
    in the path. None for a dead end (login, cart, search...), a filter combination, an unrendered template, or a
    relative-link loop. doc=True: a document under a looped path is kept (some sites link their PDFs only that way and
    the server ignores the extra folders; capture_host fetches it once, see doc_key), and up to DOC_PARAMS parameters."""
    p = urlsplit(u)
    if DEAD_END.search(p.path) or TEMPLATE.search(u) or excluded(p.hostname or host, p.path):
        return None
    if not doc and looped(p.path, host):
        return None
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not JUNK_PARAM.match(k)]
    if len(q) > (DOC_PARAMS if doc else MAX_PARAMS):
        return None
    path = _re.sub(r"/{2,}", "/", p.path) or "/"
    netloc = p.netloc.lower()                          # keep the port; hosts are case-blind; unify www / non-www below
    if p.hostname and same_site(host, p.hostname) and p.hostname != host:
        netloc = netloc.replace(p.hostname, host, 1)
    return urlunsplit((p.scheme, netloc, path, urlencode(sorted(q)), ""))


def path_key(u):
    p = urlsplit(u)
    return p.path


def links(base, html):
    import re
    pages, docs = [], []
    for m in re.finditer(rb'href=["\']([^"\'#]+)', html[:3_000_000]):
        try:
            href = _html.unescape(m.group(1).decode("utf-8", "replace").strip())   # "&amp;" in an href means "&"
            u, _ = urldefrag(urljoin(base, href))
        except ValueError:
            continue
        p = urlsplit(u)
        if p.scheme not in ("http", "https"):
            continue
        low = p.path.lower()
        if low.endswith(DOC_EXT) or low.endswith("/linkclick.aspx"):
            docs.append(u)
        elif not low.endswith(SKIP):
            pages.append(u)
    return pages, docs


class Sink:
    """Where captured bytes go.
      local    -> ingest.ingest (dev-vps-2: straight to B2 + index). Smoke tests and the local reachable sweep.
      http     -> POST /ingest (pool / India-server workers that reach archive.s1d.me).
      b2       -> upload the blob (and extracted text) straight to the archive B2 bucket and append one metadata line to a
                  local JSONL. For a vantage (vpsin) that has the B2 write key but not the index; the JSONL is rsynced
                  back and imported into the index with `archive.import_b2`. No tunnel, no large local disk use."""
    def __init__(self, mode, api=None, token=None, run_id=None, vantage="dev-vps-2", jsonl=None):
        self.mode, self.api, self.token, self.run_id, self.vantage = mode, api, token, run_id, vantage
        if mode == "http":
            import aiohttp
            # An explicit UA: Cloudflare's Browser Integrity Check 403s the default aiohttp/urllib UA (error 1010).
            self._s = aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}",
                                                     "User-Agent": "archive-pool/1.0 (+https://archive.s1d.me)"})
        elif mode == "b2":
            self._f = open(jsonl, "a")
        elif mode == "direct":
            import threading
            self._buf = []                            # metadata buffered, flushed to /ingest/batch in groups (see _direct)
            self._lock = threading.Lock()             # sends may run in parallel threads (bucket_task): one batch at a time

    async def send(self, url, status, final_url, content, content_type, linked_from=None, fetched_at=None):
        if self.mode == "local":
            from . import ingest
            return await asyncio.to_thread(ingest.ingest, url, content, content_type=content_type, vantage=self.vantage,
                                           http_status=status, final_url=final_url, run_id=self.run_id, linked_from=linked_from)
        if self.mode == "b2":
            return await asyncio.to_thread(self._to_b2, url, status, final_url, content, content_type, linked_from)
        if self.mode == "direct":
            return await asyncio.to_thread(self._direct, url, status, final_url, content, content_type, linked_from, fetched_at)
        body = {"url": url, "http_status": status, "final_url": final_url, "content_type": content_type,
                "vantage": self.vantage, "run_id": self.run_id, "linked_from": linked_from,
                "body_b64": base64.b64encode(content).decode() if content is not None else None}
        async with self._s.post(f"{self.api}/ingest", json=body) as r:
            if r.status >= 300:                      # fail loudly so a bad ingest is retried, never silently dropped
                raise RuntimeError(f"ingest {r.status}: {(await r.text())[:120]}")
            return await r.json()

    @staticmethod
    def _retry(fn, tries=6):
        """Retry a call on 5xx, 429 and network errors with backoff (5, 10, 20, 40, 60 s). The ingest API or B2 being busy
        for a minute must not fail a whole host (502s from archive.s1d.me under load did, 2026-11-09)."""
        import time as _t
        import urllib.error
        for i in range(tries):
            try:
                return fn()
            except urllib.error.HTTPError as e:
                if e.code < 500 and e.code != 429 or i == tries - 1:
                    raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if i == tries - 1:
                    raise
            _t.sleep(min(60, 5 * 2 ** i))

    def _api_post(self, path, body):
        """POST JSON to the ingest API (token + UA; the UA matters — Cloudflare 403s the default urllib UA, error 1010)."""
        import urllib.request
        data = json.dumps(body).encode()

        def go():
            req = urllib.request.Request(f"{self.api}{path}", data=data, headers={
                "Authorization": f"Bearer {self.token}", "Content-Type": "application/json",
                "User-Agent": "archive-pool/1.0 (+https://archive.s1d.me)"})
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read())
        return self._retry(go)

    def _b2_put(self, url, data):
        """Upload bytes straight to B2 via a presigned PUT URL (no B2 credential on this worker)."""
        import urllib.request

        def go():
            req = urllib.request.Request(url, data=data, method="PUT")
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.status
        return self._retry(go)

    def _direct(self, url, status, final_url, content, content_type, linked_from, fetched_at=None):
        """Scale path: hash + extract here, upload the blob (and text) straight to B2 via presigned URLs, then report
        only metadata + text to the ingest API. dev-vps-2 never touches the bytes, so the pool can run many workers."""
        from . import store, text
        meta = {"url": url, "http_status": status, "final_url": final_url, "content_type": content_type,
                "vantage": self.vantage, "run_id": self.run_id, "linked_from": linked_from,
                "content_sha": None, "size": 0, "text_sha": None, "text_chars": 0}
        if fetched_at:
            meta["fetched_at"] = fetched_at              # a Wayback copy keeps the date the page was really captured
        if content is not None:
            sha = store.sha256(content)
            txt = text.extract(content, content_type, url)
            if txt:
                txt = txt.encode("utf-8", "replace").decode("utf-8")   # no lone surrogates: the ingest API refuses them
            tsha = store.sha256(txt.encode("utf-8", "replace")) if txt else None
            # text goes to B2 (below) AND in the batch (capped) so the ingest builds the search index at capture time —
            # cheap now that writes are batched, and it keeps search current without a slow from-B2 backfill
            meta.update(content_sha=sha, size=len(content), text_sha=tsha, text_chars=(len(txt) if txt else 0),
                        text=(txt[:600000] if txt else None))
            prep = self._api_post("/blob/prepare", {"content_sha": sha, "text_sha": tsha})
            if prep.get("raw_url"):
                self._b2_put(prep["raw_url"], content)
            if txt and prep.get("txt_url"):
                self._b2_put(prep["txt_url"], txt.encode("utf-8", "replace"))
        with self._lock:
            self._buf.append(meta)                    # batch the index write: one DB txn per ~25 captures, not per capture
            batch = self._buf if len(self._buf) >= 25 else None
            if batch:
                self._buf = []
        if batch:
            self._api_post("/ingest/batch", {"items": batch})
        return {"buffered": len(self._buf)}

    async def fresh(self, urls, days):
        """The urls among `urls` that already have a 2xx capture in the last `days` days (empty when days <= 0, or when the
        API cannot answer: then the worker fetches as before; a lost check costs a download, never data)."""
        if not urls or not days or days <= 0 or self.mode not in ("direct", "http"):
            return set()
        urls, out = list(urls), set()
        for i in range(0, len(urls), 500):
            try:
                r = await asyncio.to_thread(self._api_post, "/seen", {"urls": urls[i:i + 500], "days": days})
                out.update(r.get("fresh") or ())
            except Exception:  # noqa: BLE001
                pass
        return out

    async def flush(self):
        """Write buffered captures now (the deep pass calls this before it saves a frontier)."""
        if self.mode == "direct":
            await asyncio.to_thread(self._flush)

    def _flush(self):
        with self._lock:
            batch, self._buf = self._buf, []
        if batch:
            self._api_post("/ingest/batch", {"items": batch})

    def _to_b2(self, url, status, final_url, content, content_type, linked_from):
        from . import store, text
        rec = {"url": url, "http_status": status, "final_url": final_url, "content_type": content_type,
               "vantage": self.vantage, "run_id": self.run_id, "linked_from": linked_from, "sha256": None, "size": 0,
               "text_sha": None, "text_chars": 0}
        if content is not None:
            sha, _, _ = store.put_blob(content, content_type)
            rec.update(sha256=sha, size=len(content))
            txt = text.extract(content, content_type, url)
            if txt:
                store.put_text(sha, txt)
                rec.update(text_sha=store.sha256(txt.encode("utf-8", "replace")), text_chars=len(txt))
        self._f.write(json.dumps(rec) + "\n")
        self._f.flush()
        return rec

    async def close(self):
        if self.mode == "http":
            await self._s.close()
        elif self.mode == "b2":
            self._f.close()
        elif self.mode == "direct":
            await asyncio.to_thread(self._flush)      # write any captures still buffered for this host


async def capture_host(session, start_url, sink, max_pages=2000, max_docs=20000, gap=GAP_S, proxy=None,
                       state=None, deadline=None, checkpoint=None, checkpoint_every=500, concurrency=1, doc_fresh_days=0):
    """Breadth-first walk of one host. Deep pass: `state` ({q, seen, seen_docs, pages, docs}) resumes a saved frontier,
    `deadline` (epoch seconds) stops the walk in time, and `checkpoint(state, done)` is awaited every `checkpoint_every`
    pages and at the end, so the next job continues where this one stopped. `concurrency` = requests in flight to this host
    at once (1 for the first pass; the deep pass uses more: one request at a time made a 1.7 M-page site a 19-day crawl).
    `doc_fresh_days` > 0: a document with a 2xx capture in the last N days is not fetched again (counted in "skipped")."""
    host = urlsplit(start_url).hostname
    if state:
        seen, seen_docs = set(state.get("seen", ())), set(state.get("seen_docs", ()))
        q = deque(state.get("q") or [])
        pages, docs = int(state.get("pages", 0)), int(state.get("docs", 0))
    else:
        seen, seen_docs = set(), set()
        q = deque([start_url])
        pages = docs = 0
    skipped = 0
    from collections import Counter
    per_path = Counter(path_key(u) for u in seen)      # query variants already taken per path (trap cap)
    plain = {path_key(u) for u in seen if not urlsplit(u).query}   # paths also seen without a query

    def cap(c):
        return PLAIN_CAP if path_key(c) in plain else PATH_CAP
    if state:                                          # prune a resumed frontier with the same rules
        kept = []
        for u in q:
            c = canon(u, host)
            if c and c not in seen and (not urlsplit(c).query or per_path[path_key(c)] < cap(c)):
                per_path[path_key(c)] += 1
                kept.append(c)
        q = deque(dict.fromkeys(kept))
    inflight = set()                                   # popped but not finished: kept in a checkpoint so a resume refetches them

    def take(u):
        """Queue a found link if it passes the trap guards."""
        c = canon(u, host)
        if not c or c in seen:
            return None
        k = path_key(c)
        if not urlsplit(c).query:
            plain.add(k)
        elif per_path[k] >= cap(c):
            return None
        per_path[k] += 1
        return c
    last_cp = pages

    def snapshot():
        return {"q": list(inflight) + list(q), "seen": list(seen - inflight), "seen_docs": list(seen_docs),
                "pages": pages, "docs": docs}

    def stop():
        return pages >= max_pages or (deadline and time.time() > deadline)

    async def worker():
        nonlocal pages, docs, last_cp, skipped
        while not stop():
            if not q:
                if not inflight:
                    return                             # nothing queued and nobody fetching: the host is done
                await asyncio.sleep(0.2)
                continue
            url = q.popleft()
            if url in seen:
                continue
            seen.add(url)
            inflight.add(url)
            try:
                meta, body = await _get(session, url, proxy)
                if meta["status"] == 0 and url == start_url:
                    # the start page decides whether the whole host counts as blocked: retry a connection failure (a
                    # proxied fetch fails now and then; 2026-11-18: 77 of 103 start pages from one India worker, most
                    # of which loaded through the same proxies a minute later)
                    for wait in START_RETRY_WAITS:
                        await asyncio.sleep(wait)
                        meta, body = await _get(session, url, proxy)
                        if meta["status"]:
                            break
                await sink.send(url, meta["status"], meta.get("final"), body, meta.get("ctype"))
                pages += 1
                if body and b"html" in (meta.get("ctype") or "").encode():
                    pl, dl = links(meta.get("final") or url, body)
                    for u in pl:
                        if same_site(host, urlsplit(u).hostname):
                            c = take(u)
                            if c:
                                q.append(c)
                    want = {}
                    for d in dl:
                        d = canon(d, host, doc=True) if same_site(host, urlsplit(d).hostname) else None
                        if d and doc_key(d, host) not in seen_docs:
                            want.setdefault(doc_key(d, host), d)
                    fresh = await sink.fresh(list(want.values()), doc_fresh_days) if want else set()
                    for k, d in want.items():
                        if k in seen_docs or docs >= max_docs:
                            continue
                        seen_docs.add(k)
                        if d in fresh:
                            skipped += 1
                            continue
                        dmeta, dbody = await _get(session, d, proxy)
                        await sink.send(d, dmeta["status"], dmeta.get("final"), dbody, dmeta.get("ctype"), linked_from=[url])
                        docs += 1
            finally:
                inflight.discard(url)
            if checkpoint and pages - last_cp >= checkpoint_every:
                last_cp = pages
                await checkpoint(snapshot(), False)
            await asyncio.sleep(gap)

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    complete = not q and not inflight
    if checkpoint:
        await checkpoint(snapshot(), complete)
    return {"host": host, "pages": pages, "documents": docs, "complete": complete, "queued": len(q), "skipped": skipped}


async def _get(session, url, proxy=None):
    try:
        async with session.get(url, allow_redirects=True, proxy=proxy) as r:
            buf = bytearray()                               # read the WHOLE body: content.read(n) returns only the first
            capped = False
            async for chunk in r.content.iter_chunked(1 << 16):  # chunk, which truncated larger PDFs/pages
                buf += chunk
                if len(buf) > 50 * 1024 * 1024:             # 50 MB cap per object
                    capped = True
                    break
            # integrity guard: an uncompressed body shorter than the server's declared length = an incomplete read (dropped
            # connection). Don't store a truncated blob — report it as a failed fetch so it is retried, not silently kept.
            clen = r.headers.get("Content-Length")
            enc = (r.headers.get("Content-Encoding") or "").lower()
            if clen and not enc and not capped:
                try:
                    if len(buf) < int(clen):
                        return {"status": 0, "final": str(r.url), "ctype": r.headers.get("Content-Type"),
                                "err": f"truncated {len(buf)}/{int(clen)}"}, None
                except ValueError:
                    pass
            return {"status": r.status, "final": str(r.url), "ctype": r.headers.get("Content-Type")}, bytes(buf)
    except Exception as e:  # noqa: BLE001
        return {"status": 0, "final": url, "ctype": None, "err": type(e).__name__}, None


async def run(start_urls, sink, **kw):
    async with make_session() as s:
        out = []
        for u in start_urls:
            out.append(await capture_host(s, u, sink, **kw))
        return out


def main():
    ap = argparse.ArgumentParser(prog="archive.capture")
    ap.add_argument("urls", nargs="+")
    ap.add_argument("--sink", choices=["local", "http"], default="local")
    ap.add_argument("--api", default="http://127.0.0.1:9396")
    ap.add_argument("--token", default="")
    ap.add_argument("--run-id", default=f"capture-{int(time.time())}")
    ap.add_argument("--vantage", default="dev-vps-2")
    ap.add_argument("--max-pages", type=int, default=2000)
    a = ap.parse_args()
    sink = Sink(a.sink, api=a.api, token=a.token, run_id=a.run_id, vantage=a.vantage)

    async def go():
        res = await run(a.urls, sink, max_pages=a.max_pages)
        await sink.close()
        return res
    print(json.dumps(asyncio.run(go()), indent=1))


if __name__ == "__main__":
    main()
