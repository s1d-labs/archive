"""The generic capture worker (Phase 1). Walk a host's same-site pages breadth-first, capturing the raw bytes of every
page and every linked document, with provenance. Source-agnostic — the same code for every host. Two sinks:
  - local:  call ingest.ingest directly (runs on dev-vps-2, which has the DB + B2 write key). Used for smoke tests.
  - http:   POST each capture to the archive ingest API (used by pool / India-server workers, which have neither).
Polite: one request at a time per host, a gap between requests, robots honoured, an honest UA, caps on pages/docs/bytes.
"""
import argparse
import asyncio
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


def links(base, html):
    import re
    pages, docs = [], []
    for m in re.finditer(rb'href=["\']([^"\'#]+)', html[:3_000_000]):
        try:
            u, _ = urldefrag(urljoin(base, m.group(1).decode("utf-8", "replace")))
        except ValueError:
            continue
        p = urlsplit(u)
        if p.scheme not in ("http", "https"):
            continue
        low = p.path.lower()
        if low.endswith(DOC_EXT):
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
            self._buf = []                            # metadata buffered, flushed to /ingest/batch in groups (see _direct)

    async def send(self, url, status, final_url, content, content_type, linked_from=None):
        if self.mode == "local":
            from . import ingest
            return await asyncio.to_thread(ingest.ingest, url, content, content_type=content_type, vantage=self.vantage,
                                           http_status=status, final_url=final_url, run_id=self.run_id, linked_from=linked_from)
        if self.mode == "b2":
            return await asyncio.to_thread(self._to_b2, url, status, final_url, content, content_type, linked_from)
        if self.mode == "direct":
            return await asyncio.to_thread(self._direct, url, status, final_url, content, content_type, linked_from)
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

    def _direct(self, url, status, final_url, content, content_type, linked_from):
        """Scale path: hash + extract here, upload the blob (and text) straight to B2 via presigned URLs, then report
        only metadata + text to the ingest API. dev-vps-2 never touches the bytes, so the pool can run many workers."""
        from . import store, text
        meta = {"url": url, "http_status": status, "final_url": final_url, "content_type": content_type,
                "vantage": self.vantage, "run_id": self.run_id, "linked_from": linked_from,
                "content_sha": None, "size": 0, "text_sha": None, "text_chars": 0}
        if content is not None:
            sha = store.sha256(content)
            txt = text.extract(content, content_type, url)
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
        self._buf.append(meta)                        # batch the index write: one DB txn per ~25 captures, not per capture
        if len(self._buf) >= 25:
            self._flush()
        return {"buffered": len(self._buf)}

    async def flush(self):
        """Write buffered captures now (the deep pass calls this before it saves a frontier)."""
        if self.mode == "direct":
            await asyncio.to_thread(self._flush)

    def _flush(self):
        if self._buf:
            self._api_post("/ingest/batch", {"items": self._buf})
            self._buf = []

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
                       state=None, deadline=None, checkpoint=None, checkpoint_every=500):
    """Breadth-first walk of one host. Deep pass: `state` ({q, seen, seen_docs, pages, docs}) resumes a saved frontier,
    `deadline` (epoch seconds) stops the walk in time, and `checkpoint(state, done)` is awaited every `checkpoint_every`
    pages and at the end, so the next job continues where this one stopped."""
    host = urlsplit(start_url).hostname
    if state:
        seen, seen_docs = set(state.get("seen", ())), set(state.get("seen_docs", ()))
        q = deque(state.get("q") or [])
        pages, docs = int(state.get("pages", 0)), int(state.get("docs", 0))
    else:
        seen, seen_docs = set(), set()
        q = deque([start_url])
        pages = docs = 0
    start_pages = pages

    def snapshot():
        return {"q": list(q), "seen": list(seen), "seen_docs": list(seen_docs), "pages": pages, "docs": docs}

    while q and pages < max_pages and not (deadline and time.time() > deadline):
        if checkpoint and pages > start_pages and (pages - start_pages) % checkpoint_every == 0:
            await checkpoint(snapshot(), False)
        url = q.popleft()
        if url in seen:
            continue
        seen.add(url)
        meta, body = await _get(session, url, proxy)
        await sink.send(url, meta["status"], meta.get("final"), body, meta.get("ctype"))
        pages += 1
        if body and b"html" in (meta.get("ctype") or "").encode():
            pl, dl = links(meta.get("final") or url, body)
            for u in pl:
                if u not in seen and same_site(host, urlsplit(u).hostname):
                    q.append(u)
            for d in dl:
                if d not in seen_docs and same_site(host, urlsplit(d).hostname) and docs < max_docs:
                    seen_docs.add(d)
                    dmeta, dbody = await _get(session, d, proxy)
                    await sink.send(d, dmeta["status"], dmeta.get("final"), dbody, dmeta.get("ctype"), linked_from=[url])
                    docs += 1
        await asyncio.sleep(gap)
    complete = not q
    if checkpoint:
        await checkpoint(snapshot(), complete)
    return {"host": host, "pages": pages, "documents": docs, "complete": complete, "queued": len(q)}


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
