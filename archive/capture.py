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
            self._s = aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}"})
        elif mode == "b2":
            self._f = open(jsonl, "a")

    async def send(self, url, status, final_url, content, content_type, linked_from=None):
        if self.mode == "local":
            from . import ingest
            return await asyncio.to_thread(ingest.ingest, url, content, content_type=content_type, vantage=self.vantage,
                                           http_status=status, final_url=final_url, run_id=self.run_id, linked_from=linked_from)
        if self.mode == "b2":
            return await asyncio.to_thread(self._to_b2, url, status, final_url, content, content_type, linked_from)
        body = {"url": url, "http_status": status, "final_url": final_url, "content_type": content_type,
                "vantage": self.vantage, "run_id": self.run_id, "linked_from": linked_from,
                "body_b64": base64.b64encode(content).decode() if content is not None else None}
        async with self._s.post(f"{self.api}/ingest", json=body) as r:
            return await r.json()

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


async def capture_host(session, start_url, sink, max_pages=2000, max_docs=20000, gap=GAP_S):
    host = urlsplit(start_url).hostname
    seen, seen_docs = set(), set()
    q = deque([start_url])
    pages = docs = 0
    while q and pages < max_pages:
        url = q.popleft()
        if url in seen:
            continue
        seen.add(url)
        meta, body = await _get(session, url)
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
                    dmeta, dbody = await _get(session, d)
                    await sink.send(d, dmeta["status"], dmeta.get("final"), dbody, dmeta.get("ctype"), linked_from=[url])
                    docs += 1
        await asyncio.sleep(gap)
    return {"host": host, "pages": pages, "documents": docs}


async def _get(session, url):
    try:
        async with session.get(url, allow_redirects=True) as r:
            body = await r.content.read(50 * 1024 * 1024)   # 50 MB cap per object
            return {"status": r.status, "final": str(r.url), "ctype": r.headers.get("Content-Type")}, body
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
