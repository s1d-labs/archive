"""Identifier and tender references found in archived documents, for the entity graph. Reads each blob's extracted text
from B2 (thread pool, like fts_backfill), finds exact references and stores them in doc_ref: one row per blob, kind and
value, with how often it occurs. Exact patterns only, so a match is the identifier itself, never a guessed name:
  cin      U74140DL2007PTC158804             gstin   07AQHPS2881C1ZQ (its PAN joins every state registration)
  pan      AABCU9603R (non-individuals only: the 4th letter is C, F, H, A, T, B, L, J or G; a person's PAN is skipped)
  din      only beside the word DIN          llpin   only beside the word LLPIN (the bare pattern is too common)
  tender   GePNIC tender id 2024_PWD_264464_1    gem_bid  GeM bid number GEM/2023/B/3456789
Idempotent + resumable: doc_ref_done marks each blob read. Run: python -m archive.refs [workers]
"""
import re
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from . import db, store

PATTERNS = {
    "cin": re.compile(r"\b([LU]\d{5}[A-Z]{2}\d{4}(?:PLC|PTC|GOI|SGC|NPL|ULL|ULT|OPC|FLC|FTC|GAP|GAT)\d{6})\b"),
    "gstin": re.compile(r"\b(\d{2}[A-Z]{5}\d{4}[A-Z][0-9A-Z]Z[0-9A-Z])\b"),
    "pan": re.compile(r"\b([A-Z]{3}[CFHATBLJG][A-Z]\d{4}[A-Z])\b"),
    "din": re.compile(r"\bDIN\W{0,6}(\d{8})\b"),
    "llpin": re.compile(r"\bLLPIN\W{0,6}([A-Z]{3}-\d{4})\b"),
    "tender": re.compile(r"\b(20\d\d_[A-Z0-9]{2,15}_\d{3,9}_\d{1,3})\b"),
    "gem_bid": re.compile(r"\b(GEM/20\d\d/[BR]/\d{4,9})\b"),
}
CHUNK = 500                      # texts in flight at once
CAP = 5000                       # refs kept per blob and kind (a contractor list PDF can hold thousands of GSTINs)


def refs(text: str) -> list[tuple[str, str, int]]:
    up = text.upper()
    out = []
    for kind, pat in PATTERNS.items():
        found = Counter(pat.findall(up))
        if kind == "pan":                             # a GSTIN's own PAN is already counted through the GSTIN
            inside = {g[2:12] for g in PATTERNS["gstin"].findall(up)}
            found = Counter({p: n for p, n in found.items() if p not in inside})
        out += [(kind, v, n) for v, n in found.most_common(CAP)]
    return out


def main(workers=12):
    store._s3()
    with db.conn() as c:
        shas = [r["sha256"] for r in c.execute(
            """SELECT b.sha256 FROM blob b WHERE b.text_chars > 0
               AND NOT EXISTS (SELECT 1 FROM doc_ref_done d WHERE d.sha256 = b.sha256)""").fetchall()]
    print(f"{len(shas)} blobs to read with {workers} readers", flush=True)

    def fetch(sha):
        try:
            return sha, store.get_text(sha)
        except Exception:  # noqa: BLE001  missing now; a later run tries again
            return sha, None

    seen = found = 0
    rows, done = [], []
    with db.conn() as c, ThreadPoolExecutor(workers) as ex:
        def flush():
            nonlocal rows, done
            with c.cursor() as cur:
                if rows:
                    cur.executemany("INSERT INTO doc_ref (sha256, kind, value, n) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)
                cur.executemany("INSERT INTO doc_ref_done (sha256) VALUES (%s) ON CONFLICT DO NOTHING", [(s,) for s in done])
            c.commit()
            rows, done = [], []

        # Bounded: ex.map queues every task at once, so readers ran ahead of the matcher and filled memory (3 GB in
        # two minutes). One chunk of texts in flight at a time keeps memory flat.
        for i in range(0, len(shas), CHUNK):
            for sha, txt in ex.map(fetch, shas[i:i + CHUNK]):
                seen += 1
                if txt is None:
                    continue
                r = refs(txt)
                found += len(r)
                rows += [(sha, k, v, n) for k, v, n in r]
                done.append(sha)
            flush()
            if seen % 10000 < CHUNK:
                print(f"{seen}/{len(shas)} refs={found}", flush=True)
    print(f"done: {seen} read, {found} refs", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 12)
