# archive.s1d.me

A structured, versioned archive of the Indian republic's publicly accessible data.

**Principle:** separate **capture** (generic, universal, runs at full scale now) from **understanding** (per-source
parsing, OCR, entity resolution — layered on top, never blocks capture). One growing corpus, not many scrapers.

- `archive/store.py` — content-addressed blob store on B2 (sha256 → one object, deduped).
- `archive/ingest.py` — the one door: store raw bytes once + extract text + append an immutable `capture` row.
- `archive/db.py`, `archive/schema.sql` — the Postgres index (blob / capture / document / record / entity / snapshot_diff).
- `archive/api.py` — the HTTP ingest API the Phase-1 pool workers POST to.
- `archive/capture.py` — (Phase 1) the generic capture worker (fetch pages + documents, provenance).
- `PLAN.md` — the full phase plan. Deployed by `~/setup/113-archive.sh`.

Capture runs on the scrape-hub pool (GitHub runners + `geo="IN"` India servers). Storage: B2 bucket `s1d-archive`.
Everything is team-gated (nothing public yet); personal-data flags recorded for a possible future public layer.
