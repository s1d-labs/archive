# archive.s1d.me — a structured, versioned archive of the Indian republic's public data

**Status:** plan, not yet building. Nothing is created or crawled until explicitly approved, phase by phase.
**Decided inputs (owner):** capture runs on the GitHub runner pool (+ `geo="IN"` India servers for India-only hosts);
object storage is a **new B2 bucket** (master key available at `/etc/scrape-hub/b2-admin.env`); public home is **archive.s1d.me**.

---

## 0. What this is (and what it is not)

It is **not** a list of scrapers, and not a catalogue (we built that — `/planned-sources` — and a to-do list is inert).
It is **one archive**: a single, growing, content-addressed store of everything the Indian state publishes, captured with
provenance, made searchable, then progressively structured and cross-linked. You turn it on once; it fills and deepens
forever. Every improvement enriches the *same* corpus instead of spawning a new project.

**The two things it has that no live government site has:**
- **Time** — what each page/record/document said *when*, every version kept (gov data is edited and deleted silently).
- **Cross-linking** — the same company / official / place / rupee traced across every source.

**The founding principle — separate CAPTURE from UNDERSTANDING:**
- *Capture* is generic and universal (same code for every host): fetch → snapshot with timestamp+hash → store raw +
  provenance. No per-source code. Can run at full scale immediately.
- *Understanding* is incremental and endless (per-source): parse records, OCR documents, resolve entities. It layers
  onto the already-captured archive and never blocks capture. The raw is safe and full-text-searchable from capture.

This is why it is "one thing that keeps expanding": adding a **source** = a queue entry; adding a **parser** = a plugin
that enriches rows already captured; adding an **entity type** = a graph extension over text already indexed; adding
**freshness** = re-running the same crawl. Every axis of growth is "more of the same system."

---

## 1. Architecture (the substrate everything hangs off)

```
                    ┌─────────────────────────────────────────────┐
  GitHub pool ───▶  │  CAPTURE WORKERS (archive pool family)        │
  geo=IN pool ───▶  │  fetch page + documents, polite, provenance   │
                    └───────────────┬───────────────┬──────────────┘
                                    │ raw bytes      │ capture metadata
                                    ▼                ▼
                       ┌────────────────────┐   ┌──────────────────────┐
                       │  B2 OBJECT STORE    │   │  ARCHIVE INDEX (PG)   │
                       │  content-addressed  │◀──│  captures, documents, │
                       │  raw/<sha256>       │   │  records, entities    │
                       └─────────┬──────────┘   └──────────┬───────────┘
                                 │                          │
                                 │        ┌─────────────────┼───────────────────┐
                                 ▼        ▼                 ▼                   ▼
                          ENRICHERS   FULL-TEXT SEARCH   DATASETS/API      ENTITY GRAPH
                          (parsers,   (archive.s1d.me)   (CSV/Parquet)     (investigations)
                           OCR)
```

**Components:**
1. **Object store (B2)** — every captured page and document, keyed by content hash (`raw/<sha256[:2]>/<sha256>`),
   immutable, deduplicated by content. One blob can be referenced by many captures (same PDF linked from many pages).
2. **Archive index (Postgres on dev-vps-2, DB `archive`)** — the spine. Append-only capture log + derived tables.
3. **Capture workers** — a **scrape-hub pool family** (`archive`). Reachable hosts on GitHub runners; India-only on
   `geo="IN"` servers. Workers fetch, upload raw to B2 (hub-signed, per the existing storage model), and POST capture
   metadata to the archive index API. Workers never hold the corpus; they stream to B2.
4. **Full-text search** — indexes captured text; powers archive.s1d.me.
5. **Enrichers** — per-source parsers, OCR, entity resolution. Read blobs + index, write records/entities. Offline,
   never on the capture path.
6. **Serving** — archive.s1d.me (search, item view with provenance, downloadable datasets, API) + investigation tooling.

**Why it reuses what exists:** capture *is* a pool family (the pool already does polite, geo-aware, hub-signed,
bucket-writing crawls). The map of what to capture *is* `data/survey/datarich.json`. The per-source parser pattern *is*
`gemparse`. The monitoring/provenance-of-availability *is* india-status. We are wiring existing organs into one body.

---

## 2. The data model (decide this first; everything depends on it)

Core tables in Postgres DB `archive` (sketch — exact columns settle in Phase 0):

- **`blob`** — one row per distinct content hash. `sha256` (PK), `size`, `content_type`, `stored_at`, `b2_key`,
  `text_sha` (hash of extracted text, null until parsed). The deduped physical layer.
- **`capture`** — one row per fetch (append-only, immutable). `id`, `url`, `host`, `fetched_at`, `vantage`
  (pool/geo=IN), `http_status`, `final_url`, `content_sha` → blob, `headers`, `robots_ok`, `run_id`. This is the
  provenance log: "this URL returned this content at this time from this vantage." Re-crawls append new rows; identical
  content reuses the blob.
- **`document`** — captured non-HTML files (PDF/xls/csv/…): `content_sha` → blob, `first_seen`, `source_captures`
  (which pages linked it), `text_sha`, `ocr` (bool), `pages`. (A view over blob+capture, materialized for convenience.)
- **`record`** — structured rows parsed from captures (filled by enrichers, per family): `id`, `source_capture`,
  `host`, `kind` (tender/notice/judgment/scheme/…), `type`, `fields` (jsonb), `dates`, `doc_shas` (array),
  `personal_data` (bool), `visibility` (public/gated). One normalized shape across all sources.
- **`entity`** + **`entity_mention`** + **`entity_link`** — resolved companies/people/places/money and their
  occurrences in records (filled in Phase 5). `entity` (id, type, canonical_name, ids jsonb {CIN, PAN, LGD, …});
  `entity_mention` (entity_id, record_id, raw_name, confidence); `entity_link` (entity→entity, relation, evidence).
- **`snapshot_diff`** — change-detection output (Phase 6): a capture vs the previous capture of the same URL.

**Invariants:** captures are never mutated or deleted; blobs are never overwritten; every record points back to the
capture (and thus blob + timestamp) it came from — so anything served is independently verifiable.

---

## 3. Phases

Each phase lists **goal · what we build · how it runs · storage · acceptance criteria · risks**. Phases 0–2 stand up the
foundation and a usable archive; 3 scales it; 4–6 deepen it. 4–6 run continuously once started and never "finish."

### Phase 0 — Foundation (no crawling)
- **Goal:** the spine exists and is proven end-to-end on hand-fed data.
- **Build:** repo (`s1d-labs/archive`, code public, data private); the B2 bucket + a hub-signed write key + a read key;
  Postgres DB `archive` with the Phase-2 data model (blob/capture/document); a content-addressed store library
  (put/get by sha, B2-backed); the **archive index API** (ingest endpoint: accept capture metadata + blob ref,
  write rows, dedup); fixtures + tests. Reserve archive.s1d.me (DNS/tunnel wired in Phase 2).
- **How it runs:** locally on dev-vps-2; the index API as a sandboxed systemd unit (own user), listening on loopback.
- **Storage:** bucket created; near-empty.
- **Acceptance:** feed one real page + one PDF by hand → blob in B2, capture+document rows in PG, retrievable by hash,
  re-ingest is a no-op (dedup works). Tests green.
- **Risks:** data-model churn later → keep `fields`/`ids` as jsonb so schema can evolve without migrations everywhere.

### Phase 1 — Generic capture pipeline (pilot)
- **Goal:** the firehose works on the real pool, politely, with provenance — proven on a pilot before scale.
- **Build:** the capture worker (reuse `core_fetch`/`core_crawl`): per host, walk same-site pages, download documents,
  upload raw to B2, POST capture metadata; idempotent + resumable (skip unchanged by hash/ETag); robots honoured;
  per-target politeness (hub limits). Package as the `archive` pool family (`setup_family.py`, `scrape.json`), one
  project per host-batch.
- **How it runs:** GitHub runner pool; a pilot project of ~50 reachable hosts across families (S3WaaS, a bespoke
  system, a WordPress, a doc-heavy site).
- **Storage:** a few GB.
- **Acceptance:** pilot hosts fully captured; re-running captures ~nothing new (dedup/ETag); blobs deduped across pages;
  capture rows carry correct provenance; no robots violations; politeness within monitor's own rate.
- **Risks:** runaway retries (the waste lesson) → hard per-item retry caps + park; a bad site stalling a batch → budget
  per host. Getting the pool IPs flagged → start slow, watch error rates, back off per target.

### Phase 2 — Search + the public face (make it cool early)
- **Goal:** the archive is usable and visibly real within its first weeks.
- **Build:** text extraction on ingest (HTML → text; PDF → text, OCR deferred to Phase 4); a full-text index
  (**start: Postgres FTS**; plan a migration path to a dedicated engine — Meilisearch/OpenSearch — when volume
  demands); archive.s1d.me: search UI, result → item view showing the captured content, its provenance (URL, time,
  hash, vantage) and **"download the original"** (signed B2 link); a simple stats page (what's captured). Wire the
  tunnel + DNS (like the other *.s1d.me) with its own login where needed; capture-all/publish-gated enforced here.
- **How it runs:** served by a new sandboxed unit on dev-vps-2 behind the tunnel.
- **Storage:** unchanged; + the FTS index.
- **Acceptance:** search real captured content; every hit has working provenance + original download; gated items hidden
  from public view.
- **Risks:** Postgres FTS ceiling at tens of millions of docs → the migration path is designed now, not later.

### Phase 3 — Scale capture to everything
- **Goal:** the full host list captured, and kept being captured.
- **Build:** rollout ranked by value (datarich): reachable via GitHub, India-only via `geo="IN"`; **pages first, then
  the documents pass** (the ~1 TB); storage tiering (hot index + B2 standard; cold rarely-touched); capacity/άcost
  dashboards (reuse the scrape-hub overview); the capture-all/publish-gated policy applied per source.
- **How it runs:** continuous pool dispatch; rate ceilings per target; weighted to off-peak for sensitive systems.
- **Storage:** grows to ~1 TB+ (documents dominate); cold tier for the long tail.
- **Acceptance:** every reachable + India host captured at least once; documents pass complete for high-value sources;
  cost within the agreed budget; **no bans** (the single hardest success criterion).
- **Risks:** cost blow-up (documents) → budget alerts + the "records-first, documents-selective" lever; bans → rate
  discipline, per-IP spread, the Stop button; politeness at scale is existential.

### Phase 4 — Structured records (the datasets)
- **Goal:** the corpus becomes queryable data, not just text.
- **Build:** per-family parsers (reuse `core_scrape`; `gemparse` as the template for hard PDF/result sources) that read
  captured blobs and emit normalized `record` rows; OCR for scanned PDFs; **downloadable datasets** (CSV/Parquet per
  sector/state/scheme) + a records API on archive.s1d.me. Parsers are plugins; adding one enriches the existing corpus.
- **How it runs:** offline enricher jobs on dev-vps-2 (CPU-bound, no crawling), re-runnable as parsers improve.
- **Storage:** records are small (tens of GB); + parsed-text store.
- **Acceptance:** records for the top families; dataset counts reconcile with the census estimates; a parser upgrade
  re-enriches without re-capturing.
- **Risks:** parser rot across template variants → fixtures + known-answer tests per family (the gemparse discipline).

### Phase 5 — Entity graph + investigations
- **Goal:** cross-source questions become single queries — the investigative payoff.
- **Build:** entity resolution (company via CIN/name, person via director/candidate rolls, place via LGD, money via
  tender→award→contract→PFMS); `entity`/`mention`/`link` tables; **entity pages** (everything the archive knows about a
  company/person/place); investigation tooling (saved cross-source queries, joins, export). Generalizes the
  GeM/woodpecker work to all sources.
- **How it runs:** offline resolution jobs; served on archive.s1d.me (gated/credentialed for sensitive joins).
- **Acceptance:** a real cross-source query end-to-end (e.g. "companies that won state PWD tenders *and* appear as
  electoral-bond donors *and* have a director who is a sitting legislator").
- **Risks:** false entity merges (messy names — "S S ENTERPRISES" ×2) → confidence scores, human review for high-stakes
  links, never auto-assert identity as fact.

### Phase 6 — Freshness & change detection
- **Goal:** the archive stays current and surfaces what changed.
- **Build:** scheduled re-crawl cadence by source volatility (daily listings, monthly static); `snapshot_diff`
  computation; alerts (edited award values, deleted notices, new beneficiary lists, fresh judgments). Extends the
  india-status monitor from uptime to content.
- **How it runs:** scheduled pool projects (deltas, not full re-scrapes) + offline diffing.
- **Acceptance:** an edit/deletion on a watched source is detected and surfaced with before/after.
- **Risks:** re-crawl cost → delta-only (first listing page, diff); alert noise → thresholds.

---

## 4. Cross-cutting (true for every phase)

- **Provenance & immutability:** content-addressed blobs, append-only captures, timestamps + hashes on everything.
  Anything served links to its capture → blob → time. This is what makes the archive evidence, not a scrape.
- **Politeness & anti-ban (existential):** robots honoured; honest UA with a contact/URL; per-target rate ceilings
  (hub default → family → project); start slow, watch error/latency, back off; spread across pool + India IPs; the Stop
  button ready. One careless run that gets NIC/GeM-class systems to block the pool sets everything back.
- **Governance & privacy — capture-all, publish-gated:** everything public is *captured* into the private archive;
  what is *published* (search, datasets, API) is gated per dataset. Personal data (voter rolls, beneficiary names,
  exam marks — already flagged in datarich) is archived for integrity work but never bulk-published. A per-source
  `visibility` policy decided before that source goes public. This is ethical *and* legal *and* keeps the project
  defensible.
- **Legal/ethical:** public data, but respect each source's terms for the restrictive few (GeM-style); be a transparent,
  good-citizen crawler; a clear "who we are / why / how to contact" page on archive.s1d.me.
- **Cost model:** B2 ≈ $6/TB/month storage + egress on download; documents are the cost driver (~1 TB). GitHub pool
  minutes are free on public repos; India servers are owned. Budget alerts; cold tier for the long tail; records-first
  keeps early cost tiny.
- **Security:** archive index + serving as sandboxed units; B2 keys hub-signed (write) / scoped (read), never in
  runners; gated content behind auth; the capture pipeline cannot be used to reach internal services.
- **Sustainability & ops:** cheap at rest, incremental, documented (numbered setup scripts, a runbook); not
  bus-factor-one; dashboards for health/cost; everything re-runnable and resumable.

---

## 5. Concrete infrastructure decisions (to confirm in Phase 0)

- **Repo:** `s1d-labs/archive` — capture + enricher code public; the index/data private (B2 + Postgres).
- **B2 bucket:** a new private bucket (e.g. `s1d-archive`), SSE on; a hub-signed **write** key for capture, a scoped
  **read** key for serving/enrichers; created from the master key in `/etc/scrape-hub/b2-admin.env`. **Hold until "build".**
- **Postgres:** new DB `archive` on dev-vps-2 (the work box), own role; analyst read-only grant optional.
- **Serving:** archive.s1d.me via the dev-vps-2 tunnel (`86-tunnel-add.sh` + CNAME, with owner OK), a new sandboxed
  unit, its own login for gated views; add to statusd + boot-watchdog.
- **Pool:** `archive` family on scrape-hub; reachable projects default, India-only projects `geo="IN"`; capture to the
  archive bucket (not a member bucket).
- **Search engine:** start Postgres FTS; decision point for Meilisearch/OpenSearch at Phase 2→3 by volume.

---

## 6. Decisions — RESOLVED (owner, 2026-11-04)

1. **Search engine:** **Postgres FTS** to start (migration path kept open for later).
2. **Documents pass:** **ALL documents, every source, no size limit** — 1 TB or 20 TB or more, does not matter. Capture
   everything. (So Phase 3 has no "high-value-first" document lever; it captures the lot. Cold-tier for scale.)
3. **Governance:** **capture-all, publish-NONE for now — everything team-gated.** The entire archive (search, items,
   datasets, API) sits behind team auth (id.s1d.me OIDC, like home.s1d.me / metabase). Nothing is public yet. This
   *simplifies* serving (no per-dataset public/gated split now) and defers the personal-data publication policy until
   there is ever a decision to publish. We still record the `personal_data` / `visibility` flags on records so a future
   public layer is possible without re-work; for now `visibility` is irrelevant because nothing is served publicly.
4. **Re-crawl cadence (Phase 6):** decide after the first full crawl (we'll have real volatility data then).
5. **Repo visibility:** `s1d-labs/archive` — code public, data private.
6. **Budget:** **no ceiling.** Cost is accepted. We still build cost/size dashboards so it is always *visible* (seen, not
   capped), and use a cold tier for the rarely-touched long tail to keep it sane.

### What these change in the plan
- **Phase 2** serving is **entirely team-gated** (one auth wall, no public view) — faster to build, no gating matrix.
- **Phase 3** captures **all** documents; the only levers left are order (value-ranked) and politeness, not scope.
- **Scale assumption revised upward:** plan for **tens of TB**, not ~1 TB. B2 layout, cold tiering, and the index must
  assume a very large object count (hundreds of millions of blobs possible over re-crawls). Content-addressing + dedup
  and an append-only index are built for this; just sizing the expectation correctly.

---

## 7. Sequencing & what unblocks what

```
Phase 0 (foundation) ──▶ Phase 1 (capture pilot) ──▶ Phase 2 (search + archive.s1d.me)
                                                          │
                                                          ▼
                                              Phase 3 (scale capture) ──▶ Phase 4 (records/datasets)
                                                          │                        │
                                                          └────────────┬───────────┘
                                                                       ▼
                                                           Phase 5 (entity graph)  Phase 6 (freshness)
```
0→1→2 is the critical path to "a real, searchable archive." 3 runs continuously thereafter. 4/5/6 accrete forever and
can overlap. Nothing after Phase 1 blocks capture; capture never waits on understanding.

**Definition of "started big":** at the end of Phase 2 there is a live archive.s1d.me searching real captured
government pages and documents with full provenance — one system, visibly filling, every future source and parser just
plugging into it.
