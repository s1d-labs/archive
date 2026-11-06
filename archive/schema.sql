-- archive.s1d.me — the structured, versioned archive of the Indian republic's public data.
-- Capture is append-only and immutable; blobs are content-addressed and deduplicated; everything served points back
-- to the capture (and thus the blob + timestamp) it came from, so any served item is independently verifiable.

-- The physical layer: one row per distinct content hash, stored once in B2 however many captures reference it.
CREATE TABLE IF NOT EXISTS blob (
    sha256        TEXT PRIMARY KEY,                 -- hex sha256 of the raw bytes
    size          BIGINT NOT NULL,
    content_type  TEXT,                             -- as served (or sniffed)
    b2_key        TEXT NOT NULL,                    -- raw/<sha[:2]>/<sha>
    stored_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    text_sha      TEXT,                             -- sha of extracted text (null until parsed); text itself in blob too
    text_chars    INT,                              -- length of extracted text, null until parsed
    ocr           BOOLEAN NOT NULL DEFAULT false,   -- text came from OCR (scanned PDF)
    tsv           TSVECTOR                          -- full-text index of the extracted text ('simple' config; multilingual)
);
CREATE INDEX IF NOT EXISTS blob_tsv_gin ON blob USING GIN (tsv);
-- Direct GIN updates: with fastupdate on, a busy ingest let the pending-list cleanup run for an hour and block inserts.
ALTER INDEX blob_tsv_gin SET (fastupdate = off);

-- Search the captured corpus: every URL where a matching blob appeared, newest first.
--   SELECT url, host, fetched_at FROM search WHERE tsv @@ websearch_to_tsquery('simple', 'land acquisition') ORDER BY fetched_at DESC;
CREATE OR REPLACE VIEW search AS
  SELECT c.url, c.host, c.fetched_at, c.is_document, b.content_type, b.text_chars, b.sha256 AS content_sha, b.tsv
    FROM capture c JOIN blob b ON c.content_sha = b.sha256
   WHERE b.tsv IS NOT NULL;

-- The provenance log: one row per fetch, never mutated or deleted. Re-crawls append new rows; identical content reuses
-- the blob, so a row with the same (url, content_sha) as the previous one means "unchanged at this time".
CREATE TABLE IF NOT EXISTS capture (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url           TEXT NOT NULL,
    host          TEXT NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL,
    vantage       TEXT NOT NULL,                    -- 'pool' (GitHub) | 'IN' (India servers) | 'dev-vps-2'
    http_status   INT,
    final_url     TEXT,                             -- after redirects
    content_sha   TEXT REFERENCES blob(sha256),     -- null if the fetch produced no body (error/refused)
    content_type  TEXT,
    is_document   BOOLEAN NOT NULL DEFAULT false,   -- a downloaded file (PDF/xls/csv/…) vs an HTML page
    headers       JSONB,
    robots_ok     BOOLEAN,
    run_id        TEXT,                             -- the capture run/batch that produced it
    err           TEXT
);
CREATE INDEX IF NOT EXISTS capture_url_time ON capture (url, fetched_at DESC);
CREATE INDEX IF NOT EXISTS capture_host ON capture (host);
CREATE INDEX IF NOT EXISTS capture_sha ON capture (content_sha);
CREATE INDEX IF NOT EXISTS capture_run ON capture (run_id);

-- Convenience layer over blob+capture for downloaded files: one row per distinct document, with the pages that linked it.
CREATE TABLE IF NOT EXISTS document (
    content_sha   TEXT PRIMARY KEY REFERENCES blob(sha256),
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ext           TEXT,                             -- pdf/xlsx/csv/…
    linked_from   JSONB NOT NULL DEFAULT '[]',      -- sample of capture ids / page urls that referenced it
    ok            BOOLEAN                           -- integrity check (archive.validate): null=unchecked, true=complete, false=truncated/corrupt
);

-- The structured layer: normalized rows parsed from captures by per-source enrichers (Phase 4). One shape for all sources.
CREATE TABLE IF NOT EXISTS record (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_capture BIGINT REFERENCES capture(id),
    host          TEXT NOT NULL,
    family        TEXT,
    kind          TEXT,                             -- tender/notice/judgment/scheme/result/document/…
    type          TEXT,                             -- the page type it came from
    title         TEXT,
    fields        JSONB NOT NULL DEFAULT '{}',
    dates         JSONB NOT NULL DEFAULT '{}',
    doc_shas      TEXT[] NOT NULL DEFAULT '{}',
    personal_data BOOLEAN NOT NULL DEFAULT false,
    visibility    TEXT NOT NULL DEFAULT 'team',     -- 'team' (everything, for now) | 'public' (a future, gated opt-in)
    parsed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    tsv           TSVECTOR                          -- full-text (Phase 2); also fed by blob text for page-level search
);
CREATE INDEX IF NOT EXISTS record_host ON record (host);
CREATE INDEX IF NOT EXISTS record_kind ON record (kind);
CREATE INDEX IF NOT EXISTS record_tsv ON record USING GIN (tsv);

-- The entity layer (Phase 5): companies/people/places/money resolved across sources, their mentions and links.
CREATE TABLE IF NOT EXISTS entity (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    etype         TEXT NOT NULL,                    -- company/person/place/scheme/…
    canonical_name TEXT NOT NULL,
    ids           JSONB NOT NULL DEFAULT '{}'       -- {CIN, PAN, LGD, …}
);
CREATE TABLE IF NOT EXISTS entity_mention (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id     BIGINT REFERENCES entity(id),
    record_id     BIGINT REFERENCES record(id),
    raw_name      TEXT,
    confidence    REAL
);
CREATE TABLE IF NOT EXISTS entity_link (
    src           BIGINT REFERENCES entity(id),
    dst           BIGINT REFERENCES entity(id),
    relation      TEXT,
    evidence      JSONB
);

-- Change detection (Phase 6): a capture compared with the previous capture of the same URL.
CREATE TABLE IF NOT EXISTS snapshot_diff (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url           TEXT NOT NULL,
    prev_capture  BIGINT REFERENCES capture(id),
    curr_capture  BIGINT REFERENCES capture(id),
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind          TEXT,                             -- edited/added/removed
    summary       TEXT
);

-- Exact identifier / tender references found in a blob's text (archive.refs); the entity graph reads these.
CREATE TABLE IF NOT EXISTS doc_ref (
    sha256  TEXT NOT NULL,
    kind    TEXT NOT NULL,                          -- cin | gstin | pan | din | llpin | tender | gem_bid
    value   TEXT NOT NULL,
    n       INT NOT NULL DEFAULT 1,                 -- occurrences in that text
    PRIMARY KEY (sha256, kind, value)
);
CREATE INDEX IF NOT EXISTS doc_ref_value ON doc_ref (kind, value);
CREATE TABLE IF NOT EXISTS doc_ref_done (sha256 TEXT PRIMARY KEY, at TIMESTAMPTZ NOT NULL DEFAULT now());
