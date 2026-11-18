#!/bin/bash
# Tag captures whose vantage label is wrong (table vantage_fix, view capture_provenance; schema.sql). Re-runnable: it
# recomputes every row. Run on dev-vps-3 as a sudoer: bash scripts/tag_vantage.sh
#   github-runner  : a GitHub runner job (hub runs.repository not server/...) labelled IN-proxy / IN-server. Before
#                    7917e5b (2026-11-18 21:04 IST) the task ignored the proxy, so the fetch came from outside India. A job
#                    keeps the code it started with, so the test is the job's start time, not the capture time.
#   india-server   : an India pool server job labelled IN-proxy (the fetch was from India, but not through a proxy).
#   unverified     : runs with no hub run row (archive-proxy-1, archive-proxy-smoke from dev-vps-2 on 2026-11-05).
set -euo pipefail
FIX='2026-11-18 21:04:00+05:30'
runs=$(sudo -n -u postgres psql -X -At -F $'\t' -d archive -c "SELECT run_id, vantage, count(*), max(fetched_at) < '$FIX'
  FROM capture WHERE vantage IN ('IN-proxy','IN-server') GROUP BY 1, 2")
src=$({ echo "CREATE TEMP TABLE r(run_id text, vantage text, n bigint, before bool); COPY r FROM STDIN;"; printf '%s\n' "$runs"; echo '\.'
  echo "COPY (SELECT r.run_id, r.vantage, r.n, CASE WHEN ru.id IS NULL THEN 'unverified'
               WHEN ru.repository LIKE 'server/%' THEN (CASE WHEN r.vantage='IN-server' THEN '' ELSE 'india-server' END)
               WHEN ru.started_at < '$FIX' THEN 'github-runner' ELSE '' END
         FROM r LEFT JOIN runs ru ON ru.id = (CASE WHEN r.run_id ~ '^pool-[0-9]+$' THEN substr(r.run_id, 6)::bigint END)) TO STDOUT;"
} | sudo -n -u postgres psql -X -q -At -d scrape_hub | awk -F'\t' '$4 != ""')
{ echo "BEGIN; CREATE TEMP TABLE t(run_id text, labelled text, captures bigint, actual text); COPY t FROM STDIN;"
  printf '%s\n' "$src"; echo '\.'
  cat <<'SQL'
INSERT INTO vantage_fix (run_id, labelled, actual, captures, note)
SELECT run_id, labelled, actual, captures, CASE actual
  WHEN 'github-runner' THEN 'GitHub runner job: fetched from outside India; the task ignored the hub proxy before 7917e5b'
  WHEN 'india-server' THEN 'India pool server job: fetched from India, not through a proxy'
  ELSE 'no hub run row: source not verified' END FROM t
ON CONFLICT (run_id) DO UPDATE SET labelled=EXCLUDED.labelled, actual=EXCLUDED.actual, captures=EXCLUDED.captures,
  note=EXCLUDED.note, tagged_at=now();
SELECT actual, labelled, count(*) AS runs, sum(captures) AS captures FROM vantage_fix GROUP BY 1, 2 ORDER BY 1, 2;
COMMIT;
SQL
} | sudo -n -u postgres psql -X -At -d archive -v ON_ERROR_STOP=1
