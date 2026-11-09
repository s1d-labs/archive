#!/bin/bash
# Snapshot every scrape-hub project's records into the archive (archive.import_hub), smallest project first, one at a time.
# Run as sarthak (sudo -n): psql streams as postgres, the importer runs as the archive user. Log: one JSON line per project.
set -uo pipefail
E=$(sudo -n grep -v '^#' /etc/archive/env | grep -v '^$' | xargs)
sudo -n -u postgres psql -X -At -F $'\t' -d scrape_hub -c "
  SELECT p.name, CASE WHEN p.settings->>'target' LIKE '%.%' THEN p.settings->>'target'
                      ELSE coalesce(p.settings->>'family', split_part(p.name, '-', 1)) || '.scrape-hub' END
  FROM projects p JOIN record_counts rc ON rc.project = p.name
  WHERE p.name NOT LIKE 'archive-%' AND rc.n > 0 ORDER BY rc.n" |
while IFS=$'\t' read -r p t; do
  sudo -n -u postgres psql -X -q -d scrape_hub -c "COPY (SELECT record_id, scraped_at, data FROM records WHERE project = '$p' ORDER BY record_id) TO STDOUT" |
    sudo -n -u archive env PYTHONPATH=/opt/archive $E bash -c 'cd /opt/archive && .venv/bin/python -m archive.import_hub "$0" "$1"' "$p" "$t" \
    || echo "{\"project\": \"$p\", \"error\": true}"
done
