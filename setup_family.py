"""Put the archive capture projects in the pool and seed their queues (one item per host). Run by the Setup workflow
(GitHub Actions; the hub trusts s1d-labs/archive via its OIDC token). Safe to re-run: existing queue items are kept.
This is the Phase-1 turn-on — it does not run until the archive ingest endpoint is reachable (archive.s1d.me) and the
project env carries ARCHIVE_API + ARCHIVE_INGEST_TOKEN. See PLAN.md."""
from scrape_hub_client import register_project, seed_queue, set_pool

from family import FAMILY, PROJECTS, targets

for p in PROJECTS:
    work = targets(p["targets"])
    register_project(p["name"], title=p["title"], unit_label="host", record_label="captures",
                     expected_units=len(work), expected_records=len(work))
    set_pool(p["name"], FAMILY, "capture", target=p["name"], env={"ARCHIVE_VANTAGE": p["geo"] or "pool"},
             geo=p["geo"], max_workers=p["max_workers"])
    print(p["name"], "queue:", seed_queue(p["name"], work))
