"""The archive capture family: two projects, one per vantage. Each queue item is one host (a start URL); a capture
worker walks that host and archives every page + document. Reachable hosts run on GitHub runners (geo=""); India-only
hosts run on the geo="IN" India servers (team-docs/15-india-servers.md). Targets are the value-ranked lists in targets/,
generated from the survey's datarich map (scripts/build_targets.py)."""
from pathlib import Path

FAMILY = "archive"
HERE = Path(__file__).resolve().parent

PROJECTS = [
    {"name": "archive-reachable", "title": "Archive capture — reachable hosts", "geo": "",
     "targets": "targets/reachable.txt", "max_workers": 20},
    {"name": "archive-india", "title": "Archive capture — India-only hosts", "geo": "IN",
     "targets": "targets/india.txt", "max_workers": 8},
]


def targets(path):
    """One queue item per host: {key: host, payload: {url}}. Capture walks the whole host from that start URL."""
    out = []
    for line in (HERE / path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        host = line.split("//", 1)[-1].split("/", 1)[0]
        out.append({"key": host, "payload": {"url": line}})
    return out

# The reference scan (task "refs", refs_task.py): one queue item per 3-hex-digit sha256 prefix (4,096 items), GitHub
# runners only. Registered and seeded from dev-vps-2, NOT by setup_family.py: its env carries the ingest token, and the
# Setup workflow's set_pool would replace a project's env (it would also drop the token from the capture projects).
REFS_PROJECT = {"name": "archive-refs", "title": "Archive: identifier and tender references", "task": "refs",
                "items": [{"key": f"{i:03x}", "payload": {"prefix": f"{i:03x}"}} for i in range(4096)]}
