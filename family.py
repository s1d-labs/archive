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
