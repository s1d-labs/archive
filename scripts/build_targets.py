"""Generate the value-ranked capture target lists (targets/reachable.txt, targets/india.txt) from the survey's datarich
map. Run on dev-vps-2 where data/survey/datarich.json lives; commit the output so the family repo carries its targets.
  python scripts/build_targets.py [path-to-datarich.json]
"""
import json
import sys
from pathlib import Path

src = sys.argv[1] if len(sys.argv) > 1 else "/home/sarthak/projects/indian-status/data/survey/datarich.json"
H = json.load(open(src))["hosts"]
vrank = {"high": 0, "medium": 1, "low": 2, None: 3}
crank = {"system": 0, "rich": 1, "service": 2, "template": 3, "ordinary": 4}
out = Path(__file__).resolve().parent.parent / "targets"
out.mkdir(exist_ok=True)


def key(r):
    return (crank.get(r["class"], 5), vrank.get(r.get("value")), r.get("tier") or 9, r["domain"])


for name, reaches in (("reachable", ("ok", "error")), ("india", ("unreachable-abroad", "down"))):
    rows = sorted((r for r in H if r["reach"] in reaches), key=key)
    with open(out / f"{name}.txt", "w") as f:
        f.write(f"# archive capture targets: {name} — value-ranked ({len(rows)} hosts), one start URL per line\n")
        for r in rows:
            f.write((r.get("url") or ("https://" + r["domain"])) + "\n")
    print(f"{name}: {len(rows)} hosts")
