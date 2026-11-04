"""Archive config: where the index DB and the B2 store live. Secrets come from /etc/archive/*.env, never the repo."""
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ETC = Path(os.environ.get("ARCHIVE_ETC", "/etc/archive"))
DB_URL = os.environ.get("ARCHIVE_DB_URL", "postgresql:///archive")


def read_env(path):
    """Parse KEY=VALUE lines; values are never logged."""
    out = {}
    try:
        for line in Path(path).read_text().splitlines():
            m = re.match(r"\s*([A-Z0-9_]+)=(.*)", line)
            if m:
                out[m.group(1)] = m.group(2).strip()
    except OSError:
        pass
    return out


def store_env(write=False):
    """B2 S3 creds for the archive bucket: the read key by default, the write key when write=True."""
    f = ETC / ("b2-write.env" if write else "b2-read.env")
    e = read_env(f)
    if not e.get("AWS_ACCESS_KEY_ID"):
        raise SystemExit(f"no B2 creds at {f}: run ~/setup/110-archive.sh")
    return e
