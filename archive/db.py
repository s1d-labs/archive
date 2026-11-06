"""Postgres index for the archive. Thin wrapper around psycopg; schema in schema.sql."""
import contextlib
from pathlib import Path

import psycopg

from . import config


@contextlib.contextmanager
def conn(statement_timeout=None):
    """statement_timeout (e.g. '80s') caps every statement on this connection; the ingest API passes one so a write can
    never outlive the worker's 90 s request (a GIN cleanup once held a blob row lock for 85 minutes)."""
    opts = f"-c statement_timeout={statement_timeout}" if statement_timeout else ""
    with psycopg.connect(config.DB_URL, autocommit=False, row_factory=psycopg.rows.dict_row, options=opts) as c:
        yield c


def init():
    sql = (Path(__file__).parent / "schema.sql").read_text()
    with conn() as c:
        c.execute(sql)
        c.commit()
