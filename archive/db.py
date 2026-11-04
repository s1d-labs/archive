"""Postgres index for the archive. Thin wrapper around psycopg; schema in schema.sql."""
import contextlib
from pathlib import Path

import psycopg

from . import config


@contextlib.contextmanager
def conn():
    with psycopg.connect(config.DB_URL, autocommit=False, row_factory=psycopg.rows.dict_row) as c:
        yield c


def init():
    sql = (Path(__file__).parent / "schema.sql").read_text()
    with conn() as c:
        c.execute(sql)
        c.commit()
