"""Shared helpers for init-scripts/<table>/seed_<table>.py scripts.

Not a table's own script - imported by each of them. Common logic: connect
to Oracle (host-side, via the container's exposed port), insert the table's
fixture row if missing, then generate more rows on top of whatever's already
there, varying only `recid` (PK) and one nominated date tag (spread across
an explicit --start-date/--end-date window, so daily/history window
filtering has real data - default window is the trailing 365 days ending
today if neither flag is given).
"""
from __future__ import annotations

import argparse
import copy
import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path

import oracledb
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(REPO_ROOT / ".env")

SCHEMA = os.environ.get("ORACLE_SCHEMA", "source_table")
SCHEMA_PASSWORD = os.environ.get("ORACLE_SCHEMA_PASSWORD", "source_table")
ORACLE_HOST = os.environ.get("ORACLE_HOST", "localhost")
ORACLE_PORT = os.environ.get("ORACLE_PORT", "1521")
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "XEPDB1")

DATE_SPREAD_DAYS = 365   # default window width when --start-date/--end-date are omitted
BATCH_SIZE = 500


def _date_arg(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"{s!r} is not YYYYMMDD") from None


def add_seed_args(parser: argparse.ArgumentParser, *, default_count: int, default_first_recid: int) -> None:
    """--count / --first-recid / --reset / --start-date / --end-date - shared by every seed_<table>.py."""
    parser.add_argument("--count", type=int, default=default_count,
                         help=f"rows to generate on top of what's already there (default {default_count})")
    parser.add_argument("--first-recid", type=int, default=default_first_recid,
                         help=f"start of the generated range on an empty/--reset table (default {default_first_recid})")
    parser.add_argument("--reset", action="store_true",
                         help="delete existing rows before seeding, restarting the generated range at --first-recid")
    parser.add_argument("--start-date", type=_date_arg, metavar="YYYYMMDD", default=None,
                         help=f"first business date to generate (default: {DATE_SPREAD_DAYS} days before --end-date/today)")
    parser.add_argument("--end-date", type=_date_arg, metavar="YYYYMMDD", default=None,
                         help="last business date to generate (default: today)")


def resolve_date_range(args) -> tuple[date, date]:
    """Apply --start-date/--end-date defaults and validate the pair."""
    end = args.end_date or date.today()
    start = args.start_date or (end - timedelta(days=DATE_SPREAD_DAYS - 1))
    if start > end:
        raise ValueError(f"--start-date {start:%Y%m%d} is after --end-date {end:%Y%m%d}")
    return start, end


def load_template(fixture_xml: Path) -> ET.Element:
    return ET.fromstring(fixture_xml.read_text(encoding="utf-8"))


def build_row(template: ET.Element, recid: str, date_tag: str, date_value: str) -> ET.Element:
    """Copy of the fixture row with `recid` and `date_tag`'s text swapped in."""
    row = copy.deepcopy(template)
    row.set("id", recid)
    for element in row.iter(date_tag):
        element.text = date_value
    return row


def generate_rows(template: ET.Element, count: int, first_recid: int, date_tag: str,
                   start_date: date, end_date: date):
    """Cycle `count` generated rows' dates forward through [start_date, end_date]
    (inclusive), wrapping around if count exceeds the window's span."""
    span_days = (end_date - start_date).days + 1
    for offset in range(count):
        recid = str(first_recid + offset)
        date_value = (start_date + timedelta(days=offset % span_days)).strftime("%Y%m%d")
        row = build_row(template, recid, date_tag, date_value)
        yield recid, ET.tostring(row, encoding="unicode")


def existing_row_count(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
    return cursor.fetchone()[0]


def next_start_recid(cursor, table: str, first_recid: int) -> int:
    """Highest generated recid already in the table, plus one -- or
    `first_recid` if the generated range (>= first_recid) is still empty."""
    cursor.execute(
        f"SELECT MAX(TO_NUMBER(recid)) FROM {SCHEMA}.{table} WHERE TO_NUMBER(recid) >= :first_recid",
        first_recid=first_recid,
    )
    existing_max = cursor.fetchone()[0]
    return int(existing_max) + 1 if existing_max is not None else first_recid


def run_seed(*, table: str, fixture_xml: Path, date_tag: str, args) -> None:
    template = load_template(fixture_xml)
    fixture_recid = template.get("id")

    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    print(f"[seed_{table}] connecting to {SCHEMA}@{dsn}")
    with oracledb.connect(user=SCHEMA, password=SCHEMA_PASSWORD, dsn=dsn) as conn:
        with conn.cursor() as cur:
            if args.reset:
                count = existing_row_count(cur, table)
                if count:
                    print(f"[seed_{table}] --reset: deleting {count} existing row(s)")
                    cur.execute(f"DELETE FROM {SCHEMA}.{table}")
                    conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table} WHERE recid = :recid",
                        recid=fixture_recid)
            if cur.fetchone()[0] == 0:
                cur.execute(
                    f"INSERT INTO {SCHEMA}.{table} (recid, xmlrecord) VALUES (:1, XMLTYPE(:2))",
                    [fixture_recid, ET.tostring(template, encoding="unicode")],
                )
                conn.commit()
                print(f"[seed_{table}] inserted fixture row {fixture_recid}")

            start_recid = next_start_recid(cur, table, args.first_recid)

        start_date, end_date = resolve_date_range(args)
        print(f"[seed_{table}] generating {args.count} row(s) starting at recid {start_recid}, "
              f"dates {start_date:%Y%m%d}..{end_date:%Y%m%d}")
        rows = list(generate_rows(template, args.count, start_recid, date_tag, start_date, end_date))

        with conn.cursor() as cur:
            insert_sql = f"INSERT INTO {SCHEMA}.{table} (recid, xmlrecord) VALUES (:1, XMLTYPE(:2))"
            for start in range(0, len(rows), BATCH_SIZE):
                cur.executemany(insert_sql, rows[start:start + BATCH_SIZE])
                conn.commit()
                print(f"[seed_{table}] inserted {min(start + BATCH_SIZE, len(rows))}/{len(rows)}")

        with conn.cursor() as cur:
            print(f"[seed_{table}] {table} row count = {existing_row_count(cur, table)}")
