#!/usr/bin/env python3
"""Seed source_table.account with test data (run after 00_setup.sh creates
the empty table).

Usage:
    pip install -r init-scripts/requirements.txt
    python init-scripts/account/seed_account.py [--count N] [--reset]

Inserts the fixture row (reference/account_xml_data_sample.xml), then
generates --count more by varying only `recid` (PK / MERGE key) and `c167`
(config.py's date_field, spread across the trailing 365 days so both
`daily` and `history` runs have data). Everything else is left as-is - this
is for exercising the parsing pipeline and its timings, not field realism.

Reruns are additive, continuing from the highest existing recid; --reset
wipes the table first.
"""
from __future__ import annotations

import argparse
import copy
import os
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

import oracledb
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_XML = REPO_ROOT / "reference" / "account_xml_data_sample.xml"

load_dotenv(REPO_ROOT / ".env")

SCHEMA = os.environ.get("ORACLE_SCHEMA", "source_table")
SCHEMA_PASSWORD = os.environ.get("ORACLE_SCHEMA_PASSWORD", "source_table")
ORACLE_HOST = os.environ.get("ORACLE_HOST", "localhost")
ORACLE_PORT = os.environ.get("ORACLE_PORT", "1521")
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "XEPDB1")

DEFAULT_FIRST_RECID = 9000000120000001
DEFAULT_ROW_COUNT = 10000
DATE_SPREAD_DAYS = 365   # window's date_field (c167); trailing year ending today
BATCH_SIZE = 500


def build_row(template: ET.Element, recid: str, c167: str) -> ET.Element:
    """Copy of the fixture row with `recid` and `c167` swapped in."""
    row = copy.deepcopy(template)
    row.set("id", recid)
    for element in row.iter("c167"):
        element.text = c167
    return row


def load_template() -> ET.Element:
    return ET.fromstring(FIXTURE_XML.read_text(encoding="utf-8"))


def generate_rows(template: ET.Element, count: int, first_recid: int):
    today = date.today()
    for offset in range(count):
        recid = str(first_recid + offset)
        c167 = (today - timedelta(days=offset % DATE_SPREAD_DAYS)).strftime("%Y%m%d")
        row = build_row(template, recid, c167)
        yield recid, ET.tostring(row, encoding="unicode")


def existing_row_count(cursor) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.account")
    return cursor.fetchone()[0]


def next_start_recid(cursor, first_recid: int) -> int:
    """Highest generated recid already in the table, plus one -- or
    `first_recid` if the generated range (>= first_recid) is still empty."""
    cursor.execute(
        f"SELECT MAX(TO_NUMBER(recid)) FROM {SCHEMA}.account WHERE TO_NUMBER(recid) >= :first_recid",
        first_recid=first_recid,
    )
    existing_max = cursor.fetchone()[0]
    return int(existing_max) + 1 if existing_max is not None else first_recid


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=DEFAULT_ROW_COUNT,
                         help=f"rows to generate on top of what's already there (default {DEFAULT_ROW_COUNT})")
    parser.add_argument("--first-recid", type=int, default=DEFAULT_FIRST_RECID,
                         help=f"start of the generated range on an empty/--reset table (default {DEFAULT_FIRST_RECID})")
    parser.add_argument("--reset", action="store_true",
                         help="delete existing rows from account before seeding, restarting the generated range at --first-recid")
    args = parser.parse_args()

    template = load_template()
    fixture_recid = template.get("id")

    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    print(f"[seed_account] connecting to {SCHEMA}@{dsn}")
    with oracledb.connect(user=SCHEMA, password=SCHEMA_PASSWORD, dsn=dsn) as conn:
        with conn.cursor() as cur:
            if args.reset:
                count = existing_row_count(cur)
                if count:
                    print(f"[seed_account] --reset: deleting {count} existing row(s)")
                    cur.execute(f"DELETE FROM {SCHEMA}.account")
                    conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.account WHERE recid = :recid",
                        recid=fixture_recid)
            if cur.fetchone()[0] == 0:
                cur.execute(
                    f"INSERT INTO {SCHEMA}.account (recid, xmlrecord) VALUES (:1, XMLTYPE(:2))",
                    [fixture_recid, ET.tostring(template, encoding="unicode")],
                )
                conn.commit()
                print(f"[seed_account] inserted fixture row {fixture_recid}")

            start_recid = next_start_recid(cur, args.first_recid)

        print(f"[seed_account] generating {args.count} row(s) starting at recid {start_recid}")
        rows = list(generate_rows(template, args.count, start_recid))

        with conn.cursor() as cur:
            insert_sql = (
                f"INSERT INTO {SCHEMA}.account (recid, xmlrecord) "
                "VALUES (:1, XMLTYPE(:2))"
            )
            for start in range(0, len(rows), BATCH_SIZE):
                cur.executemany(insert_sql, rows[start:start + BATCH_SIZE])
                conn.commit()
                print(f"[seed_account] inserted {min(start + BATCH_SIZE, len(rows))}/{len(rows)}")

        with conn.cursor() as cur:
            print(f"[seed_account] account row count = {existing_row_count(cur)}")


if __name__ == "__main__":
    main()
