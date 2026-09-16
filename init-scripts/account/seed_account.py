#!/usr/bin/env python3
"""Seed source_table.account with test data.

00_setup.sh only creates the (empty) table on container start. This script
does the actual data loading, run manually once the stack is up:

    pip install -r init-scripts/requirements.txt
    python init-scripts/account/seed_account.py

It inserts the one hand-written fixture row (reference/account_xml_data_sample.xml)
unchanged, then generates --count more rows by copying that fixture and
rewriting a handful of fields per row (recid, name, currency, balances,
dates, ...) so each row is unique. Same idea as the old XSLT-in-SQL
generator, just as plain Python so it's actually readable.
"""
from __future__ import annotations

import argparse
import copy
import os
import xml.etree.ElementTree as ET
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
BATCH_SIZE = 500

# tag name -> how to derive its new text from the generated recid.
# Mirrors the old XSLT stylesheet's per-tag templates field for field.
_MONEY_TAGS = {"c23", "c24", "c25", "c26", "c27", "c29", "c32",
               "c35", "c38", "c41", "c44", "c77", "c122", "c149"}
_DATE_TAGS = {"c28", "c31", "c34", "c37", "c40", "c43", "c46", "c47",
              "c48", "c49", "c50", "c78", "c79", "c121", "c167"}
_CURRENCY_MAP = {"1": "USD", "2": "EUR", "3": "GBP"}


def substr(value: str, start: int, length: int) -> str:
    """1-based XPath-style substring: substr("abcdef", 2, 3) == "bcd"."""
    return value[start - 1 : start - 1 + length]


def derive_text(tag: str, recid: str) -> str | None:
    """New text for `tag` given the row's `recid`, or None to leave it as-is."""
    if tag == "c1":
        return "90" + substr(recid, 11, 6)
    if tag in ("c2", "c85"):
        return "65" + substr(recid, 15, 1) + "0"
    if tag in ("c3", "c5"):
        return f"Test account {recid}"
    if tag in ("c8", "c93", "c95"):
        return _CURRENCY_MAP.get(recid[-1], "EGP")
    if tag in _MONEY_TAGS:
        return f"{(int(substr(recid, 11, 6)) % 900000) / 100:.2f}"
    if tag in _DATE_TAGS:
        month = (int(substr(recid, 15, 2)) % 12) + 1
        day = (int(substr(recid, 13, 2)) % 28) + 1
        return f"2026{month:02d}{day:02d}"
    if tag in ("c249", "c251"):
        return f"TEST_USER_{substr(recid, 11, 6)}"
    return None


def build_row(template: ET.Element, recid: str) -> ET.Element:
    """Copy of the fixture row with `recid` and its derived fields swapped in."""
    row = copy.deepcopy(template)
    row.set("id", recid)
    for element in row.iter():
        new_text = derive_text(element.tag, recid)
        if new_text is not None:
            element.text = new_text
    return row


def generate_rows(count: int, first_recid: int):
    template = ET.fromstring(FIXTURE_XML.read_text(encoding="utf-8"))
    yield template.get("id"), ET.tostring(template, encoding="unicode")
    for offset in range(count):
        recid = str(first_recid + offset)
        row = build_row(template, recid)
        yield recid, ET.tostring(row, encoding="unicode")


def existing_row_count(cursor) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.account")
    return cursor.fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=DEFAULT_ROW_COUNT,
                         help=f"rows to generate in addition to the fixture row (default {DEFAULT_ROW_COUNT})")
    parser.add_argument("--first-recid", type=int, default=DEFAULT_FIRST_RECID,
                         help=f"first generated recid (default {DEFAULT_FIRST_RECID})")
    parser.add_argument("--reset", action="store_true",
                         help="delete existing rows from account before seeding")
    args = parser.parse_args()

    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    print(f"[seed_account] connecting to {SCHEMA}@{dsn}")
    with oracledb.connect(user=SCHEMA, password=SCHEMA_PASSWORD, dsn=dsn) as conn:
        with conn.cursor() as cur:
            count = existing_row_count(cur)
            if count and args.reset:
                print(f"[seed_account] --reset: deleting {count} existing row(s)")
                cur.execute(f"DELETE FROM {SCHEMA}.account")
                conn.commit()
            elif count:
                print(f"[seed_account] account already has {count} row(s); "
                      "pass --reset to wipe and reseed. Nothing to do.")
                return

        print(f"[seed_account] generating {args.count + 1} rows "
              f"(1 fixture + {args.count} derived)")
        rows = list(generate_rows(args.count, args.first_recid))

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
