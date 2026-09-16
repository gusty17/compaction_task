#!/usr/bin/env python3
"""Seed source_table.funds_transfer with test data (run after 00_setup.sh creates
the empty table).

Usage:
    pip install -r init-scripts/requirements.txt
    python init-scripts/funds_transfer/seed_funds_transfer.py [--count N] [--reset]

Inserts the fixture row (reference/funds_transfer_xml_data_sample.xml), then
generates --count more by varying only `recid` (PK / MERGE key) and `c121`
(transaction_date -- its own tag number, unrelated to account's c167),
spread across the trailing 365 days so both `daily` and `history` runs
have data. Everything else is left as-is - this is for exercising the
parsing pipeline and its timings, not field realism.

Reruns are additive, continuing from the highest existing recid; --reset
wipes the table first.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _seed_lib as lib  # noqa: E402

TABLE = "funds_transfer"
FIXTURE_XML = lib.REPO_ROOT / "reference" / "funds_transfer_xml_data_sample.xml"
DATE_TAG = "c121"
DEFAULT_FIRST_RECID = 9000000160000001
DEFAULT_ROW_COUNT = 10000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=DEFAULT_ROW_COUNT,
                         help=f"rows to generate on top of what's already there (default {DEFAULT_ROW_COUNT})")
    parser.add_argument("--first-recid", type=int, default=DEFAULT_FIRST_RECID,
                         help=f"start of the generated range on an empty/--reset table (default {DEFAULT_FIRST_RECID})")
    parser.add_argument("--reset", action="store_true",
                         help="delete existing rows before seeding, restarting the generated range at --first-recid")
    args = parser.parse_args()

    lib.run_seed(table=TABLE, fixture_xml=FIXTURE_XML, date_tag=DATE_TAG, args=args)


if __name__ == "__main__":
    main()
