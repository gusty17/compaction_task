#!/usr/bin/env python3
"""Seed source_table.stmt_entry with test data (run after 00_setup.sh creates
the empty table).

Usage:
    pip install -r requirements.txt
    python init-scripts/stmt_entry/seed_stmt_entry.py [--count N] [--start-date YYYYMMDD] [--end-date YYYYMMDD] [--reset]

Inserts the fixture row (reference/stmt_entry_xml_data_sample.xml), then
generates --count more by varying only `recid` (PK / MERGE key) and `c78`
(trans_date -- its own tag number, unrelated to account's c167), spread
across --start-date/--end-date (default: the trailing 365 days ending today) so both `daily` and `history` runs have
data. Everything else is left as-is - this is for exercising the parsing
pipeline and its timings, not field realism.

Reruns are additive, continuing from the highest existing recid; --reset
wipes the table first.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _seed_lib as lib  # noqa: E402

TABLE = "stmt_entry"
FIXTURE_XML = lib.REPO_ROOT / "reference" / "stmt_entry_xml_data_sample.xml"
DATE_TAG = "c78"
DEFAULT_FIRST_RECID = 9000000140000001
DEFAULT_ROW_COUNT = 10000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    lib.add_seed_args(parser, default_count=DEFAULT_ROW_COUNT, default_first_recid=DEFAULT_FIRST_RECID)
    args = parser.parse_args()

    lib.run_seed(table=TABLE, fixture_xml=FIXTURE_XML, date_tag=DATE_TAG, args=args)


if __name__ == "__main__":
    main()
