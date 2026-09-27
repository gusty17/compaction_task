#!/usr/bin/env python3
"""Seed source_table.customer with test data (run after 00_setup.sh creates
the empty table).

Usage:
    pip install -r requirements.txt
    python init-scripts/customer/seed_customer.py --count 100 --start-date 20031209 --end-date 20031209 

Inserts the fixture row (reference/customer_xml_data_sample.xml), then
generates --count more by varying only `recid` (PK / MERGE key) and `c167`
(last_review_date -- the same tag number account's own date_field uses),
spread across --start-date/--end-date (default: the trailing 365 days ending today) so both `daily` and `history` runs
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

TABLE = "customer"
FIXTURE_XML = lib.REPO_ROOT / "reference" / "customer_xml_data_sample.xml"
DATE_TAG = "c167"
DEFAULT_FIRST_RECID = 9000000130000001
DEFAULT_ROW_COUNT = 10000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    lib.add_seed_args(parser, default_count=DEFAULT_ROW_COUNT, default_first_recid=DEFAULT_FIRST_RECID)
    args = parser.parse_args()

    lib.run_seed(table=TABLE, fixture_xml=FIXTURE_XML, date_tag=DATE_TAG, args=args)


if __name__ == "__main__":
    main()
