from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import timedelta
from typing import NamedTuple

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)


class Dataset(NamedTuple):
    name: str
    oracle_table: str
    date_tag: str           # XML tag under /row holding the business date
    bronze_table: str
    bronze_date_column: str  # what date_tag is named once parsed into bronze


 #dataset(name, oracle_table, date_tag, bronze_table, bronze_date_column)
DATASETS = [
    Dataset("account", "account", "c167", "bronze.account_wide", "date_last_update"),
    Dataset("customer", "customer", "c167", "bronze.customer_wide", "last_review_date"),
    Dataset("stmt_entry", "stmt_entry", "c78", "bronze.stmt_entry_wide", "trans_date"),
    Dataset("loan", "loan", "c167", "bronze.loan_wide", "disbursement_date"),
    Dataset("funds_transfer", "funds_transfer", "c121", "bronze.funds_transfer_wide", "transaction_date"),
    Dataset("collateral", "collateral", "c50", "bronze.collateral_wide", "valuation_date"),
]


def oracle_counts_sql(schema: str, table: str, date_tag: str) -> str:

    date_expr = (
        f"XMLCAST(XMLQUERY('/row/{date_tag}/text()' PASSING a.xmlrecord "
        f"RETURNING CONTENT) AS VARCHAR2(8))"
    )
    # Bare COUNT(*) is an unbounded Oracle NUMBER, which the connector refuses
    # to map ("DECIMAL precision must be in range [1, 38]: 0").
    inner = (
        f"SELECT {date_expr} AS business_date, CAST(COUNT(*) AS NUMBER(19,0)) AS row_count "
        f"FROM {schema}.{table} a GROUP BY {date_expr}"
    )
    return f"SELECT * FROM TABLE(oracle.system.query(query => '{inner.replace(chr(39), 2 * chr(39))}'))"


def iceberg_counts_sql(table: str, date_col: str) -> str:

    return (
        f"SELECT {date_col} AS business_date, COUNT(*) AS row_count "
        f"FROM iceberg.{table} GROUP BY {date_col}"
    )


@contextmanager
def trino_cursor():
    import trino

    conn = trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ["TRINO_PORT"]),
        user="airflow",
        catalog="iceberg",
        http_scheme="http",
    )
    try:
        yield conn.cursor()
    finally:
        conn.close()


def fetch_counts(cur, sql: str) -> dict[str | None, int]:
    cur.execute(sql)
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def diff_counts(oracle: dict[str | None, int], iceberg: dict[str | None, int]) -> list[str]:

    gaps = []
    # A row whose XML carries no date tag groups under None on both sides; sort
    # it last rather than comparing None to str.
    for business_date in sorted(set(oracle) | set(iceberg), key=lambda d: (d is None, d)):
        o, i = oracle.get(business_date, 0), iceberg.get(business_date, 0)
        if o > i:
            gaps.append(f"{business_date or '(no date)'}: oracle={o} bronze={i} (missing {o - i})")
    return gaps


# airflow dags trigger oracle_gap_check
@dag(
    dag_id="oracle_gap_check",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
    },
    params={"datasets": "all"},
    tags=["oracle", "iceberg", "data-quality"],
)
def oracle_gap_check():
    @task(task_id="check_gaps")
    def check_gaps(**context) -> None:
        selector = context["params"]["datasets"]
        datasets = (
            DATASETS if selector == "all"
            else [d for d in DATASETS if d.name in selector.split()]
        )
        if not datasets:
            known = " ".join(d.name for d in DATASETS)
            raise ValueError(f"no dataset matches {selector!r} -- known datasets: {known}")

        schema = os.environ["ORACLE_SCHEMA"]
        failures = []

        with trino_cursor() as cur:
            for ds in datasets:
                oracle = fetch_counts(cur, oracle_counts_sql(schema, ds.oracle_table, ds.date_tag))
                iceberg = fetch_counts(cur, iceberg_counts_sql(ds.bronze_table, ds.bronze_date_column))
                gaps = diff_counts(oracle, iceberg)

                log.info(
                    "%-16s oracle %d rows / %d dates -> bronze %d rows / %d dates  %s",
                    ds.name, sum(oracle.values()), len(oracle),
                    sum(iceberg.values()), len(iceberg),
                    "OK" if not gaps else f"{len(gaps)} date(s) MISSING ROWS",
                )
                for line in gaps:
                    log.warning("  %s %s", ds.name, line)
                    failures.append(f"{ds.name} {line}")

        if failures:
            raise RuntimeError(
                f"{len(failures)} date(s) with rows in Oracle missing from bronze:\n"
                + "\n".join(failures)
            )

    check_gaps()


oracle_gap_check()
