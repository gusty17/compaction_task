from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

# Step 1: merge small files into large ones and apply merge-on-read deletes
# (rewrites files below file_size_threshold, default 100MB). Makes queries fast again.
# Step 2: expire snapshots older than the retention threshold.
COMPACT_STEPS = [
    "ALTER TABLE {table} EXECUTE optimize",
    "ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')",
]
# Step 3: remove files not referenced by any snapshot -- the step that actually
# deletes from the file system. Runs in its own task, only after compact_tables
# succeeded, and is skipped when the `orphans` param is off.
ORPHAN_STEPS = [
    "ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')",
]


def stats_sql(schema: str, table: str) -> str:
    """One row: data_files, delete_files, bytes, stored_records, live_rows.
    """
    return f"""
        WITH meta_data_file AS (
            SELECT count_if(content = 0) AS data_files,
                   count_if(content > 0) AS delete_files,
                   coalesce(sum(file_size_in_bytes), 0) AS bytes,
                   coalesce(sum(record_count) FILTER (WHERE content = 0), 0) AS stored_records
            FROM iceberg.{schema}."{table}$files"
        ),
        live_rows AS (
            SELECT count(*) AS live_rows
            FROM iceberg.{schema}.{table}
        )
        SELECT f.data_files, f.delete_files, f.bytes, f.stored_records, l.live_rows
        FROM meta_data_file f CROSS JOIN  live_rows l
    """


def render_statements(steps: list[str], fq_table: str, retention: str) -> list[str]:
    """`steps` for one table, ready for cur.execute()."""
    return [s.format(table=fq_table, retention=retention) for s in steps]


def fetch_stats(cur, schema: str, table: str) -> dict:
    cur.execute(stats_sql(schema, table))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def fmt(stats: dict) -> str:
    return f"{stats['data_files']} data + {stats['delete_files']} delete, {stats['bytes'] // 1024} KB"


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


def discover_tables(cur, schemas: list[str]) -> list[tuple[str, str]]:
    tables: list[tuple[str, str]] = []
    for schema in schemas:
        cur.execute(f"SHOW TABLES FROM iceberg.{schema}")
        tables.extend((schema, name) for (name,) in cur.fetchall())
    log.info("discovered %d table(s) across schemas: %s", len(tables), schemas)
    return tables


def run_steps(cur, tables: list[tuple[str, str]], steps: list[str], retention: str) -> None:
    """Per table: stats, run `steps` (none = report only), stats, log. After all
    tables, raise if any table's live row count changed."""
    failures = []
    for schema, table in tables:
        fq = f"iceberg.{schema}.{table}"
        t0 = time.perf_counter()
        before = fetch_stats(cur, schema, table)

        for stmt in render_statements(steps, fq, retention):
            cur.execute(stmt)
            cur.fetchall()  # drive it to completion -- Trino's protocol is poll-based

        after = fetch_stats(cur, schema, table)
        dt = time.perf_counter() - t0

        log.info("%-30s %s -> %s  %.2fs", f"{schema}.{table}", fmt(before), fmt(after), dt)

        # Maintenance should only ever change how data is stored, never what
        # data exists. A mismatch here is a real-bug signal, not something to
        # retry past -- and it runs before remove_orphans, so a bad compaction
        # never reaches the step that physically deletes files.
        if steps and before["live_rows"] != after["live_rows"]:
            failures.append(f"{fq}: row count changed {before['live_rows']} -> {after['live_rows']}")

    if failures:
        raise RuntimeError("row-count guard failed:\n" + "\n".join(failures))


#airflow dags trigger compact_iceberg
@dag(
    dag_id="compact_iceberg",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two concurrent runs would race the row-count guard
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=1),
    },
    params={
        "retention": "0s",
        "orphans": 1,
        "schemas": "bronze staging",
        "check_only": False,
    },
    tags=["iceberg", "maintenance"],
)
def compact_iceberg():
    @task(task_id="compact_tables")
    def compact_tables(**context) -> None:
        params = context["params"]
        # check_only = report-only: no steps, just the before/after stats
        steps = [] if params["check_only"] else COMPACT_STEPS
        with trino_cursor() as cur:
            tables = discover_tables(cur, params["schemas"].split())
            run_steps(cur, tables, steps, params["retention"])

    @task(task_id="remove_orphans")
    def remove_orphans(**context) -> None:
        params = context["params"]
        if params["check_only"] or not params["orphans"]:
            raise AirflowSkipException("orphan sweep off (orphans=0 or check_only=true)")
        with trino_cursor() as cur:
            tables = discover_tables(cur, params["schemas"].split())
            run_steps(cur, tables, ORPHAN_STEPS, params["retention"])

    # orphans are only swept after compaction (and its row-count guard) passed
    compact_tables() >> remove_orphans()


compact_iceberg()
