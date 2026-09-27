# Documentation

Per-DAG reference docs for the Airflow pipelines in [`airflow/dags/`](../airflow/dags/).
Each covers: what the DAG does, how it works step by step, its parameters,
and — the part that matters most before running these in production — every
known limitation with the concrete fix for it.

- [`compact_iceberg.md`](compact_iceberg.md) — daily Iceberg table
  maintenance (compaction, snapshot expiry, orphan-file removal).
- [`oracle_gap_check.md`](oracle_gap_check.md) — daily data-quality check
  comparing Oracle source row counts against parsed Iceberg `bronze` row
  counts, per business date.
