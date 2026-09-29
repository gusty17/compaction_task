# `oracle_gap_check` — Oracle vs. Bronze Data-Quality DAG

File: [`airflow/dags/oracle_gap_check.py`](../airflow/dags/oracle_gap_check.py)

## Purpose

Daily data-quality check comparing per-business-date row counts between
Oracle's raw source tables and their parsed Iceberg `bronze` counterparts, to
catch any date where rows exist in Oracle but never made it into bronze.
This matters specifically because the bank's core system (T24) has a short
retention window and purges old rows from Oracle on its own schedule — once
a row is gone from Oracle, the parsed copy in bronze may be the only one
that will ever exist. A missed date caught late may be unrecoverable.

## How it works

1. `DATASETS` is a fixed list mapping, per table: the Oracle table name, its
   XML date tag (e.g. `c167`), the corresponding bronze table, and what that
   date tag is named once parsed (e.g. `date_last_update`).
2. For each selected dataset (`params.datasets`: `"all"` or a space-separated
   subset of names):
   - `oracle_counts_sql()` builds a Trino passthrough query
     (`oracle.system.query`) that runs an `XMLQUERY`/`XMLCAST` extraction of
     the business date **inside Oracle itself**, grouped and counted per
     date.
   - `iceberg_counts_sql()` runs the equivalent grouped count against the
     already-parsed bronze column.
   - Both optionally take a `(start_date, end_date)` window (see Parameters)
     to narrow the comparison to a specific date range instead of the whole
     table's history.
3. `diff_counts()` compares the two per-date count maps and flags every date
   where **Oracle's count is strictly greater than bronze's** — i.e., rows
   exist in the source that are missing from bronze. (See Limitation 2 — the
   reverse case is not flagged.)
4. Any gaps found are logged per dataset/date and passed to a second task,
   `alert_gaps`, which emails them (first 50 lines, full list in the log) to
   `GAP_ALERT_EMAIL_TO` over SMTP (`SMTP_HOST`/`SMTP_PORT`/`SMTP_USER`/
   `SMTP_PASSWORD`, STARTTLS). The run stays **green**
   when gaps are found, so red means the check itself broke (Trino/Oracle
   down, bad params), not a data finding. A failed send retries just
   `alert_gaps`; the Oracle scan isn't re-run.
   - If `GAP_ALERT_EMAIL_TO` is unset, `alert_gaps` fails with the gap list
     instead. Oracle purges old rows, so a gap nobody sees can become
     permanent loss, which makes a silent green run unacceptable.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `datasets` | `"all"` | `"all"`, or a space-separated subset of dataset names (`account customer ...`). |
| `start_date` / `end_date` | `""` / `""` | `YYYYMMDD`, both optional but must be given **together**. Empty = full history (every date ever seen on either side). |

## Limitations, and the fix for each, before production

### 1. This only catches missing rows — not wrong data

A row that parsed incorrectly (a field-mapping bug, a wrong value) but still
landed in bronze counts the same on both sides and will never be flagged.
This check validates *presence/count* per date, not *content*.

**Fix, if content-level assurance matters:** a separate reconciliation pass
(checksums, or a spot comparison of specific fields) — genuinely out of
scope for what this DAG is designed to do, not a gap in how it's built.

### 2. Real race with Oracle's own purge schedule

Because T24/Oracle purges old rows on its own retention schedule, if this
DAG's daily run happens to execute **after** that day's purge but the
corresponding ingestion run hasn't caught up yet (or failed silently
upstream), the "missing" rows are already gone from Oracle's side too — the
counts look reconciled even though data was genuinely lost before this check
ever saw it.

**Fix:** schedule this DAG with enough margin after the ingestion job's own
daily run that ingestion is guaranteed complete first. If ingestion is ever
wired into Airflow itself, an explicit `ExternalTaskSensor`/DAG dependency
would remove the guesswork entirely.

### 3. No coordination needed with concurrent writes — lower risk than `compact_iceberg` by design

This DAG only reads. Iceberg's snapshot isolation makes that safe regardless
of concurrent writes or compaction happening on the same tables — it never
commits anything to Iceberg itself, so it doesn't carry the commit-conflict
risk documented for `compact_iceberg`. Worth stating explicitly so it isn't
assumed to need the same locking/scheduling care.

### 4. `start_date`/`end_date` must be given together, by design

Giving only one raises `"start_date and end_date must be given together"`
rather than silently defaulting the other — a deliberate strictness so a
partial param can't quietly narrow the check in an unintended way. Worth
knowing so it isn't mistaken for a bug if someone hits it.
