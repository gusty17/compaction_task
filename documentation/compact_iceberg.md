# `compact_iceberg` — Iceberg Table Maintenance DAG

File: [`airflow/dags/compact_iceberg.py`](../airflow/dags/compact_iceberg.py)

## Purpose

Daily maintenance for every Iceberg table under the schemas this DAG is pointed
at (default: `bronze` and `staging`). Iceberg tables never overwrite data in
place — every write adds new files and a new snapshot — so over time a table
accumulates:

- many small data files (one write's worth at a time, rather than a few large
  ones) → slow queries,
- a long chain of old snapshots (kept for time-travel/rollback) → growing
  metadata,
- files from superseded snapshots that nothing references anymore
  ("orphans") → wasted storage.

This DAG addresses all three, per table, across two dependent tasks.

## How it works

### Task 1: `compact_tables`

1. Reads its run params (`retention`, `schemas`, `check_only`).
2. `discover_tables()` runs `SHOW TABLES FROM iceberg.<schema>` for every
   schema in `params.schemas` (space-separated), building the full list of
   tables to process.
3. For each table, `compact_with_rollback()`:
   - Captures **before** stats: data/delete file count, total bytes, live row
     count (via Iceberg's `"$files"` metadata table plus a plain row count).
   - Runs the maintenance SQL in this specific order (skipped entirely when
     `check_only=True` — a dry run that only reports stats):
     ```sql
     ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')
     -- capture the resulting current snapshot id here, before optimize runs --
     ALTER TABLE {table} EXECUTE optimize
     ```
   - Captures **after** stats and logs before → after.
   - **Correctness guard**: if the live row count changed as a result of
     `optimize`, that's a bug signal — maintenance should only ever change
     *how* data is stored, never *what* data exists. On a guard failure,
     **this table alone is automatically rolled back**
     (`rollback_to_snapshot(snapshot_id => {the id captured right before
     optimize})`) and added to a failure list — every other table is
     unaffected and still gets attempted (see Limitation 2's current status).
4. If any table failed its guard, the task raises after every table in the
   loop has been attempted.

### Task 2: `remove_orphans`

- Only runs after `compact_tables` succeeds
  (`compact_tables() >> remove_orphans()`).
- Skips entirely (`AirflowSkipException`) when `params.orphans=0` or
  `params.check_only=True`.
- Re-discovers the same tables and runs the one genuinely destructive step:
  ```sql
  ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')
  ```
  This is the only statement anywhere in the DAG that physically deletes
  files from the object store. `optimize` and `expire_snapshots` only
  rewrite/prune *metadata* pointers (see below).

### Why `expire_snapshots` then `optimize` (not the reverse), and what each actually deletes

- `expire_snapshots` prunes old, now-superseded snapshots from the table's
  history, and — as part of that same commit — **immediately** deletes the
  manifest and manifest-list files that were exclusively used by those
  pruned snapshots. It never touches the *current* (live) snapshot, no
  matter how short the retention is set. Run first, it only ever prunes
  history left over from *previous* runs.
- `optimize` rewrites small data files into fewer, larger ones and commits a
  **new** snapshot. The files it replaces are untouched — they simply stop
  being part of the *current* live file set; the snapshot they belonged to
  is still fully valid, just no longer current.
- Running `expire_snapshots` **before** `optimize` is what makes rollback
  possible: since `expire_snapshots` already ran for today, the snapshot
  `optimize` is about to supersede stays alive until *tomorrow's* run
  instead of being expired in the same breath it's created — giving
  `rollback_to_snapshot` something real to target if the guard trips.
- The old data files `optimize` replaces become **orphans**: physically
  present, no longer referenced by anything. They're only found and
  physically deleted by `remove_orphan_files`, which is exactly why it's a
  separate, later task — it's the only step that touches actual data, so it
  only ever runs once compaction has been verified safe by the row-count
  guard.

**Verified live** (see the conversation this doc was generated from): a
disposable test table was compacted, its old manifests/manifest-lists were
confirmed physically deleted immediately by `expire_snapshots` while its old
*data* files survived until a separate `remove_orphan_files` call removed
them — and separately, `rollback_to_snapshot` was confirmed to fully restore
both row count and row contents after a simulated bad compaction.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `retention` | `"0s"` | Passed to both `expire_snapshots` and `remove_orphan_files`. **Local-stack-only value — see Limitation 1.** |
| `orphans` | `1` | Whether `remove_orphans` actually runs (`0` = always skip). |
| `schemas` | `"bronze staging"` | Space-separated list of schemas to discover tables from. |
| `check_only` | `False` | Dry run — reports before/after stats with **no** statements executed; `remove_orphans` always skips. |

## Limitations, and the fix for each, before production

### 1. `retention="0s"` is a local-testing value, not a production one

`trino-catalog/iceberg.properties` deliberately lowers Trino's own safety
floor (`iceberg.expire-snapshots.min-retention=0s`,
`iceberg.remove-orphan-files.min-retention=0s`) specifically so this DAG
would do anything visible against snapshots that are only minutes old. In
production, a retention this short risks deleting files a slow, still-running
read is depending on, or files a concurrent writer has staged but not yet
committed.

**Fix:** raise `retention` (and the connector's `min-retention` settings) to
a value comfortably longer than the slowest realistic read or write in
production. Trino's own un-overridden default (7 days) is a reasonable
starting point, not an arbitrary one.

### 2. ~~A bad table doesn't stop the run, and nothing rolls it back automatically~~ — rollback: FIXED, loop behavior: still open

**Fixed:** `compact_with_rollback()` now captures each table's snapshot id
before `optimize`, and on a guard failure automatically issues
`rollback_to_snapshot(snapshot_id => {captured_id})` against that one table,
restoring it fully before recording the failure. Confirmed live: a simulated
bad compaction was fully reverted, both row count and row contents.

**Still true, and an open decision, not a bug:** the loop still continues
through every remaining table after one fails (now safely rolled back)
rather than stopping immediately — so a systemic bug can still touch every
table in one run before the DAG fails, it just no longer leaves any of them
in a wrong state. Stopping the loop on the first failure instead (to bound
how many tables a systemic problem gets to touch at all) is a separate,
still-open change if that tighter blast-radius control is wanted.

### 3. ~~Today's step order gives zero rollback window~~ — FIXED

**Fixed:** the DAG now runs `expire_snapshots` first, `optimize` second (see
"Why `expire_snapshots` then `optimize`" above). Iceberg never expires the
*current* snapshot regardless of retention, so this order prunes only
already-superseded history from prior runs; the snapshot `optimize` is about
to supersede survives until the *next* scheduled run — which is exactly what
makes the rollback in Limitation 2 possible at all, not just a longer manual
window.

### 4. First production run on a real backlog will not resemble local testing

Local runs finish in roughly 3–5s per table against a handful of files. A
production table compacted for the first time against a genuine backlog
(10,000+ accumulated small files) is a fundamentally different workload —
dominated by per-file object-store round-trips and total data volume, not a
linear scale-up from the local number. A realistic range is minutes to a few
hours for that first pass.

**Fix:** `default_args.execution_timeout` is currently `timedelta(hours=1)`
— either raise it specifically for the first catch-up run, or run that first
pass manually/out-of-band before turning on the regular `@daily` schedule,
so the routine timeout isn't tested against an atypically large one-time
backlog.

### 5. No coordination with concurrent writers (dbt, or Spark ingestion via `local_parsing`)

Iceberg's own optimistic-concurrency commit protocol guarantees no silent
corruption if this DAG runs at the same moment as a write to the same table
— but a genuine overlap (specifically: `optimize` rewriting a data file while
a concurrent `MERGE` adds delete-markers against that same file) can make
either side's commit fail outright once Iceberg's own automatic retries are
exhausted, requiring a manual re-run (`dbt retry`, or re-running
`run_parsing.py`). Nothing here detects or waits for that.

Also relevant: Spark's writes (`local_parsing`) go directly to the Iceberg
REST catalog and object store — they never go through Trino — so Trino's
own `system.runtime.queries` can't be used to see them coming, even as a
heuristic.

**Fix options, roughly in order of effort:**
- Simplest: schedule this DAG at a time known not to overlap with
  ingestion/dbt runs.
- More robust: an explicit lock — e.g. a small table in `iceberg-catalog-db`
  (already in this stack) that writers acquire before committing and release
  in a `finally` block, checked by an Airflow sensor here before
  `compact_tables` touches a given table. Not implemented today; requires
  matching changes on both the writer side and this DAG.

### 6. `check_only` isn't a complete no-op

It skips every mutating statement and always skips `remove_orphans`, but
`compact_tables` still runs `discover_tables()`/`fetch_stats()` against
Trino/Oracle even in dry-run mode. Cheap, but not literally zero activity.
