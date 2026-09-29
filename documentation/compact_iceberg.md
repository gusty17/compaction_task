# `compact_iceberg` — Iceberg Table Maintenance DAG

File: [`airflow/dags/compact_iceberg.py`](../airflow/dags/compact_iceberg.py)

## Purpose

Daily maintenance for every Iceberg table under the schemas this DAG is pointed
at (default: `bronze` and `staging`). Iceberg tables never overwrite anything
in place — every write adds new files and a new snapshot — so over time a
table accumulates:

- many small data files (one write's worth at a time) → slow queries,
- many small manifest files (one per write) → slow query planning,
- a long chain of old snapshots, and the files only they use → growing
  storage,
- true "orphans": files a crashed or aborted write left behind that no
  snapshot ever referenced → wasted storage.

This DAG addresses all four, per table, across two dependent tasks.

## Iceberg in one picture

```
metadata.json            one per commit — the table's current state
  └─ snapshot            one per commit — one version of the table
       └─ manifest list  exactly one per snapshot (snap-*.avro)
            └─ manifest files   lists of data files (*-m*.avro)
                 └─ data files + delete files   (*.parquet)
```

Every step below that changes something (`optimize`, `optimize_manifests`,
`rollback_to_snapshot`) writes a **new** snapshot — it never edits existing
files. That's what makes rollback possible: the previous snapshot and all the
files it uses are still there.

**The one rule for deletion:** a file can only be deleted once **no snapshot
that is still kept** uses it.

## What each step does

| Step | What it does | Deletes | Adds |
|---|---|---|---|
| `expire_snapshots` | Drops every snapshot older than `retention` (never the current one) | Manifest lists, manifest files, data files and delete files that no remaining snapshot uses | 1 `metadata.json` |
| `optimize` | Rewrites small data files, and files with delete files, into fewer larger ones | Nothing | Data file(s), manifests, manifest list, snapshot, `metadata.json` |
| `optimize_manifests` | Rewrites all manifest files into a few larger ones | Nothing | Manifest(s), manifest list, snapshot, `metadata.json` |
| `remove_orphan_files` | Lists the table's storage folder; deletes files no snapshot references, older than `retention` | Only never-committed leftovers of crashed/aborted writes | Nothing |

The files `optimize` and `optimize_manifests` replace are therefore deleted
by the **next** run's `expire_snapshots` — not by `remove_orphan_files`, and
not in the same run. That one-day delay is what keeps the rollback target
usable.

Old `metadata.json` files are not deleted by any of the four steps — see
[Table properties](#table-properties-and-trino-catalog-settings).

## How it works

### Task 1: `compact_tables`

1. Reads its run params (`retention`, `schemas`, `check_only`).
2. `discover_tables()` runs `SHOW TABLES FROM iceberg.<schema>` for every
   schema in `params.schemas` (space-separated).
3. For each table, `compact_with_rollback()`:
   - Captures **before** stats: data/delete file count and total bytes (from
     Iceberg's `$files` metadata table) plus a plain live row count.
   - With `check_only=True`, logs those stats and moves to the next table —
     no statement is executed.
   - Otherwise runs, in this order:
     ```sql
     ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')
     -- capture the current snapshot id here: the rollback target --
     ALTER TABLE {table} EXECUTE optimize
     ALTER TABLE {table} EXECUTE optimize_manifests
     ```
   - Captures **after** stats and logs before → after.
   - **Row-count guard:** maintenance must only change *how* data is stored,
     never *what* data exists. If the live row count changed, that table is
     rolled back (`rollback_to_snapshot` to the captured id) and added to a
     failure list; the remaining tables are still processed.
   - **Crash guard:** if `optimize`, `optimize_manifests` or the after-stats
     query raises (Trino error, `execution_timeout`, …), anything already
     committed was never checked by the row-count guard, so the table is
     rolled back to the captured id and the task fails immediately.
     - If nothing was committed yet (the crash was inside `optimize`
       itself), the table is unchanged and no rollback is issued.
     - The rollback is best effort: if Trino itself is down it fails too,
       and the original error is what gets raised.
   - The captured id is the table's **current** snapshot: the newest
     `is_current_ancestor` row of `$history`. Not simply the newest snapshot
     — after a rollback, the snapshot rolled away from is still the newest
     row in `$snapshots` and `$history`.
4. If any table failed the row-count guard, the task raises after every
   table has been attempted.

### Task 2: `remove_orphans`

- Only runs after `compact_tables` succeeded
  (`compact_tables() >> remove_orphans()`), so it never runs after a failed
  or rolled-back compaction.
- Skips (`AirflowSkipException`) when `params.orphans=0` or
  `params.check_only=True`.
- Re-discovers the tables and runs, per table:
  ```sql
  ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')
  ```
- Also checks the row count before and after and raises if it changed (no
  rollback here: `remove_orphan_files` creates no snapshot to roll back from).

### Why this order

- **`expire_snapshots` first.** It only prunes history left over from
  *previous* runs. If it ran after `optimize`, it would expire the
  pre-optimize snapshot straight away, and a failed guard would have nothing
  to roll back to.
- **`optimize` before `optimize_manifests`.** So the merged manifests already
  point at the compacted data files; the other way round, `optimize` would
  immediately add new small manifests again.
- **`remove_orphan_files` last, as a separate task.** It's the only step that
  decides what to delete by listing storage instead of reading the table's
  metadata, so it only runs once compaction has passed its guards.

### When `expire_snapshots` deletes data files

A data file replaced by `optimize` is only deleted once the snapshot that
**replaced** it is itself expired — not while that snapshot is still current.
So on a fresh table:

| Run | `expire_snapshots` deletes |
|---|---|
| Day 1 | old manifest lists only — every data file is still used by the current snapshot |
| Day 2 | the data + delete files and manifests day 1's `optimize` / `optimize_manifests` replaced |

This is why `expire_snapshots` can look like it "never deletes data files"
when tested once in isolation.

### What `optimize` picks, and how big the output is

- Every data file smaller than `file_size_threshold` — Trino's default
  100MB, which the DAG uses.
- Every data file that has delete files attached, **whatever its size** —
  its deletes are applied and the delete files dropped. (Bronze tables are
  merge-on-read, so updates/deletes create delete files.)
- A single small file with nothing to merge it with is left alone.
- Output files are capped at `iceberg.target-max-file-size` (512MB here,
  Trino's default is 1GB). It's a target, not a hard cap: files can end up
  slightly larger.

### How big merged manifests are

`optimize_manifests` fills one manifest until it reaches the table property
`commit.manifest.target-size-bytes`, then starts the next. The DAG doesn't
set it, so Iceberg's default of **8MB** applies — tens of thousands of file
entries per manifest, so for these tables one run normally ends with a
single manifest. Each run rewrites all manifests from scratch.

The manifest *list* can't be compacted — there is exactly one per snapshot —
and old ones are deleted with their snapshot by `expire_snapshots`.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `retention` | `"0s"` | Passed to both `expire_snapshots` and `remove_orphan_files`. **Local-stack-only value — see Limitation 1.** |
| `orphans` | `1` | Whether `remove_orphans` runs (`0` = always skip). |
| `schemas` | `"bronze staging"` | Space-separated list of schemas to discover tables from. |
| `check_only` | `False` | Dry run — reports stats with **no** statements executed; `remove_orphans` always skips. |

## Table properties and Trino catalog settings

Settings in [`trino-catalog/iceberg.properties`](../trino-catalog/iceberg.properties)
(Trino must be restarted to pick up changes: `docker compose restart trino`):

| Setting | Value here | Trino default | Effect |
|---|---|---|---|
| `iceberg.expire-snapshots.min-retention` | `0s` | 7d | Trino refuses to run `expire_snapshots` with a lower `retention_threshold`. **Local only — Limitation 1.** |
| `iceberg.remove-orphan-files.min-retention` | `0s` | 7d | Same, for `remove_orphan_files`. **Local only — Limitation 1.** |
| `iceberg.target-max-file-size` | `512MB` | 1GB | Target size of files Trino writes, including `optimize` output. |
| `iceberg.delete-after-commit-enabled` | `true` | — | Default for new tables: delete old `metadata.json` files on each commit… |
| `iceberg.max-previous-versions` | `5` | — | …keeping the current one plus this many previous. |

**Old `metadata.json` files.** Every commit writes a new one; none of the
four steps deletes old ones. They're trimmed on every commit by two table
properties:

| Iceberg table property | Trino table property | Catalog default above |
|---|---|---|
| `write.metadata.delete-after-commit.enabled` | `delete_after_commit_enabled` | `iceberg.delete-after-commit-enabled` |
| `write.metadata.previous-versions-max` | `max_previous_versions` | `iceberg.max-previous-versions` |

- The catalog defaults only apply to tables **Trino creates** (as in
  production). A single table can override them:
  `CREATE TABLE … WITH (max_previous_versions = N)`.
- Tables created elsewhere keep their own values. Locally that's every
  bronze table: `local_parsing` (Spark) creates them with
  `previous-versions-max = 5` (older tables may still have 10). Change one
  with `ALTER TABLE … SET PROPERTIES max_previous_versions = N`; the extra
  files are trimmed on the next commit.
- Check a table's values:
  ```sql
  SELECT key, value FROM iceberg.bronze."account_wide$properties"
  WHERE key LIKE 'write.metadata%';
  ```

## Verified

On an isolated copy of this stack (same Trino, Iceberg REST and MinIO images
and config; a format-v2 merge-on-read table like bronze), each step was run
alone and the files in object storage were listed before and after:

- Each step's deletes/adds match the [table above](#what-each-step-does).
- Two DAG runs in a row: day 1's `expire_snapshots` deleted no data files;
  day 2's deleted the 5 data + 2 delete files day 1's `optimize` replaced.
  `remove_orphan_files` deleted only a planted never-committed file.
- `optimize` output follows `iceberg.target-max-file-size`; a file above
  `file_size_threshold` is still rewritten once it has a delete file.
- `optimize_manifests` follows `commit.manifest.target-size-bytes`.
- A crash after `optimize` committed was rolled back to the exact
  pre-optimize snapshot; a crash inside `optimize` left the table unchanged
  and issued no rollback.
- `iceberg.max-previous-versions` applied to Trino-created tables only;
  `ALTER TABLE … SET PROPERTIES max_previous_versions` fixed an existing one.

## Limitations, and the fix for each, before production

### 1. `retention="0s"` is a local-testing value, not a production one

`trino-catalog/iceberg.properties` deliberately lowers Trino's own safety
floor (`iceberg.expire-snapshots.min-retention=0s`,
`iceberg.remove-orphan-files.min-retention=0s`) so this DAG does something
visible against snapshots that are only minutes old. In production, a
retention this short risks deleting files a slow, still-running read depends
on, or files a concurrent writer has written but not yet committed (which
look exactly like orphans to `remove_orphan_files`).

**Fix:** raise `retention` (and both `min-retention` settings) to a value
comfortably longer than the slowest realistic read or write. Trino's
default of 7 days is a reasonable starting point.

### 2. A rollback also discards other writers' commits

`rollback_to_snapshot` returns the table to the captured snapshot — so
anything *another* writer committed after it (e.g. an ingestion run landing
mid-compaction) is discarded too.

**Fix:** schedule this DAG away from ingestion (see Limitation 5).

### 3. The loop continues after a failed table — an open decision, not a bug

After a table fails the row-count guard (and is rolled back), the loop still
processes every remaining table before the task fails. A systemic bug can
therefore touch every table in one run — none is left in a wrong state, but
all get rolled back. Stopping on the first failure would bound that; it's a
small change if that tighter control is wanted. (A *crash* already stops
the loop immediately.)

### 4. First production run on a real backlog will not resemble local testing

Local runs take a few seconds per table against a handful of files. A
production table compacted for the first time against a real backlog
(10,000+ small files) is a different workload — dominated by object-store
round-trips and total data volume. Expect minutes to a few hours for that
first pass.

**Fix:** `default_args.execution_timeout` is `timedelta(hours=1)` — raise it
for the first catch-up run, or run that first pass manually before turning
on the `@daily` schedule. (A timeout mid-compaction is rolled back by the
crash guard, so the risk is wasted work, not a broken table.)

### 5. No coordination with concurrent writers (dbt, or Spark ingestion via `local_parsing`)

Iceberg's optimistic-concurrency commits guarantee no silent corruption if
this DAG runs at the same moment as a write to the same table — but a real
overlap (e.g. `optimize` rewriting a data file while a concurrent `MERGE`
adds delete files against it) can make either side's commit fail once
Iceberg's automatic retries are exhausted, requiring a re-run. Combined with
Limitation 2, a rollback can also discard the other writer's commit.

Spark's writes go directly to the Iceberg REST catalog and object store,
never through Trino, so Trino's `system.runtime.queries` can't see them
coming.

**Fix options, roughly in order of effort:**
- Simplest: schedule this DAG at a time known not to overlap with
  ingestion/dbt runs.
- More robust: an explicit lock — e.g. a small table in `iceberg-catalog-db`
  that writers acquire before committing and release in a `finally` block,
  checked by an Airflow sensor before `compact_tables` touches a table.
  Needs matching changes on the writer side.

### 6. `check_only` isn't a complete no-op

It skips every mutating statement and always skips `remove_orphans`, but
`compact_tables` still runs `discover_tables()` and `fetch_stats()` (a full
row count per table) against Trino. Cheap on these tables, but not zero.
