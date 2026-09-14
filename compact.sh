#!/usr/bin/env bash
#
# Iceberg table maintenance, run as Trino SQL.
#
#   bash compact.sh                          # maintain every table in bronze + staging
#   bash compact.sh bronze.account_wide      # just one table
#   bash compact.sh --check                  # report only, change nothing
#   RETENTION=0s bash compact.sh             # local testing (see RETENTION below)
#   ORPHANS=0 bash compact.sh                # skip step 3
#
# WHY THIS EXISTS
# ---------------
# Every run of run_parsing.py leaves the table more fragmented. The Spark
# write produces one small Parquet file per task, and because the tables are
# merge-on-read, each daily MERGE also writes a delete file marking the old
# version of every updated row. Nothing cleans either up. Queries then have to
# open every data file AND every delete file and subtract deleted rows on the
# fly, so read cost grows with the number of runs rather than the amount of
# data. Measured here: 2 daily runs on a 10k-row table produced 3 data files +
# 2 delete files holding 10,097 stored records for 10,001 live rows.
#
# THE THREE STEPS
# ---------------
#   1. optimize            Merges small files into large ones and applies the
#                          merge-on-read deletes, so the delete files go away.
#   2. expire_snapshots    Drops old table versions. Step 1 does NOT delete the
#                          files it replaced -- Iceberg keeps them so you can
#                          still time-travel -- so after step 1 alone storage
#                          GOES UP. This is the step that reclaims it.
#   3. remove_orphan_files Deletes files no snapshot references at all. Step 2
#                          only reaches files it can still trace through
#                          metadata; measured here, 5 files / 888 KB survived
#                          step 2 and needed this to be freed.
#
# RETENTION (default 7d)
# ----------------------
# How much table history to keep. This is also your time-travel window --
# anything older is permanently gone. Trino refuses a value below the
# catalog's min-retention (trino-catalog/iceberg.properties, set to 0s on this
# local stack only).
#
# SAFETY: step 3 deletes any unreferenced file older than RETENTION, and a file
# a concurrently running write job has staged but not yet committed looks
# exactly like an unreferenced file. Run maintenance when writers are idle, and
# do not shorten RETENTION in production to reclaim space faster.
set -euo pipefail

RETENTION="${RETENTION:-7d}"
SCHEMAS="${SCHEMAS:-bronze staging}"
ORPHANS="${ORPHANS:-1}"
CHECK_ONLY=0

TABLES=()
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    -*) echo "unknown flag: $arg" >&2; exit 2 ;;
    *) TABLES+=("$arg") ;;
  esac
done

trino_q() { docker compose exec -T trino trino --catalog iceberg --execute "$1" 2>/dev/null; }

# "<data files> <delete files> <bytes> <stored records> <live rows>"
stats() {
  local counts rows
  counts=$(trino_q "SELECT count_if(content = 0), count_if(content > 0),
                           coalesce(sum(file_size_in_bytes), 0), coalesce(sum(record_count), 0)
                    FROM iceberg.$1.\"$2\$files\"" | tr -d '"' | tr ',' ' ')
  rows=$(trino_q "SELECT count(*) FROM iceberg.$1.$2" | tr -d '"')
  echo "$counts $rows"
}

fmt() {  # data delete bytes -> "3 data + 2 delete, 867 KB"
  printf '%s data + %s delete, %s KB' "$1" "$2" "$(( $3 / 1024 ))"
}

maintain() {
  local schema="$1" table="$2" fq="iceberg.$1.$2"
  local d0 x0 b0 r0 n0 d1 x1 b1 r1 n1
  read -r d0 x0 b0 r0 n0 <<<"$(stats "$schema" "$table")"

  if [ "$CHECK_ONLY" = "1" ]; then
    printf '%-30s %s\n' "$schema.$table" "$(fmt "$d0" "$x0" "$b0")"
    # Stored records above live rows = dead rows still on disk behind delete files.
    if [ "$r0" -gt "$n0" ]; then
      printf '%-30s   %s stored records for %s live rows (%s dead)\n' "" "$r0" "$n0" "$((r0 - n0))"
    fi
    return 0
  fi

  trino_q "ALTER TABLE $fq EXECUTE optimize" >/dev/null
  trino_q "ALTER TABLE $fq EXECUTE expire_snapshots(retention_threshold => '$RETENTION')" >/dev/null
  if [ "$ORPHANS" = "1" ]; then
    trino_q "ALTER TABLE $fq EXECUTE remove_orphan_files(retention_threshold => '$RETENTION')" >/dev/null
  fi

  read -r d1 x1 b1 r1 n1 <<<"$(stats "$schema" "$table")"
  printf '%-30s %-28s -> %s\n' "$schema.$table" "$(fmt "$d0" "$x0" "$b0")" "$(fmt "$d1" "$x1" "$b1")"

  if [ "$n0" != "$n1" ]; then
    echo "  ERROR: row count changed $n0 -> $n1 for $fq" >&2
    return 1
  fi
}

if [ "${#TABLES[@]}" -eq 0 ]; then
  for schema in $SCHEMAS; do
    for table in $(trino_q "SHOW TABLES FROM iceberg.$schema" | tr -d '"'); do
      TABLES+=("$schema.$table")
    done
  done
fi

[ "$CHECK_ONLY" = "1" ] || echo "retention: $RETENTION   orphan sweep: $([ "$ORPHANS" = 1 ] && echo on || echo off)"
for t in "${TABLES[@]}"; do maintain "${t%%.*}" "${t#*.}"; done
