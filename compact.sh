#!/usr/bin/env bash
#
# Runner for compact.sql -- the maintenance logic lives there, this only
# feeds it to Trino per table and checks the result.
#
#   bash compact.sh                          # maintain every table in bronze + staging
#   bash compact.sh bronze.account_wide      # just one table
#   bash compact.sh --check                  # report fragmentation, change nothing
#   RETENTION=0s bash compact.sh             # local testing (see compact.sql)
#   ORPHANS=0 bash compact.sh                # skip step 3 of compact.sql
#
# Trino has no procedural SQL -- no loops, and ALTER TABLE ... EXECUTE needs a
# literal table name -- so table discovery, the before/after measurement and
# the row-count guard have to live out here rather than in the .sql.
#
# Every run's output (per-table + TOTAL compaction time) is appended to
# compact.log alongside the terminal, mirroring run_parsing.py's log file.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$HERE/compact.sql"
LOG_FILE="$HERE/compact.log"
RETENTION="${RETENTION:-0s}"
SCHEMAS="${SCHEMAS:-bronze staging}"
ORPHANS="${ORPHANS:-1}"
CHECK_ONLY=0

# terminal + log file, like run_parsing.py's dual logging
exec > >(tee -a "$LOG_FILE") 2>&1
echo "---- $(date -Iseconds) ----"

TABLES=()
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    -*) echo "unknown flag: $arg" >&2; exit 2 ;;
    *) TABLES+=("$arg") ;;
  esac
done

[ -f "$SQL_FILE" ] || { echo "missing $SQL_FILE" >&2; exit 1; }

trino_q() { docker compose exec -T trino trino --catalog iceberg --execute "$1" 2>/dev/null; }

# One "<data files> <delete files> <bytes> <stored records> <live rows>" row --
# both of the old stats() queries (file/byte counts + row count) merged into
# one SELECT via CROSS JOIN, so before+after fits in one trino_q call too.
stats_sql() {
  echo "SELECT f.data_files, f.delete_files, f.bytes, f.stored_records, t.live_rows
        FROM (SELECT count_if(content = 0) AS data_files, count_if(content > 0) AS delete_files,
                     coalesce(sum(file_size_in_bytes), 0) AS bytes,
                     coalesce(sum(record_count) FILTER (WHERE content = 0), 0) AS stored_records
              FROM iceberg.$1.\"$2\$files\") f
        CROSS JOIN (SELECT count(*) AS live_rows FROM iceberg.$1.$2) t;"
}

fmt() { printf '%s data + %s delete, %s KB' "$1" "$2" "$(( $3 / 1024 ))"; }

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }

TOTAL_COMPACT_SECONDS=0
# Delimits before/after stats blocks inside one batched trino_q call (see
# maintain()) -- a SELECT of this literal, so it comes back as its own CSV
# line ("MARK") regardless of how many rows the ALTER TABLE ... EXECUTE
# statements between them print (that row count varies: 0 when there's
# nothing to expire, several otherwise -- unsafe to split on a fixed offset).
SPLIT_MARK="COMPACTSH_SPLIT_MARKER"

# compact.sql with the placeholders filled in; step 3 dropped when ORPHANS=0.
render_sql() {
  local sql
  sql=$(sed -e "s|__TABLE__|$1|g" -e "s|__RETENTION__|$RETENTION|g" "$SQL_FILE")
  if [ "$ORPHANS" != "1" ]; then
    sql=$(echo "$sql" | grep -v 'EXECUTE remove_orphan_files')
  fi
  echo "$sql"
}

maintain() {
  local schema="$1" table="$2" fq="iceberg.$1.$2"

  if [ "$CHECK_ONLY" = "1" ]; then
    local d0 x0 b0 r0 n0
    read -r d0 x0 b0 r0 n0 <<<"$(trino_q "$(stats_sql "$schema" "$table")" | tr -d '"' | tr ',' ' ')"
    printf '%-30s %s\n' "$schema.$table" "$(fmt "$d0" "$x0" "$b0")"
    # Stored records above live rows = dead rows still on disk behind delete files.
    if [ "$r0" -gt "$n0" ]; then
      printf '%-30s   %s stored records for %s live rows (%s dead)\n' "" "$r0" "$n0" "$((r0 - n0))"
    fi
    return 0
  fi

  # before-stats ; MARK ; optimize/expire_snapshots/[remove_orphan_files] ; MARK ; after-stats
  # -- one docker exec / Trino CLI launch for the whole table instead of five.
  local batch out before after
  batch="$(stats_sql "$schema" "$table")
SELECT '$SPLIT_MARK';
$(render_sql "$fq")
SELECT '$SPLIT_MARK';
$(stats_sql "$schema" "$table")"

  local t0 t1 dt
  t0=$(now)
  out="$(trino_q "$batch")"
  t1=$(now)
  dt=$(elapsed "$t0" "$t1")
  TOTAL_COMPACT_SECONDS=$(awk -v a="$TOTAL_COMPACT_SECONDS" -v b="$dt" 'BEGIN{printf "%.2f", a+b}')

  before="$(awk -v m="\"$SPLIT_MARK\"" '$0==m{exit} {print}' <<<"$out")"
  after="$(awk -v m="\"$SPLIT_MARK\"" 'seen==2{print} $0==m{seen++}' <<<"$out")"

  local d0 x0 b0 r0 n0 d1 x1 b1 r1 n1
  read -r d0 x0 b0 r0 n0 <<<"$(tr -d '"' <<<"$before" | tr ',' ' ')"
  read -r d1 x1 b1 r1 n1 <<<"$(tr -d '"' <<<"$after" | tr ',' ' ')"

  printf '%-30s %-28s -> %-28s %6ss\n' "$schema.$table" "$(fmt "$d0" "$x0" "$b0")" "$(fmt "$d1" "$x1" "$b1")" "$dt"

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

if [ "$CHECK_ONLY" != "1" ]; then
  echo "retention: $RETENTION   orphan sweep: $([ "$ORPHANS" = 1 ] && echo on || echo off)"
fi

COMPACT_START=$(now)
for t in "${TABLES[@]}"; do maintain "${t%%.*}" "${t#*.}"; done

if [ "$CHECK_ONLY" != "1" ]; then
  echo "TOTAL compact step: $(elapsed "$COMPACT_START" "$(now)")s wall-clock" \
       "(sum per-table ${TOTAL_COMPACT_SECONDS}s) across ${#TABLES[@]} table(s)"
fi
