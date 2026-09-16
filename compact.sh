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
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$HERE/compact.sql"
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

[ -f "$SQL_FILE" ] || { echo "missing $SQL_FILE" >&2; exit 1; }

trino_q() { docker compose exec -T trino trino --catalog iceberg --execute "$1" 2>/dev/null; }

# "<data files> <delete files> <bytes> <stored records> <live rows>"
stats() {
  local counts rows
  # record_count must be summed over data files only -- a delete file's
  # record_count is its number of delete pointers, not stored rows.
  counts=$(trino_q "SELECT count_if(content = 0), count_if(content > 0),
                           coalesce(sum(file_size_in_bytes), 0),
                           coalesce(sum(record_count) FILTER (WHERE content = 0), 0)
                    FROM iceberg.$1.\"$2\$files\"" | tr -d '"' | tr ',' ' ')
  rows=$(trino_q "SELECT count(*) FROM iceberg.$1.$2" | tr -d '"')
  echo "$counts $rows"
}

fmt() { printf '%s data + %s delete, %s KB' "$1" "$2" "$(( $3 / 1024 ))"; }

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

  trino_q "$(render_sql "$fq")" >/dev/null

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

if [ "$CHECK_ONLY" != "1" ]; then
  echo "retention: $RETENTION   orphan sweep: $([ "$ORPHANS" = 1 ] && echo on || echo off)"
fi
for t in "${TABLES[@]}"; do maintain "${t%%.*}" "${t#*.}"; done
