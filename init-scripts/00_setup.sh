
# This script sets up the Oracle database for the Trino XML project.
# It creates the necessary users and tables -- schema only, no data.
# Runs once, automatically, on a fresh DB volume (gvenzl's init mechanism).
#
# Data is seeded separately, per table, from the host -- e.g.:
#   pip install -r requirements.txt
#   python init-scripts/account/seed_account.py
#
# Adding a table: drop init-scripts/<table>/create_<table>.sql (schema-
# qualified via __SCHEMA__, same pattern as init-scripts/account/) -- this
# script picks it up automatically, no edits needed here.

set -euo pipefail

SCHEMA="${ORACLE_SCHEMA:-source_table}"
SCHEMA_PWD="${ORACLE_SCHEMA_PASSWORD:-source_table}"
APP_USER="${ORACLE_APP_USER:-}"
APP_PWD="${ORACLE_APP_PASSWORD:-}"
PDB="XEPDB1"                       # fixed PDB name for Oracle XE 21c
FIXTURES="/opt/fixtures"

echo "[00_setup] owning schema = ${SCHEMA} ; app user = ${APP_USER:-<none>}"

# ── 1 & 2: users, as SYSDBA ────────────────────────────────────────────────────
sqlplus -s -L / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = ${PDB};

DECLARE
  n INT;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_users WHERE username = UPPER('${SCHEMA}');
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER ${SCHEMA} IDENTIFIED BY "${SCHEMA_PWD}"';
  END IF;
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO ${SCHEMA}';
  EXECUTE IMMEDIATE 'ALTER USER ${SCHEMA} QUOTA UNLIMITED ON USERS';
END;
/

DECLARE
  u VARCHAR2(128) := UPPER('${APP_USER}');
  n INT;
BEGIN
  IF u IS NOT NULL AND u NOT IN ('SYS', 'SYSTEM') THEN
    SELECT COUNT(*) INTO n FROM dba_users WHERE username = u;
    IF n = 0 THEN
      EXECUTE IMMEDIATE 'CREATE USER ${APP_USER} IDENTIFIED BY "${APP_PWD}"';
    END IF;
    EXECUTE IMMEDIATE 'GRANT CONNECT TO ${APP_USER}';
  END IF;
END;
/
SQL

# ── 3: create tables, schema-qualified via __SCHEMA__ substitution ────────────
RENDERED="$(mktemp -d)"
trap 'rm -rf "${RENDERED}"' EXIT

for TABLE_SQL in "${FIXTURES}"/*/create_*.sql; do
  [ -e "${TABLE_SQL}" ] || continue
  NAME="$(basename "${TABLE_SQL}")"
  echo "[00_setup] creating table from ${NAME}"
  sed "s/__SCHEMA__/${SCHEMA}/g" "${TABLE_SQL}" > "${RENDERED}/${NAME}"
  sqlplus -s -L "${SCHEMA}/${SCHEMA_PWD}@localhost:1521/${PDB}" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET DEFINE OFF
SET SQLBLANKLINES ON
@${RENDERED}/${NAME}
SQL
done

# ── 4: let everyone read the source tables ───────────────────────────────────
echo "[00_setup] GRANT SELECT ON ${SCHEMA}.* TO PUBLIC"
sqlplus -s -L / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = ${PDB};
BEGIN
  FOR t IN (SELECT table_name FROM dba_tables WHERE owner = UPPER('${SCHEMA}')) LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON ${SCHEMA}.' || t.table_name || ' TO PUBLIC';
  END LOOP;
END;
/
SQL

echo "[00_setup] done"
