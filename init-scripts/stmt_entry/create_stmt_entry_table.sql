-- Schema only -- no data. Run by init-scripts/00_setup.sh on a fresh DB.
-- Data is seeded separately: see stmt_entry/seed_stmt_entry.py.
--
-- Standalone re-run:
--   sed "s/__SCHEMA__/source_table/g" init-scripts/stmt_entry/create_stmt_entry_table.sql | docker exec -i oracle-xe sqlplus -s -L source_table/source_table@localhost:1521/XEPDB1

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE __SCHEMA__.stmt_entry';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE __SCHEMA__.stmt_entry (
  recid      VARCHAR2(255) NOT NULL PRIMARY KEY,
  xmlrecord  XMLTYPE
);
