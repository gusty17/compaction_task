-- Schema only -- no data. Run by init-scripts/00_setup.sh on a fresh DB.
-- Data is seeded separately: see funds_transfer/seed_funds_transfer.py.
--
-- Standalone re-run:
--   sed "s/__SCHEMA__/source_table/g" init-scripts/funds_transfer/create_funds_transfer_table.sql | docker exec -i oracle-xe sqlplus -s -L source_table/source_table@localhost:1521/XEPDB1

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE __SCHEMA__.funds_transfer';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE __SCHEMA__.funds_transfer (
  recid      VARCHAR2(255) NOT NULL PRIMARY KEY,
  xmlrecord  XMLTYPE
);
