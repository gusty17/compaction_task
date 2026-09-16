-- Schema only -- no data. Run by init-scripts/00_setup.sh on a fresh DB.
-- Data is seeded separately: see customer/seed_customer.py.
--
-- Standalone re-run:
--   sed "s/__SCHEMA__/source_table/g" init-scripts/customer/create_customer_table.sql | docker exec -i oracle-xe sqlplus -s -L source_table/source_table@localhost:1521/XEPDB1

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE __SCHEMA__.customer';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE __SCHEMA__.customer (
  recid      VARCHAR2(255) NOT NULL PRIMARY KEY,
  xmlrecord  XMLTYPE
);
