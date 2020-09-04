\prompt 'Enter table name: ' tabname

SELECT
c.nspname as "Schema_Name",
b.relname as "Table_Name",
a.attname as "Column",
pg_catalog.format_type(a.atttypid, a.atttypmod) as "Datatype",
not(a.attnotnull) AS "Nullable"
FROM
pg_attribute_cust a, pg_class_cust b, pg_namespace_cust c
WHERE
a.attrelid=b.oid
AND b.relnamespace=c.oid
AND a.attnum > 0
AND NOT a.attisdropped
AND a.attrelid = (
SELECT c.oid
FROM pg_class_cust c LEFT JOIN pg_namespace_cust n ON n.oid = c.relnamespace
WHERE c.relname=lower(:'tabname'));
