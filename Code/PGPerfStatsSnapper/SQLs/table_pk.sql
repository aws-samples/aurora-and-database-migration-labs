\prompt 'Enter table name: ' tabname

SELECT n.nspname as "Schema_Name", c.relname as "Table_Name", a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
FROM   pg_index_cust i
JOIN   pg_attribute_cust a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
JOIN   pg_class_cust c on a.attrelid = c.oid
JOIN   pg_namespace_cust n on c.relnamespace = n.oid
WHERE  c.relname = :'tabname'
AND    i.indisprimary;