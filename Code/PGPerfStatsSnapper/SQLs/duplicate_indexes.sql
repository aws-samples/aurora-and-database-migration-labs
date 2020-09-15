SELECT pc.relname AS TableName ,array_agg(pci.relname) AS Indexes
FROM pg_index_cust pi , pg_class_cust pc, pg_class_cust pci
where pi.indrelid=pc.oid
and pi.indexrelid=pci.oid
GROUP BY pc.relname ,pi.indkey
HAVING COUNT(*) > 1;
