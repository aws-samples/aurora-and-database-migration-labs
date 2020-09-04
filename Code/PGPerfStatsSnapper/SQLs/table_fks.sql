\prompt 'Enter Parent table name: ' parent_tab_name

select 
	con.schema_name,
    con.constraint_name,
	con.child_table,
    att2.attname as "child_column", 
    cl.relname as "parent_table", 
    att.attname as "parent_column"
from
   (select 
        unnest(con1.conkey) as "parent", 
        unnest(con1.confkey) as "child", 
        con1.conname as constraint_name,
        con1.confrelid, 
        con1.conrelid,
        cl.relname as child_table,
        ns.nspname as schema_name
    from 
        pg_class_cust cl
        join pg_namespace_cust ns on cl.relnamespace = ns.oid
        join pg_constraint_cust con1 on con1.conrelid = cl.oid
    where  con1.contype = 'f'
   ) con
   join pg_attribute_cust att on
       att.attrelid = con.confrelid and att.attnum = con.child
   join pg_class_cust cl on
       cl.oid = con.confrelid
   join pg_attribute_cust att2 on
       att2.attrelid = con.conrelid and att2.attnum = con.parent
   where cl.relname=lower(:'parent_tab_name'); 
