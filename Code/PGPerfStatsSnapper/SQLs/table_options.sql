select nspname "schema_name",relname,reloptions 
from pg_class_cust pc,pg_namespace_cust pn
where pc.relnamespace=pn.oid
and reloptions is not null
and nspname not in ('pg_catalog','pg_toast')
order by 1,2;