select extname,extversion,rolname as owner,nspname as schema 
from pg_extension_cust pe, pg_roles_cust pr, pg_namespace_cust pn 
where pe.extowner=pr.oid and pe.extnamespace=pn.oid;