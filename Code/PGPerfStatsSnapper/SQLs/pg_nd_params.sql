\qecho 'Non-default Parameters'
\set QUIET 1

select name, source from pg_settings_cust where source != 'default' order by name;

\prompt 'Enter variable name to see details: ' var_name

\x

select name, setting, unit, boot_val, source from pg_settings_cust where name = :'var_name';

\x
\set QUIET 0
