\prompt 'Enter table name: ' tabname

\set wildchar_tabname '%' :tabname '%'

select snap_id,queryid,query from pg_stat_statements_history where lower(query) like lower(:'wildchar_tabname') and snap_id = (select max(snap_id) from pg_awr_snapshots_cust);
