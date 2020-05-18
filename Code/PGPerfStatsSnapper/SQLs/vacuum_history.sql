select a.snap_id, pid, usename, query, state, b.sample_start_time - query_start Query_Age, b.sample_start_time - xact_start Trans_Age
from pg_stat_activity_history a, pg_awr_snapshots_cust b
where a.snap_id=b.snap_id
and (query like 'autovacuum%' or query like 'vacuum%')
and a.snap_id between :begin_snap_id and :end_snap_id
order by b.sample_start_time - xact_start desc;
