select a.snap_id,application_name,pid,wait_event_type,wait_event,b.sample_start_time-least(query_start,xact_start) as runtime, query as current_query
from pg_stat_activity_history a, pg_awr_snapshots_cust b
where a.snap_id=b.snap_id
and query NOT ILIKE '%pg_stat_activity%'
and usename!='rdsadmin'
and a.snap_id between :begin_snap_id and :end_snap_id
order by a.snap_id,runtime;
