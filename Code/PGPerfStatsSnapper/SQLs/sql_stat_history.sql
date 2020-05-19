\prompt 'Enter queryid (shown in Top x queries reports): ' query_id

select distinct query from pg_stat_statements_history
where snap_id between :begin_snap_id and :end_snap_id
and queryid = :query_id;

with get_sql_id as (
select sample_start_time,a.snap_id,dbid,userid,queryid,round(avg(calls)) AS calls,round(avg(total_time)) AS total_time,round(avg(rows)) AS rows,
round(avg(shared_blks_hit)) AS shared_blks_hit,round(avg(shared_blks_read)) AS shared_blks_read, round(avg(shared_blks_dirtied)) AS shared_blks_dirtied,
round(avg(shared_blks_written)) AS shared_blks_written, round(avg(local_blks_hit)) AS local_blks_hit, round(avg(local_blks_read)) AS local_blks_read,
round(avg(local_blks_dirtied)) As local_blks_dirtied, round(avg(local_blks_written)) AS local_blks_written, round(avg(temp_blks_read)) AS temp_blks_read,
round(avg(temp_blks_written)) AS temp_blks_written, round(avg(blk_read_time)) AS blk_read_time, round(avg(blk_write_time)) AS blk_write_time
from pg_stat_statements_history a, pg_awr_snapshots_cust b where a.snap_id = b.snap_id
and a.snap_id between :begin_snap_id and :end_snap_id
and queryid = :query_id
group by sample_start_time, a.snap_id, dbid, userid, queryid
order by dbid,userid,queryid,a.snap_id
),
get_lag_data as (
select sample_start_time,dbid,userid,queryid,snap_id,calls,
case WHEN (calls-lag(calls::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = calls then null
else (calls-lag(calls::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS delta_calls,
case WHEN (total_time-lag(total_time::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = total_time then null
else (total_time-lag(total_time::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END as Delta_total_time,
case WHEN (rows-lag(rows::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = rows then null
else (rows-lag(rows::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END as Delta_rows,
case WHEN (shared_blks_hit-lag(shared_blks_hit::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_hit then null
else (shared_blks_hit-lag(shared_blks_hit::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS Delta_shared_blks_hit,
case WHEN (shared_blks_read-lag(shared_blks_read::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_read then null
else (shared_blks_read-lag(shared_blks_read::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_shared_blks_read,
case WHEN (shared_blks_dirtied-lag(shared_blks_dirtied::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_dirtied then null
else (shared_blks_dirtied-lag(shared_blks_dirtied::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_shared_blks_dirtied,
case WHEN (shared_blks_written-lag(shared_blks_written::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_written then null
else (shared_blks_written-lag(shared_blks_written::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_shared_blks_written,
case WHEN (temp_blks_written-lag(temp_blks_written::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = temp_blks_written then null
else (temp_blks_written-lag(temp_blks_written::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_temp_blks_written,
case WHEN (blk_read_time-lag(blk_read_time::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = blk_read_time then null
else (blk_read_time-lag(blk_read_time::bigint,1,0::bigint) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_blk_read_time
from get_sql_id
where  snap_id between :begin_snap_id-1 and :end_snap_id
)
select sample_start_time,dbid,userid,queryid,snap_id,calls as calls,delta_calls as delta_calls,
round((Delta_rows/(case when delta_calls <=0 then 1 else delta_calls end))::numeric) as "rows/exec",
round((delta_total_time/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "elapsed_time_msec/exec",
round((Delta_shared_blks_hit/(case when delta_calls <=0 then 1 else delta_calls end))::numeric) as "shared_blk_hit/exec",
round((DELTA_shared_blks_read/(case when delta_calls <=0 then 1 else delta_calls end))::numeric) as  "shared_blks_read/exec",
round((DELTA_shared_blks_written/(case when delta_calls <=0 then 1 else delta_calls end))::numeric) as "shared_blks_written/exec",
round((DELTA_temp_blks_written/(case when delta_calls <=0 then 1 else delta_calls end))::numeric) as "temp_blks_written/exec",
round((DELTA_blk_read_time/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "blk_read_time/exec"
from get_lag_data
order by dbid,userid,queryid,snap_id;
