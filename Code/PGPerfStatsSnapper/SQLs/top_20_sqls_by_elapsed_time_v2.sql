with get_sql_id as (
select sample_start_time,a.snap_id,dbid,userid,queryid,round(avg(calls)) AS calls,round(avg(total_plan_time + total_exec_time)) AS total_time,round(avg(rows)) AS rows,
round(avg(shared_blks_hit)) AS shared_blks_hit,round(avg(shared_blks_read)) AS shared_blks_read, round(avg(shared_blks_dirtied)) AS shared_blks_dirtied,
round(avg(shared_blks_written)) AS shared_blks_written, round(avg(local_blks_hit)) AS local_blks_hit, round(avg(local_blks_read)) AS local_blks_read,
round(avg(local_blks_dirtied)) As local_blks_dirtied, round(avg(local_blks_written)) AS local_blks_written, round(avg(temp_blks_read)) AS temp_blks_read,
round(avg(temp_blks_written)) AS temp_blks_written, round(avg(blk_read_time)) AS blk_read_time, round(avg(blk_write_time)) AS blk_write_time
from pg_stat_statements_history a, pg_awr_snapshots_cust b where a.snap_id = b.snap_id
and a.snap_id between :begin_snap_id and :end_snap_id
group by sample_start_time,a.snap_id,dbid,userid,queryid
order by dbid,userid,queryid,a.snap_id
),
get_lag_data as (
select sample_start_time,dbid,userid,queryid,snap_id,calls,
case WHEN (calls-lag(calls::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = calls then null
else (calls-lag(calls::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS delta_calls,
case WHEN (total_time-lag(total_time::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = total_time then null
else (total_time-lag(total_time::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END as Delta_total_time,
case WHEN (rows-lag(rows::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = rows then null
else (rows-lag(rows::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END as Delta_rows,
case WHEN (shared_blks_hit-lag(shared_blks_hit::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_hit then null
else (shared_blks_hit-lag(shared_blks_hit::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS Delta_shared_blks_hit,
case WHEN (shared_blks_read-lag(shared_blks_read::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_read then null
else (shared_blks_read-lag(shared_blks_read::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_shared_blks_read,
case WHEN (shared_blks_dirtied-lag(shared_blks_dirtied::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_dirtied then null
else (shared_blks_dirtied-lag(shared_blks_dirtied::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_shared_blks_dirtied,
case WHEN (shared_blks_written-lag(shared_blks_written::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = shared_blks_written then null
else (shared_blks_written-lag(shared_blks_written::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_shared_blks_written,
case WHEN (temp_blks_written-lag(temp_blks_written::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = temp_blks_written then null
else (temp_blks_written-lag(temp_blks_written::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_temp_blks_written,
case WHEN (blk_read_time-lag(blk_read_time::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) = blk_read_time then null
else (blk_read_time-lag(blk_read_time::numeric,1,0::numeric) OVER (partition by dbid, userid, queryid ORDER BY snap_id) ) END AS DELTA_blk_read_time
from get_sql_id
where  snap_id between :begin_snap_id-1 and :end_snap_id
)
,time_partitioned_data as (
select dbid,userid,queryid,round((sum(Delta_total_time)/sum(delta_calls))::numeric,2) AS AVG_ELAPSED_TIME,
sum(delta_calls) AS calls,
round((sum(Delta_shared_blks_hit)/sum(delta_calls))::numeric) AS avg_shared_blks_hit,
round((sum(Delta_rows)/sum(delta_calls))::numeric) AS avg_rows,
round((sum(DELTA_shared_blks_dirtied)/sum(delta_calls))::numeric) AS avg_shared_blks_dirtied,
round((sum(DELTA_shared_blks_read)/sum(delta_calls))::numeric) AS avg_shared_blks_read,
round((sum(DELTA_shared_blks_written)/sum(delta_calls))::numeric) AS avg_shared_blks_written,
round((sum(DELTA_temp_blks_written)/sum(delta_calls))::numeric) AS avg_temp_blks_written,
round((sum(DELTA_blk_read_time)/sum(delta_calls))::numeric,2) AS avg_blk_read_time
from get_lag_data
where Delta_total_time is not null
and (delta_calls is not null )
group by dbid,userid,queryid
having sum(delta_calls)>0
)
select * from time_partitioned_data
order by AVG_ELAPSED_TIME desc
limit 20;
