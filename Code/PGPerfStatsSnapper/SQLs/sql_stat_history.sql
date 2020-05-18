\prompt 'Enter hashed queryid (shown in Top x queries reports): ' hashed_queryid

select distinct query from pg_stat_statements_history
where snap_id between :begin_snap_id-1 and :end_snap_id
and hashtext(query) = :hashed_queryid;

with get_sql_id as (
select sample_start_time,a.snap_id,hashtext(query) AS hashed_queryid,sum(calls) AS calls,sum(total_time) AS total_time,sum(rows) AS rows,
sum(shared_blks_hit) AS shared_blks_hit,sum(shared_blks_read) AS shared_blks_read, sum(shared_blks_dirtied) AS shared_blks_dirtied,
sum(shared_blks_written) AS shared_blks_written, sum(local_blks_hit) AS local_blks_hit, sum(local_blks_read) AS local_blks_read,
sum(local_blks_dirtied) As local_blks_dirtied, sum(local_blks_written) AS local_blks_written, sum(temp_blks_read) AS temp_blks_read,
sum(temp_blks_written) AS temp_blks_written, sum(blk_read_time) AS blk_read_time, sum(blk_write_time) AS blk_write_time
from pg_stat_statements_history a, pg_awr_snapshots_cust b where a.snap_id = b.snap_id
and a.snap_id between :begin_snap_id and :end_snap_id
and hashtext(query) = :hashed_queryid
group by hashtext(query),a.snap_id,sample_start_time order by hashed_queryid,a.snap_id
),
get_lag_data as (
select sample_start_time,hashed_queryid,snap_id,calls,
case WHEN (calls-lag(calls::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = calls then null
else (calls-lag(calls::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS delta_calls,
case WHEN (total_time-lag(total_time::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = total_time then null
else (total_time-lag(total_time::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END as Delta_total_time,
case WHEN (rows-lag(rows::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = rows then null
else (rows-lag(rows::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END as Delta_rows,
case WHEN (shared_blks_hit-lag(shared_blks_hit::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = shared_blks_hit then null
else (shared_blks_hit-lag(shared_blks_hit::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS Delta_shared_blks_hit,
case WHEN (shared_blks_read-lag(shared_blks_read::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = shared_blks_read then null
else (shared_blks_read-lag(shared_blks_read::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS DELTA_shared_blks_read,
case WHEN (shared_blks_dirtied-lag(shared_blks_dirtied::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = shared_blks_dirtied then null
else (shared_blks_dirtied-lag(shared_blks_dirtied::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS DELTA_shared_blks_dirtied,
case WHEN (shared_blks_written-lag(shared_blks_written::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = shared_blks_written then null
else (shared_blks_written-lag(shared_blks_written::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS DELTA_shared_blks_written,
case WHEN (temp_blks_written-lag(temp_blks_written::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = temp_blks_written then null
else (temp_blks_written-lag(temp_blks_written::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS DELTA_temp_blks_written,
case WHEN (blk_read_time-lag(blk_read_time::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) = blk_read_time then null
else (blk_read_time-lag(blk_read_time::bigint,1,0::bigint) OVER (partition by hashed_queryid ORDER BY hashed_queryid) ) END AS DELTA_blk_read_time
from get_sql_id
where  snap_id between :begin_snap_id-1 and :end_snap_id
)
select sample_start_time,hashed_queryid,snap_id,calls as calls,delta_calls as delta_calls,
round((Delta_rows/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "rows/exec",
round((delta_total_time/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "elapsed_time_msec/exec",
round((Delta_shared_blks_hit/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "shared_blk_hit/exec",
round((DELTA_shared_blks_read/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as  "shared_blks_read/exec",
round((DELTA_shared_blks_written/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "shared_blks_written/exec",
round((DELTA_temp_blks_written/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "temp_blks_written/exec",
round((DELTA_blk_read_time/(case when delta_calls <=0 then 1 else delta_calls end))::numeric,2) as "blk_read_time/exec"
from get_lag_data
order by snap_id;
