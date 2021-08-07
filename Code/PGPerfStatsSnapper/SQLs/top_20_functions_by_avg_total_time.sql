with get_func as (
select sample_start_time,a.snap_id, schemaname AS schema_name, funcname AS func_name,round(calls) AS calls,
round(total_time) AS total_time, round(self_time) AS self_time
from pg_stat_user_functions_history a, pg_awr_snapshots_cust b 
where a.snap_id = b.snap_id
and a.snap_id between :begin_snap_id and :end_snap_id
order by schemaname,funcname,a.snap_id
),
get_lag_data as (
select sample_start_time,schema_name,func_name,snap_id,calls,
case WHEN (calls-lag(calls::numeric,1,0::numeric) OVER (partition by schema_name,func_name ORDER BY snap_id) ) = calls then null
else (calls-lag(calls::numeric,1,0::numeric) OVER (partition by schema_name,func_name ORDER BY snap_id) ) END AS delta_calls,
case WHEN (total_time-lag(total_time::numeric,1,0::numeric) OVER (partition by schema_name,func_name ORDER BY snap_id) ) = total_time then null
else (total_time-lag(total_time::numeric,1,0::numeric) OVER (partition by schema_name,func_name ORDER BY snap_id) ) END as Delta_total_time,
case WHEN (self_time-lag(self_time::numeric,1,0::numeric) OVER (partition by schema_name,func_name ORDER BY snap_id) ) = self_time then null
else (self_time-lag(self_time::numeric,1,0::numeric) OVER (partition by schema_name,func_name ORDER BY snap_id) ) END as Delta_self_time
from get_func
where  snap_id between :begin_snap_id-1 and :end_snap_id
)
,time_partitioned_data as (
select schema_name,func_name,round((sum(Delta_total_time)/sum(delta_calls))::numeric,2) AS AVG_TOTAL_TIME,
sum(delta_calls) AS calls,
round((sum(Delta_self_time)/sum(delta_calls))::numeric) AS AVG_SELF_TIME
from get_lag_data
where Delta_total_time is not null
and delta_calls is not null
group by schema_name,func_name
having sum(delta_calls)>0
)
select * from time_partitioned_data
order by AVG_TOTAL_TIME desc
limit 20;
