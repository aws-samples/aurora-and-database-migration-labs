with get_cpu_percentage as (
SELECT snap_id,dbid,userid,queryid,substring(query, 1, 50) AS short_query,
round((total_plan_time + total_exec_time)::numeric, 2) AS total_time_ms,
calls,
round((mean_plan_time+mean_exec_time)::numeric, 2) AS mean_time_ms,
round((100 * (total_plan_time + total_exec_time) / sum((total_plan_time + total_exec_time)::numeric) OVER (partition by snap_id))::numeric, 2) AS percentage_cpu
FROM  pg_stat_statements_history
where snap_id between :begin_snap_id and :end_snap_id
),
get_cpu_percentage_rank as (
select snap_id,dbid,userid,queryid,short_query,total_time_ms,calls,mean_time_ms,percentage_cpu,
rank() OVER (PARTITION BY snap_id ORDER BY percentage_cpu DESC) as pos
from get_cpu_percentage
)
select snap_id,dbid,userid,queryid,short_query,total_time_ms,calls,mean_time_ms,percentage_cpu from get_cpu_percentage_rank where pos <= 10;