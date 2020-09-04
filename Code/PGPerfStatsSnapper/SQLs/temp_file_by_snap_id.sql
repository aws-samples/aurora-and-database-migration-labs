-- Stats related to temporary files created for sorting, hashing etc. because of work_mem spill 

select a.snap_id,sample_start_time, datname,
case WHEN (temp_files-lag(temp_files::numeric,1,0::numeric) OVER (partition by datname ORDER BY a.snap_id) ) = temp_files then null
else (temp_files-lag(temp_files::numeric,1,0::numeric) OVER (partition by datname ORDER BY a.snap_id) ) END AS delta_temp_files,
case WHEN (temp_bytes-lag(temp_bytes::numeric,1,0::numeric) OVER (partition by datname ORDER BY a.snap_id) ) = temp_bytes then null
else (pg_size_pretty(temp_bytes-lag(temp_bytes::numeric,1,0::numeric) OVER (partition by datname ORDER BY a.snap_id)) ) END AS delta_total_temp_bytes
from pg_stat_database_history a,  pg_awr_snapshots_cust b 
where a.snap_id = b.snap_id
and datname not in ('template0','template1','rdsadmin')
and a.snap_id between :begin_snap_id and :end_snap_id;
