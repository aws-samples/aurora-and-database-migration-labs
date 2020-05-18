--tup_returned_per_sec : tuples read/scanned by quries
--tup_fetched_per_sec: tuples returned as query output
--High value of tup_returned_per_sec compared to tup_fetched_per_sec indicates indexing opportunties

with pg_stat_database_vw as (
select a.snap_id,sample_start_time,datname,numbackends,
xact_commit ,
xact_rollback   ,
blks_read  as blks_read       ,
blks_hit  as blks_hit      ,
tup_returned  as tup_returned  ,
tup_fetched   as tup_fetched  ,
tup_inserted   as tup_inserted ,
tup_updated    as tup_updated ,
tup_deleted   as tup_deleted ,
conflicts       ,
temp_files      ,
temp_bytes   as temp_bytes   ,
deadlocks       ,
blk_read_time   ,
blk_write_time  ,
stats_reset
from pg_stat_database_history a, pg_awr_snapshots_cust b where a.snap_id = b.snap_id
and a.snap_id between :begin_snap_id and :end_snap_id
and datname not in ('rdsadmin','template0','template1')
)
,get_lag_data as (select snap_id,sample_start_time,datname,
(lead(xact_commit::bigint,0) over (partition by datname order by snap_id)-lead(xact_commit::bigint,-1) over (partition by datname order by snap_id))/60 as delta_xact_commit,
(lead(xact_rollback::bigint,0) over (partition by datname order by snap_id)-lead(xact_rollback::bigint,-1) over (partition by datname order by snap_id))/60 as delta_xact_rollback,
(lead(blks_read::bigint,0) over (partition by datname order by snap_id)-lead(blks_read::bigint,-1) over (partition by datname order by snap_id))/60 as delta_blks_read,
(lead(blks_hit::bigint,0) over (partition by datname order by snap_id)-lead(blks_hit::bigint,-1) over (partition by datname order by snap_id))/60 as delta_blks_hit,
(lead(tup_returned::bigint,0) over (partition by datname order by snap_id)-lead(tup_returned::bigint,-1) over (partition by datname order by snap_id))/60 as delta_tup_returned,
(lead(tup_fetched::bigint,0) over (partition by datname order by snap_id)-lead(tup_fetched::bigint,-1) over (partition by datname order by snap_id))/60 as delta_tup_fetched,
(lead(tup_inserted::bigint,0) over (partition by datname order by snap_id)-lead(tup_inserted::bigint,-1) over (partition by datname order by snap_id))/60 as delta_tup_inserted,
(lead(tup_updated::bigint,0) over (partition by datname order by snap_id)-lead(tup_updated::bigint,-1) over (partition by datname order by snap_id))/60 as delta_tup_updated,
(lead(tup_deleted::bigint,0) over (partition by datname order by snap_id)-lead(tup_deleted::bigint,-1) over (partition by datname order by snap_id))/60 as delta_tup_deleted,
(lead(conflicts::bigint,0) over (partition by datname order by snap_id)-lead(conflicts::bigint,-1) over (partition by datname order by snap_id))/60 as delta_conflicts,
(lead(temp_files::bigint,0) over (partition by datname order by snap_id)-lead(temp_files::bigint,-1) over (partition by datname order by snap_id))/60 as delta_temp_files,
(lead(temp_bytes::bigint,0) over (partition by datname order by snap_id)-lead(temp_bytes::bigint,-1) over (partition by datname order by snap_id))/60 as delta_temp_bytes,
(lead(deadlocks::bigint,0) over (partition by datname order by snap_id)-lead(deadlocks::bigint,-1) over (partition by datname order by snap_id))/60 as delta_deadlocks,
(lead(blk_read_time::bigint,0) over (partition by datname order by snap_id)-lead(blk_read_time::bigint,-1) over (partition by datname order by snap_id))/60 as delta_blk_read_time,
(lead(blk_write_time::bigint,0) over (partition by datname order by snap_id)-lead(blk_write_time::bigint,-1) over (partition by datname order by snap_id))/60 as delta_blk_write_time,
stats_reset
from pg_stat_database_vw)
select datname,trunc(avg(delta_xact_commit)) as xact_commit_per_sec,
trunc(avg(delta_xact_rollback)) as xact_rollback_per_sec,
trunc(avg(delta_blks_read)) as blks_read_per_sec,
trunc(avg(delta_blks_hit)) as blks_hit_per_sec,
trunc(avg(delta_tup_returned)) as tup_returned_per_sec,
trunc(avg(delta_tup_fetched)) as tup_fetched_per_sec,
trunc(avg(delta_tup_inserted)) as tup_inserted_per_sec,
trunc(avg(delta_tup_updated)) as tup_updated_per_sec,
trunc(avg(delta_tup_deleted)) as tup_deleted_per_sec,
trunc(avg(delta_conflicts)) as conflicts_per_sec,
trunc(avg(delta_temp_files)) as temp_files_per_sec,
trunc(avg(delta_deadlocks)) as deadlocks_per_sec,
trunc(avg(delta_blk_read_time)) as  blk_read_time_per_sec,
trunc(avg(delta_blk_write_time)) as blk_write_time_per_sec
from get_lag_data
group by datname;
