with pg_stat_all_indexes_vw as  (
 select a.snap_id ,
 sample_start_time    ,
 relid          ,
 indexrelid     ,
 schemaname     ,
 relname        ,
 indexrelname   ,
 idx_scan       ,
 idx_tup_read   ,
 idx_tup_fetch  from pg_stat_all_indexes_history a, pg_awr_snapshots_cust b where a.snap_id = b.snap_id
 and a.snap_id between :begin_snap_id and :end_snap_id),
 get_delta_data as (select snap_id,sample_start_time,indexrelid,schemaname,relname,indexrelname,
 case WHEN (idx_scan-lag(idx_scan::bigint,1,0::bigint) OVER (partition by schemaname,relname,indexrelname ORDER BY snap_id) ) = idx_scan then null
 else (idx_scan-lag(idx_scan::bigint,1,0::bigint) OVER (partition by schemaname,relname,indexrelname ORDER BY snap_id) ) END AS delta_idx_scan,
 case WHEN (idx_tup_read-lag(idx_tup_read::bigint,1,0::bigint) OVER (partition by schemaname,relname,indexrelname ORDER BY snap_id) ) = idx_tup_read then null
 else (idx_tup_read-lag(idx_tup_read::bigint,1,0::bigint) OVER (partition by schemaname,relname,indexrelname ORDER BY snap_id) ) END AS delta_idx_tup_read,
 case WHEN (idx_tup_fetch-lag(idx_tup_fetch::bigint,1,0::bigint) OVER (partition by schemaname,relname,indexrelname ORDER BY snap_id) ) = idx_tup_fetch then null
 else (idx_tup_fetch-lag(idx_tup_fetch::bigint,1,0::bigint) OVER (partition by schemaname,relname,indexrelname ORDER BY snap_id) ) END AS delta_idx_tup_fetch
 from pg_stat_all_indexes_vw)
 select indexrelid,schemaname,relname,indexrelname,sum(delta_idx_scan) AS total_index_scan,round((sum(delta_idx_tup_read)/sum(delta_idx_scan))::numeric,2) AS avg_index_tuple_reads,
 round((sum(delta_idx_tup_fetch)/sum(delta_idx_scan))::numeric,2) AS avg_index_tuple_fetch
 from get_delta_data
 where delta_idx_scan>0
 group by indexrelid,schemaname,relname,indexrelname
 having (sum(delta_idx_scan) is not null and sum(delta_idx_tup_read) is not null)
  order by 6 DESC,7 DESC
limit 20;
