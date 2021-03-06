with pg_stat_all_tables_vw as  (
 select
 a.snap_id             ,
 sample_start_time         ,
 relid               ,
 schemaname          ,
 relname             ,
 seq_scan            ,
 seq_tup_read        ,
 idx_scan            ,
 idx_tup_fetch       ,
 n_tup_ins           ,
 n_tup_upd           ,
 n_tup_del           ,
 n_tup_hot_upd       ,
 n_live_tup          ,
 n_dead_tup          ,
 autovacuum_count    
 from pg_stat_all_tables_history a,  pg_awr_snapshots_cust b where a.snap_id = b.snap_id
 and a.snap_id between :begin_snap_id and :end_snap_id
 and schemaname not in ('pg_catalog')),
 get_delta_data as (select snap_id,relid,sample_start_time,schemaname,relname,
 case WHEN (seq_scan-lag(seq_scan::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = seq_scan then null
 else (seq_scan-lag(seq_scan::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_seq_scan,
 case WHEN (seq_tup_read-lag(seq_tup_read::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = seq_tup_read then null
 else (seq_tup_read-lag(seq_tup_read::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_seq_tup_read,
 case WHEN (idx_scan-lag(idx_scan::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = idx_scan then null
 else (idx_scan-lag(idx_scan::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_idx_scan,
 case WHEN (idx_tup_fetch-lag(idx_tup_fetch::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = idx_tup_fetch then null
 else (idx_tup_fetch-lag(idx_tup_fetch::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_idx_tup_fetch,
 case WHEN (n_tup_ins-lag(n_tup_ins::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = n_tup_ins then null
 else (n_tup_ins-lag(n_tup_ins::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_n_tup_ins,
 case WHEN (n_tup_upd-lag(n_tup_upd::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = n_tup_upd then null
 else (n_tup_upd-lag(n_tup_upd::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_n_tup_upd,
 case WHEN (n_tup_del-lag(n_tup_del::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = n_tup_del then null
 else (n_tup_del-lag(n_tup_del::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_n_tup_del,
 case WHEN (n_tup_hot_upd-lag(n_tup_hot_upd::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = n_tup_hot_upd then null
 else (n_tup_hot_upd-lag(n_tup_hot_upd::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_n_tup_hot_upd,
 case WHEN (n_live_tup-lag(n_live_tup::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = n_live_tup then null
 else (n_live_tup-lag(n_live_tup::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_n_live_tup,
 case WHEN (n_dead_tup-lag(n_dead_tup::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = n_dead_tup then null
 else (n_dead_tup-lag(n_dead_tup::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_n_dead_tup,
 case WHEN (autovacuum_count-lag(autovacuum_count::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) = autovacuum_count then null
 else (autovacuum_count-lag(autovacuum_count::bigint,1,0::bigint) OVER (partition by schemaname,relname ORDER BY snap_id) ) END AS delta_autovacuum_count
 from pg_stat_all_tables_vw)
 select relid,schemaname,relname,sum(delta_seq_scan) AS total_full_table_scan,
 round((sum(delta_seq_tup_read)/sum(delta_seq_scan))::numeric,2) AS avg_rows_per_fts,
 round((sum(delta_idx_tup_fetch)/sum(delta_seq_scan))::numeric,2) AS avg_rows_fetch_per_index,
 round((sum(delta_n_tup_ins)/sum(delta_seq_scan))::numeric,2) AS avg_rows_inserted,
 round((sum(delta_n_tup_upd)/sum(delta_seq_scan))::numeric,2) AS avg_rows_updated,
 round((sum(delta_n_tup_del)/sum(delta_seq_scan))::numeric,2) AS avg_rows_deleted,
 round((sum(delta_n_tup_hot_upd)/sum(delta_seq_scan))::numeric,2) AS avg_rows_hot_updated,
 max(delta_n_live_tup) AS max_live_rows,
 max(delta_n_dead_tup)AS max_dead_rows,
 sum(delta_autovacuum_count) AS total_autovaccum_initiated
 from get_delta_data
 where delta_seq_scan>0
 group by schemaname,relname,relid
 having (sum(delta_seq_scan) is not null )
 order by total_full_table_scan DESC
limit 20;
