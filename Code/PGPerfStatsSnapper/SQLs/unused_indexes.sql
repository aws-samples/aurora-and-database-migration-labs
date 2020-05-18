with max_snap_id as (select max(snap_id) as snap_id from pg_awr_snapshots_cust)
SELECT idx_stat.relid, idx_stat.indexrelid,
        idx_stat.schemaname, idx_stat.relname as tablename,
        idx_stat.indexrelname as indexname,
        idx_stat.idx_scan,
        indexdef ~* 'USING btree' AS idx_is_btree
FROM pg_stat_all_indexes_history as idx_stat
JOIN pg_index_cust USING (indexrelid)
JOIN pg_indexes_cust as indexes ON idx_stat.schemaname = indexes.schemaname AND idx_stat.relname = indexes.tablename AND idx_stat.indexrelname = indexes.indexname
JOIN max_snap_id USING (snap_id)
WHERE pg_index_cust.indisunique = FALSE
and idx_stat.idx_scan=0
order by 3,4;
