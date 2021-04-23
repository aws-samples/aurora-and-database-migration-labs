\pset pager off
\pset footer off
\qecho ' '
\qecho ' '

\qecho '==SNAPSHOT DETAILS=='
\qecho ' '
\qecho 'list_snaps.sql                      			List snapshots available with time window'
\qecho ' '
\qecho ' '

\qecho '==SET SNAPSHOT WINDOW=='
\qecho ' '
\qecho 'set_snaps.sql                                   	Set Begin and End Snapshot ID for Analysis'
\qecho ' '
\qecho ' '


\qecho '==INSTANCE AND DATABASE STATS=='
\qecho ' '
\qecho 'db_and_schema_sizes.sql                 		Database and Schema Sizes'
\qecho 'tables_and_indexes_tot_size.sql   		        Top 20 Tables and Indexes by total Size'
\qecho 'cache_hit_ratio.sql                 			Cache hit ratio in a time window'
\qecho 'db_stats.sql                         			Database Level statistics in a time window'
\qecho 'checkpoint_stats_by_snap_id.sql                         Checkpoints stats in a time window'
\qecho 'temp_file_by_snap_id.sql                                Temp file stats by Snap ID'
\qecho 'temp_table_cnt_by_snap_id.sql                           Temp tables count by Snap ID'
\qecho ' '
\qecho ' '

\qecho '==SESSION STATS=='
\qecho ' '
\qecho 'session_cnt.sql                      			Total Sessions and Session count by state in a time window'
\qecho 'session_activity_hist.sql           			Sessions activity with wait events in a time window'
\qecho 'blockers_and_waiters_hist.sql       			Blocking and Waiting Sessions in a time window'
\qecho 'vacuum_history.sql                  			Vacuum activity in a time window'
\qecho ' '
\qecho ' '

\qecho '==SQL STATS=='
\qecho ' '
\qecho 'top_20_sqls_by_calls.sql               			Top 20 queries by Executions/Calls in a time window'
\qecho 'top_20_sqls_by_elapsed_time.sql        			Top 20 queries by Elapsed time in a time window'
\qecho 'top_10_sqls_by_cpu_by_snap_id.sql        		Top 10 SQL queries by CPU by Snap ID'
\qecho 'sql_stat_history.sql                			Execution trend of a query of interest in a time window'
\qecho ' '
\qecho ' '

\qecho '==TABLE STATS=='
\qecho ' '
\qecho 'table_cols.sql      					Details of Table columns'
\qecho 'table_pk.sql      					Details of Table Primary Key'
\qecho 'table_fks.sql      					Details of Foreign Keys referencing the Primary Key of the Parent Table'
\qecho 'table_options.sql					Table Options for fill factor and Vacumming'
\qecho 'top_20_tables_by_seq_scans.sql      			Top 20 Tables by number of Sequential or Full scans'
\qecho 'top_20_tables_by_dmls.sql           			Top 20 Tables by DML activity'
\qecho 'table_bloat.sql           				Table Bloat Analysis'
\qecho 'sqls_touching_table.sql                                 List SQLs touching a table'
\qecho ' '
\qecho ' '

\qecho '==INDEX STATS=='
\qecho ' '
\qecho 'indexes_on_table.sql                			Indexes on a table'
\qecho 'fks_with_no_index.sql                			Foreign Keys with no Index'
\qecho 'needed_indexes.sql                			Needed Indexes'
\qecho 'top_20_indexes_by_scans.sql         			Top 20 Indexes by number of Scans initiated in the index'
\qecho 'top_20_indexes_by_avg_tuple_reads.sql       		TOP 20 Indexes by average Tuples Reads/Scan'
\qecho 'unused_indexes.sql                  			Unused Indexes'
\qecho 'duplicate_indexes.sql                                   Duplicate Indexes'
\qecho 'index_bloat.sql                  			Index Bloat Analysis'
\qecho ' '
\qecho ' '
