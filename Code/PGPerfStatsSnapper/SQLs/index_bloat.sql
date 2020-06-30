-- based on https://github.com/ioguix/pgsql-bloat-estimation/blob/master/btree/btree_bloat.sql

-- current_database: name of the current database
-- schemaname: schema of the table
-- tblname: the table name
-- idxname: the index name
-- real_size: real size of the index
-- extra_size: estimated extra size not used/needed by the index. This extra size is composed by the fillfactor, bloat and alignment padding spaces.
-- extra_ratio: estimated ratio of the real size used by extra_size.
-- fillfactor: the fillfactor of the index.
-- bloat_size: estimated size of the bloat without the extra space kept for the fillfactor.
-- bloat_ratio: estimated ratio of the real size used by bloat_size.
-- is_na: is the estimation "Not Applicable" ? If true, do not trust the stats.

select * from index_bloats;