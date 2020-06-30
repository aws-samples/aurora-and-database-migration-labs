-- based on https://github.com/pgexperts/pgx_scripts/blob/master/indexes/needed_indexes.sql

-- Checks for tables which are getting too much sequential scan activity and might need additional indexing. Reports in four groups based on table size, number of scans, write activity, and number of existing indexes.

select * from needed_indexes;