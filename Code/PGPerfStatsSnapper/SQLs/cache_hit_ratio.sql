select snap_id,datname,blks_hit*100/(blks_hit+blks_read) as hit_ratio
from pg_stat_database_history
where snap_id between :begin_snap_id and :end_snap_id
and datname not in ('template0','template1','rdsadmin')
and blks_read <> 0
order by 2,1;
