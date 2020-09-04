select snap_id, count(*) temp_table_cnt 
from pg_temp_table_history 
where snap_id between :begin_snap_id and :end_snap_id
group by snap_id order by 1;
