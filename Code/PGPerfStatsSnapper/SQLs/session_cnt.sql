select snap_id,datname,numbackends from pg_stat_database_history
where datname not in ('template0','template1','rdsadmin')
and snap_id between :begin_snap_id and :end_snap_id
order by 2,1;

select snap_id,state as session_state,count(*) from pg_stat_activity_history
where datname not in ('template0','template1','rdsadmin')
and snap_id between :begin_snap_id and :end_snap_id
group by snap_id,state
order by 1,2;
