SELECT blocked_locks.pid     AS blocked_pid,
blocking_locks.pid     AS blocking_pid,
blocked_activity.application_name as application_name,
blocked_activity.usename  AS blocked_user,
blocking_activity.usename AS blocking_user,
blocked_activity.query    AS blocked_statement,
blocking_activity.query   AS blocking_statement
FROM  pg_locks_history         blocked_locks
JOIN pg_stat_activity_history blocked_activity  ON blocked_activity.pid = blocked_locks.pid AND blocked_activity.snap_id = blocked_locks.snap_id
JOIN pg_locks_history         blocking_locks
ON blocking_locks.locktype = blocked_locks.locktype
AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid                                                                                                                                                                            
AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid                                                                                                                                                                      
AND blocking_locks.pid != blocked_locks.pid
JOIN pg_stat_activity_history blocking_activity ON blocking_activity.pid = blocking_locks.pid AND blocking_activity.snap_id = blocking_locks.snap_id
WHERE NOT blocked_locks.GRANTED
and blocked_locks.snap_id between :begin_snap_id and :end_snap_id;
