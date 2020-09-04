-- checkpoints_timed: Number of scheduled checkpoints (due to checkpoint_timeout being reached) that have been performed. Desirable.
-- checkpoints_req: Number of checkpoints that had to be requested due to heavy write activity (due to max_wal_size being reached)
-- buffers_checkpoint: Number of buffers written during checkpoints
-- buffers_clean: Number of buffers written by the background writer (bgwriter) to help offload work off of checkpoint processes. Desirable.
-- buffers_backend: Number of buffers written directly by a backend processes (clients) because it couldn't find empty buffers in shared_buffers. Not desirable.
-- buffers_alloc: Number of new buffers allocated by backend processes (clients)

-- buffers_clean should be greater than buffers_backend. Otherwise, you should increase bgwriter_lru_multiplier and decrease bgwriter_delay. Note, it also may be a sign that you have insufficient shared buffers and hot part of your data don’t fit into shared buffers and forced to travel between RAM and disks.


select a.snap_id, sample_start_time,
case WHEN (checkpoints_timed-lag(checkpoints_timed::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) = checkpoints_timed then null
else (checkpoints_timed-lag(checkpoints_timed::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) END AS delta_checkpoints_timed,
case WHEN (checkpoints_req-lag(checkpoints_req::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) = checkpoints_req then null
else (checkpoints_req-lag(checkpoints_req::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) END AS delta_checkpoints_req,
case WHEN (buffers_checkpoint-lag(buffers_checkpoint::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) = buffers_checkpoint then null
else (buffers_checkpoint-lag(buffers_checkpoint::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) END AS delta_buffers_checkpoint,
case WHEN (buffers_clean-lag(buffers_clean::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) = buffers_clean then null
else (buffers_clean-lag(buffers_clean::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) END AS delta_buffers_bgwriter,
case WHEN (buffers_backend-lag(buffers_backend::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) = buffers_backend then null
else (buffers_backend-lag(buffers_backend::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) END AS delta_buffers_backend,
case WHEN (buffers_alloc-lag(buffers_alloc::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) = buffers_alloc then null
else (buffers_alloc-lag(buffers_alloc::numeric,1,0::numeric) OVER (ORDER BY a.snap_id) ) END AS delta_new_buffers_alloc
from pg_stat_bgwriter_history a,  pg_awr_snapshots_cust b
where a.snap_id = b.snap_id;
