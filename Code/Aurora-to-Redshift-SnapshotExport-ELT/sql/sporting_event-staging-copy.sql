
CREATE TABLE sporting_event_tmp (LIKE sports.sporting_event) BACKUP NO;

INSERT INTO sporting_event_tmp SELECT id, sport_type_name, home_team_id, away_team_id, location_id, start_date_time::timestamp, start_date::date, sold_out from spectrum_schema.snapexpdemo_dms_sample_sporting_event
;

DELETE FROM sports.sporting_event USING sporting_event_tmp WHERE sports.sporting_event.id = sporting_event_tmp.id ;

ALTER TABLE sports.sporting_event APPEND FROM sporting_event_tmp;

DROP TABLE sporting_event_tmp;

VACUUM DELETE ONLY sports.sporting_event;

ANALYZE sports.sporting_event;