CREATE TABLE sport_location_tmp (LIKE sports.sport_location)  BACKUP NO;

insert into sport_location_tmp select id::integer, name, city, seating_capacity::INTEGER, levels, sections
from spectrum_schema.snapexpdemo_dms_sample_sport_location
;

DELETE FROM sports.sport_location USING sport_location_tmp WHERE sports.sport_location.id = sport_location_tmp.id ;

ALTER TABLE sports.sport_location APPEND FROM sport_location_tmp;

DROP TABLE sport_location_tmp;

VACUUM DELETE ONLY sports.sport_location;

ANALYZE sports.sport_location;