
CREATE TABLE sport_team_tmp (LIKE sports.sport_team)  BACKUP NO;

insert into sport_team_tmp select id, name, abbreviated_name, home_field_id::integer, sport_type_name, sport_league_short_name, sport_division_short_name 
from spectrum_schema.snapexpdemo_dms_sample_sport_team
;

DELETE FROM sports.sport_team USING sport_team_tmp WHERE sports.sport_team.id = sport_team_tmp.id ;

ALTER TABLE sports.sport_team APPEND FROM sport_team_tmp;

DROP TABLE sport_team_tmp;

VACUUM DELETE ONLY sports.sport_team;

ANALYZE sports.sport_team;