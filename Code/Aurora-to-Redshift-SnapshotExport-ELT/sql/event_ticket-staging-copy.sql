
CREATE TABLE sporting_event_ticket_tmp (LIKE sports.sporting_event_ticket)  BACKUP NO;

insert into sporting_event_ticket_tmp select id, sporting_event_id, sport_location_id, seat_level, seat_section, seat_row, seat, ticketholder_id, ticket_price 
from spectrum_schema.snapexpdemo_dms_sample_sporting_event_ticket
;

DELETE FROM sports.sporting_event_ticket USING sporting_event_ticket_tmp WHERE sports.sporting_event_ticket.id = sporting_event_ticket_tmp.id ;

ALTER TABLE sports.sporting_event_ticket APPEND FROM sporting_event_ticket_tmp;

DROP TABLE sporting_event_ticket_tmp;

VACUUM DELETE ONLY sports.sporting_event_ticket;

ANALYZE sports.sporting_event_ticket;