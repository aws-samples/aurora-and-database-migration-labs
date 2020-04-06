set search_path=public;
\set AUTOCOMMIT off

/* This file contains DDL for dropping relational schema in PostgreSQL */


drop table trips cascade;
drop table payment cascade;
drop table billing cascade;
drop table riders cascade;
drop table drivers cascade;
drop table rate_codes cascade;
drop table trip_status cascade;
drop table payment_types cascade;
drop table car_model cascade;
drop table cab_types cascade;

commit;
