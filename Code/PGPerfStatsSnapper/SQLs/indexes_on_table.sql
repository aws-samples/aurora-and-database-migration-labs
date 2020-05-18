\prompt 'Enter table name: ' tabname

select * from pg_indexes_cust where tablename = :'tabname';

