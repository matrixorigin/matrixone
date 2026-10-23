-- DATABASE() / SCHEMA() return NULL when there is no default database (MySQL 8 semantics).
drop database if exists func_db_no_default;
create database func_db_no_default;
use func_db_no_default;
select database(), schema(), database() is null;
drop database func_db_no_default;
-- the dropped database was the default one, so there is no default database now
select database(), schema(), database() is null, schema() is null;
select ifnull(database(), '(none)'), coalesce(schema(), '(none)');
select concat('db=', database()), length(database());
prepare s1 from 'select database() is null';
execute s1;
deallocate prepare s1;
create database func_db_no_default;
use func_db_no_default;
select database(), database() is null;
prepare s1 from 'select database() is null';
execute s1;
deallocate prepare s1;
drop database func_db_no_default;
