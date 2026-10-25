-- DATABASE() / SCHEMA() return NULL when there is no default database (MySQL 8 semantics).
drop database if exists func_db_no_default;
create database func_db_no_default;
use func_db_no_default;
select database(), schema(), database() is null;
prepare s1 from 'select database(), schema()';
execute s1;
drop database func_db_no_default;
-- the dropped database was the default one, so there is no default database now
select database(), schema(), database() is null, schema() is null;
select ifnull(database(), '(none)'), coalesce(schema(), '(none)');
select concat('db=', database()), length(database());
execute s1;
create database func_db_no_default;
create table func_db_no_default.ctas_database as select database() as d, schema() as s;
select d is null, s is null from func_db_no_default.ctas_database;
select column_name, is_nullable from information_schema.columns
where table_schema = 'func_db_no_default' and table_name = 'ctas_database'
order by ordinal_position;
use func_db_no_default;
select database(), database() is null;
execute s1;
deallocate prepare s1;
drop database func_db_no_default;
