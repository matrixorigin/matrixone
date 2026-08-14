drop database if exists sql_select_limit_27035_db;
create database sql_select_limit_27035_db;
use sql_select_limit_27035_db;

drop table if exists sql_select_limit_27035;
create table sql_select_limit_27035 (id int primary key);
insert into sql_select_limit_27035 values (1), (2), (3), (4), (5);

set @@sql_select_limit = 3;
select id from sql_select_limit_27035 order by id;

-- An explicit LIMIT takes precedence, even when it is less restrictive.
select id from sql_select_limit_27035 order by id limit 5;

-- The session value is resolved when a prepared plan executes, not when the
-- plan is prepared.
set @@sql_select_limit = default;
prepare sql_select_limit_stmt from 'select id from sql_select_limit_27035 order by id';
set @@sql_select_limit = 2;
execute sql_select_limit_stmt;
set @@sql_select_limit = 4;
execute sql_select_limit_stmt;
deallocate prepare sql_select_limit_stmt;

set @@sql_select_limit = 0;
select id from sql_select_limit_27035 order by id;

set @@sql_select_limit = default;
drop database sql_select_limit_27035_db;
