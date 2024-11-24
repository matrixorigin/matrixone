drop account if exists acc_mts;
create account acc_mts admin_name='root' identified by '111';
-- @session:id=1&user=acc_mts:root&password=111

drop database if exists mts_db;
create database mts_db;
use mts_db;

--- test variables
create table mts_t2 (a int);
insert into mts_t2 select * from generate_series(1, 1000)g;

select mo_table_rows("mts_db", "mts_t2");

set mo_table_stats.force_update = yes;
select mo_table_rows("mts_db", "mts_t2");

insert into mts_t2 values (1001);

set mo_table_stats.force_update = no;
select mo_table_rows("mts_db", "mts_t2");

set mo_table_stats.force_update = yes;
select mo_table_rows("mts_db", "mts_t2");

insert into mts_t2 values(1002);

set mo_table_stats.force_update = no;
set mo_table_stats.reset_update_time = yes;
select mo_table_rows("mts_db", "mts_t2");

drop table mts_t2;
drop database mts_db;

-- @session
drop account acc_mts;
