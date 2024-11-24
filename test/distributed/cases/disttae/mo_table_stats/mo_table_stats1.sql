drop database if exists mts_db;
create database mts_db;
use mts_db;

-- test mo_ctl
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "restore_default_setting:true");
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "move_on:false");

create table mts_t1 (a int);
insert into mts_t1 select * from generate_series(1, 1000)g;

select mo_table_rows("mts_db", "mts_t1");

-- @ignore:0
select mo_ctl("cn", "MoTableStats", "force_update:true");
select mo_table_rows("mts_db", "mts_t1");

insert into mts_t1 values(1001);

-- @ignore:0
select mo_ctl("cn", "MoTableStats", "force_update:false");
select mo_table_rows("mts_db", "mts_t1");

-- @ignore:0
select mo_ctl("cn", "MoTableStats", "force_update:true");
select mo_table_rows("mts_db", "mts_t1");

-- @ignore:0
select mo_ctl("cn", "MoTableStats", "force_update:false");

select count(*) from mts_t1;

drop table mts_t1;

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

-- @ignore:0
select mo_ctl("cn", "MoTableStats", "restore_default_setting:true");

select count(*) from mts_t2;

drop table mts_t2;
drop database mts_db;


