-- issue #31688: a snapshot-qualified table is read-only; DML/DDL that targets
-- it is rejected instead of silently modifying (or dropping) the live table.
drop database if exists snap_write_target;
create database snap_write_target;
use snap_write_target;
create table snap_t(id int primary key, v varchar(10));
create table o(id int primary key, v varchar(10));
insert into snap_t values (1, 'a');
insert into o values (1, 'o');
drop snapshot if exists snap_write_target_sp;
create snapshot snap_write_target_sp for table snap_write_target snap_t;
insert into snap_t values (2, 'b');
insert into snap_t{snapshot='snap_write_target_sp'} values (50, 'fifty');
replace into snap_t{snapshot='snap_write_target_sp'} values (51, 'x');
update snap_t{snapshot='snap_write_target_sp'} set v = 'UPD' where id = 1;
update snap_t{snapshot='snap_write_target_sp'} s join o on o.id = s.id set s.v = 'J';
delete from snap_t{snapshot='snap_write_target_sp'} where id = 1;
delete s from snap_t{snapshot='snap_write_target_sp'} s join o on s.id = o.id;
alter table snap_t{snapshot='snap_write_target_sp'} add column extra int default 7;
truncate table snap_t{snapshot='snap_write_target_sp'};
drop table snap_t{snapshot='snap_write_target_sp'};
-- the live table and the snapshot are unchanged
select * from snap_t order by id;
select * from snap_t{snapshot='snap_write_target_sp'} order by id;
-- snapshot-qualified tables stay valid as read sources
update o join snap_t{snapshot='snap_write_target_sp'} s on o.id = s.id set o.v = s.v;
select * from o order by id;
create table copied(id int primary key, v varchar(10));
insert into copied select * from snap_t{snapshot='snap_write_target_sp'};
select * from copied order by id;
delete o from o join snap_t{snapshot='snap_write_target_sp'} s on s.id = o.id;
select count(*) from o;
drop snapshot snap_write_target_sp;
drop database snap_write_target;
