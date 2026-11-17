-- Combined regression cases for #31714 (SET GLOBAL time_zone validation),
-- #31547/#31550 (saving DATA BRANCH DIFF temporal results) and #31688
-- (snapshot-qualified write targets).
-- SET GLOBAL time_zone rejects an unknown zone like SET SESSION does, so a
-- typo cannot be persisted for every new session (issue #31714).
select @@global.time_zone;
set global time_zone = 'Mars/Phobos';
set global time_zone = '+25:00';
set global time_zone = 'not a zone';
select @@global.time_zone;
set time_zone = 'Mars/Phobos';
select @@session.time_zone;
-- #31547 / #31550: saving a DATA BRANCH DIFF result (save_query_result = on)
-- must not crash on DATETIME/TIME columns, and must keep the fractional
-- seconds of TIME/DATETIME/TIMESTAMP and the session time zone of TIMESTAMP.
drop database if exists diff_save_result_temporal;
create database diff_save_result_temporal;
use diff_save_result_temporal;
set time_zone = '+00:00';

create table b(id int primary key, v timestamp(6), v3 timestamp(3), d datetime(6), t time(6), dd date);
insert into b values
  (1, '2023-01-01 00:00:00.000000', '2023-01-01 00:00:00.000', '2024-01-01 00:00:00.123456', '01:02:03.123456', '2024-01-01'),
  (2, '2023-01-01 00:00:00.000000', '2023-01-01 00:00:00.000', '2024-01-01 00:00:00.123456', '01:02:03.123456', '2024-01-01'),
  (3, '2023-01-01 00:00:00.000000', '2023-01-01 00:00:00.000', '2024-01-01 00:00:00.123456', '01:02:03.123456', '2024-01-01');
data branch create table c from b;
update c set
  v = case id when 1 then '2024-01-01 00:00:00.123456' when 2 then '2024-01-01 00:00:00.500000' else '2024-01-01 00:00:00.999999' end,
  v3 = '2024-01-01 00:00:00.654',
  d = '2024-01-02 00:00:00.654321',
  t = '04:05:06.654321',
  dd = '2024-02-02'
where id in (1, 2, 3);
update c set v = null, d = null, t = null, dd = null where id = 3;

set save_query_result = on;
data branch diff c against b;
set @qid = last_query_id();
select id, v, v3, d, t, dd from result_scan(@qid) as r order by id;

create table copied(id int primary key, v timestamp(6), v3 timestamp(3), d datetime(6), t time(6), dd date);
insert into copied select id, v, v3, d, t, dd from result_scan(@qid) as r;
set save_query_result = off;
select * from copied order by id;
select count(*) from copied c1 join c c2 on c1.id = c2.id
where c1.v <=> c2.v and c1.v3 <=> c2.v3 and c1.d <=> c2.d and c1.t <=> c2.t and c1.dd <=> c2.dd;

set time_zone = 'SYSTEM';
drop database diff_save_result_temporal;
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
