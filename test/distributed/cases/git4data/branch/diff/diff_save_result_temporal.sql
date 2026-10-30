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
