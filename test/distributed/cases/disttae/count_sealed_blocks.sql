drop database if exists bvt_count_sealed_blocks;
create database bvt_count_sealed_blocks;
use bvt_count_sealed_blocks;

create table counted (id bigint primary key, payload varchar(30));
select count(*) as rows_seen from counted;
insert into counted values (1, 'keep'), (2, 'old'), (3, 'remove');
update counted set payload = 'updated' where id = 2;
delete from counted where id = 3;
select count(*) as rows_seen from counted;
select count(case when length(payload) > 0 then id end) as rows_seen from counted;

begin;
delete from counted where id = 1;
insert into counted values (4, 'new'), (5, 'new');
select count(*) as rows_seen from counted;
select count(case when length(payload) > 0 then id end) as rows_seen from counted;
rollback;

select count(*) as rows_seen from counted;
delete from counted where id > 0;
select count(*) as rows_seen from counted;
select count(case when length(payload) > 0 then id end) as rows_seen from counted;
drop database bvt_count_sealed_blocks;
