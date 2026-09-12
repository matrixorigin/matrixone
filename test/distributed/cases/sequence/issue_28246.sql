-- Sequence updates share the outer transaction and retain read-your-own-writes.
drop database if exists issue_28246;
create database issue_28246;
use issue_28246;
create sequence s increment 1 start with 1 no cycle;
begin;
select nextval('s') as n;
rollback;
select nextval('s') as n;
begin;
select nextval('s') as n;
select nextval('s') as n;
commit;
select nextval('s') as n;
select setval('s', 10) as n;
select currval('s') as n;
select nextval('s') as n;
create table input_rows (id int);
insert into input_rows values (1), (2);
select nextval('s') as n from input_rows order by n;
select last_seq_num, is_called from s;
create table union_left (id int);
create table union_right (id int);
insert into union_left values (1);
insert into union_right values (1);
select nextval('s') as n from union_left
union all select nextval('s') as n from union_right order by n;
select last_seq_num, is_called from s;
-- An internal executor must never turn SETVAL into an ordinary-table update.
create table ordinary (last_seq_num bigint);
insert into ordinary values (7);
select setval('ordinary', 8) as n;
select * from ordinary;
drop database issue_28246;
