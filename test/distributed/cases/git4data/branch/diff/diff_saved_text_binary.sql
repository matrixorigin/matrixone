-- A saved DATA BRANCH DIFF must preserve VARCHAR text and VARBINARY bytes
-- through RESULT_SCAN and INSERT ... SELECT (issue #31535).

drop database if exists test_diff_save_bytes;
create database test_diff_save_bytes;
use test_diff_save_bytes;

create table b(
id int primary key,
v varchar(40),
bin varbinary(16)
);

insert into b values
(1, 'old', X'00FF'),
(2, NULL, NULL);

data branch create table child from b;
update child set v = '中文', bin = X'FF000A' where id = 1;

set save_query_result = on;
data branch diff child against b;
set @qid = last_query_id();
select * from result_scan(@qid) as r;
create table copied(id int primary key, v varchar(40), bin varbinary(40));
insert into copied select id, v, bin from result_scan(@qid) as r;
select id, v, hex(bin) from copied;
select id, v, hex(bin) from copied where v is null or bin is null;
set save_query_result = off;

-- The original source table must be untouched.
select id, v, hex(bin) from b;
drop table copied;
drop table child;
drop table b;
drop database test_diff_save_bytes;
