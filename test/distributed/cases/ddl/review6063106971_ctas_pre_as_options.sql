-- review 6063106971 / issue 32542: pre-AS CTAS options
create table ctas606a charset utf8mb4 as select 1 as a;
select * from ctas606a order by a;
create table ctas606n as select 3 as c;
select c from ctas606n order by c;
drop table ctas606n;
create table ctas606b compression 'zlib' comment 'c' as select 2 as b;
select b from ctas606b order by b;
show create table ctas606b;
create table ctas606_bad as 1;
select count(*) from ctas606_bad;
drop table ctas606a;
drop table ctas606b;
drop table ctas606_bad;
