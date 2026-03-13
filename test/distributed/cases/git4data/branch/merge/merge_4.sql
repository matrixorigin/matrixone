drop database if exists test_merge_nopk;
create database test_merge_nopk;
use test_merge_nopk;

-- case 1: no PK with duplicates and NULLs, merge should delete only one duplicate
create table t0(a int, b varchar(10), c int);
insert into t0 values
(1,'dup',null),
(1,'dup',null),
(2,null,2),
(3,'x',3);

drop snapshot if exists sp0;
create snapshot sp0 for table test_merge_nopk t0;

data branch create table t1 from t0{snapshot="sp0"};
data branch create table t2 from t0{snapshot="sp0"};

-- keep a base-only row in destination
insert into t1 values(99,'base',99);

-- source changes: delete one duplicate, delete a NULL row, update to NULL, insert new row
delete from t2 where a=1 and b='dup' and c is null limit 1;
delete from t2 where a=2 and b is null and c=2;
update t2 set b = null, c = 30 where a=3 and b='x' and c=3;
insert into t2 values(4,'new',null);

data branch merge t2 into t1;

select a, b, c, count(*) as cnt from t1 group by a, b, c order by a, b, c;
select * from t1 where a=99;

drop snapshot sp0;
drop table t0;
drop table t1;
drop table t2;
drop database test_merge_nopk;
