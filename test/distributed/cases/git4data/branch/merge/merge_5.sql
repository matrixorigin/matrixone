drop database if exists test_merge_pk;
create database test_merge_pk;
use test_merge_pk;

-- case 1: PK update/delete/insert with LCA, keep base-only row
create table t0(a int primary key, b int, note varchar(10));
insert into t0 values (1,10,'a'),(2,20,'b'),(3,30,'c');

drop snapshot if exists sp0;
create snapshot sp0 for table test_merge_pk t0;

data branch create table t1 from t0{snapshot="sp0"};
data branch create table t2 from t0{snapshot="sp0"};

-- destination base-only row
insert into t1 values(9,90,'base');

-- source changes
update t2 set b = 22, note = 'b2' where a=2;
delete from t2 where a=3;
insert into t2 values(4,40,'d');

data branch merge t2 into t1;

select * from t1 order by a;

drop snapshot sp0;
drop table t0;
drop table t1;
drop table t2;

-- case 2: composite PK with NULL payload changes
create table t0(a int, b int, c varchar(10), primary key(a, b));
insert into t0 values(1,1,'x'),(1,2,null),(2,1,'y');

drop snapshot if exists sp1;
create snapshot sp1 for table test_merge_pk t0;

data branch create table t1 from t0{snapshot="sp1"};
data branch create table t2 from t0{snapshot="sp1"};

update t2 set c = 'x2' where a=1 and b=1;
delete from t2 where a=1 and b=2;
insert into t2 values(2,2,null);

data branch merge t2 into t1;

select * from t1 order by a, b;

drop snapshot sp1;
drop table t0;
drop table t1;
drop table t2;

drop database test_merge_pk;
