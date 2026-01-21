drop database if exists test;
create database test;
use test;

-- refer to data_branch.go#buildHashmapForTable
-- case 1.1
create table t1 (a int primary key, b int);
insert into t1 values(1,1);
insert into t1 values(2,2);

create snapshot sp for table test t1;

update t1 set b=2 where a = 1;
-- @ignore:0
select mo_ctl("dn", "flush", "test.t1");

update t1 set b=3 where a = 1;
-- @ignore:0
select mo_ctl("dn", "flush", "test.t1");

update t1 set b=4 where a = 1;

data branch diff t1 against t1{snapshot="sp"};

-- case 1.2
select mo_ctl("dn", "flush", "test.t1");
delete from t1 where a = 1;

data branch diff t1 against t1{snapshot="sp"};
drop snapshot sp;
drop table t1;

create table t1 (a int primary key, b int);

create snapshot sp for table test t1;

insert into t1 values(1,1);
insert into t1 values(2,2);

update t1 set b=2 where a = 1;
-- @ignore:0
select mo_ctl("dn", "flush", "test.t1");

update t1 set b=3 where a = 1;
-- @ignore:0
select mo_ctl("dn", "flush", "test.t1");

update t1 set b=4 where a = 1;

-- case 2.1
data branch diff t1 against t1{snapshot="sp"};

select mo_ctl("dn", "flush", "test.t1");
delete from t1 where a = 1;

-- case 2.2
data branch diff t1 against t1{snapshot="sp"};
drop snapshot sp;
drop table t1;
