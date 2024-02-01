-- 0.  insert, update, delete
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1;
create index idx1 using master on t1(a,b);
insert into t1 values("Changing","Expanse", "4");
update t1 set a = "Altering" where c = "4";
delete from t1 where c = "2";

-- 1. failure on create index on non strings.
create table t2(a varchar(30), b bigint, c varchar(30) primary key);
insert into t2 values("Congress",1, "1");
insert into t2 values("Juniper",2, "2");
insert into t2 values("Nightingale",3, "3");
create index idx2 using master on t2(a,b);

-- 2.1.a Insert Normal (from Test Document)
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
create index idx1 using master on t1(a,b);
insert into t1 values("Alberta","Blvd", "4");


-- 2.1.b Insert Duplicates
insert into t1 values("Nightingale","Lane", "5");

-- 2.1.c Insert Nulls
insert into t1 values(NULL,"Lane", "6");

-- 2.1.d Insert Into Select *
drop table if exists t2;
create table t2(a varchar(30), b varchar(30), c varchar(30));
insert into t2 values("arjun", "sk", "7");
insert into t2 values("albin", "john", "8");
insert into t1 select * from t2;

-- 2.2.a Update a record to duplicate
update t1 set a="albin" ,b="john" where c="7";

-- 2.2.b Update a record to NULL
update t1 set a=NULL ,b="john" where c="7";

-- 2.2.c Delete a record
delete from t1 where c="7";

-- 2.2.d truncate
truncate table t1;