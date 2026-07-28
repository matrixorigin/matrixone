drop database if exists test;
create database test;
use test;

-- i. simple self diff with snapshot and no pk
create table t1 (a int, b vecf32(3), c vecf64(3), d json);

drop snapshot if exists sp0;
create snapshot sp0 for table test t1;

insert into t1 values(1,'[0.1,0.1,0.1]', '[0.11, 0.11,0.11]', '{"key1":"val1 \', \\n, \\t "}');
insert into t1 values(2,'[0.2,0.2,0.2]', '[0.22, 0.22,0.22]', '{"key2":"val2 \', \\n, \\t "}');
insert into t1 values(3,'[0.3,0.3,0.3]', '[0.33, 0.33,0.33]', '{"key2":"val2 \', \\n, \\t "}');

drop snapshot if exists sp1;
create snapshot sp1 for table test t1;

insert into t1 values(4,'[0.4,0.4,0.4]', '[0.44, 0.44,0.44]', '{"key4":"val4 \', \\n, \\t "}');
insert into t1 values(5,'[0.5,0.5,0.5]', '[0.55, 0.55,0.55]', '{"key5":"val5 \', \\n, \\t "}');
insert into t1 values(6,'[0.6,0.6,0.6]', '[0.66, 0.66,0.66]', '{"key6":"val6 \', \\n, \\t "}');

drop snapshot if exists sp2;
create snapshot sp2 for table test t1;

data branch diff t1{snapshot="sp1"} against t1{snapshot="sp0"} output count;
data branch diff t1{snapshot="sp1"} against t1{snapshot="sp0"};

data branch diff t1{snapshot="sp2"} against t1{snapshot="sp1"} output count;
data branch diff t1{snapshot="sp2"} against t1{snapshot="sp1"};

data branch diff t1{snapshot="sp2"} against t1{snapshot="sp0"} output count;
data branch diff t1{snapshot="sp2"} against t1{snapshot="sp0"};


-- data branch diff t1{snapshot="sp2"} against t1{snapshot="sp0"} output file '$resources/load_data/';
-- data branch diff t1{snapshot="sp2"} against t1{snapshot="sp1"} output file '$resources/load_data/';

drop snapshot sp0;
drop snapshot sp1;
drop snapshot sp2;
drop table t1;

-- flush insert and tombstone
-- has pk
create table t0(a int, b int, primary key (a));
insert into t0 values(0,0);

create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
insert into t1 select *,* from generate_series(1, 16384)g;

update t1 set b = b+10 where a = 0;
update t1 set b = b+10 where a > 0;
delete from t1 where a > 1;

select * from t1 order by a asc;

data branch diff t1 against t0{snapshot="sp0"} output count;
data branch diff t1 against t0{snapshot="sp0"};

drop snapshot sp0;
drop table t0;
drop table t1;

-- no pk
create table t0(a int, b int);
insert into t0 values(0,0);

create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
insert into t1 select *,* from generate_series(1, 16384)g;

update t1 set b = b+10 where a = 0;
update t1 set b = b+10 where a > 0;
delete from t1 where a > 1;

select * from t1 order by a asc;

data branch diff t1 against t0{snapshot="sp0"} output count;
data branch diff t1 against t0{snapshot="sp0"};

drop snapshot sp0;
drop table t0;
drop table t1;

-- more delete/insert/update case
-- fake pk
create table t1(a int, b int);
insert into t1 select *,* from generate_series(1, 20000)g;
data branch create table t1_copy1 from t1;

update t1_copy1 set b = b+1 where a in (111, 1111, 11111);
update t1_copy1 set b = b+1 where a in (99, 999, 9999);
delete from t1_copy1 where a in (1, 3, 5);
insert into t1_copy1 values(1, 10);

data branch diff t1_copy1 against t1;

-- auto increment pk
create table t2(a int auto_increment, b int, primary key(a));
insert into t2 select *,* from generate_series(1, 20000)g;

data branch create table t2_copy1 from t2;

update t2_copy1 set b = b+1 where a in (111, 1111, 11111);
update t2_copy1 set b = b+1 where a in (99, 999, 9999);
delete from t2_copy1 where a in (1, 3, 5);
insert into t2_copy1 values(1, 10);

data branch diff t2_copy1 against t2;

drop database test;