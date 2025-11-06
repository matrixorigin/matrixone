drop database if exists test;
create database test;
use test;

drop snapshot if exists sp1;

create table t0(a int, b int, primary key(a));
insert into t0 select *,* from generate_series(1, 100)g;

drop snapshot if exists sp1;
create snapshot sp1 for table test t0;

create table t1 clone t0{snapshot="sp1"};

insert into t1 select *,* from generate_series(200,300,2)g;
update t1 set b = b + 10 where a in (1, 2, 3);
delete from t1 where a in (7, 8, 9);

data branch diff t1 against t0{snapshot="sp1"} output sql '$resources/load_data/diff.sql';

source $resources/load_data/diff.sql;
select * from t1 except select * from t0;

drop snapshot if exists sp1;
drop table t0;
drop table t1;

-----------------------------------------------------------

create table t0(a int, b int, c int, primary key(a,b));
insert into t0 select *,*, * from generate_series(1, 100)g;

drop snapshot if exists sp1;
create snapshot sp1 for table test t0;

create table t1 clone t0{snapshot="sp1"};

insert into t1 select *,*,* from generate_series(200,300,2)g;
update t1 set b = b + 10 where a in (1, 2, 3);
delete from t1 where a in (7, 8, 9);

data branch diff t1 against t0{snapshot="sp1"} output sql '$resources/load_data/diff.sql';

source $resources/load_data/diff.sql;
select * from t1 except select * from t0;

drop snapshot if exists sp1;
drop table t0;
drop table t1;

-----------------------------------------------------------

create table t0(a int, b int, c int, primary key(a,b));
insert into t0 select *,*, * from generate_series(1, 100)g;

drop snapshot if exists sp1;
create snapshot sp1 for table test t0;

create table t1 clone t0{snapshot="sp1"};

insert into t1 select *,*,* from generate_series(200,300,2)g;
update t1 set b = b + 10 where a in (1, 2, 3);
delete from t1 where a in (7, 8, 9);

data branch diff t1 against t0{snapshot="sp1"} output sql '$resources/load_data/diff.sql';

source $resources/load_data/diff.sql;
select * from t1 except select * from t0;

drop snapshot sp1;
drop database test;