
-- test DDL uuid type
drop table if exists t1;
create table t1(a uuid);
desc t1;
show create table t1;
drop table t1;

drop table if exists t1;
-- @bvt:issue#5414
create table t1 (a int default uuid());
-- @bvt:issue
drop table t1


drop table if exists t2;
create table t2(a uuid primary key);
desc t2;
show create table t2;
drop table t2;


-- test insert uuid value
drop table if exists t3;
create table t3(a uuid primary key );
insert into t3 values (uuid());
select length(cast(a as varchar)) from t3;
drop table t3;


-- test default uuid
drop table if exists t4;
create table t4(a uuid default uuid());
desc t4;
show create table t4;
insert into t4 values ();
insert into t4 values (uuid());
select length(cast(a as varchar)) from t4;
drop table t4;


-- test cast string
select length(cast(uuid() as varchar));
select length(cast(uuid() as char));
select length(cast(uuid() as text));

