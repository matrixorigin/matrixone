
-- no primary key or unique
create table t1(
id int,
data varchar(64)
);

replace into t1 values (1, 'new_1');
select * from t1;

replace into t1 values (1, 'new_2');
select * from t1;

replace into t1 values (1, 'new_1');
select * from t1;

-- A single column of primary keys exists
create table t2(
id int,
data varchar(64),
primary key (id)
);

replace into t2 values (1, 'test_1');
select * from t2;

replace into t2 values (2, 'test_1');
select * from t2;

replace into t2 values (1, 'old_1');
select * from t2;

replace into t2 values (2, 'old_2');
select * from t2;


-- Multiple columns of primary keys exist
create table t3(
id int,
data varchar(64),
name varchar(64) default null,
primary key (id, data)
);

replace into t3 values (1, 'test_3', '');
select * from t3;

replace into t3 values (1, 'old_3', 'name1');
select * from t3;

replace into t3 values (2, 'test_3', 'name2');
select * from t3;

replace into t3 values (1, 'test_3', 'replace_name_1');
select * from t3;

replace into t3 values (2, 'test_3', 'replace_name_2');
select * from t3;


-- A uniquely constrained primary key
create table t4 (
a int unique key,
b varchar(64)
);
-- @bvt:issue#11324
replace into t4 values (1, 'a');
select * from t4;
replace into t4 values (2, 'a');
select * from t4;
replace into t4 values (1, 'replace_name_1');
select * from t4;
replace into t4 values (2, 'replace_name_2');
select * from t4;
-- @bvt:issue

-- Multiple uniquely constrained primary key
create table t5(
a int,
b int,
c varchar(64),
unique key(a, b)
);

-- @bvt:issue#11324
replace into t5 values (1, 1, '');
select * from t5;
replace into t5 values (1, 1, 'replace');
select * from t5;
replace into t5 values (1, 2, '');
select * from t5;
-- @bvt:issue

-- clear table
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
