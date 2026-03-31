drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 4: Different data types
-- ================================================================

-- case 1: varchar primary key
create table t1 (name varchar(50), val int, primary key(name));
insert into t1 values ('alice',1),('bob',2),('charlie',3);

create table t2 (name varchar(50), val int, primary key(name));
insert into t2 values ('alice',1),('bob',2),('dave',4),('eve',5);

data branch diff t2 against t1;

-- pick 'dave' only
data branch pick t2 into t1 keys('dave');
select * from t1 order by name asc;

-- pick 'eve'
data branch pick t2 into t1 keys('eve');
select * from t1 order by name asc;

drop table t1;
drop table t2;

-- case 2: bigint primary key
create table t1 (id bigint, data varchar(10), primary key(id));
insert into t1 values (1000000001,'a'),(1000000002,'b');

create table t2 (id bigint, data varchar(10), primary key(id));
insert into t2 values (1000000001,'a'),(1000000003,'c'),(1000000004,'d');

data branch pick t2 into t1 keys(1000000003);
select * from t1 order by id asc;

drop table t1;
drop table t2;

-- case 3: multiple column types in table with int pk
create table t1 (
    id int primary key,
    name varchar(50),
    score float,
    active bool
);
insert into t1 values (1,'alice',95.5,true),(2,'bob',88.0,true);

create table t2 (
    id int primary key,
    name varchar(50),
    score float,
    active bool
);
insert into t2 values (1,'alice',95.5,true),(3,'charlie',92.0,false),(4,'dave',78.5,true);

data branch pick t2 into t1 keys(3,4);
select * from t1 order by id asc;

drop table t1;
drop table t2;

-- case 4: negative integer keys
create table t1 (a int, b int, primary key(a));
insert into t1 values (-5,1),(-3,2),(0,3);

create table t2 (a int, b int, primary key(a));
insert into t2 values (-5,1),(-4,2),(-2,3),(1,4);

data branch pick t2 into t1 keys(-4,-2);
select * from t1 order by a asc;

drop table t1;
drop table t2;

drop database test;
