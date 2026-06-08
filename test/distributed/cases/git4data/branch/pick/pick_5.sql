drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 5: KEYS with subquery
-- ================================================================

-- case 1: keys from subquery — select specific PKs
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (4,4),(5,5),(6,6),(7,7);

-- pick keys selected by a subquery: pick even numbers > 3
data branch pick t2 into t1 keys(select a from t2 where a > 3 and a % 2 = 0);
select * from t1 order by a asc;

-- verify: pk=5,7 still in diff
data branch diff t2 against t1;

-- pick remaining via subquery
data branch pick t2 into t1 keys(select a from t2 where a in (5,7));
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 2: keys from another table
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5);

-- helper table with keys to pick
create table pick_keys (k int);
insert into pick_keys values (2),(4);

data branch pick t2 into t1 keys(select k from pick_keys);
select * from t1 order by a asc;

drop table pick_keys;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 3: subquery with string literal predicate
-- ----------------------------------------------------------------

create table orders (order_id int primary key, customer varchar(20), amount int);
insert into orders values (1,'Alice',10),(2,'Bob',20),(3,'Carol',30);

data branch create table orders_fix from orders;
insert into orders_fix values (4,'Grace',40),(5,'Heidi',50),(6,'Grace',60);

data branch pick orders_fix into orders keys(select order_id from orders_fix where customer = 'Grace') when conflict accept;
select * from orders order by order_id asc;

-- verify: non-Grace row is still in diff
data branch diff orders_fix against orders;

drop table orders;
drop table orders_fix;

-- ----------------------------------------------------------------
-- case 4: empty subquery result — should be no-op
-- ----------------------------------------------------------------

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (4,4),(5,5);

-- subquery returns empty set
data branch pick t2 into t1 keys(select a from t2 where a > 1000);
select * from t1 order by a asc;
-- expect: {1,2,3} unchanged

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 5: large subquery — pick 25 out of 100 new rows
-- ----------------------------------------------------------------

create table t1 (a int, b varchar(20), primary key(a));
insert into t1 select *, 'orig' from generate_series(1, 50) g;

create table t2 (a int, b varchar(20), primary key(a));
insert into t2 select *, 'data' from generate_series(1, 150) g;

-- pick every 4th row from 51..150 via subquery
data branch pick t2 into t1 keys(select a from t2 where a > 50 and a % 4 = 0);
select count(*) from t1;
-- expect: 75 (50 original + 25 picked)

-- verify boundary rows
select * from t1 where a in (52, 100, 148) order by a;

drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 6: subquery with DISTINCT and ORDER BY
-- ----------------------------------------------------------------

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (3,3),(4,4),(5,5),(6,6);

-- helper with duplicate keys
create table dup_keys (k int);
insert into dup_keys values (3),(3),(4),(4),(5);

data branch pick t2 into t1 keys(select distinct k from dup_keys order by k asc);
select * from t1 order by a asc;
-- expect: {1,2,3,4,5}

drop table dup_keys;
drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 7: subquery keys must not contain NULL
-- ----------------------------------------------------------------

create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(3,3);

create table pick_keys (k int);
insert into pick_keys values (2),(null);

data branch pick t2 into t1 keys(select k from pick_keys order by k);
select * from t1 order by a asc;
-- expect: unchanged because the statement is rejected

drop table pick_keys;
drop table t1;
drop table t2;

drop database test;
