-- Test: Generated Columns (GENERATED ALWAYS AS ... STORED)

-- ============================================================
-- 1. Basic CREATE TABLE with generated column
-- ============================================================
drop database if exists test_generated_col;
create database test_generated_col;
use test_generated_col;

create table t1 (a int, b int, c int generated always as (a + b) stored);
show create table t1;
desc t1;

-- ============================================================
-- 2. INSERT and computed values
-- ============================================================
insert into t1(a, b) values (1, 2);
insert into t1(a, b) values (10, 20), (100, 200);
select * from t1 order by a;

-- Implicit column list (generated column auto-excluded)
insert into t1 values (5, 6);
select * from t1 order by a;

-- NULL propagation
insert into t1(a, b) values (null, 3);
select * from t1 order by a;

-- ============================================================
-- 3. Error cases
-- ============================================================
-- Cannot insert into generated column explicitly
-- Cannot insert into generated column explicitly
insert into t1(a, b, c) values (1, 2, 3);

-- Cannot update generated column directly
update t1 set c = 999 where a = 1;

-- ============================================================
-- 4. UPDATE recomputes generated column
-- ============================================================
update t1 set a = 50, b = 60 where a = 10;
select * from t1 order by a;

-- ============================================================
-- 5. Multiple generated columns
-- ============================================================
create table t2 (a int, b int, c int generated always as (a + b) stored, d int generated always as (a * b) stored);
insert into t2(a, b) values (3, 4);
select * from t2;

-- ============================================================
-- 6. Chained generated columns (d references c)
-- ============================================================
create table t3 (a int, b int, c int generated always as (a + b) stored, d int generated always as (c * 2) stored);
insert into t3(a, b) values (5, 10);
select * from t3;

-- ============================================================
-- 7. Shorthand AS (expr) STORED syntax
-- ============================================================
create table t4 (x int, y int, z int as (x - y) stored);
show create table t4;
insert into t4(x, y) values (100, 30);
select * from t4;

-- ============================================================
-- 8. String expression
-- ============================================================
create table t5 (first_name varchar(50), last_name varchar(50), full_name varchar(101) generated always as (concat(first_name, ' ', last_name)) stored);
insert into t5(first_name, last_name) values ('John', 'Doe'), ('Jane', 'Smith');
select * from t5 order by first_name;

-- ============================================================
-- 9. Table with primary key and generated column
-- ============================================================
create table t6 (id int primary key, val int, doubled int generated always as (val * 2) stored);
insert into t6(id, val) values (1, 10), (2, 20);
select * from t6 order by id;
update t6 set val = 15 where id = 1;
select * from t6 order by id;

-- ============================================================
-- 10. Cleanup
-- ============================================================
drop database test_generated_col;
