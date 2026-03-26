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
-- 10. VIRTUAL generated columns
-- ============================================================
create table t7 (a int, b int, c int generated always as (a + b) virtual);
show create table t7;
insert into t7(a, b) values (1, 2);
insert into t7(a, b) values (10, 20), (100, 200);
select * from t7 order by a;

-- Implicit column list with VIRTUAL
insert into t7 values (5, 6);
select * from t7 order by a;

-- Cannot insert into VIRTUAL column
insert into t7(a, b, c) values (1, 2, 3);

-- Cannot update VIRTUAL column
update t7 set c = 999 where a = 1;

-- UPDATE recomputes VIRTUAL column
update t7 set a = 50, b = 60 where a = 10;
select * from t7 order by a;

-- ============================================================
-- 11. Default type is VIRTUAL (shorthand without keyword)
-- ============================================================
create table t8 (x int, y int, z int as (x * y));
show create table t8;
insert into t8(x, y) values (3, 7);
select * from t8;

-- ============================================================
-- 12. VIRTUAL with string expression
-- ============================================================
create table t9 (first_name varchar(50), last_name varchar(50), full_name varchar(101) generated always as (concat(first_name, ' ', last_name)) virtual);
insert into t9(first_name, last_name) values ('Alice', 'Bob');
select * from t9;

-- ============================================================
-- 13. Chained VIRTUAL columns
-- ============================================================
create table t10 (a int, b int, c int as (a + b) virtual, d int as (c * 2) virtual);
insert into t10(a, b) values (5, 10);
select * from t10;

-- ============================================================
-- 14. DESC / SHOW COLUMNS shows STORED GENERATED / VIRTUAL GENERATED
-- ============================================================
create table t11 (a int, b int, c int generated always as (a + b) stored, d varchar(100) as (concat(cast(a as varchar), '-', cast(b as varchar))) virtual);
desc t11;
show full columns from t11;

-- ============================================================
-- 15. INFORMATION_SCHEMA.COLUMNS shows GENERATION_EXPRESSION
-- ============================================================
select column_name, extra, generation_expression from information_schema.columns where table_schema = 'test_generated_col' and table_name = 't11' and column_name in ('a','b','c','d') order by ordinal_position;

-- ============================================================
-- 16. ALTER TABLE DROP COLUMN dependency check
-- ============================================================
create table t12 (a int, b int, c int generated always as (a + b) stored);
-- @pattern
-- Column 'a' has a generated column dependency on column 'c'
alter table t12 drop column a;
-- dropping non-referenced column succeeds
alter table t12 drop column b;

-- ============================================================
-- 17. ALTER TABLE ADD generated column
-- ============================================================
create table t13 (a int, b int);
insert into t13 values (1,2),(3,4);
alter table t13 add column c int generated always as (a + b) stored;
desc t13;

-- ============================================================
-- 18. REPLACE INTO with generated column
-- ============================================================
create table t14 (id int primary key, a int, b int, c int generated always as (a * b) stored);
replace into t14 (id, a, b) values (1, 3, 4);
select * from t14;
replace into t14 (id, a, b) values (1, 5, 6);
select * from t14;

-- ============================================================
-- 19. CREATE TABLE LIKE preserves generated columns
-- ============================================================
create table t15 like t11;
show create table t15;
insert into t15 (a, b) values (10, 20);
select * from t15;

-- ============================================================
-- 20. Generated column with index
-- ============================================================
create table t16 (a int, b int, c int generated always as (a + b) stored, index idx_c (c));
insert into t16 (a, b) values (1,2),(3,4),(5,6);
select * from t16 where c = 7;

-- ============================================================
-- 21. WHERE filter on generated column
-- ============================================================
select * from t16 where c > 5 order by c;

-- ============================================================
-- 22. Drop the generated column itself (should succeed)
-- ============================================================
create table t17 (a int, b int, c int, d int generated always as (a + b) stored);
alter table t17 drop column d;
desc t17;

-- ============================================================
-- 23. NOT NULL constraint on generated column
-- ============================================================
create table t18 (a int, b int, c int not null generated always as (a + b) stored);
desc t18;
insert into t18 (a, b) values (1, 2);
select * from t18;

-- ============================================================
-- 24. ALTER TABLE CHANGE self-reference prevention
-- ============================================================
create table t19 (a int, b int);
-- Should fail: generated expression references the column being changed
alter table t19 change column a a int generated always as (a + 1) stored;

-- ============================================================
-- 25. Cleanup
-- ============================================================
drop database test_generated_col;
