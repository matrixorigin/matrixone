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
create table t12 (a int, b int, d int, c int generated always as (a + b) stored);
-- @pattern
-- Cannot modify column 'a': generated column 'c' depends on it
alter table t12 drop column a;
-- dropping another referenced base column should also fail
alter table t12 drop column b;
-- dropping a non-referenced column should succeed
alter table t12 drop column d;
desc t12;

-- ============================================================
-- 17. ALTER TABLE ADD generated column
-- ============================================================
create table t13 (a int, b int);
insert into t13 values (1,2),(3,4);
alter table t13 add column c int generated always as (a + b) stored;
desc t13;
select * from t13 order by a;

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
-- 25. ALTER TABLE RENAME COLUMN with generated dependency
-- ============================================================
create table t20 (a int, b int, c int generated always as (a + b) stored);
-- Should fail: column 'a' is referenced by generated column 'c'
alter table t20 rename column a to a2;
-- Should succeed: column 'c' is not referenced by any other generated column
alter table t20 rename column c to c2;
desc t20;

-- ============================================================
-- 26. ALTER TABLE CHANGE COLUMN with generated dependency
-- ============================================================
create table t21 (a int, b int, c int generated always as (a + b) stored);
-- Should fail: column 'a' is referenced by generated column 'c' (name change)
alter table t21 change column a a2 int;
-- Should succeed: no name change, just type change (not dependent)
alter table t21 change column a a bigint;
desc t21;

-- ============================================================
-- 27. ALTER TABLE CHANGE - circular dependency detection
-- ============================================================
create table t22 (a int, b int generated always as (a + 1) stored);
-- Should fail: changing 'a' to reference 'b' creates a cycle (a -> b -> a)
alter table t22 change column a a int generated always as (b + 1) stored;

-- ============================================================
-- 28. CREATE TABLE forward reference to base column
-- ============================================================
-- Should succeed: generated column references base column 'b' defined later
create table t23 (a int, c int generated always as (a + b) stored, b int);
insert into t23 (a, b) values (1, 2);
select * from t23;
desc t23;

-- ============================================================
-- 29. Forward reference to generated column should fail
-- ============================================================
-- Should fail: generated column 'c' references generated column 'd' defined later
create table t24_fail (a int, c int generated always as (d + 1) stored, d int generated always as (a + 1) stored);
-- Should fail: generated column cannot refer to itself
create table t24_self_fail (a int generated always as (a + 1) stored);

-- ============================================================
-- 30. INSERT with generated column = DEFAULT
-- ============================================================
create table t25 (a int, b int, c int generated always as (a + b) stored);
-- Should succeed: DEFAULT value for generated column is allowed
insert into t25 (a, b, c) values (1, 2, default);
select * from t25;
-- Should succeed: multiple rows with DEFAULT
insert into t25 (a, b, c) values (3, 4, default), (5, 6, default);
select * from t25;
-- Should fail: value count still needs to match the explicit column list
insert into t25 (a, b, c) values (7, 8);
-- Should fail: non-DEFAULT value for generated column
insert into t25 (a, b, c) values (9, 10, 99);

-- ============================================================
-- 31. UPDATE SET generated column = DEFAULT
-- ============================================================
create table t26 (a int, b int, c int generated always as (a + b) stored);
insert into t26 (a, b) values (1, 2);
select * from t26;
-- Should succeed: SET gen_col = DEFAULT is allowed
update t26 set a = 10, c = default;
select * from t26;
-- Should fail: SET gen_col = explicit value
update t26 set c = 99;

-- ============================================================
-- 32. Non-deterministic function in generated column
-- ============================================================
-- Should fail: rand() is volatile
create table t27_fail (a int, b double generated always as (rand()) stored);
-- Should fail: uuid() is volatile
create table t28_fail (a int, b varchar(36) generated always as (uuid()) stored);

-- ============================================================
-- 33. ON DUPLICATE KEY UPDATE with generated columns
-- ============================================================
create table t29_dup (a int primary key, b int, c int generated always as (b + 1) stored);
insert into t29_dup (a, b) values (1, 1);
insert into t29_dup (a, b) values (1, 2) on duplicate key update b = values(b);
select * from t29_dup;
insert into t29_dup (a, b) values (1, 3) on duplicate key update c = 99;
insert into t29_dup (a, b) values (1, 4) on duplicate key update c = default, b = values(b);
select * from t29_dup;

-- ============================================================
-- 34. Generated expression cannot refer to AUTO_INCREMENT or variables
-- ============================================================
create table t30_fail (id int auto_increment primary key, x int generated always as (id + 1) stored);
create table t31_fail (a int, b varchar(200) generated always as (@@sql_mode) stored);

-- ============================================================
-- 35. FOREIGN KEY cannot reference a VIRTUAL generated column
-- ============================================================
create table t32_parent (a int, b int generated always as (a + 1) virtual, unique key uk_b (b));
create table t32_child (c int, constraint fk_t32 foreign key (c) references t32_parent (b));

-- ============================================================
-- 36. LOAD DATA with generated columns
-- ============================================================
create table t33_load (a int, b int, c int generated always as (a + b) stored);
load data inline format='csv', data='1,2' into table t33_load fields terminated by ',';
select * from t33_load;
load data inline format='csv', data='3,4,99' into table t33_load fields terminated by ',' (a, b, c);

-- ============================================================
-- 37. ALTER TABLE ADD COLUMN FIRST/AFTER remaps generated col ColPos
-- ============================================================
create table t34_remap (a int, b int, c int generated always as (a + b) stored);
insert into t34_remap (a, b) values (1, 2);
alter table t34_remap add column x int default 0 first;
insert into t34_remap (x, a, b) values (10, 3, 4);
select * from t34_remap;
-- ADD COLUMN AFTER
create table t35_remap (a int, b int, c int generated always as (a * b) stored);
insert into t35_remap (a, b) values (3, 5);
alter table t35_remap add column y int default 0 after a;
insert into t35_remap (a, y, b) values (4, 99, 6);
select * from t35_remap;
-- DROP COLUMN remaps
create table t36_remap (a int, b int, d int, c int generated always as (a + b) stored);
insert into t36_remap (a, b, d) values (1, 2, 99);
alter table t36_remap drop column d;
insert into t36_remap (a, b) values (5, 6);
select * from t36_remap;

-- ============================================================
-- 38. ODKU with generated column in PRIMARY KEY
-- ============================================================
create table t37_odku_pk (a int, b int generated always as (a*2) stored, primary key(b));
insert into t37_odku_pk (a) values (1);
insert into t37_odku_pk (a) values (1) on duplicate key update a=5;
select * from t37_odku_pk;

-- ============================================================
-- 39. MODIFY COLUMN dependency check
-- ============================================================
create table t38_modify (a int, b int, c int generated always as (a + b) stored);
alter table t38_modify modify column a bigint;

-- ============================================================
-- 40. VIRTUAL generated column cannot be PRIMARY KEY
-- ============================================================
create table t39_vpk (a int, b int generated always as (a+1) virtual, primary key(b));

-- ============================================================
-- 41. Qualified column names rejected in generated expression
-- ============================================================
create table t40_qualname (a int, b int generated always as (t40_qualname.a + 1) stored);

-- ============================================================
-- 42. Cleanup
-- ============================================================
drop database test_generated_col;
