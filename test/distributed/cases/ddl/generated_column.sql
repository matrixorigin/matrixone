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

-- Explicit ordinary columns continue to omit the generated column.
insert into t1(a, b) values (5, 6);
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

-- Explicit ordinary columns continue to omit the VIRTUAL generated column.
insert into t7(a, b) values (5, 6);
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
-- ODKU that can change a generated primary key is rejected as unsupported DML.
insert into t37_odku_pk (a) values (1) on duplicate key update a=5;
select * from t37_odku_pk;

-- ============================================================
-- 39. MODIFY COLUMN with generated-column dependencies
-- ============================================================
create table t38_modify (
    id int primary key,
    a int,
    b int,
    g bigint generated always as (a + 5) stored,
    key idx_g(g),
    key idx_b(b)
);
insert into t38_modify (id, a, b) values (1, 10, 20);
alter table t38_modify modify column a bigint;
insert into t38_modify (id, a, b) values (2, 4000000000, 30);
select id, a, g from t38_modify order by id;
select count(*) as matched from t38_modify force index (idx_g) where g = 15;
select count(*) as matched from t38_modify force index (idx_g) where g = 4000000005;
select count(*) as matched from t38_modify force index (idx_b) where b = 20;

-- UNSIGNED, NOT NULL, and column reordering keep generated expressions valid.
create table t38_unsigned (a int, b int, g bigint generated always as (a + b) stored);
insert into t38_unsigned (a, b) values (10, 20);
alter table t38_unsigned modify column a int unsigned;
alter table t38_unsigned modify column a int unsigned not null;
insert into t38_unsigned (a, b) values (4000000000, 2);
select count(*) as matched from t38_unsigned where (a = 10 and g = 30) or (a = 4000000000 and g = 4000000002);

create table t38_reorder (a int, b int, g int generated always as (a * 100 + b) stored);
insert into t38_reorder (a, b) values (1, 2);
alter table t38_reorder modify column a int after b;
insert into t38_reorder (a, b) values (3, 4);
select * from t38_reorder order by a;

-- Decimal128 -> Decimal256 widening must retain exact generated values.
create table t38_decimal (
    id int primary key,
    d decimal(38, 0),
    g decimal(41, 0) generated always as (d + 1) stored
);
insert into t38_decimal (id, d) values (1, 1);
alter table t38_decimal modify column d decimal(40, 0);
insert into t38_decimal (id, d) values (2, cast('9999999999999999999999999999999999999999' as decimal(40, 0)));
select count(*) as good_rows from t38_decimal where (id = 1 and d = 1 and g = 2) or
    (id = 2 and d = cast('9999999999999999999999999999999999999999' as decimal(40, 0)) and
     g = cast('10000000000000000000000000000000000000000' as decimal(41, 0)));

-- A value-changing conversion must rebuild dependent generated indexes while
-- an unrelated secondary index remains valid.
create table t38_index_refresh (
    id int primary key,
    a decimal(10, 1),
    payload int,
    g int generated always as (a * 10) stored,
    g2 int generated always as (g * 10) stored,
    key idx_g2(g2),
    key idx_payload(payload)
);
insert into t38_index_refresh (id, a, payload) values (1, 1.1, 7), (2, 1.4, 8);
alter table t38_index_refresh modify column a int;
select count(*) as matched from t38_index_refresh force index (idx_g2) where g2 = 100;
select count(*) as matched from t38_index_refresh force index (idx_g2) where g2 in (110, 140);
select count(*) as matched from t38_index_refresh ignore index (idx_g2) where g2 = 100;
select count(*) as matched from t38_index_refresh force index (idx_payload) where payload in (7, 8);

-- A generated primary-key change also requires rebuilding unrelated secondary
-- indexes because their entries carry the primary-key value.
create table t38_generated_pk (
    a decimal(10, 1),
    payload int,
    g bigint generated always as (a * 10) stored,
    primary key (g),
    unique key idx_payload(payload)
);
insert into t38_generated_pk (a, payload) values (1.1, 7), (2.1, 8);
alter table t38_generated_pk modify column a int;
select g from t38_generated_pk order by g;
select a, g, payload from t38_generated_pk force index (idx_payload) where payload in (7, 8) order by payload;
select a, g, payload from t38_generated_pk ignore index (idx_payload) where payload in (7, 8) order by payload;

-- Failed COPY must leave the old nullable schema/data usable.
create table t38_rollback (id int primary key, a int, g int generated always as (a + 1) stored);
insert into t38_rollback (id, a) values (1, null);
-- @regex("Column 'a' cannot be null",true)
alter table t38_rollback modify column a int not null;
insert into t38_rollback (id, a) values (2, null);
select count(*) as null_rows from t38_rollback where a is null and g is null;

create table t38_unique_rollback (
    id int primary key,
    a decimal(10, 1),
    g int generated always as (a * 10) stored,
    unique key uk_g(g)
);
insert into t38_unique_rollback (id, a) values (1, 1.1), (2, 1.4);
-- The conversion collides on the generated unique key after both values map to 1.
-- @regex("Duplicate entry",true)
alter table t38_unique_rollback modify column a int;
select count(*) as old_keys from t38_unique_rollback force index (uk_g) where g in (11, 14);
insert into t38_unique_rollback (id, a) values (3, 2.0);
select count(*) as total_rows from t38_unique_rollback;
select count(*) as leaked_copy_tables from information_schema.tables
where table_schema = database()
  and (table_name like 't38_rollback_copy_%' or table_name like 't38_unique_rollback_copy_%');

-- ============================================================
-- 40. VIRTUAL generated column cannot be PRIMARY KEY
-- ============================================================
create table t39_vpk (a int, b int generated always as (a+1) virtual, primary key(b));

-- ============================================================
-- 41. Qualified column names rejected in generated expression
-- ============================================================
create table t40_qualname (a int, b int generated always as (t40_qualname.a + 1) stored);

-- ============================================================
-- 42. LOAD DATA multi-row correctness
-- ============================================================
create table t41_load_bulk (id int, a int, b int, c int generated always as (a + b) stored);
load data inline format='csv', data='1,10,20\n2,30,40\n3,50,60\n' into table t41_load_bulk fields terminated by ',' (id, a, b);
select count(*) as bad_rows from t41_load_bulk where c != a + b;
select * from t41_load_bulk order by id;

-- ============================================================
-- 43. Large-scale DML churn correctness
-- ============================================================
create table t42_churn (id int primary key, a int, b int, c int generated always as (a + b) stored);
insert into t42_churn (id, a, b) select result, result, result * 2 from generate_series(1, 10000, 1) g;
update t42_churn set a = a + 3 where id % 2 = 0;
update t42_churn set b = b + 5 where id % 5 = 0;
insert into t42_churn (id, a, b) values (1, 7, 8) on duplicate key update a = values(a), b = values(b);
replace into t42_churn (id, a, b) values (2, 9, 10);
select count(*) as bad_rows from t42_churn where c != a + b;
select * from t42_churn where id between 1 and 5 order by id;

-- ============================================================
-- 44. ODKU generated UNIQUE key safety (#28051)
-- ============================================================
create table t43_odku_generated_unique (id int primary key, doc json, kind varchar(20) generated always as (doc ->> '$.kind') stored, payload int, unique key uk_kind(kind));
insert into t43_odku_generated_unique (id, doc, payload) values (1, '{"kind":"alpha"}', 10), (2, '{"kind":"beta"}', 20);
-- The original #28051 shape must be rejected before it can make two rows share beta.
insert into t43_odku_generated_unique (id, doc, payload) values (3, '{"kind":"alpha"}', 30) on duplicate key update doc = json_set(doc, '$.kind', 'beta');
select id, kind, payload from t43_odku_generated_unique order by id;
select id, kind from t43_odku_generated_unique force index (uk_kind) order by id;
-- An update of an unrelated column remains supported.
insert into t43_odku_generated_unique (id, doc, payload) values (1, '{"kind":"alpha"}', 99) on duplicate key update payload = values(payload);
select id, kind, payload from t43_odku_generated_unique order by id;
-- A multi-row statement is rejected atomically when its update may change the key.
insert into t43_odku_generated_unique (id, doc, payload) values (1, '{"kind":"gamma"}', 111), (2, '{"kind":"delta"}', 222) on duplicate key update doc = values(doc);
select id, kind, payload from t43_odku_generated_unique order by id;
-- Ordinary UPDATE keeps its existing duplicate-key behavior as the control group.
update t43_odku_generated_unique set doc = json_set(doc, '$.kind', 'beta') where id = 1;

-- ============================================================
-- 45. ODKU generated non-unique index maintenance
-- ============================================================
create table t44_odku_generated_index (id int primary key, source int, payload int, generated_key int generated always as (source * 2) stored, key idx_generated_key(generated_key));
insert into t44_odku_generated_index (id, source, payload) values (1, 1, 10), (2, 2, 20);
insert into t44_odku_generated_index (id, source, payload) values (1, 2, 11) on duplicate key update source = values(source);
select id, generated_key from t44_odku_generated_index force index (idx_generated_key) where generated_key = 2;
select id, generated_key from t44_odku_generated_index force index (idx_generated_key) where generated_key = 4 order by id;
-- A predicate-free forced index scan exposes a stale hidden key as a duplicate base row.
select id, generated_key from t44_odku_generated_index force index for order by (idx_generated_key) order by generated_key, id;

-- ============================================================
-- 46. Implicit VALUES requires DEFAULT at generated positions (#28241)
-- ============================================================
create table t45_implicit_values (id int primary key, a int, g int generated always as (a + 1) stored, payload int);
-- Full visible tuple is required; generated position must be DEFAULT.
insert into t45_implicit_values values (1, 10, default, 100), (2, 20, default, 200);
select id, a, g, payload from t45_implicit_values order by id;
-- A short tuple cannot silently omit the generated column.
insert into t45_implicit_values values (3, 30, 300);
-- A supplied non-DEFAULT value for the generated position is rejected.
insert into t45_implicit_values values (3, 30, 31, 300);
-- A later bad tuple must not leave the earlier tuple from this statement behind.
insert into t45_implicit_values values (8, 80, default, 800), (9, 90, 91, 900);
select count(*) as partial_rows from t45_implicit_values where id in (3, 8, 9);
-- The explicit ordinary-column and implicit INSERT ... SELECT mappings remain unchanged.
insert into t45_implicit_values (id, a, payload) values (5, 50, 500);
insert into t45_implicit_values select 6, 60, 600;
-- REPLACE also consumes the full visible tuple and recomputes the generated value.
replace into t45_implicit_values values (1, 99, default, 999);
select id, a, g, payload from t45_implicit_values order by id;
-- Rebinding a retained PREPARE AST after a schema change must preserve the generated DEFAULT slot.
prepare t45_replace from 'replace into t45_implicit_values values (7, 70, default, 700)';
execute t45_replace;
alter table t45_implicit_values modify column payload bigint;
execute t45_replace;
deallocate prepare t45_replace;
select count(*) as prepared_rows from t45_implicit_values where id = 7;
select id, a, g, payload from t45_implicit_values where id = 7;
drop table t45_implicit_values;

-- ============================================================
-- 47. Cleanup
-- ============================================================
drop database test_generated_col;
