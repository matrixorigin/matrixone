-- Keep table rel_logical_id (logical table_id) stable across DDL (ALTER/TRUNCATE)
set global enable_privilege_cache = off;
-- ============================================================
-- Test 1: ALTER TABLE ADD COLUMN keeps rel_logical_id stable
-- ============================================================

drop table if exists t1;
create table t1(id int primary key, v int);

alter table t1 add column v2 int;

-- DML: new column exists and is writable
insert into t1(id, v, v2) values (1, 10, 20);
select id, v, v2 from t1 order by id;


-- ============================================================
-- Test 2: Complex ALTER chain keeps rel_logical_id stable
-- ============================================================

drop table if exists t2;
create table t2(id int primary key, a int, b varchar(20));

alter table t2 add column c bool;
alter table t2 modify column b varchar(100);
alter table t2 drop column a;
alter table t2 alter column c set default true;

-- DDL result: column layout and default
show columns from t2;

-- DML: insert without specifying c should use default TRUE
insert into t2(id, b) values (1, 'abc');
select id, b, c from t2 order by id;


-- ============================================================
-- Test 3: TRUNCATE TABLE keeps rel_logical_id but clears data
-- ============================================================

drop table if exists t3;
create table t3(id int primary key, v int);

insert into t3 values (1, 100), (2, 200);
truncate table t3;

-- DML: after TRUNCATE only new rows should exist
insert into t3 values (3, 300);
select id, v from t3 order by id;


-- ============================================================
-- Test 4: Multiple TRUNCATE + ALTER chain keeps rel_logical_id
-- ============================================================

drop table if exists t4;
create table t4(id int primary key, v int);

insert into t4 values (1, 10), (2, 20);
truncate table t4;

alter table t4 add column comment varchar(50);
truncate table t4;
alter table t4 drop column v;

-- DDL result: v dropped, comment exists
show columns from t4;

-- DML on final layout
insert into t4(id, comment) values (1, 'ok');
select id, comment from t4 order by id;


-- ============================================================
-- Test 5: Index / primary key changes keep rel_logical_id
-- ============================================================

drop table if exists t5;
create table t5(id int, v int, name varchar(50));

alter table t5 add primary key (id);
create index idx_t5_v on t5(v);
drop index idx_t5_v on t5;

-- DML: primary key constraint should prevent duplicate id, and update/delete work
delete from t5;
insert into t5 values (1, 10, 'a');
-- Second insert with same id should fail with duplicate key error
insert into t5 values (1, 20, 'b');
insert into t5 values (2, 30, 'b');
update t5 set v = v + 1 where id = 1;
delete from t5 where name = 'b';
select * from t5 order by id, v, name;

drop table if exists t5;


-- ============================================================
-- Test 6: UNIQUE INDEX keeps rel_logical_id and enforces uniqueness
-- ============================================================

drop table if exists t6;
create table t6(
    id   int primary key,
    name varchar(50),
    email varchar(100)
);

create unique index idx_t6_email on t6(email);

-- DML: unique index should prevent duplicate email
insert into t6 values (1, 'alice', 'alice@test.com');
insert into t6 values (2, 'bob',   'bob@test.com');
alter table t6 add column comment varchar(50);
-- This insert should fail due to duplicate email
-- @pattern
insert into t6 values (3, 'alice2', 'alice@test.com','default');

select * from t6 order by id;
show index from t6;

drop table if exists t6;


-- ============================================================
-- Test 7: IVF (ivfflat) vector index keeps rel_logical_id
--          and basic vector search still works
-- ============================================================

drop table if exists t7;
create table t7 (
    id int primary key,
    embedding vecf32(4) default null
);

create index idx_t7_ivf using ivfflat on t7(embedding) lists=2 op_type 'vector_l2_ops';

-- DML: insert vectors and run a simple ANN query
insert into t7 values
    (1, '[0.1, 0.2, 0.3, 0.4]'),
    (2, '[0.2, 0.1, 0.4, 0.3]'),
    (3, '[0.9, 0.9, 0.9, 0.9]');

select id, L2_DISTANCE(embedding, '[0.1, 0.2, 0.3, 0.4]') as dist
from t7
order by dist
limit 3;
truncate table t7;
insert into t7 values
    (1, '[0.1, 0.2, 0.3, 0.4]'),
    (2, '[0.2, 0.1, 0.4, 0.3]'),
    (3, '[0.9, 0.9, 0.9, 0.9]');
select count(*) from t7;
drop index idx_t7_ivf on t7;

drop table if exists t7;


-- ============================================================
-- Test 8: RENAME COLUMN keeps rel_logical_id and DML works
-- ============================================================

drop table if exists t8;
create table t8(a int, b int);
insert into t8 values (1, 10), (2, 20);

alter table t8 rename column a to a_new;

-- DDL result
show columns from t8;

-- DML using renamed column
update t8 set a_new = a_new + 1 where b = 10;
delete from t8 where a_new > 3;
select a_new, b from t8 order by a_new;

drop table if exists t8;


-- ============================================================
-- Test 9: Partitioned table ALTER/TRUNCATE keep rel_logical_id
-- ============================================================

drop table if exists tp1;
create table tp1 (
    id int,
    dt date,
    v  int,
    primary key(id)
) partition by range columns (dt) (
    partition p0 values less than ('2025-01-01'),
    partition p1 values less than ('2026-01-01')
);

insert into tp1 values
  (1, '2024-12-31', 100),
  (2, '2025-06-01', 200);

-- Add a new partition then truncate
alter table tp1 add partition (
    partition p2 values less than ('2027-01-01')
);
truncate table tp1;

-- DML after TRUNCATE and partition change
insert into tp1 values
  (3, '2025-02-02', 300),
  (4, '2026-06-06', 400);
update tp1 set v = v * 2 where id = 3;
delete from tp1 where dt >= '2026-01-01';
select id, dt, v from tp1 order by id;

drop table if exists tp1;
set global enable_privilege_cache = on;
