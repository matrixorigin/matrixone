-- Test: INSERT ON DUPLICATE KEY UPDATE on tables that previously fell back
-- to the legacy operator path. Covers tables without an explicit primary key
-- (fake PK + unique key) and tables with foreign keys, all handled by the
-- modern dedup-join + multi-update path.

-- ============================================================
-- Part 1: fake primary key + single-column unique key
-- ============================================================
drop table if exists t_odku_fakepk;
create table t_odku_fakepk (a int, b varchar(20), unique key(a));

insert into t_odku_fakepk values (1, 'x');
-- conflict on unique key -> update
insert into t_odku_fakepk values (1, 'y') on duplicate key update b = 'updated';
select * from t_odku_fakepk order by a;

-- no conflict -> insert new row
insert into t_odku_fakepk values (2, 'new') on duplicate key update b = 'z';
select * from t_odku_fakepk order by a;

-- update referencing VALUES()
insert into t_odku_fakepk values (1, 'fromvalues') on duplicate key update b = values(b);
select * from t_odku_fakepk order by a;

-- batch mixing conflict and new rows
insert into t_odku_fakepk values (1, 'b1'), (3, 'b3'), (2, 'b2')
    on duplicate key update b = values(b);
select * from t_odku_fakepk order by a;

drop table if exists t_odku_fakepk;

-- ============================================================
-- Part 2: fake primary key + multi-column unique key
-- ============================================================
drop table if exists t_odku_fakepk_multi;
create table t_odku_fakepk_multi (a int, b int, c varchar(20), unique key(a, b));

insert into t_odku_fakepk_multi values (1, 1, 'first');
-- conflict on composite unique key -> update
insert into t_odku_fakepk_multi values (1, 1, 'dup') on duplicate key update c = 'merged';
select * from t_odku_fakepk_multi order by a, b;

-- partial overlap on composite unique key is NOT a conflict -> insert
insert into t_odku_fakepk_multi values (1, 2, 'second') on duplicate key update c = 'should_not_apply';
select * from t_odku_fakepk_multi order by a, b;

drop table if exists t_odku_fakepk_multi;

-- ============================================================
-- Part 3: fake primary key + no unique key (degrades to plain insert)
-- ============================================================
drop table if exists t_odku_nopk;
create table t_odku_nopk (a int, b varchar(20));

insert into t_odku_nopk values (1, 'one');
-- without any key there is no duplicate concept, the update clause is inert
insert into t_odku_nopk values (1, 'two') on duplicate key update b = 'never';
select * from t_odku_nopk order by a, b;

drop table if exists t_odku_nopk;

-- ============================================================
-- Part 4: foreign key table ODKU
-- ============================================================
drop table if exists t_odku_child;
drop table if exists t_odku_parent;
create table t_odku_parent (id int primary key, name varchar(20));
create table t_odku_child (
    cid int primary key,
    pid int,
    v int,
    foreign key (pid) references t_odku_parent(id)
);

insert into t_odku_parent values (1, 'p1'), (2, 'p2');
insert into t_odku_child values (10, 1, 100);

-- ODKU conflict on PK, update a non-foreign-key column
insert into t_odku_child values (10, 1, 999) on duplicate key update v = 999;
select * from t_odku_child order by cid;

-- ODKU insert a new row with a valid foreign key
insert into t_odku_child values (20, 2, 200) on duplicate key update v = 200;
select * from t_odku_child order by cid;

-- ODKU insert a new row with an invalid foreign key -> must fail
insert into t_odku_child values (30, 99, 300) on duplicate key update v = 300;
select * from t_odku_child order by cid;

-- plain insert with valid foreign key still works on the modern path
insert into t_odku_child values (40, 1, 400);
select * from t_odku_child order by cid;

-- plain insert with invalid foreign key -> must fail
insert into t_odku_child values (50, 99, 500);
select * from t_odku_child order by cid;

drop table if exists t_odku_child;
drop table if exists t_odku_parent;

-- ============================================================
-- Part 5: real primary key + multiple unique keys (MySQL-aligned)
-- Any unique-key conflict updates the first conflicting row; priority is
-- PRIMARY > unique keys in definition order.
-- ============================================================
drop table if exists t_odku_realpk;
create table t_odku_realpk (
    id int primary key,
    uk1 int unique,
    uk2 int unique,
    val int
);
insert into t_odku_realpk values (1, 10, 100, 1000), (2, 20, 200, 2000);

-- PK conflict has highest priority: updates row id=1
insert into t_odku_realpk values (1, 99, 999, 5) on duplicate key update val = val + 1;
select * from t_odku_realpk order by id;

-- single unique-key (uk1) conflict, new PK: updates the conflicting row, keeps id=1
insert into t_odku_realpk values (3, 10, 888, 5) on duplicate key update val = val + 1;
select * from t_odku_realpk order by id;

-- cross-row conflict: uk1 hits row 1, uk2 hits row 2 -> uk1 wins (definition order)
insert into t_odku_realpk values (4, 10, 200, 5) on duplicate key update val = val + 1;
select * from t_odku_realpk order by id;

-- PK exists and a unique key hits a different row -> PRIMARY wins, updates row id=2
insert into t_odku_realpk values (2, 10, 222, 5) on duplicate key update val = val + 1;
select * from t_odku_realpk order by id;

-- both unique keys NULL -> no conflict -> plain insert
insert into t_odku_realpk values (5, NULL, NULL, 5) on duplicate key update val = val + 1;
select * from t_odku_realpk order by id;

-- In-batch protection: two brand-new rows sharing a new unique-key value still
-- error deterministically (avoids a duplicated unique-index entry).
insert into t_odku_realpk values (20, 77, 701, 5), (21, 77, 702, 5) on duplicate key update val = val + 1;
select * from t_odku_realpk order by id;

drop table if exists t_odku_realpk;

-- ============================================================
-- Part 6: table carrying an irregular (fulltext) index
-- Irregular indexes (fulltext / vector) are stripped from the modern insert
-- path by getValidIndexes and maintained asynchronously, so ON DUPLICATE KEY
-- UPDATE works on such tables instead of falling back to the legacy path.
-- (A vector/ivfflat index follows the exact same code path.)
-- ============================================================
set experimental_fulltext_index = 1;
drop table if exists t_odku_ft;
create table t_odku_ft(id int primary key, uk int unique, body text, val int);
insert into t_odku_ft values (1, 10, 'hello world', 100), (2, 20, 'foo bar', 200);
create fulltext index ftidx on t_odku_ft(body);

-- PK conflict on a fulltext-indexed table: modern path, updates row 1
insert into t_odku_ft values (1, 99, 'changed', 5) on duplicate key update val = val + 1;
select id, uk, val from t_odku_ft order by id;

-- unique-key conflict (uk=10 hits row 1): updates the conflicting row, keeps id=1
insert into t_odku_ft values (3, 10, 'new', 5) on duplicate key update val = val + 1;
select id, uk, val from t_odku_ft order by id;

drop table if exists t_odku_ft;

-- ============================================================
-- Part 7: table carrying an irregular (ivfflat vector) index
-- Index maintenance upserts a version counter into the index metadata table via
-- ON DUPLICATE KEY UPDATE; the modern path must handle that internal ODKU so the
-- vector index can be created, and user ON DUPLICATE KEY UPDATE on the base table
-- then follows the same MySQL-aligned conflict resolution.
-- ============================================================
drop table if exists t_odku_vec;
create table t_odku_vec(id int primary key, uk int unique, embedding vecf32(3), val int);
insert into t_odku_vec values (1, 10, '[1,2,3]', 100), (2, 20, '[4,5,6]', 200);
create index idx_vec using ivfflat on t_odku_vec(embedding) lists = 2 op_type 'vector_l2_ops';

-- PK conflict on a vector-indexed table: modern path, updates row 1
insert into t_odku_vec values (1, 99, '[7,8,9]', 5) on duplicate key update val = val + 1;
select id, uk, val from t_odku_vec order by id;

-- unique-key conflict (uk=10 hits row 1): updates the conflicting row, keeps id=1
insert into t_odku_vec values (3, 10, '[1,1,1]', 5) on duplicate key update val = val + 1;
select id, uk, val from t_odku_vec order by id;

-- no conflict: plain insert
insert into t_odku_vec values (4, 40, '[2,2,2]', 400) on duplicate key update val = val + 1;
select id, uk, val from t_odku_vec order by id;

drop table if exists t_odku_vec;
