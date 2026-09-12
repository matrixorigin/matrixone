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

-- Statement-local arbitration: the first row inserts and publishes uk1=77;
-- the later row resolves that conflict as UPDATE of the first row.
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
create table t_odku_ft(id int primary key, uk int unique, body text, embedding vecf32(3), val int);
insert into t_odku_ft values (1, 10, 'hello world', '[1,2,3]', 100), (2, 20, 'foo bar', '[4,5,6]', 200);
create fulltext index ftidx on t_odku_ft(body);
select id from t_odku_ft where match(body) against('hello') order by id;

-- PK conflict updates only a non-indexed scalar. The raw vector has no ANN
-- index, and the unchanged fulltext posting must remain searchable.
insert into t_odku_ft values (1, 99, 'changed', '[7,8,9]', 5) on duplicate key update val = val + 1;
select id, uk, val from t_odku_ft order by id;
select id from t_odku_ft where match(body) against('hello') order by id;

-- unique-key conflict (uk=10 hits row 1): updates the conflicting row, keeps id=1
insert into t_odku_ft values (3, 10, 'new', '[1,1,1]', 5) on duplicate key update val = val + 1;
select id, uk, val from t_odku_ft order by id;
select id from t_odku_ft where match(body) against('hello') order by id;

-- One statement can contain a conflict and a fresh insert. The conflict must
-- skip fulltext rebuild while the fresh row still receives postings.
insert into t_odku_ft values (1, 99, 'ignored', '[9,9,9]', 5), (3, 30, 'fresh token', '[3,3,3]', 300) on duplicate key update val = val + 1;
select id, uk, val from t_odku_ft order by id;
select id from t_odku_ft where match(body) against('hello') order by id;
select id from t_odku_ft where match(body) against('fresh') order by id;

drop table if exists t_odku_ft;

-- ODKU updating the fulltext-indexed column itself must drop the old tokens and
-- index the new ones synchronously, so the row becomes searchable by the new
-- words and not the old ones.
drop table if exists t_odku_ft2;
create table t_odku_ft2(id int primary key, body text);
create fulltext index ftidx2 on t_odku_ft2(body);
insert into t_odku_ft2 values (1, 'hello world'), (2, 'foo bar');
insert into t_odku_ft2 values (1, 'ignored') on duplicate key update body = 'alpha beta';
select id, body from t_odku_ft2 order by id;
select id from t_odku_ft2 where match(body) against('alpha') order by id;
select id from t_odku_ft2 where match(body) against('hello') order by id;
select id from t_odku_ft2 where match(body) against('foo') order by id;
-- Rollback must undo both the base-row update and synchronous posting changes.
begin;
insert into t_odku_ft2 values (1, 'ignored') on duplicate key update body = 'rollback token';
rollback;
select id from t_odku_ft2 where match(body) against('rollback') order by id;
select id from t_odku_ft2 where match(body) against('alpha') order by id;
-- Replaying the same final indexed value remains one visible document.
insert into t_odku_ft2 values (1, 'ignored') on duplicate key update body = 'alpha beta';
select id from t_odku_ft2 where match(body) against('alpha') order by id;
-- no-conflict insert is also indexed
insert into t_odku_ft2 values (3, 'gamma delta') on duplicate key update body = 'nope';
select id from t_odku_ft2 where match(body) against('gamma') order by id;
drop table if exists t_odku_ft2;

-- If the assignment list includes the fulltext column but the final stored
-- value is unchanged, a conflicting row must keep its postings without a
-- rebuild. A fresh row in the same batch must still be tokenized and indexed.
drop table if exists t_odku_ft_value_noop;
create table t_odku_ft_value_noop(id int primary key, body text, payload int);
create fulltext index ftidx_value_noop on t_odku_ft_value_noop(body);
insert into t_odku_ft_value_noop values (1, 'same posting', 10), (2, 'untouched token', 20);
-- 100% conflict: the assignment mentions body, but its final stored value is equal.
insert into t_odku_ft_value_noop values (1, 'same posting', 11) on duplicate key update body = values(body), payload = values(payload);
select id, body, payload from t_odku_ft_value_noop order by id;
-- Mixed batch: the equal conflict is filtered while the new row is indexed.
insert into t_odku_ft_value_noop values (1, 'same posting', 12), (3, 'fresh token', 30) on duplicate key update body = values(body), payload = values(payload);
select id, body, payload from t_odku_ft_value_noop order by id;
select id from t_odku_ft_value_noop where match(body) against('same') order by id;
select id from t_odku_ft_value_noop where match(body) against('untouched') order by id;
select id from t_odku_ft_value_noop where match(body) against('fresh') order by id;
drop table if exists t_odku_ft_value_noop;

-- NULL-safe equality boundaries: NULL->NULL is a no-op, while NULL->text and
-- text->NULL must rebuild the posting set.
drop table if exists t_odku_ft_nulls;
create table t_odku_ft_nulls(id int primary key, body text);
insert into t_odku_ft_nulls values (1, NULL), (2, 'oldtoken');
create fulltext index ftidx_nulls on t_odku_ft_nulls(body);
insert into t_odku_ft_nulls values (1, NULL) on duplicate key update body = values(body);
select id, body from t_odku_ft_nulls order by id;
insert into t_odku_ft_nulls values (1, 'newtoken') on duplicate key update body = values(body);
select id from t_odku_ft_nulls where match(body) against('newtoken') order by id;
insert into t_odku_ft_nulls values (2, NULL) on duplicate key update body = values(body);
select id from t_odku_ft_nulls where match(body) against('oldtoken') order by id;
select id, body from t_odku_ft_nulls order by id;
drop table if exists t_odku_ft_nulls;

-- A multi-column FULLTEXT index can skip maintenance only when every indexed
-- value is equal. Changing either part must replace the whole posting set.
drop table if exists t_odku_ft_multi;
create table t_odku_ft_multi(id int primary key, title text, body text, payload int);
insert into t_odku_ft_multi values (1, 'alpha title', 'beta body', 10), (2, 'stable title', 'stable body', 20);
create fulltext index ftidx_multi on t_odku_ft_multi(title, body);
insert into t_odku_ft_multi values (1, 'alpha title', 'beta body', 11) on duplicate key update title = values(title), body = values(body), payload = values(payload);
select id, title, body, payload from t_odku_ft_multi order by id;
select id from t_odku_ft_multi where match(title, body) against('beta') order by id;
insert into t_odku_ft_multi values (1, 'alpha title', 'gamma body', 12) on duplicate key update title = values(title), body = values(body), payload = values(payload);
select id from t_odku_ft_multi where match(title, body) against('alpha') order by id;
select id from t_odku_ft_multi where match(title, body) against('beta') order by id;
select id from t_odku_ft_multi where match(title, body) against('gamma') order by id;
drop table if exists t_odku_ft_multi;

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

-- ODKU updating the vector-indexed column itself must drop the old entries and
-- index the new vector synchronously: a KNN query reflects the new position and
-- the entries table keeps no stale rows.
drop table if exists t_odku_vec2;
create table t_odku_vec2(id int primary key, embedding vecf32(3));
insert into t_odku_vec2 values (1, '[1,1,1]'), (2, '[9,9,9]');
create index idx2 using ivfflat on t_odku_vec2(embedding) lists = 2 op_type 'vector_l2_ops';
insert into t_odku_vec2 values (1, '[0,0,0]') on duplicate key update embedding = '[100,100,100]';
select id, embedding from t_odku_vec2 order by id;
select id from t_odku_vec2 order by l2_distance(embedding, '[100,100,100]') asc limit 1;
select id from t_odku_vec2 order by l2_distance(embedding, '[1,1,1]') asc limit 1;
drop table if exists t_odku_vec2;

-- single-column unique PREFIX index: ODKU conflict resolution must use the stored
-- prefix key, not the raw column. 'abcdyyyy' shares the 4-char prefix 'abcd' with
-- existing 'abcdxxxx', so it conflicts on UNIQUE KEY u(body(4)) and updates the
-- existing row instead of failing with duplicate-entry.
drop table if exists t_odku_prefix;
create table t_odku_prefix(id int primary key, body varchar(64), v int, unique key u(body(4)));
insert into t_odku_prefix values (1, 'abcdxxxx', 10);
insert into t_odku_prefix values (2, 'abcdyyyy', 20) on duplicate key update v = v + 100;
select id, body, v from t_odku_prefix order by id;
-- a different prefix is a genuine insert
insert into t_odku_prefix values (3, 'wxyzzzzz', 30) on duplicate key update v = v + 100;
select id, body, v from t_odku_prefix order by id;
drop table if exists t_odku_prefix;

-- ---------------------------------------------------------------------------
-- Modern child→parent foreign-key handling is row-scoped (validates only the
-- statement's own rows), so it neither false-positives on unrelated orphan rows
-- nor scales with table size.
-- ---------------------------------------------------------------------------
drop table if exists t_odku_fk_child;
drop table if exists t_odku_fk_parent;
create table t_odku_fk_parent(pid int primary key, pname varchar(20));
create table t_odku_fk_child(
  cid int primary key,
  pid int,
  val int not null,
  constraint ck_odku_fk_action check(val >= 0),
  foreign key(pid) references t_odku_fk_parent(pid)
);
insert into t_odku_fk_parent values (1, 'P1'), (2, 'P2');
insert into t_odku_fk_child values (1, 1, 100);
-- seed an unrelated orphan row under FOREIGN_KEY_CHECKS=0 (pid=99 has no parent)
set foreign_key_checks=0;
insert into t_odku_fk_child values (2, 99, 200);
-- CHECK/NOT NULL still require the ordered action stream while FK checking is
-- disabled, but the planner must not consume FK eligibility columns that were
-- deliberately not produced.
insert into t_odku_fk_child values (1, 999, 100)
  on duplicate key update val = values(val);
select cid, pid, val from t_odku_fk_child where cid = 1;
insert into t_odku_fk_child values (1, 999, -1)
  on duplicate key update val = values(val);
select cid, pid, val from t_odku_fk_child where cid = 1;
insert into t_odku_fk_child values (1, 999, 102)
  on duplicate key update val = if(values(val) = 102, null, values(val));
select cid, pid, val from t_odku_fk_child where cid = 1;
set foreign_key_checks=1;
-- A conflicting no-op must not revalidate the unchanged orphan reference. The
-- row remains in the stream for CLIENT_FOUND_ROWS accounting only.
insert into t_odku_fk_child values (2, 99, 0) on duplicate key update val = val;
select row_count(), cid, pid, val from t_odku_fk_child where cid = 2;
-- Mentioning an FK column in the assignment is not enough to make it eligible:
-- the final FK tuple is compared with the stored tuple using null-safe equality.
insert into t_odku_fk_child values (2, 99, 0) on duplicate key update pid = pid;
select row_count(), cid, pid, val from t_odku_fk_child where cid = 2;
-- A real update to a non-FK column also leaves the FK tuple outside the check's
-- mutation domain and must succeed.
insert into t_odku_fk_child values (2, 99, 0) on duplicate key update val = val + 1;
select row_count(), cid, pid, val from t_odku_fk_child where cid = 2;
-- Eligibility is row-scoped: retain the orphan no-op while validating and
-- inserting the valid new row in the same batch.
insert into t_odku_fk_child values (2, 99, 0), (4, 2, 400) on duplicate key update val = val;
select row_count(), cid, pid, val from t_odku_fk_child order by cid;
-- ODKU on the valid row (cid=1) must succeed: it validates only this statement's
-- final row image, not the whole table, so the pre-existing orphan is ignored.
insert into t_odku_fk_child values (1, 1, 5) on duplicate key update val = val + 1;
select cid, pid, val from t_odku_fk_child order by cid;
-- ODKU that updates the FK column to a non-existent parent must still fail.
insert into t_odku_fk_child values (1, 1, 5) on duplicate key update pid = 999;
-- a genuine insert referencing a missing parent must still fail.
insert into t_odku_fk_child values (3, 888, 1) on duplicate key update val = val + 1;
select cid, pid, val from t_odku_fk_child order by cid;
-- Correcting a historical orphan changes the FK tuple, so the new valid parent
-- must be checked and the update must be accepted.
insert into t_odku_fk_child values (2, 99, 0) on duplicate key update pid = 2;
select row_count(), cid, pid, val from t_odku_fk_child where cid = 2;
-- Constraint checks are action-ordered, not final-image-only. Both statements
-- must roll back even though a later duplicate would repair the FK value.
insert into t_odku_fk_child values (1, 999, 0), (1, 1, 0)
  on duplicate key update pid = values(pid);
select cid, pid, val from t_odku_fk_child where cid = 1;
insert into t_odku_fk_child values (5, 999, 0), (5, 1, 0)
  on duplicate key update pid = values(pid);
select count(*) from t_odku_fk_child where cid = 5;
drop table if exists t_odku_fk_child;
drop table if exists t_odku_fk_parent;

drop table if exists t_odku_action_check;
create table t_odku_action_check(id int primary key, v int, constraint ck_action check(v >= 0));
insert into t_odku_action_check values (1, 1);
insert into t_odku_action_check values (1, -1), (1, 2)
  on duplicate key update v = values(v);
select * from t_odku_action_check;
insert into t_odku_action_check values (2, -1), (2, 2)
  on duplicate key update v = values(v);
select count(*) from t_odku_action_check where id = 2;
-- Nearest positive control: every action is valid and the final image survives.
insert into t_odku_action_check values (1, 3), (1, 4)
  on duplicate key update v = values(v);
select * from t_odku_action_check;
drop table t_odku_action_check;

-- A group created by this statement remains insert-originated at its final
-- image. An unrelated CHECK or FK must not lose eligibility merely because
-- the last duplicate action changed only another constrained column.
drop table if exists t_odku_final_insert_check;
create table t_odku_final_insert_check(
  id int primary key,
  pid int,
  v int,
  constraint ck_final_insert_pid check(pid > 0),
  constraint ck_final_insert_v check(v >= 0)
);
insert into t_odku_final_insert_check values (1, -1, 0), (1, 10, 1)
  on duplicate key update v = values(v);
select count(*) from t_odku_final_insert_check;
insert into t_odku_final_insert_check values (2, 1, 0), (2, 10, 1)
  on duplicate key update v = values(v);
select id, pid, v from t_odku_final_insert_check;
drop table t_odku_final_insert_check;

drop table if exists t_odku_final_insert_fk_child;
drop table if exists t_odku_final_insert_fk_parent;
create table t_odku_final_insert_fk_parent(id int primary key);
create table t_odku_final_insert_fk_child(
  id int primary key,
  pid int,
  v int,
  constraint ck_final_insert_fk_v check(v >= 0),
  foreign key(pid) references t_odku_final_insert_fk_parent(id)
);
insert into t_odku_final_insert_fk_parent values (1);
insert into t_odku_final_insert_fk_child values (1, 999, 0), (1, 1, 1)
  on duplicate key update v = values(v);
select count(*) from t_odku_final_insert_fk_child;
insert into t_odku_final_insert_fk_child values (2, 1, 0), (2, 999, 1)
  on duplicate key update v = values(v);
select id, pid, v from t_odku_final_insert_fk_child;
drop table t_odku_final_insert_fk_child;
drop table t_odku_final_insert_fk_parent;

-- INSERT IGNORE on a child table drops the rows whose parent does not exist
-- (MySQL row-skip semantics) instead of failing the whole statement.
drop table if exists t_ign_fk_child;
drop table if exists t_ign_fk_parent;
create table t_ign_fk_parent(pid int primary key, pname varchar(20));
create table t_ign_fk_child(cid int primary key, pid int, foreign key(pid) references t_ign_fk_parent(pid));
insert into t_ign_fk_parent values (1, 'P1'), (2, 'P2'), (3, 'P3');
-- pid=4 has no parent and is skipped; pid=NULL satisfies the constraint and is kept.
insert ignore into t_ign_fk_child values (10, 1), (11, 2), (12, 4), (13, 1), (14, NULL);
select cid, pid from t_ign_fk_child order by cid;
drop table if exists t_ign_fk_child;
drop table if exists t_ign_fk_parent;

-- Multi-FK ON DUPLICATE KEY UPDATE: per-FK MATCH SIMPLE. Setting one FK column to NULL
-- skips only its own FK; another non-NULL FK pointing at a missing parent must still
-- fail. (The old global isnotnull pre-filter dropped the row from the entire check as
-- soon as ANY FK column was NULL, so it skipped every FK's validation.)
drop table if exists t_odku_mfk_child;
drop table if exists t_odku_mfk_p1;
drop table if exists t_odku_mfk_p2;
create table t_odku_mfk_p1(id int primary key);
create table t_odku_mfk_p2(id int primary key);
create table t_odku_mfk_child(cid int primary key, a int, b int, foreign key(a) references t_odku_mfk_p1(id), foreign key(b) references t_odku_mfk_p2(id));
insert into t_odku_mfk_p1 values (1);
insert into t_odku_mfk_p2 values (1);
insert into t_odku_mfk_child values (1, 1, 1);
-- a set NULL (its FK skipped), b set to a non-existent parent -> must still fail on b.
insert into t_odku_mfk_child values (1, 1, 1) on duplicate key update a = NULL, b = 999;
-- a set NULL (skipped), b valid -> must pass.
insert into t_odku_mfk_child values (1, 1, 1) on duplicate key update a = NULL, b = 1;
select cid, a, b from t_odku_mfk_child order by cid;
drop table if exists t_odku_mfk_child;
drop table if exists t_odku_mfk_p1;
drop table if exists t_odku_mfk_p2;

-- ODKU no-op on a table with an implicit ON UPDATE CURRENT_TIMESTAMP column:
-- the auto-update column must not defeat physical no-op detection or advance.
-- Under mo-tester's CLIENT_FOUND_ROWS connection the logical affected count is
-- one even though no row is written.
drop table if exists t_odku_onupdate;
create table t_odku_onupdate (
  id int primary key,
  v int,
  updated_at timestamp default current_timestamp on update current_timestamp
);
insert into t_odku_onupdate(id, v) values (1, 10);
set @ts0 = (select updated_at from t_odku_onupdate where id = 1);
select sleep(2);
insert into t_odku_onupdate(id, v) values (1, 10) on duplicate key update v = v;
select updated_at = @ts0 as ts_unchanged from t_odku_onupdate where id = 1;
-- a real change must count as delete+insert (2) and advance the timestamp
insert into t_odku_onupdate(id, v) values (1, 99) on duplicate key update v = values(v);
select v, updated_at > @ts0 as ts_advanced from t_odku_onupdate where id = 1;
drop table if exists t_odku_onupdate;

-- CHAR assignments use PAD SPACE equality for both logical affected-row and
-- physical-write decisions. VARCHAR is the nearest non-equivalent control.
drop table if exists t_odku_char_pad;

drop table if exists t_odku_json_equal;
create table t_odku_json_equal (
  id int primary key,
  j json,
  updated_at timestamp default '2000-01-01 00:00:00' on update current_timestamp
);
insert into t_odku_json_equal values (1, '1', '2000-01-01 00:00:00');
insert into t_odku_json_equal(id, j) values (1, '1.0')
  on duplicate key update j = values(j);
select row_count(), json_type(j), updated_at = '2000-01-01 00:00:00' as auto_unchanged
  from t_odku_json_equal;
drop table t_odku_json_equal;
create table t_odku_char_pad (
  id int primary key,
  c char(4),
  v varchar(4),
  updated_at timestamp default '2000-01-01 00:00:00' on update current_timestamp
);
insert into t_odku_char_pad values (1, 'a', 'a', '2000-01-01 00:00:00');
insert into t_odku_char_pad(id, c, v) values (1, 'a   ', 'zzzz')
  on duplicate key update c = values(c);
select row_count(), hex(c), hex(v), updated_at = '2000-01-01 00:00:00' as auto_unchanged
  from t_odku_char_pad;
insert into t_odku_char_pad(id, c, v) values (1, 'zzzz', 'a ')
  on duplicate key update v = values(v);
select row_count(), hex(c), hex(v), updated_at > '2000-01-01 00:00:00' as auto_updated
  from t_odku_char_pad;
drop table if exists t_odku_char_pad;

-- A synthesized ON UPDATE value must not reach CHECK evaluation for a pure
-- no-op. The mixed batch also proves that restoring the old image does not
-- suppress an unrelated insert. CLIENT_FOUND_ROWS counts the no-op conflict as
-- one; a real change still evaluates CHECK against the new timestamp and is
-- rejected.
drop table if exists t_odku_onupdate_check;
create table t_odku_onupdate_check (
  id int primary key,
  v int,
  updated_at timestamp default '2000-01-01 00:00:00' on update current_timestamp,
  constraint chk_old_timestamp check (updated_at < '2020-01-01 00:00:00')
);
insert into t_odku_onupdate_check(id, v) values (1, 10);
insert into t_odku_onupdate_check(id, v) values (1, 10) on duplicate key update v = v;
select row_count(), id, v, updated_at from t_odku_onupdate_check order by id;
insert into t_odku_onupdate_check(id, v) values (1, 10), (2, 20) on duplicate key update v = v;
select row_count(), id, v, updated_at from t_odku_onupdate_check order by id;
insert into t_odku_onupdate_check(id, v) values (1, 11) on duplicate key update v = values(v);
select id, v, updated_at from t_odku_onupdate_check order by id;
drop table if exists t_odku_onupdate_check;

-- same, plus a stored generated column derived from the ON UPDATE column:
-- its recomputed value must not defeat no-op detection either.
drop table if exists t_odku_onupdate_gen;
create table t_odku_onupdate_gen (
  id int primary key,
  v int,
  updated_at timestamp default current_timestamp on update current_timestamp,
  g timestamp as (updated_at) stored
);
insert into t_odku_onupdate_gen(id, v) values (1, 10);
set @g0 = (select g from t_odku_onupdate_gen where id = 1);
select sleep(2);
insert into t_odku_onupdate_gen(id, v) values (1, 10) on duplicate key update v = v;
select updated_at = @g0 as ts_unchanged, g = @g0 as g_unchanged from t_odku_onupdate_gen where id = 1;
insert into t_odku_onupdate_gen(id, v) values (1, 99) on duplicate key update v = values(v);
select v, g > @g0 as g_advanced from t_odku_onupdate_gen where id = 1;
drop table if exists t_odku_onupdate_gen;

-- ODKU into a nullable UNIQUE key: an all-NULL row never conflicts, so it must
-- be inserted (not silently dropped by the no-op guard comparing NULL images).
drop table if exists t_odku_null_unique;
create table t_odku_null_unique (a int unique key, b int);
insert into t_odku_null_unique values (null, null) on duplicate key update b = values(b);
select count(*) as after_first from t_odku_null_unique;
insert into t_odku_null_unique values (null, null) on duplicate key update b = values(b);
select count(*) as after_second from t_odku_null_unique;
select a, b from t_odku_null_unique;
-- non-NULL keys on the same table still follow normal ODKU semantics
insert into t_odku_null_unique values (1, 10) on duplicate key update b = values(b);
insert into t_odku_null_unique values (1, 20) on duplicate key update b = values(b);
select a, b from t_odku_null_unique where a = 1;
-- a no-op update on the conflicting key must leave the row untouched
insert into t_odku_null_unique values (1, 20) on duplicate key update b = values(b);
select a, b from t_odku_null_unique where a = 1;
drop table if exists t_odku_null_unique;

-- ODKU whose conflict is resolved through a SECONDARY unique key while the
-- incoming PK differs from the existing row's PK. v = v writes the old value, so
-- it is a no-op even though the incoming v (99) differs from the stored v (5):
-- the guard must compare the old value against the final written value (old),
-- not the raw incoming image. It must also not compare the immutable PK. So the
-- ON UPDATE timestamp must NOT advance.
drop table if exists t_odku_sec_uk;
create table t_odku_sec_uk (
  id int primary key,
  u int unique,
  v int,
  updated_at timestamp default current_timestamp on update current_timestamp
);
insert into t_odku_sec_uk(id, u, v) values (1, 10, 5);
set @ts_sec = (select updated_at from t_odku_sec_uk where u = 10);
select sleep(2);
insert into t_odku_sec_uk(id, u, v) values (2, 10, 99) on duplicate key update v = v;
select id, u, v, updated_at = @ts_sec as ts_unchanged from t_odku_sec_uk order by id;
insert into t_odku_sec_uk(id, u, v) values (3, 10, 5) on duplicate key update v = v + 1;
select id, u, v, updated_at > @ts_sec as ts_advanced from t_odku_sec_uk order by id;
drop table if exists t_odku_sec_uk;

-- A child table with an auto-increment primary key and a secondary UNIQUE key
-- can make the DEDUP build row wider than the incoming row. The conflict-target
-- primary key exists only on the stored row and must survive the FK lock barrier.
drop table if exists t_odku_fk_uk_child;
drop table if exists t_odku_fk_uk_parent;

-- Ordered assignment semantics and logical affected rows survive in-batch key
-- collapse. ROW_COUNT is checked separately from the final physical row image.
drop table if exists t_odku_order_count;
create table t_odku_order_count(id int primary key, a int, b int);
insert into t_odku_order_count values (1, 10, 20);
insert into t_odku_order_count values (1, 100, 200)
  on duplicate key update a = a + 1, b = a;
select row_count(), id, a, b from t_odku_order_count;
update t_odku_order_count set a = 10, b = 20 where id = 1;
insert into t_odku_order_count values (1, 100, 200)
  on duplicate key update a = a + 1, a = a + 1;
select row_count(), id, a, b from t_odku_order_count;
drop table t_odku_order_count;

drop table if exists t_odku_repeat_count;
create table t_odku_repeat_count(id int primary key, v int, key iv(v));
insert into t_odku_repeat_count values (1, 10);
insert into t_odku_repeat_count values (1, 11), (1, 12), (1, 13)
  on duplicate key update v = values(v);
select row_count(), id, v from t_odku_repeat_count;
truncate table t_odku_repeat_count;
insert into t_odku_repeat_count values (1, 11), (1, 12), (1, 13)
  on duplicate key update v = values(v);
select row_count(), id, v from t_odku_repeat_count;
truncate table t_odku_repeat_count;
insert into t_odku_repeat_count values (1, 10);
insert into t_odku_repeat_count values (1, 11), (1, 10)
  on duplicate key update v = values(v);
select row_count(), id, v from t_odku_repeat_count;
insert into t_odku_repeat_count values (1, 10)
  on duplicate key update v = values(v);
select row_count(), id, v from t_odku_repeat_count;
select count(*) from t_odku_repeat_count force index(iv) where v = 10;
drop table t_odku_repeat_count;

-- Conflict targets evolve in input order across every unique constraint. An
-- input row that becomes UPDATE must not reserve its unused PK/UNIQUE values.
drop table if exists t_odku_statement_keys;
create table t_odku_statement_keys(id int primary key, u int unique, v int);
insert into t_odku_statement_keys values (1, 10, 1), (1, 11, 2)
  on duplicate key update v = values(v);
select row_count(), id, u, v from t_odku_statement_keys order by id;
truncate table t_odku_statement_keys;
insert into t_odku_statement_keys values (1, 10, 1), (2, 10, 2)
  on duplicate key update v = values(v);
select row_count(), id, u, v from t_odku_statement_keys order by id;
truncate table t_odku_statement_keys;
insert into t_odku_statement_keys values (1, 10, 1), (2, 10, 2), (2, 20, 3)
  on duplicate key update v = values(v);
select row_count(), id, u, v from t_odku_statement_keys order by id;
truncate table t_odku_statement_keys;
insert into t_odku_statement_keys values (1, 10, 1), (2, 20, 2);
insert into t_odku_statement_keys values (1, 20, 99)
  on duplicate key update v = values(v);
select row_count(), id, u, v from t_odku_statement_keys order by id;
drop table t_odku_statement_keys;

drop table if exists t_odku_statement_fake_pk;
create table t_odku_statement_fake_pk(u int unique, v int);
insert into t_odku_statement_fake_pk values (10, 1), (10, 2)
  on duplicate key update v = values(v);
select row_count(), u, v from t_odku_statement_fake_pk;
drop table t_odku_statement_fake_pk;

drop table if exists t_odku_statement_composite;
create table t_odku_statement_composite(
  id int primary key,
  a int,
  b int,
  v int,
  unique key uk_ab(a, b)
);
insert into t_odku_statement_composite values (1, 10, 20, 1), (2, 10, 20, 2)
  on duplicate key update v = values(v);
select row_count(), id, a, b, v from t_odku_statement_composite;
drop table t_odku_statement_composite;

-- Generated columns observe the ordered final row. A later CHECK failure must
-- roll back both a preceding insert and all base/index maintenance.
drop table if exists t_odku_order_generated;
create table t_odku_order_generated(
  id int primary key,
  a int,
  b int,
  g int as (a + b),
  constraint ck_odku_order check(a = b),
  key ig(g)
);
insert into t_odku_order_generated(id, a, b) values (1, 10, 10);
insert into t_odku_order_generated(id, a, b) values (1, 99, 99)
  on duplicate key update a = a + 1, b = a;
select row_count(), id, a, b, g from t_odku_order_generated;
insert into t_odku_order_generated(id, a, b) values (2, 20, 20), (1, 99, 99)
  on duplicate key update a = a + 1, b = a + 2;
select count(*) from t_odku_order_generated;
select count(*) from t_odku_order_generated force index(ig) where g = 22;
drop table t_odku_order_generated;

-- The logical-action stream and the final physical image are distinct. A
-- change followed by a restore still has four affected rows and must preserve
-- an implicit ON UPDATE effect; a pure no-op must not update it and contributes
-- one logical affected row under CLIENT_FOUND_ROWS.
drop table if exists t_odku_repeat_onupdate;
create table t_odku_repeat_onupdate(
  id int primary key,
  v int,
  updated_at timestamp(6) default '2000-01-01 00:00:00.000000'
    on update current_timestamp(6)
);
insert into t_odku_repeat_onupdate values (1, 10, '2000-01-01 00:00:00.000000');
insert into t_odku_repeat_onupdate(id, v) values (1, 11), (1, 10)
  on duplicate key update v = values(v);
select row_count(), v, updated_at > '2000-01-01 00:00:00.000000' as auto_updated
  from t_odku_repeat_onupdate;
update t_odku_repeat_onupdate set updated_at = '2000-01-01 00:00:00.000000';
insert into t_odku_repeat_onupdate(id, v) values (1, 10)
  on duplicate key update v = values(v);
select row_count(), v, updated_at = '2000-01-01 00:00:00.000000' as auto_unchanged
  from t_odku_repeat_onupdate;
drop table t_odku_repeat_onupdate;

-- NOT NULL is an action constraint, not merely a final-row storage check. The
-- first duplicate action must abort even though the later action would restore
-- a non-NULL final image; the original row must remain unchanged after rollback.
drop table if exists t_odku_action_not_null;
create table t_odku_action_not_null(id int primary key, v int not null, selector int);
insert into t_odku_action_not_null values (1, 10, 0);
insert into t_odku_action_not_null values (1, 10, 1), (1, 10, 2)
  on duplicate key update
    v = if(values(selector) = 1, null, 20),
    selector = values(selector);
select id, v, selector from t_odku_action_not_null;
drop table t_odku_action_not_null;

create table t_odku_fk_uk_parent(pid int primary key);
create table t_odku_fk_uk_child(
  id bigint auto_increment primary key,
  pid int not null,
  attr_id int not null,
  val varchar(20),
  unique key uk_pid_attr(pid, attr_id),
  foreign key(pid) references t_odku_fk_uk_parent(pid)
);
insert into t_odku_fk_uk_parent values (1);
insert into t_odku_fk_uk_child(pid, attr_id, val) values (1, 5, 'old');
insert into t_odku_fk_uk_child(pid, attr_id, val) values (1, 5, 'new')
  on duplicate key update val = values(val);
select id, pid, attr_id, val from t_odku_fk_uk_child order by id;
drop table if exists t_odku_fk_uk_child;
drop table if exists t_odku_fk_uk_parent;
