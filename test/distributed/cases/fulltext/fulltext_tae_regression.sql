-- ============================================================
-- TAE stability regression: ensure FTS changes don't affect
-- non-FTS tables' flush/merge/compaction behavior
-- ============================================================

drop database if exists test_fts_tae_regression;
create database test_fts_tae_regression;
use test_fts_tae_regression;

-- ============================================================
-- SECTION 1: Non-FTS table flush/merge unaffected
-- ============================================================
create table t_no_fts (id int primary key, val varchar(100), num decimal(10,2));

-- large enough to trigger flush + merge
insert into t_no_fts select result, concat('value_', cast(result as varchar)), result * 1.5 from generate_series(1, 50000) g;
-- @wait:5

select count(*) from t_no_fts;
select count(*) from t_no_fts where val like 'value_100%';

-- DML on non-FTS table should work normally
update t_no_fts set val = 'updated' where id <= 100;
select count(*) from t_no_fts where val = 'updated';

delete from t_no_fts where id <= 50;
select count(*) from t_no_fts;

-- ============================================================
-- SECTION 2: FTS table and non-FTS table coexist
-- ============================================================
create table t_with_fts (id int primary key, body text);
create fulltext index ftidx on t_with_fts (body);

-- interleaved operations on both tables
insert into t_with_fts select result, concat('fulltext document ', cast(result as varchar)) from generate_series(1, 10000) g;
insert into t_no_fts select result + 50000, concat('no_fts_', cast(result as varchar)), result from generate_series(1, 10000) g;

-- @wait:5

-- both tables should return correct results
select count(*) from t_with_fts where match(body) against('fulltext');
select count(*) from t_no_fts where id > 50000;

-- ============================================================
-- SECTION 3: Tombstone table unaffected
-- (FTS sidecar skips tombstone objects)
-- ============================================================
create table t_tombstone_check (id int primary key, body text, extra int);
create fulltext index ftidx on t_tombstone_check (body);

insert into t_tombstone_check select result, 'tombstone test row', result from generate_series(1, 5000) g;
-- @wait:5

-- mass delete to generate tombstone objects
delete from t_tombstone_check where id <= 4000;
select count(*) from t_tombstone_check;
select count(*) from t_tombstone_check where match(body) against('tombstone');

-- ============================================================
-- SECTION 4: DDL operations with FTS
-- ============================================================

-- drop fulltext index
create table t_ddl (id int primary key, body text);
create fulltext index ftidx on t_ddl (body);
insert into t_ddl values (1, 'test drop index');
-- @wait:3
select * from t_ddl where match(body) against('test');
drop index ftidx on t_ddl;
-- after drop index, fulltext query should error
select * from t_ddl where match(body) against('test');

-- add fulltext index to existing table with data
create table t_ddl2 (id int primary key, body text);
insert into t_ddl2 select result, concat('existing data row ', cast(result as varchar)) from generate_series(1, 1000) g;
create fulltext index ftidx on t_ddl2 (body);
-- @wait:3
select count(*) from t_ddl2 where match(body) against('existing');

-- truncate table with FTS
create table t_trunc (id int primary key, body text);
create fulltext index ftidx on t_trunc (body);
insert into t_trunc values (1, 'before truncate');
-- @wait:3
select count(*) from t_trunc where match(body) against('truncate');
truncate t_trunc;
select count(*) from t_trunc where match(body) against('truncate');

-- ============================================================
-- SECTION 5: Vector index coexistence
-- (FTS should not affect vector queries)
-- ============================================================
create table t_vec_fts (
  id int primary key,
  body text,
  embedding vecf32(4)
);
create fulltext index ftidx on t_vec_fts (body);

insert into t_vec_fts values
  (1, 'vector and fulltext coexist', '[1.0, 0.0, 0.0, 0.0]'),
  (2, 'separate index paths', '[0.0, 1.0, 0.0, 0.0]'),
  (3, 'no interference between them', '[0.0, 0.0, 1.0, 0.0]');

-- @wait:5

-- FTS query
select id from t_vec_fts where match(body) against('vector') order by id;

-- vector query (should work independently)
select id from t_vec_fts order by l2_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') limit 2;

-- ============================================================
-- CLEANUP
-- ============================================================
drop database test_fts_tae_regression;
