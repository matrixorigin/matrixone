-- ============================================================
-- Native FTS correctness test cases
-- Tests native sidecar path vs v1 fallback parity
-- ============================================================

-- ============================================================
-- SECTION 1: Basic native sidecar generation after flush
-- Verify: insert → flush → native query works
-- ============================================================
drop database if exists test_fts_native;
create database test_fts_native;
use test_fts_native;

create table t_basic (id bigint primary key, body varchar, title text);
create fulltext index ftidx on t_basic (body, title);

insert into t_basic values
  (1, 'matrix origin database', 'cloud native'),
  (2, 'distributed storage engine', 'shared nothing'),
  (3, 'full text search native sidecar', 'inverted index'),
  (4, 'matrix cloud computing platform', 'edge computing'),
  (5, NULL, 'null body row'),
  (6, 'null title row', NULL),
  (7, NULL, NULL);

-- force flush to generate sidecar
-- @wait:5

-- natural language mode
select id from t_basic where match(body, title) against('matrix') order by id;
select id from t_basic where match(body, title) against('native') order by id;
select id from t_basic where match(body, title) against('sidecar') order by id;

-- boolean mode
select id from t_basic where match(body, title) against('+matrix +cloud' in boolean mode) order by id;
select id from t_basic where match(body, title) against('+matrix -cloud' in boolean mode) order by id;
select id from t_basic where match(body, title) against('"full text search"' in boolean mode) order by id;

-- prefix
select id from t_basic where match(body, title) against('comput*' in boolean mode) order by id;

-- score projection
select id, match(body, title) against('matrix') as score from t_basic where match(body, title) against('matrix') order by score desc;

-- NULL handling: rows with NULL columns should not crash
select count(*) from t_basic where match(body, title) against('null');

-- ============================================================
-- SECTION 2: DELETE + tombstone visibility
-- Verify: deleted rows not returned by native path
-- ============================================================
delete from t_basic where id = 1;
select id from t_basic where match(body, title) against('matrix') order by id;

delete from t_basic where id = 4;
select id from t_basic where match(body, title) against('matrix') order by id;
select id from t_basic where match(body, title) against('computing') order by id;

-- ============================================================
-- SECTION 3: UPDATE + tombstone + new version
-- Verify: old version filtered, new version visible after flush
-- ============================================================
update t_basic set body = 'updated content orange' where id = 2;
-- old term should not match id=2
select id from t_basic where match(body, title) against('distributed') order by id;
-- new term should match id=2
select id from t_basic where match(body, title) against('orange') order by id;

-- ============================================================
-- SECTION 4: Large batch insert → flush → merge cycle
-- Verify: sidecar survives merge/compaction
-- ============================================================
drop table if exists t_merge;
create table t_merge (id int primary key, body varchar(200));
create fulltext index ftidx on t_merge (body);

-- batch 1
insert into t_merge select result, concat('batch one document number ', cast(result as varchar)) from generate_series(1, 5000) g;
-- @wait:5

-- batch 2 (will create second object, trigger merge later)
insert into t_merge select result + 5000, concat('batch two record number ', cast(result as varchar)) from generate_series(1, 5000) g;
-- @wait:5

-- query should find results from both batches
select count(*) from t_merge where match(body) against('+batch +one' in boolean mode);
select count(*) from t_merge where match(body) against('+batch +two' in boolean mode);
select count(*) from t_merge where match(body) against('document' in natural language mode);
select count(*) from t_merge where match(body) against('record' in natural language mode);

-- ============================================================
-- SECTION 5: Chinese text (3-gram tokenizer)
-- ============================================================
drop table if exists t_cjk;
create table t_cjk (id bigint primary key, content text);
create fulltext index ftidx on t_cjk (content);

insert into t_cjk values
  (1, '全文索引是数据库的重要功能'),
  (2, '矩阵起源是一个云原生数据库'),
  (3, '分布式存储引擎支持高可用'),
  (4, '全文检索支持中文分词和英文分词');

-- @wait:5

select id from t_cjk where match(content) against('数据库') order by id;
select id from t_cjk where match(content) against('全文') order by id;
select id from t_cjk where match(content) against('+全文 +中文' in boolean mode) order by id;
select id from t_cjk where match(content) against('云原生') order by id;

-- ============================================================
-- SECTION 6: JSON parser
-- ============================================================
drop table if exists t_json;
create table t_json (id bigint primary key, data json);
create fulltext index ftidx on t_json (data) with parser json;

insert into t_json values
  (1, '{"title": "matrix origin", "tag": "database"}'),
  (2, '{"title": "full text search", "tag": "native"}'),
  (3, '{"title": "cloud computing", "tag": "infrastructure"}');

-- @wait:5

select id from t_json where match(data) against('matrix' in boolean mode) order by id;
select id from t_json where match(data) against('+database +matrix' in boolean mode) order by id;
select id from t_json where match(data) against('native' in boolean mode) order by id;

-- ============================================================
-- SECTION 7: Composite primary key
-- ============================================================
drop table if exists t_cpk;
create table t_cpk (pk1 varchar, pk2 int, body text, primary key(pk1, pk2));
create fulltext index ftidx on t_cpk (body);

insert into t_cpk values ('a', 1, 'red apple'), ('a', 2, 'blue sky'), ('b', 1, 'red car'), ('b', 2, 'green tree');

-- @wait:5

select pk1, pk2 from t_cpk where match(body) against('red') order by pk1, pk2;
select pk1, pk2 from t_cpk where match(body) against('+red -car' in boolean mode) order by pk1, pk2;

delete from t_cpk where pk1 = 'b' and pk2 = 1;
select pk1, pk2 from t_cpk where match(body) against('red') order by pk1, pk2;

-- ============================================================
-- SECTION 8: Empty table / empty result
-- ============================================================
drop table if exists t_empty;
create table t_empty (id int primary key, body varchar);
create fulltext index ftidx on t_empty (body);

-- query empty table
select count(*) from t_empty where match(body) against('anything');

-- insert then delete all
insert into t_empty values (1, 'temporary data');
delete from t_empty;
select count(*) from t_empty where match(body) against('temporary');

-- ============================================================
-- SECTION 9: Multiple fulltext indexes on same table
-- ============================================================
drop table if exists t_multi_idx;
create table t_multi_idx (id int primary key, title varchar, body text, tags varchar);
create fulltext index ftidx_title on t_multi_idx (title);
create fulltext index ftidx_body on t_multi_idx (body);
create fulltext index ftidx_all on t_multi_idx (title, body);

insert into t_multi_idx values
  (1, 'database tutorial', 'learn how to use fulltext search', 'education'),
  (2, 'cooking recipe', 'how to make pasta with tomato sauce', 'food'),
  (3, 'database optimization', 'fulltext index improves search performance', 'tech');

-- @wait:5

-- different indexes should return different results
select id from t_multi_idx where match(title) against('database') order by id;
select id from t_multi_idx where match(body) against('fulltext') order by id;
select id from t_multi_idx where match(title, body) against('+database +fulltext' in boolean mode) order by id;

-- ============================================================
-- SECTION 10: BM25 scoring
-- ============================================================
drop table if exists t_bm25;
create table t_bm25 (id int primary key, body text);
create fulltext index ftidx on t_bm25 (body);

insert into t_bm25 values
  (1, 'search search search search search'),
  (2, 'search engine'),
  (3, 'database search engine optimization'),
  (4, 'unrelated content about cooking');

-- @wait:5

-- id=1 should have highest score (highest TF)
select id, match(body) against('search') as score from t_bm25 where match(body) against('search') order by score desc;

-- ============================================================
-- SECTION 11: Concurrent insert + query (stress)
-- ============================================================
drop table if exists t_stress;
create table t_stress (id int primary key, body varchar(500));
create fulltext index ftidx on t_stress (body);

-- rapid inserts
insert into t_stress select result, concat('stress test document ', cast(result as varchar), ' with keyword alpha') from generate_series(1, 10000) g;
-- query immediately (may hit v1 fallback or native depending on flush timing)
select count(*) from t_stress where match(body) against('alpha');
-- @wait:10
-- query after flush (should hit native)
select count(*) from t_stress where match(body) against('alpha');
select count(*) from t_stress where match(body) against('+stress +alpha' in boolean mode);

-- ============================================================
-- CLEANUP
-- ============================================================
drop database test_fts_native;
