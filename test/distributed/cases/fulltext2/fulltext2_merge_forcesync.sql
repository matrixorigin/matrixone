-- fulltext2 MERGE / REBUILD via FORCE_SYNC — SYNCHRONOUS (no async CDC), so this
-- lives in the sync cases dir. FORCE_SYNC runs the reindex INLINE during the ALTER
-- (a plain reindex is async — CompactSegments would run in a background goroutine
-- that may not finish before mo-service exits), exercising, all synchronously:
--   REINDEX ... MERGE FORCE_SYNC    -> CompactSegments (compact.go) folds the base
--                                      sub-segments + loads/persists chunks (storage.go)
--   REINDEX ... FORCE_SYNC (rebuild)-> buildFromSource + chunk persist (storage.go)
--   MATCH after each                -> loads the (changed) index into the per-CN
--                                      cache (search_cache.go) + streamChunksToFile
-- A small max_index_capacity makes several tag=0 base sub-segments so MERGE folds
-- more than one. No post-create DML — all segment work is driven by the synchronous
-- build + FORCE_SYNC reindex.

set experimental_fulltext2_index = 1;
drop database if exists ft2_merge_fs;
create database ft2_merge_fs;
use ft2_merge_fs;

-- max_index_capacity 2 => the 6 rows seal into ~3 tag=0 base sub-segments.
create table t (id bigint primary key, body text);
insert into t values
 (1,'alpha beta gamma'),(2,'beta gamma delta'),(3,'gamma delta epsilon'),
 (4,'delta epsilon zeta'),(5,'epsilon zeta alpha'),(6,'zeta alpha beta');
create fulltext2 index ft on t(body) max_index_capacity 2;
show create table t;

-- initial multi-segment query: loads the freshly built base into the per-CN cache.
select id from t where match(body) against('alpha' in bm25 mode) order by id;
select id from t where match(body) against('gamma' in bm25 mode) order by id;
-- exact phrase (positional) over the multi-segment base.
select id from t where match(body) against('gamma delta') order by id;

-- MERGE FORCE_SYNC: fold the base sub-segments synchronously (CompactSegments +
-- storage load/persist run inline here).
alter table t alter reindex ft fulltext2 merge force_sync;
-- post-merge query: the index changed, so the cache reloads the merged base.
select id from t where match(body) against('alpha' in bm25 mode) order by id;
select id from t where match(body) against('gamma delta') order by id;

-- REBUILD FORCE_SYNC: rebuild the base from source synchronously.
alter table t alter reindex ft fulltext2 force_sync;
select id from t where match(body) against('alpha' in bm25 mode) order by id;
select id from t where match(body) against('zeta' in bm25 mode) order by id;
select id from t where match(body) against('epsilon zeta') order by id;

drop database ft2_merge_fs;
