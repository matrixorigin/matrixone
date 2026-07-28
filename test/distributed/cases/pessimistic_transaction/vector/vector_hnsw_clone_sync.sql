-- Regression: cloning a table with a SYNC hnsw index must clone BOTH of the
-- index's hidden tables (metadata + storage), not only one.
--
-- Same root cause as vector_ivf_clone_sync: the table_clone.go reader/relation
-- maps were keyed by IndexName, which collides across the two IndexDefs of an
-- hnsw index (they share one IndexName but differ by IndexAlgoTableType:
-- metadata vs storage). Only the last hidden table survived the map and got
-- cloned; the cloned index was then unusable. Fixed by keying on
-- (IndexName, IndexAlgoTableType).
--
-- Sync clone (no async rebuild), so the breakage would be permanent pre-fix.
-- Data is well separated so the approximate hnsw search is deterministic.

set experimental_hnsw_index=1;

drop database if exists hnsw_clone_sync;
create database hnsw_clone_sync;
use hnsw_clone_sync;

create table src(a bigint primary key, v vecf32(3));
insert into src values (1,'[1,1,1]'),(2,'[2,2,2]'),(3,'[3,3,3]'),(4,'[8,8,8]');
create index ix using hnsw on src(v) op_type "vector_l2_ops" max_index_capacity 1000000;

-- sync clone: completes in-txn, no async rebuild to mask a partial clone
create table dst clone src;
select count(*) from dst;
show create table dst;

-- the cloned index must answer search (pre-fix: a missing hidden table breaks it)
select a from dst order by l2_distance(v, '[1,1,1]') asc limit 2;
select a from dst order by l2_distance(v, '[8,8,8]') asc limit 1;

drop database hnsw_clone_sync;
