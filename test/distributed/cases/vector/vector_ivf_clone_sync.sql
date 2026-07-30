-- Regression: cloning a table with a SYNC ivfflat index must clone ALL of the
-- index's hidden tables (metadata/centroids/entries), not only entries.
--
-- Before the table_clone.go fix, the per-index reader/relation maps were keyed
-- by IndexName. An ivfflat index has three IndexDefs that share one IndexName
-- but differ by IndexAlgoTableType (metadata/centroids/entries), so the key
-- collided and only the last hidden table (entries) was cloned; metadata and
-- centroids ended up empty and the cloned index errored
--   internal error: version not found
-- on the first vector search. A SYNC index has no async REINDEX to rebuild the
-- model, so the breakage is permanent (ASYNC masked it).
--
-- This case is the regression guard: the clone is sync (no sleep), and every
-- search below errors pre-fix and returns the exact neighbour post-fix.
-- probe_limit >= lists makes the nearest neighbour exact and deterministic.

SET probe_limit=10;

drop database if exists ivf_clone_sync;
create database ivf_clone_sync;
use ivf_clone_sync;

create table src(a int primary key, b vecf32(3));
insert into src values
  (1,'[1,1,1]'),(2,'[2,2,2]'),
  (10,'[100,100,100]'),(11,'[101,101,101]'),
  (20,'[500,500,500]'),(21,'[501,501,501]');
create index idx using ivfflat on src(b) lists=3 op_type 'vector_l2_ops';

-- sync clone: completes in-txn, no async rebuild to mask a partial clone
create table dst clone src;
select count(*) from dst;
show create table dst;

-- the cloned index must answer search (pre-fix: "version not found")
select a from dst order by l2_distance(b,'[1,1,1]') limit 1;
select a from dst order by l2_distance(b,'[100,100,100]') limit 1;
select a from dst order by l2_distance(b,'[500,500,500]') limit 1;

drop database ivf_clone_sync;
