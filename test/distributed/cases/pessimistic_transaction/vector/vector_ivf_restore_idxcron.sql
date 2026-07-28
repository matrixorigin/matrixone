-- Execution-level proof for the clone/restore path of an ASYNC ivfflat index
-- (the idxcron / CDC-backed maintenance path re-registered by Scope.RestoreTable).
--
-- This goes beyond metadata/wiring checks and drives the real surface:
--   (a) CREATE TABLE ... CLONE of a table carrying an ASYNC ivfflat index
--       succeeds;
--   (b) the CLONED index still answers vector search (correct neighbors);
--   (c) the background maintenance path is actually RE-ARMED on the clone:
--       rows inserted AFTER the clone are incorporated into the cloned index
--       and become searchable.
--
-- Why a successful search proves the index is live: ivfflat has no brute-force
-- fallback — searching an index whose model is missing errors with
-- "internal error: version not found". So every search that returns a row
-- below is served by a working index, not a full scan. With probe_limit >=
-- lists, every centroid list is probed, so the returned nearest neighbor is
-- exact and deterministic regardless of kmeans sampling.

SET probe_limit=10;

drop database if exists ivf_restore;
create database ivf_restore;
use ivf_restore;

-- Source: ASYNC ivfflat over three well-separated clusters (unambiguous NN).
create table src(a int primary key, b vecf32(3));
insert into src values
  (1,'[1,1,1]'),(2,'[2,2,2]'),
  (10,'[100,100,100]'),(11,'[101,101,101]'),
  (20,'[500,500,500]'),(21,'[501,501,501]');
create index idx using ivfflat on src(b) lists=3 op_type 'vector_l2_ops' ASYNC;

-- let the async build finish, then confirm the source index answers search
select sleep(30);
select a from src order by l2_distance(b,'[1,1,1]') limit 1;

-- (a) clone succeeds and copies the rows + index definition
create table dst clone src;
select count(*) from dst;
show create table dst;

-- let RestoreTable's re-registered CDC run its FORCE_SYNC reindex on the clone
select sleep(30);

-- (b) the cloned index answers search (no "version not found")
select a from dst order by l2_distance(b,'[1,1,1]') limit 1;
select a from dst order by l2_distance(b,'[500,500,500]') limit 1;

-- (c) maintenance re-armed: rows inserted AFTER the clone get indexed
insert into dst values (30,'[1000,1000,1000]'),(31,'[1001,1001,1001]');
select sleep(45);
-- the post-clone row is the nearest neighbor for its own vector, served by the
-- cloned index -> the CDC maintenance path on the clone is live
select a from dst order by l2_distance(b,'[1000,1000,1000]') limit 1;
-- pre-existing cloned rows remain searchable too
select a from dst order by l2_distance(b,'[100,100,100]') limit 1;

drop database ivf_restore;
