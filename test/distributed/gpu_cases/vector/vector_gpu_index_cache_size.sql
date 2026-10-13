-- =====================================================================
-- vector_gpu_index_cache_size.sql -- max_gpu_index_cache_size actually bounds
-- resident VRAM, and an evicted GPU index reloads correctly.
--
-- GPU REQUIRED.
--
-- This is the device-arena half of the byte governor, and the one path unit
-- tests cannot reach: they stub the budget, so they prove the DECISION but not
-- the sequence. Here a cap far below one CAGRA index forces the real sequence on
-- a real card -- charge device bytes, evict, re-admit against actual free VRAM
-- (deviceFitsFreeNow, which runs in Load AFTER the cache has evicted), then
-- deserialize. Before that gate moved out of Preload, an index that did not fit
-- CURRENT free VRAM was refused even when evicting would have made room.
--
-- Isolated by ACCOUNT: SET GLOBAL is per-account, so this cap governs only this
-- account's entries and the CN-wide SYS value is untouched -- a concurrently
-- running GPU case keeps its warm indexes.
--
-- With no SQL surface for cache residency the assertion is behavioural: under a
-- binding cap every search must still return the SAME exact-match answer. CAGRA
-- is approximate, so every probe below is an exact match of an indexed row and
-- only top-1 is asserted.
--
-- THREE indexes share a 1 MiB device budget that cannot hold even one of them, and
-- the queries alternate between them. Each query therefore has to evict a sibling
-- and re-admit its own index against whatever VRAM that eviction just freed --
-- the sequence repeated, not observed once. If the situational gate ran before
-- eviction (as it did in Preload), the second query would be REFUSED rather than
-- served, because free VRAM at that moment still holds the previous index.
-- =====================================================================

drop account if exists acc_gpu_idx_cap;
create account acc_gpu_idx_cap admin_name 'admin' identified by '123456';

-- @session:id=1&user=acc_gpu_idx_cap:admin&password=123456
SET experimental_cagra_index = 1;
drop database if exists gpu_idx_cap_db;
create database gpu_idx_cap_db;
use gpu_idx_cap_db;

create table t1(id bigint primary key, v vecf32(8));
create table t2(id bigint primary key, v vecf32(8));
create table t3(id bigint primary key, v vecf32(8));
insert into t1 values
 (1,'[1,1,1,1,1,1,1,1]'), (2,'[2,2,2,2,2,2,2,2]'), (3,'[3,3,3,3,3,3,3,3]'),
 (4,'[4,4,4,4,4,4,4,4]'), (5,'[5,5,5,5,5,5,5,5]'), (6,'[6,6,6,6,6,6,6,6]'),
 (7,'[7,7,7,7,7,7,7,7]'), (8,'[8,8,8,8,8,8,8,8]'), (9,'[9,9,9,9,9,9,9,9]'),
 (10,'[10,10,10,10,10,10,10,10]');
insert into t2 select * from t1;
insert into t3 select * from t1;
create index c1 using cagra on t1(v) op_type 'vector_l2_ops';
create index c2 using cagra on t2(v) op_type 'vector_l2_ops';
create index c3 using cagra on t3(v) op_type 'vector_l2_ops';

-- The default is 0: no operator limit, budget derived from the GPUs present. It does not bind.
select @@global.max_gpu_index_cache_size;
select id from t1 order by l2_distance(v, '[3,3,3,3,3,3,3,3]') asc limit 1;
select id from t2 order by l2_distance(v, '[3,3,3,3,3,3,3,3]') asc limit 1;
select id from t3 order by l2_distance(v, '[3,3,3,3,3,3,3,3]') asc limit 1;

-- 1 MiB: below even ONE of the three indexes, so every load evicts a sibling.
set global max_gpu_index_cache_size = 1048576;
select @@global.max_gpu_index_cache_size;
-- @session}

-- @session:id=2&user=acc_gpu_idx_cap:admin&password=123456
use gpu_idx_cap_db;
select @@global.max_gpu_index_cache_size;

-- Alternating across all three, twice round. Every one of these evicts a sibling
-- and re-admits itself; a refusal or a wrong id means the gate order regressed.
select id from t1 order by l2_distance(v, '[3,3,3,3,3,3,3,3]') asc limit 1;
select id from t2 order by l2_distance(v, '[7,7,7,7,7,7,7,7]') asc limit 1;
select id from t3 order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 1;
select id from t1 order by l2_distance(v, '[10,10,10,10,10,10,10,10]') asc limit 1;
select id from t2 order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;
select id from t3 order by l2_distance(v, '[9,9,9,9,9,9,9,9]') asc limit 1;

-- Two indexes in ONE statement: both must be resident together under a cap that
-- fits neither, so the join cannot be served by evicting one for the other.
select a.id, b.id from
  (select id from t1 order by l2_distance(v, '[2,2,2,2,2,2,2,2]') asc limit 1) a,
  (select id from t2 order by l2_distance(v, '[8,8,8,8,8,8,8,8]') asc limit 1) b;

-- The host arena is independent: bounding VRAM must not disturb it.
select @@global.max_index_cache_size;

-- Back to the default, 0; answers unchanged.
set global max_gpu_index_cache_size = 0;
select id from t1 order by l2_distance(v, '[3,3,3,3,3,3,3,3]') asc limit 1;
select id from t3 order by l2_distance(v, '[9,9,9,9,9,9,9,9]') asc limit 1;

drop database if exists gpu_idx_cap_db;
-- @session}

drop account if exists acc_gpu_idx_cap;
