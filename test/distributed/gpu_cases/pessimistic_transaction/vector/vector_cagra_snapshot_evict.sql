-- =====================================================================
-- vector_cagra_snapshot_evict.sql -- named-snapshot generations are evicted and
-- RELOADED under a binding cap, and each one still answers for its own moment.
--
-- GPU REQUIRED.
--
-- Every named-snapshot read is its own cache entry, keyed
-- "<index table>@<physical>-<logical>", so reading N snapshots makes N resident
-- generations of the SAME index. That is the only way to put several
-- generations of one index under the byte governor at once, and it is what makes
-- eviction observable here: under a cap far below their total, each arrival has
-- to reclaim an idle sibling before it can be admitted.
--
-- What this pins that the other snapshot cases cannot: they read a snapshot
-- while its generation is still resident. Here every re-read happens after the
-- entry has been evicted, so the generation is loaded again from storage and
-- must still be bound to ITS timestamp -- a reload that resolved the current
-- generation instead would answer with today's rows.
--
-- Isolated by ACCOUNT: SET GLOBAL is per-account, so this cap governs only this
-- account's entries and the CN-wide SYS value is untouched.
--
-- Assertions are behavioural (cache residency has no SQL surface): each snapshot
-- keeps its own answer across repeated evict/reload cycles, and the current
-- generation keeps its own. Probes are exact matches with a unique nearest row,
-- so no equidistant tie decides the result.
-- =====================================================================

drop account if exists acc_cagra_snap_evict;
create account acc_cagra_snap_evict admin_name 'admin' identified by '123456';

-- @session:id=1&user=acc_cagra_snap_evict:admin&password=123456
SET experimental_cagra_index = 1;
drop database if exists cagra_snap_evict;
create database cagra_snap_evict;
use cagra_snap_evict;

create table t(id bigint primary key, v vecf32(8));
insert into t values
    (1, '[10,10,10,10,10,10,10,10]'),   (2, '[20,20,20,20,20,20,20,20]'),
    (3, '[40,40,40,40,40,40,40,40]'),   (4, '[80,80,80,80,80,80,80,80]'),
    (5, '[160,160,160,160,160,160,160,160]'), (6, '[320,320,320,320,320,320,320,320]'),
    (7, '[640,640,640,640,640,640,640,640]'), (8, '[1280,1280,1280,1280,1280,1280,1280,1280]');
create index ix using cagra on t(v) op_type 'vector_l2_ops'
    intermediate_graph_degree=8 graph_degree=4 itopk_size=16;

-- Delete the row the probe would otherwise match. It goes into the cdc_tail, so
-- EVERY generation below carries a delete that has to be replayed when its index
-- is loaded -- and that replay is what materialises the native id map for the
-- whole index (index_base.hpp, ensure_id_index), the map delete_id is the only
-- reader of. Under the cap set below, each reload rebuilds it.
delete from t where id = 5;
select sleep(30);
select count(*) from t where id = 5;

-- Generation 1: id 5 is deleted, so the nearest to [161]*8 is id 4 ([80]*8).
create snapshot cagra_snap_evict_s1 for account;

-- A closer row lands after s1, so the two generations disagree on the same probe.
insert into t values (9, '[150,150,150,150,150,150,150,150]');
select sleep(30);
create snapshot cagra_snap_evict_s2 for account;

-- A third generation, an exact match, closer still.
insert into t values (10, '[161,161,161,161,161,161,161,161]');
select sleep(30);

-- A cap far below what three generations of this index need together.
set global max_gpu_index_cache_size = 4096;
select @@global.max_gpu_index_cache_size;

-- Each read below loads a generation that the previous read's arrival evicted, and
-- each load replays that generation's tail delete before answering.
-- s1: id 5 deleted, no 9/10 -> 4.  s2: has 9 -> 9.  current: has 10 (exact) -> 10.
-- id 5 must not reappear in ANY of them: a reload that skipped the replay would
-- return it, since it is the exact match for the probe.
select id from t {snapshot='cagra_snap_evict_s1'} order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;
select id from t {snapshot='cagra_snap_evict_s2'} order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;
select id from t order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;
select id from t {snapshot='cagra_snap_evict_s1'} order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;
select id from t {snapshot='cagra_snap_evict_s2'} order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;
select id from t order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;
select id from t {snapshot='cagra_snap_evict_s1'} order by l2_distance(v,'[161,161,161,161,161,161,161,161]') limit 1;

-- And the delete held across every eviction and reload.
select count(*) from t where id = 5;

set global max_gpu_index_cache_size = 0;
select @@global.max_gpu_index_cache_size;

drop snapshot cagra_snap_evict_s1;
drop snapshot cagra_snap_evict_s2;
drop database cagra_snap_evict;
-- @session

drop account if exists acc_cagra_snap_evict;
