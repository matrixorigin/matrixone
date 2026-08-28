-- =====================================================================
-- vector_ivfpq_multi_subindex.sql — IVF-PQ sub-index ROTATION
--
-- GPU REQUIRED.
--
-- Nothing else in the suite exercises rotation: vector_ivfpq.sql sets
-- ivfpq_max_index_capacity = 99999 against 20 rows, so it always builds a
-- single sub-index. Rotation is the mechanism the VRAM-bounded capacity
-- work depends on — when a build is split, each finished sub-index is
-- packed to a tar and its GPU memory released before the next one starts.
-- Untested, a regression there is invisible until a large build OOMs.
--
-- 256 rows at capacity 64 → exactly 4 sub-indexes, one metadata row each.
-- lists=4 sits well under the capacity so no sub-index falls below the
-- k-means minimum and nothing is diverted to the brute-force CDC tail.
-- kmeans_train_percent=100 because 4 centroids need >= 4 training rows out
-- of each 64-row chunk, and probe_limit >= lists scans every cell so the
-- coarse quantizer cannot drop the matching row.
--
-- m=8 over a vecf32(8) is one sub-quantizer per dimension (pq_len=1) with
-- 256 levels, so the PQ residual cannot invert a zero-distance row at k=1
-- and every top-1 below is deterministic.
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 64;
SET kmeans_train_percent = 100;
SET kmeans_max_iteration = 12;
SET probe_limit = 16;

drop database if exists ivfpq_rotation;
create database ivfpq_rotation;
use ivfpq_rotation;

create table t (id bigint primary key, v vecf32(8));

-- 256 distinct, collinear vectors: v = [id,id,...]. Exact-match probes below
-- are then unambiguous.
insert into t
select result,
       cast(concat('[', result, ',', result, ',', result, ',', result, ',',
                        result, ',', result, ',', result, ',', result, ']') as vecf32(8))
from generate_series(1, 256) g;

select count(*) as rows_loaded from t;

create index ix using ivfpq on t (v)
    op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8;

-- The rotation itself: 256 rows / capacity 64 = 4 sub-indexes, and the
-- metadata table carries exactly one row per sub-index.
set @mtbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='ivfpq_rotation')
      and name='ix' and algo_table_type='ivfpq_meta');
set @q = concat('select count(*) as sub_indexes from `', @mtbl, '`');
prepare s from @q;
execute s;
deallocate prepare s;

-- Every sub-index was packed and stored: each metadata row must have a
-- non-empty checksum and a positive file size. A sub-index freed without
-- being persisted would show up here as a zero.
set @q = concat('select count(*) as persisted from `', @mtbl,
                '` where checksum <> '''' and filesize > 0');
prepare s from @q;
execute s;
deallocate prepare s;

-- Correctness across the rotation boundary: probes chosen to land in the
-- 1st, 2nd, 3rd and 4th sub-index respectively (chunks are 1-64, 65-128,
-- 129-192, 193-256 in insertion order). Each must still find its exact row,
-- which only works if every sub-index was built, saved and reloaded.
select id from t order by l2_distance(v, '[7,7,7,7,7,7,7,7]') limit 1;
select id from t order by l2_distance(v, '[80,80,80,80,80,80,80,80]') limit 1;
select id from t order by l2_distance(v, '[150,150,150,150,150,150,150,150]') limit 1;
select id from t order by l2_distance(v, '[233,233,233,233,233,233,233,233]') limit 1;

-- A search that spans sub-indexes still merges to a globally correct top-k.
select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 3;

drop index ix on t;

-- A capacity below the k-means minimum is refused rather than silently
-- routing the whole table into the brute-force CDC tail.
SET ivfpq_max_index_capacity = 2;
create index ixbad using ivfpq on t (v)
    op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8;

SET ivfpq_max_index_capacity = 99999;
drop database ivfpq_rotation;
