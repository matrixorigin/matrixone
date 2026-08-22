-- =====================================================================
-- vector_cagra_multi_subindex.sql — CAGRA sub-index ROTATION
--
-- GPU REQUIRED.
--
-- The cagra twin of vector_ivfpq_multi_subindex.sql, and the last gap in
-- that pair: every other cagra case sets cagra_max_index_capacity = 99999
-- against 20 rows, so all of them build exactly one sub-index and none of
-- them touches rotation. Rotation is where a finished sub-index is packed
-- to a tar and its GPU memory released before the next one starts, so an
-- untested regression there stays invisible until a large build OOMs.
--
-- Rotation matters more for cagra than for ivfpq, not less: a cagra index
-- keeps its DATASET device-resident in order to search (it walks the graph
-- and reads the actual vectors), which is why the cagra build is still
-- sized against dim*sizeof(Q) while ivfpq no longer is. Freeing each
-- retired sub-index is the only thing bounding that.
--
-- 256 rows at capacity 64 -> exactly 4 sub-indexes, one metadata row each.
-- The graph degrees sit well under the capacity so no sub-index falls below
-- the cuVS minimum graph size and nothing is diverted to the brute-force
-- CDC tail.
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 6;
SET cagra_max_index_capacity = 64;

drop database if exists cagra_rotation;
create database cagra_rotation;
use cagra_rotation;

create table t (id bigint primary key, v vecf32(8));

-- 256 distinct, collinear vectors: v = [id,id,...]. Exact-match probes below
-- are then unambiguous.
insert into t
select result,
       cast(concat('[', result, ',', result, ',', result, ',', result, ',',
                        result, ',', result, ',', result, ',', result, ']') as vecf32(8))
from generate_series(1, 256) g;

select count(*) as rows_loaded from t;

create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=64;

-- The rotation itself: 256 rows / capacity 64 = 4 sub-indexes, and the
-- metadata table carries exactly one row per sub-index.
set @mtbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='cagra_rotation')
      and name='ix' and algo_table_type='cagra_meta');
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

SET cagra_max_index_capacity = 99999;
drop database cagra_rotation;
