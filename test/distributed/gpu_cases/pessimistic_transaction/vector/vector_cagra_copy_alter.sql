-- =====================================================================
-- vector_cagra_copy_alter.sql — a COPY ALTER must leave the CAGRA index with a
-- BASE (tag=0) sub-index, not just the CDC tail.
--
-- GPU REQUIRED.
--
-- CAGRA is identity-always-async, so cloneUnaffectedIndexes SKIPS the whole index
-- (SkipWholeIndex) and the replacement table's hidden index tables start EMPTY. CagraSync
-- is stateless across flushes: it only APPENDS tag=1 event chunks and never writes a tag=0
-- sub-index, which is written only by cagra_create. With AlterCopyInitSQL returning
-- (false, "") the ts=0 CDC replay therefore left the whole table in the tail with no base
-- index at all — every query brute-forced the overflow, and a table large enough made that
-- overflow refuse admission. It now returns the REINDEX FORCE_SYNC InitSQL, run post-commit
-- by the CDC's first iteration, so the replacement carries a real base. #29011
-- =====================================================================

SET experimental_cagra_index = 1;

drop database if exists cagra_copy_alter;
create database cagra_copy_alter;
use cagra_copy_alter;

create table t (id bigint primary key, v vecf32(8), a int);
insert into t
select result,
       cast(concat('[', result, ',', result, ',', result, ',', result, ',',
                        result, ',', result, ',', result, ',', result, ']') as vecf32(8)),
       result
from generate_series(1,200) g;

create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4;

-- The original index has a tag=0 base.
set @stbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='cagra_copy_alter')
      and name='ix' and algo_table_type='cagra_index');
set @q = concat('select count(*) > 0 as has_base from `', @stbl, '` where tag = 0');
prepare s from @q;
-- @wait_expect(2, 30)
execute s;
deallocate prepare s;

-- COPY ALTER. The added column is not the indexed one, so `ix` is UNAFFECTED: its hidden
-- tables are skipped and the replacement's CDC is seeded by AlterCopyInitSQL.
alter table t add column b int;

-- The replacement index must carry a tag=0 base once the CDC's first iteration has run the
-- REINDEX InitSQL. Pre-fix this stayed 0 forever (tail only). Re-resolve the hidden table:
-- the COPY ALTER replaced both the base table and its index tables.
set @stbl2 = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='cagra_copy_alter')
      and name='ix' and algo_table_type='cagra_index');
set @q2 = concat('select count(*) > 0 as has_base from `', @stbl2, '` where tag = 0');
prepare s2 from @q2;
-- @wait_expect(5, 120)
execute s2;
deallocate prepare s2;

-- The rebuilt index answers: exact match on a stored vector is its own row. CAGRA stores the
-- full f32 vectors, so a distance-0 hit is stable and the id can be pinned (the IVF-PQ twin
-- asserts a neighbourhood instead, because its 8-bit codebook answer is seed-dependent).
select id from t order by l2_distance(v, '[7,7,7,7,7,7,7,7]') limit 1;
select id from t order by l2_distance(v, '[123,123,123,123,123,123,123,123]') limit 1;

-- The added column survived the rebuild, and no row was lost.
select count(*) from t;
select a, b from t where id = 7;

drop database cagra_copy_alter;
