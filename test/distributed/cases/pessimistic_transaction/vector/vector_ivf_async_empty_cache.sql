-- Regression for #29011 (ivfflat): an early query issued while an ASYNC ivfflat build is still in
-- its not-ready window loads an EMPTY generation. An empty ivfflat index has exactly one centroid
-- row whose vector is NULL (id=1), so LoadCentroids skips it and leaves Index.Centroids nil; every
-- query then routes to bucket 1. Before the fix the query CN retained that empty generation under
-- the version key, so after the build committed the real centroids IN PLACE (same version) that CN
-- kept routing to bucket 1 and returned the wrong nearest neighbor -- and, having no IsStale,
-- ivfflat never self-healed until a reindex/restart. The fix makes the cache NOT retain an empty
-- generation (IvfflatSearch.EmptyGeneration), so the next query cold-reloads the real centroids.
-- SCOPE: end-to-end SMOKE coverage of the async-build -> warm -> query flow, NOT a pre/post
-- regression gate. The not-ready window cannot be held open from SQL and the stale read is
-- placement-dependent (a single CN self-heals via RemoveIdle on the consumer CN), so these
-- assertions also pass on pre-fix code. The deterministic mechanism proof is
-- TestIvfflatEmptyGeneration in pkg/vectorindex/ivfflat, which covers both the empty generation
-- and the bucket-1 generation that must stay cached.
SET experimental_ivf_index=1;
drop database if exists ivf_async_partial_cache;
CREATE DATABASE ivf_async_partial_cache;
USE ivf_async_partial_cache;

CREATE TABLE t(id BIGINT PRIMARY KEY, v VECF32(2));
INSERT INTO t
SELECT result, CAST(CONCAT('[', result, ',0]') AS VECF32(2))
FROM generate_series(1,100) g;

CREATE INDEX ix USING ivfflat ON t(v)
  LISTS=4 OP_TYPE 'vector_l2_ops' ASYNC;

SET probe_limit=4;

-- Warm the ivf index cache DURING the not-ready window: right after CREATE ... ASYNC the centroids
-- table holds only the NULL placeholder, so this loads the empty generation on the query CN. It is
-- a best-effort probe -- the not-ready result is unstable (0 or 1 row, and which row), so it is
-- wrapped to a stable count (always 1). If the async build already finished, it simply warms the
-- complete generation instead; either way the assertions below must hold.
select count(*) >= 0 as warmed
from (select id from t order by l2_distance(v,'[50,0]') limit 1) x;

-- Wait until the async build has committed the complete generation: all 100 entries under the
-- current metadata version (the readiness signal vector_ivf_async uses).
set @ix_entries = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ix' and algo = 'ivfflat' and algo_table_type = 'entries'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1
);
set @ix_metadata = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ix' and algo = 'ivfflat' and algo_table_type = 'metadata'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1
);
set @wait_ix_sql = concat(
    'select count(*) as active_entries from `', database(), '`.`', @ix_entries,
    '` where `__mo_index_centroid_fk_version` = ',
    '(select cast(`__mo_index_val` as bigint) from `', database(), '`.`', @ix_metadata,
    '` where `__mo_index_key` = ''version'')'
);
prepare wait_ix from @wait_ix_sql;
-- @wait_expect(2, 120)
execute wait_ix;
deallocate prepare wait_ix;

-- After the build committed the real centroids IN PLACE, the query must return the true nearest
-- neighbor of [50,0] -- row 50 (distance 0) -- not bucket-1's stale content. Pre-fix, a CN that
-- warmed the empty generation kept returning the wrong row; post-fix the empty generation was not
-- retained, so this cold-reloads the real centroids. Run 3x (the issue observed the stale value was
-- sticky across repeated reads).
SELECT id FROM t ORDER BY l2_distance(v,'[50,0]') LIMIT 1 BY RANK WITH OPTION 'mode=include';
SELECT id FROM t ORDER BY l2_distance(v,'[50,0]') LIMIT 1 BY RANK WITH OPTION 'mode=include';
SELECT id FROM t ORDER BY l2_distance(v,'[50,0]') LIMIT 1 BY RANK WITH OPTION 'mode=include';

drop database ivf_async_partial_cache;
