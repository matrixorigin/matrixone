-- @suit

-- @case
-- @desc:a LOW max_index_cache_size actually bounds the vector index cache, and evicted indexes reload correctly
-- @label:bvt

-- The sibling case (index_cache_size.sql) covers scope/readback with values so large they
-- never bind, deliberately: the SYS value caps EVERY tenant on the CN and is memoized for 15s,
-- so a small SYS value would evict a concurrently running case's warm indexes.
--
-- This case takes the other half -- a value small enough to BIND -- and keeps it isolated by
-- setting it on a dedicated ACCOUNT. SET GLOBAL is per-account, so the tenant cap applies only
-- to this account's entries and the CN-wide SYS value stays 0.
--
-- With no SQL surface for cache residency, the assertion is behavioural: under a cap far below
-- one index, every query must still return the SAME correct rows. That exercises the path the
-- cap engages -- charge, evict, reload -- and fails if eviction corrupts or loses an index.

drop account if exists acc_idx_cap;
create account acc_idx_cap admin_name 'admin' identified by '123456';

-- @session:id=1&user=acc_idx_cap:admin&password=123456
set experimental_hnsw_index = 1;
drop database if exists idx_cap_db;
create database idx_cap_db;
use idx_cap_db;

create table t(id bigint primary key, v vecf32(3));
insert into t values (1,'[0,0,0]'), (2,'[1,1,1]'), (3,'[9,9,9]');
create index h using hnsw on t(v) op_type 'vector_l2_ops';

set @meta = (select index_table_name from mo_catalog.mo_indexes
    where name = 'h' and algo = 'hnsw' and algo_table_type = 'hnsw_meta'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @wait_sql = concat('select count(*) >= 1 as ready from `', database(), '`.`', @meta, '`');
prepare wait_ready from @wait_sql;
-- @wait_expect(1, 120)
execute wait_ready;
deallocate prepare wait_ready;

-- Baseline at the default ceiling, which never binds.
select @@global.max_index_cache_size;
select id from t order by l2_distance(v, '[0,0,0]') asc limit 2;

-- Now a cap far below any index. 4096 bytes cannot hold an hnsw generation, so every load
-- charges over the cap and the governor reclaims -- on this account only.
set global max_index_cache_size = 4096;
select @@global.max_index_cache_size;

-- @session}

-- A fresh session picks up the new global for this account.
-- @session:id=2&user=acc_idx_cap:admin&password=123456
use idx_cap_db;
select @@global.max_index_cache_size;

-- Same answers, repeatedly: each query reloads an index the previous one had evicted.
select id from t order by l2_distance(v, '[0,0,0]') asc limit 2;
select id from t order by l2_distance(v, '[0,0,0]') asc limit 2;
select id from t order by l2_distance(v, '[9,9,9]') asc limit 1;

-- Deliberately no write-then-search step here: hnsw maintains its index asynchronously, so a
-- row inserted now is not in the index yet and the answer would depend on CDC timing, not on
-- the cap. Index freshness is covered by the vector cases; this one is about the bound.

-- 0 resolves to the arena ceiling, not to unbounded; the answers do not change.
set global max_index_cache_size = 0;
select @@global.max_index_cache_size;
select id from t order by l2_distance(v, '[0,0,0]') asc limit 2;

drop database if exists idx_cap_db;
-- @session}

drop account if exists acc_idx_cap;
