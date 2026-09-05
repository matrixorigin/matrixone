-- @suit

-- @case
-- @desc:max_index_cache_size / max_gpu_index_cache_size are operator variables: GLOBAL scope only, defaulting to an arena ceiling rather than to unlimited
-- @label:bvt

-- Isolated by ACCOUNT. SET GLOBAL is per-account, so a dedicated account keeps two
-- properties this case needs: the CN-wide SYS caps are untouched (a concurrently
-- running vector/fulltext case keeps its warm indexes), and every run starts from the
-- bootstrap defaults, so the default assertions below hold on a re-run and not only
-- on a virgin cluster.
drop account if exists acc_idx_var;
create account acc_idx_var admin_name 'admin' identified by '123456';

-- @session:id=1&user=acc_idx_var:admin&password=123456

-- Defaults are the per-arena ceilings, not 0: the advertised maximum is a number an
-- operator can read, and an unconfigured deployment is charged and evictable rather than
-- unbounded. Host is far larger than device because RAM and VRAM are orders of magnitude apart.
select @@global.max_index_cache_size;
select @@global.max_gpu_index_cache_size;

-- GLOBAL scope: a session cannot set either, in any of the session spellings
set max_index_cache_size = 1048576;
set session max_index_cache_size = 1048576;
set @@session.max_index_cache_size = 1048576;
set max_gpu_index_cache_size = 1048576;
set session max_gpu_index_cache_size = 1048576;
set @@session.max_gpu_index_cache_size = 1048576;

-- a refused session set leaves the global value alone
select @@global.max_index_cache_size;
select @@global.max_gpu_index_cache_size;

-- SET GLOBAL is how an operator sets them. The magnitudes below are far larger than any
-- index this account holds: the semantics under test -- scope, readback, arena
-- independence -- do not depend on the value binding, and an unreachable value keeps this
-- account's own searches out of the governor's way.
set global max_index_cache_size = 1099511627776;
select @@global.max_index_cache_size;
show global variables like 'max_index_cache_size';

set global max_gpu_index_cache_size = 549755813888;
select @@global.max_gpu_index_cache_size;
show global variables like 'max_gpu_index_cache_size';

-- host and device are independent budgets: moving one must not move the other
set global max_index_cache_size = 2199023255552;
select @@global.max_index_cache_size;
select @@global.max_gpu_index_cache_size;

-- 0 is still accepted; the governor resolves it to the arena ceiling rather than to
-- genuinely unbounded, because an upgraded cluster keeps a persisted 0.
set global max_index_cache_size = 0;
select @@global.max_index_cache_size;
set global max_gpu_index_cache_size = 0;
select @@global.max_gpu_index_cache_size;

-- a negative byte budget is not a value either variable accepts
set global max_index_cache_size = -1;
set global max_gpu_index_cache_size = -1;
select @@global.max_index_cache_size;
select @@global.max_gpu_index_cache_size;
-- @session}

drop account if exists acc_idx_var;
