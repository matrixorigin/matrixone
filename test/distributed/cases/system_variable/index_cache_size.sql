-- @suit

-- @case
-- @desc:max_index_cache_size / max_gpu_index_cache_size are operator variables: GLOBAL scope only, default 0 = unlimited
-- @label:bvt

-- defaults: 0 means no limit, so an unconfigured deployment is unbounded as before
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

-- SET GLOBAL is how an operator sets them. The magnitudes below are deliberately far larger
-- than any index this cluster holds: the SYS value caps EVERY tenant's index cache on the CN,
-- and the governor memoizes it for 15s, so a small value here would make a concurrently running
-- vector/fulltext case evict its warm indexes for up to 15 seconds after this case has moved on.
-- The semantics under test -- scope, readback, arena independence -- do not depend on the value
-- binding, so nothing is lost by keeping every value unreachable.
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

-- 0 puts each back to unlimited
set global max_index_cache_size = 0;
select @@global.max_index_cache_size;
set global max_gpu_index_cache_size = 0;
select @@global.max_gpu_index_cache_size;

-- a negative byte budget is not a value either variable accepts
set global max_index_cache_size = -1;
set global max_gpu_index_cache_size = -1;
select @@global.max_index_cache_size;
select @@global.max_gpu_index_cache_size;
