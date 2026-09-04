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

-- SET GLOBAL is how an operator sets them
set global max_index_cache_size = 1073741824;
select @@global.max_index_cache_size;
show global variables like 'max_index_cache_size';

set global max_gpu_index_cache_size = 4294967296;
select @@global.max_gpu_index_cache_size;
show global variables like 'max_gpu_index_cache_size';

-- host and device are independent budgets: moving one must not move the other
set global max_index_cache_size = 2048;
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
