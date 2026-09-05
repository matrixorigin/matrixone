-- @suit

-- @case
-- @desc:max_index_cache_size / max_gpu_index_cache_size are operator variables: GLOBAL scope only, defaulting to 0 meaning the governor derives the budget from the machine
-- @label:bvt

-- Isolated by ACCOUNT. SET GLOBAL is per-account, so a dedicated account keeps two
-- properties this case needs: the CN-wide SYS caps are untouched (a concurrently
-- running vector/fulltext case keeps its warm indexes), and every run starts from the
-- bootstrap defaults, so the default assertions below hold on a re-run and not only
-- on a virgin cluster.
drop account if exists acc_idx_var;
create account acc_idx_var admin_name 'admin' identified by '123456';

-- @session:id=1&user=acc_idx_var:admin&password=123456

-- Both default to 0, which means "no operator limit". It does NOT mean unbounded: the
-- governor then derives each arena's budget from this machine -- a share of total RAM for
-- host, a CUDA query of the devices present for device -- so an unconfigured deployment is
-- still charged and still evictable. A fixed non-zero default would have been a number that
-- describes no particular machine, and would have taken priority over the derived one.
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

-- Setting 0 explicitly returns to the default: no operator limit, budget derived from the
-- machine. Never genuinely unbounded, which matters because an upgraded cluster carries a
-- persisted 0 from before these variables existed.
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
