-- SQL-to-Window coverage for the cost-based hash partition design (#27943).
-- The table is deliberately larger than one vector batch and ANALYZE supplies
-- low-NDV cardinality evidence for the automatic HASH admission decision.
drop database if exists window_hash_partition;
create database window_hash_partition;
use window_hash_partition;

create table t (
    id int primary key,
    k int,
    ck char(3),
    v int
);
insert into t
select result, result % 50, cast(result % 50 as char(3)), result
from generate_series(1, 10000) g;
analyze table t(k, ck);

-- SORT is the session default; COST and HASH are explicit comparison controls.
select @@window_partition_algorithm;

-- The default stays on the legacy path until the multi-scope acceptance gate
-- and independent design approval are complete.
-- @regex("Hash Partition",false)
explain (check '["Partition"]')
select sum(v) over (partition by k) from t;

set window_partition_algorithm = 'COST';
-- COST is an explicit validation control for the admission model.
-- @regex("Hash Partition",true)
explain (check '["Hash Partition"]')
select sum(v) over (partition by k) from t;

set window_partition_algorithm = 'SORT';
-- @regex("Hash Partition",false)
explain (check '["Partition"]')
select sum(v) over (partition by k) from t;

set window_partition_algorithm = 'HASH';
-- @regex("Hash Partition",true)
explain (check '["Hash Partition"]')
select sum(v) over (partition by k) from t;

set window_partition_algorithm = default;
select @@window_partition_algorithm;

-- Aggregate / unordered Window contract.
select sum(part_sum) from (
    select sum(v) over (partition by k) as part_sum from t
) q;

-- Ranking and value functions with a Window ORDER BY.
select sum(rnk) from (
    select rank() over (partition by k order by v) as rnk from t
) q;
select sum(first_v) from (
    select first_value(v) over (partition by k order by v) as first_v from t
) q;

-- ROWS and RANGE frames preserve the Window contract on the HASH path.
select sum(frame_sum) from (
    select sum(v) over (
        partition by k order by v rows between 1 preceding and current row
    ) as frame_sum from t
) q;
select sum(frame_sum) from (
    select sum(v) over (
        partition by k order by v range between 50 preceding and current row
    ) as frame_sum from t
) q;

-- CHAR remains an intentional SORT control: padding equality is not hash-safe.
-- @regex("Hash Partition",false)
explain (check '["Partition"]')
select sum(v) over (partition by ck) from t;
select sum(part_sum) from (
    select sum(v) over (partition by ck) as part_sum from t
) q;

drop database window_hash_partition;
