-- BVT for cte_max_memory_bytes (Issue #11297)
-- Verifies that data-driven exponential recursive CTEs are stopped by the
-- memory quota with a clear query-level error instead of node OOM.
--
-- Scoping notes (not testable in single-CN BVT):
--   - The quota is per-operator, per-CN. Two CTEs in one query each have
--     an independent Accountant. With N CN nodes the cluster-wide bound is
--     N x cte_max_memory_bytes. The error message says "on this node" to
--     reflect this.
--   - Vector.Size() is approximate (see pkg/container/vector/vector.go).
--     The quota is an OOM defense, not byte-precise accounting.
--   - The Accountant is not mutex-protected; safe because the merge
--     operator funnels parallel feeders through a single-threaded Call.

-- recursive_cte.sql leaves cte_max_recursion_depth = 100 in this session;
-- restore the default so the recursion-depth gate doesn't preempt the
-- memory-quota gate we're testing here.
set cte_max_recursion_depth = 1000;

-- The session variable should be visible and dynamically settable.
show variables like 'cte_max_memory_bytes';
set cte_max_memory_bytes = 1073741824;
show variables like 'cte_max_memory_bytes';

-- Setting 0 disables the quota (escape hatch for ops).
set cte_max_memory_bytes = 0;
show variables like 'cte_max_memory_bytes';

-- Restore default for the rest of the test.
set cte_max_memory_bytes = 1073741824;

-- Reasonable deep linear recursion must keep working under default quota.
-- Payload per row is tiny so 500 levels is well below 1 GiB.
with recursive seq(v) as (
    select 1
    union all
    select v+1 from seq where v < 500
) select count(*) from seq;

-- Issue #11297 reproduction: data-driven exponential growth.
-- Self-join blows up the working set; with a tight quota the query must
-- abort with the memory quota error rather than running until the node
-- is OOM-killed.
drop table if exists product_q;
create table product_q (id int primary key, p_id int, p_name varchar(25), price decimal(10,3));
insert into product_q values
(3,2,'bed',3560.98),
(2,null,'chair',1599.00),
(4,1,'desk',2999.99),
(5,3,'door',8123.09),
(6,3,'mirrors',698.00),
(7,4,'tv',5678.00);

-- Tighten the quota so the test fails fast and deterministically.
set cte_max_memory_bytes = 16777216;
-- @regex("recursive CTE memory quota exceeded on this node",true)
with recursive cte_ab_8(productID, price) as (
    select p_id, price from product_q
    union all
    select c.productID, p.price
    from product_q p join cte_ab_8 c on p.p_id = c.productID
) select count(*) from cte_ab_8;

-- Convergent linear recursion is unaffected by the small quota.
set cte_max_memory_bytes = 1073741824;
with recursive seq2(v) as (
    select 1
    union all
    select v+1 from seq2 where v < 100
) select count(*) from seq2;

-- Two recursive CTEs in one query must use independent per-operator quotas.
-- If they shared an Accountant the combined working set would exceed the
-- tight quota; with independent instances each CTE stays within its own
-- limit and the query completes successfully.
set cte_max_memory_bytes = 33554432;
with recursive seqA(v) as (
    select 1
    union all
    select v+1 from seqA where v < 200
),
seqB(v) as (
    select 1
    union all
    select v+1 from seqB where v < 200
)
select (select count(*) from seqA) + (select count(*) from seqB) as total;

drop table if exists product_q;
set cte_max_memory_bytes = 1073741824;
set cte_max_recursion_depth = 1000;
