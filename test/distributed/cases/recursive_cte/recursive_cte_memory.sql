-- BVT for cte_max_memory_bytes (Issue #11297)
-- Verifies that data-driven exponential recursive CTEs are stopped by the
-- memory quota with a clear query-level error instead of node OOM.

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

drop table if exists product_q;
set cte_max_memory_bytes = 1073741824;
