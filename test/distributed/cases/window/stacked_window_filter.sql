
-- Regression coverage for issue #23882:
-- Stacked WINDOW nodes where outer filter references earlier window output
-- could bind to wrong slot, causing panic: unsafe slice cast length mismatch.

-- ============================================================
-- Setup
-- ============================================================
drop table if exists swf_orders;
create table swf_orders (
    customer_id int,
    amount decimal(12, 2),
    order_date date
);
insert into swf_orders values
    (1, 10.00, '2024-01-01'), (1, 20.00, '2024-01-02'), (1, 30.00, '2024-01-03'),
    (2, 7.00, '2024-01-01'), (2, 9.00, '2024-01-02'),
    (3, 100.00, '2024-01-01'), (3, 200.00, '2024-01-02'), (3, 50.00, '2024-01-03'), (3, 25.00, '2024-01-04');

-- ============================================================
-- Case 1: Original reproduction — ROW_NUMBER + SUM(decimal), filter on ROW_NUMBER
-- The prior window (ROW_NUMBER, int64/8 bytes) filter gets mis-bound to
-- the later window (SUM decimal128/16 bytes) slot => panic
-- ============================================================
select x.customer_id, x.cum_amount
from (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as cum_amount
    from swf_orders
    where year(order_date) = 2024
) as x
where x.rn = 2
order by x.customer_id;

-- ============================================================
-- Case 2: Filter on the LATER window column (SUM) instead of the earlier one
-- Ensures current-window filter also remaps correctly
-- ============================================================
select x.customer_id, x.rn
from (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as cum_amount
    from swf_orders
    where year(order_date) = 2024
) as x
where x.cum_amount > 50.00
order by x.customer_id, x.rn;

-- ============================================================
-- Case 3: Three window functions — filter on the FIRST window
-- Deeper stacking: ROW_NUMBER, RANK, SUM; filter references ROW_NUMBER
-- ============================================================
select x.customer_id, x.cum_amount, x.rnk
from (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        rank() over (partition by customer_id order by amount desc) as rnk,
        sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as cum_amount
    from swf_orders
    where year(order_date) = 2024
) as x
where x.rn = 1
order by x.customer_id;

-- ============================================================
-- Case 4: Three window functions — filter on the MIDDLE window
-- ============================================================
select x.customer_id, x.rn, x.cum_amount
from (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        rank() over (partition by customer_id order by amount desc) as rnk,
        sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as cum_amount
    from swf_orders
    where year(order_date) = 2024
) as x
where x.rnk = 1
order by x.customer_id;

-- ============================================================
-- Case 5: Multiple outer filters referencing DIFFERENT window columns
-- Both rn (early) and cum_amount (late) are filtered simultaneously
-- ============================================================
select x.customer_id, x.cum_amount
from (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as cum_amount
    from swf_orders
    where year(order_date) = 2024
) as x
where x.rn >= 2 and x.cum_amount > 20.00
order by x.customer_id, x.cum_amount;

-- ============================================================
-- Case 6: Different type widths — AVG(decimal) produces wider intermediate
-- ============================================================
select x.customer_id, x.avg_amount
from (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        avg(amount) over (partition by customer_id order by order_date rows unbounded preceding) as avg_amount
    from swf_orders
    where year(order_date) = 2024
) as x
where x.rn = 3
order by x.customer_id;

-- ============================================================
-- Case 7: Window with CTE — ensure stacked window filter works through CTE
-- ============================================================
with ranked as (
    select customer_id,
        row_number() over (partition by customer_id order by order_date) as rn,
        sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as cum_amount
    from swf_orders
    where year(order_date) = 2024
)
select customer_id, cum_amount from ranked where rn = 2 order by customer_id;

-- ============================================================
-- Case 8: Issue #24081 original shape
-- ROW_NUMBER and SUM(decimal) in the same derived table; the outer
-- filter compares the ROW_NUMBER output and must not use decimal
-- comparison state from the later SUM window column.
-- ============================================================
drop table if exists swf_issue_24081_orders;
create table swf_issue_24081_orders (
    id int auto_increment primary key,
    customer_id int,
    order_date date,
    amount decimal(10, 2)
);
insert into swf_issue_24081_orders (customer_id, order_date, amount) values
    (1, '2024-01-01', 100.00), (1, '2024-02-01', 150.00), (1, '2024-03-01', 50.00),
    (2, '2024-01-10', 80.00), (2, '2024-04-02', 120.00), (3, '2024-05-05', 60.00);

select x.__mf_part_key as customer_id,
       x.__mf_cum_val as second_cum_amount
from (
    select customer_id as __mf_part_key,
           row_number() over (partition by customer_id order by order_date) as __mf_rn,
           sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as __mf_cum_val
    from swf_issue_24081_orders
    where year(order_date) = 2024
) as x
where x.__mf_rn = 2
order by customer_id;

-- Case 9: Arithmetic expression on ROW_NUMBER result.
select x.__mf_part_key as customer_id,
       x.__mf_cum_val as second_cum_amount
from (
    select customer_id as __mf_part_key,
           row_number() over (partition by customer_id order by order_date) as __mf_rn,
           sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as __mf_cum_val
    from swf_issue_24081_orders
    where year(order_date) = 2024
) as x
where x.__mf_rn + 0 = 2
order by customer_id;

-- Case 10: BETWEEN expression on ROW_NUMBER result.
select x.__mf_part_key as customer_id,
       x.__mf_cum_val as second_cum_amount
from (
    select customer_id as __mf_part_key,
           row_number() over (partition by customer_id order by order_date) as __mf_rn,
           sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as __mf_cum_val
    from swf_issue_24081_orders
    where year(order_date) = 2024
) as x
where x.__mf_rn between 2 and 2
order by customer_id;

-- Case 11: IN expression on ROW_NUMBER result.
select x.__mf_part_key as customer_id,
       x.__mf_cum_val as second_cum_amount
from (
    select customer_id as __mf_part_key,
           row_number() over (partition by customer_id order by order_date) as __mf_rn,
           sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as __mf_cum_val
    from swf_issue_24081_orders
    where year(order_date) = 2024
) as x
where x.__mf_rn in (2)
order by customer_id;

-- Case 12: Mixed predicates on ROW_NUMBER and SUM(decimal) aliases.
select x.__mf_part_key as customer_id,
       x.__mf_cum_val as second_cum_amount
from (
    select customer_id as __mf_part_key,
           row_number() over (partition by customer_id order by order_date) as __mf_rn,
           sum(amount) over (partition by customer_id order by order_date rows unbounded preceding) as __mf_cum_val
    from swf_issue_24081_orders
    where year(order_date) = 2024
) as x
where x.__mf_rn = 2 and x.__mf_cum_val >= 200.00
order by customer_id;

-- ============================================================
-- Cleanup
-- ============================================================
drop table if exists swf_orders;
drop table if exists swf_issue_24081_orders;
