-- Test LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE window functions
-- GitHub Issue: #23405

-- Setup test table
drop table if exists test_value_window;
create table test_value_window (
    id int,
    year int,
    revenue decimal(10, 2),
    category varchar(20)
);

insert into test_value_window values
(1, 2020, 1000.00, 'A'),
(2, 2021, 1500.00, 'A'),
(3, 2022, 1800.00, 'A'),
(4, 2023, 2000.00, 'A'),
(5, 2020, 500.00, 'B'),
(6, 2021, 600.00, 'B'),
(7, 2022, 750.00, 'B');

-- Test LAG function
-- LAG(expr) - returns value from previous row
select id, year, revenue, LAG(revenue) over (order by year) as prev_revenue from test_value_window where category = 'A';

-- LAG with partition
select id, year, revenue, category, LAG(revenue) over (partition by category order by year) as prev_revenue from test_value_window order by category, year;

-- Test LEAD function
-- LEAD(expr) - returns value from next row
select id, year, revenue, LEAD(revenue) over (order by year) as next_revenue from test_value_window where category = 'A';

-- LEAD with partition
select id, year, revenue, category, LEAD(revenue) over (partition by category order by year) as next_revenue from test_value_window order by category, year;

-- Test FIRST_VALUE function
select id, year, revenue, FIRST_VALUE(revenue) over (order by year) as first_revenue from test_value_window where category = 'A';

-- FIRST_VALUE with partition
select id, year, revenue, category, FIRST_VALUE(revenue) over (partition by category order by year) as first_revenue from test_value_window order by category, year;

-- Test LAST_VALUE function
select id, year, revenue, LAST_VALUE(revenue) over (order by year rows between unbounded preceding and unbounded following) as last_revenue from test_value_window where category = 'A';

-- LAST_VALUE with partition
select id, year, revenue, category, LAST_VALUE(revenue) over (partition by category order by year rows between unbounded preceding and unbounded following) as last_revenue from test_value_window order by category, year;

-- Test NTH_VALUE function
select id, year, revenue, NTH_VALUE(revenue, 1) over (order by year) as first_revenue from test_value_window where category = 'A';

-- Test LAG with aggregate function (original issue case)
drop table if exists test_revenue_cost;
create table test_revenue_cost (
    gjahr int,
    zxssr decimal(15, 2),
    ctgry varchar(10)
);

insert into test_revenue_cost values
(2020, 10000.00, 'LG'),
(2021, 12000.00, 'LG'),
(2022, 15000.00, 'LG'),
(2023, 18000.00, 'LG');

-- Original issue query pattern - LAG with SUM aggregate
select 
    gjahr as year,
    round(sum(zxssr), 2) as total_revenue,
    LAG(sum(zxssr)) over (order by gjahr) as prev_revenue
from test_revenue_cost  
where ctgry = 'LG'
group by gjahr
order by gjahr;

-- Test keywords as identifiers (role names)
drop role if exists lag;
drop role if exists lead;
drop role if exists first_value;
drop role if exists last_value;
drop role if exists nth_value;

-- Cleanup
drop table if exists test_value_window;
drop table if exists test_revenue_cost;
