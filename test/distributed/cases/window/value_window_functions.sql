drop table if exists test_value_window;
create table test_value_window (id int, year_col int, revenue decimal(10, 2), category varchar(20));
insert into test_value_window values (1, 2020, 1000.00, 'A'), (2, 2021, 1500.00, 'A'), (3, 2022, 1800.00, 'A'), (4, 2023, 2000.00, 'A'), (5, 2020, 500.00, 'B'), (6, 2021, 600.00, 'B'), (7, 2022, 750.00, 'B');

-- Test LAG function
select id, year_col, revenue, LAG(revenue) over (order by year_col) as prev_revenue from test_value_window where category = 'A';

-- Test LEAD function
select id, year_col, revenue, LEAD(revenue) over (order by year_col) as next_revenue from test_value_window where category = 'A';

-- Test FIRST_VALUE function
select id, year_col, revenue, FIRST_VALUE(revenue) over (order by year_col) as first_revenue from test_value_window where category = 'A';

-- Test LAST_VALUE function with explicit frame
select id, year_col, revenue, LAST_VALUE(revenue) over (order by year_col rows between unbounded preceding and unbounded following) as last_revenue from test_value_window where category = 'A';

-- Test NTH_VALUE function
select id, year_col, revenue, NTH_VALUE(revenue, 1) over (order by year_col) as first_revenue from test_value_window where category = 'A';

-- Test LAG with partition
select id, year_col, revenue, category, LAG(revenue) over (partition by category order by year_col) as prev_revenue from test_value_window order by category, year_col;

-- Test keywords as role names
drop role if exists lag;
drop role if exists lead;

-- Cleanup
drop table if exists test_value_window;
