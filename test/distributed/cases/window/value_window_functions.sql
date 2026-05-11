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

-- Test with varchar type (varlen) to cover IsVarlen branch
select id, category, LAG(category) over (order by id) as prev_category from test_value_window where category = 'A';
select id, category, LEAD(category) over (order by id) as next_category from test_value_window where category = 'A';
select id, category, FIRST_VALUE(category) over (order by id) as first_category from test_value_window where category = 'A';
select id, category, LAST_VALUE(category) over (order by id rows between unbounded preceding and unbounded following) as last_category from test_value_window where category = 'A';

-- Test with NULL values
drop table if exists test_null_window;
create table test_null_window (id int, val int);
insert into test_null_window values (1, 100), (2, null), (3, 300), (4, null);
select id, val, LAG(val) over (order by id) as prev_val from test_null_window;
select id, val, LEAD(val) over (order by id) as next_val from test_null_window;
select id, val, FIRST_VALUE(val) over (order by id) as first_val from test_null_window;
drop table if exists test_null_window;

-- Test with different integer types
drop table if exists test_int_types;
create table test_int_types (id int, val_i8 tinyint, val_i16 smallint, val_i32 int, val_i64 bigint);
insert into test_int_types values (1, 10, 100, 1000, 10000), (2, 20, 200, 2000, 20000), (3, 30, 300, 3000, 30000);
select id, LAG(val_i8) over (order by id) as prev_i8 from test_int_types;
select id, LAG(val_i64) over (order by id) as prev_i64 from test_int_types;
select id, LEAD(val_i32) over (order by id) as next_i32 from test_int_types;
drop table if exists test_int_types;

-- Test with float types
drop table if exists test_float_types;
create table test_float_types (id int, val_f32 float, val_f64 double);
insert into test_float_types values (1, 1.1, 1.11), (2, 2.2, 2.22), (3, 3.3, 3.33);
select id, LAG(val_f32) over (order by id) as prev_f32 from test_float_types;
select id, LEAD(val_f64) over (order by id) as next_f64 from test_float_types;
drop table if exists test_float_types;

-- Test keywords as role names
drop role if exists lag;
drop role if exists lead;

-- Cleanup
drop table if exists test_value_window;
