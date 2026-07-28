-- Test UNION ALL with ORDER BY FIELD() function
-- This test verifies that ORDER BY with complex expressions like FIELD() works correctly
-- with UNION/UNION ALL queries

drop database if exists test_union_order_field;
create database test_union_order_field;
use test_union_order_field;

-- Create test table
create table t1 (
    id int auto_increment primary key,
    name varchar(100),
    value1 decimal(18,2),
    value2 decimal(18,2)
);

insert into t1 (name, value1, value2) values
('apple', 100.00, 120.00),
('banana', 50.00, 60.00),
('cherry', 80.00, 90.00),
('date', 200.00, 210.00);

-- Test 1: Basic UNION ALL with ORDER BY FIELD()
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
order by field(name, 'apple', 'banana', 'cherry', 'date');

-- Test 2: UNION ALL with ORDER BY FIELD() descending
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
order by field(name, 'apple', 'banana', 'cherry', 'date') desc;

-- Test 3: Multiple UNION ALL with ORDER BY FIELD()
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
union all
select name, value1, value2 from t1 where name = 'cherry'
order by field(name, 'apple', 'banana', 'cherry', 'date');

-- Test 4: UNION (not UNION ALL) with ORDER BY FIELD()
select name, value1, value2 from t1 where name = 'apple'
union
select name, value1, value2 from t1 where name = 'banana'
order by field(name, 'apple', 'banana', 'cherry', 'date');

-- Test 5: UNION ALL with ORDER BY expression (arithmetic)
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
order by value2 - value1;

-- Test 6: UNION ALL with ORDER BY simple column
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
order by value1;

-- Test 7: UNION ALL with ORDER BY column position
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
order by 2;

-- Test 8: UNION ALL with ORDER BY FIELD() and LIMIT
select name, value1, value2 from t1 where name = 'apple'
union all
select name, value1, value2 from t1 where name = 'banana'
union all
select name, value1, value2 from t1 where name = 'cherry'
order by field(name, 'cherry', 'banana', 'apple', 'date')
limit 2;

-- Cleanup
drop database test_union_order_field;
