-- Test mo_tuple_expr function for decoding tuple bytes

-- Test 1: NULL input
select mo_tuple_expr(NULL) as result;

-- Test 2: Invalid input  
select mo_tuple_expr('invalid_bytes') as result;

-- Test 3: Empty string
select mo_tuple_expr('') as result;

-- Test 4: Create table with multiple composite indexes of different types
drop database if exists test_tuple;
create database test_tuple;
use test_tuple;

create table products (
    id int primary key,
    name varchar(50),
    price decimal(10,2),
    quantity int,
    discount decimal(5,2),
    category varchar(20),
    -- Composite index: varchar + int
    key idx_name_qty (name, quantity),
    -- Composite index: decimal + int  
    key idx_price_qty (price, quantity),
    -- Composite index: varchar + decimal
    key idx_cat_discount (category, discount)
);

insert into products values 
    (1, 'Apple', 12.50, 100, 0.10, 'Fruit'),
    (2, 'Banana', 8.30, 150, 0.15, 'Fruit'),
    (3, 'Laptop', 1299.99, 20, 0.05, 'Electronics');

-- Test 5: Verify secondary index tables were created (should have 3 indexes)
select count(distinct name) as index_count
from mo_catalog.mo_indexes 
where name in ('idx_name_qty', 'idx_price_qty', 'idx_cat_discount');

-- Test 6: Decode tuples from idx_name_qty (varchar + int)
set @tbl1 = (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx_name_qty' limit 1);
set @sql1 = concat('select mo_tuple_expr(__mo_index_idx_col) as decoded_tuple from test_tuple.`', @tbl1, '` order by __mo_index_pri_col');
prepare stmt1 from @sql1;
execute stmt1;
deallocate prepare stmt1;

-- Test 7: Decode tuples from idx_price_qty (decimal + int)
set @tbl2 = (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx_price_qty' limit 1);
set @sql2 = concat('select mo_tuple_expr(__mo_index_idx_col) as decoded_tuple from test_tuple.`', @tbl2, '` order by __mo_index_pri_col');
prepare stmt2 from @sql2;
execute stmt2;
deallocate prepare stmt2;

-- Test 8: Decode tuples from idx_cat_discount (varchar + decimal)
set @tbl3 = (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx_cat_discount' limit 1);
set @sql3 = concat('select mo_tuple_expr(__mo_index_idx_col) as decoded_tuple from test_tuple.`', @tbl3, '` order by __mo_index_pri_col');
prepare stmt3 from @sql3;
execute stmt3;
deallocate prepare stmt3;

-- Clean up
drop database if exists test_tuple;
