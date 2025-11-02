drop database if exists rollup_test;
create database rollup_test;
use rollup_test;



-- single column rollup
drop table if exists sales;
create table sales(col1 float, col2 double);
insert into sales values (2000, 4525.32321);
insert into sales values (2001, 31214321.3432423);
insert into sales values (null, null);
select * from sales;
select col1, sum(col2) as profit from sales group by col1 with rollup;
select col1, avg(col2) as profit from sales group by col1 with rollup;
drop table sales;




-- multi column rollup
drop table if exists sales;
create table sales(year int, quarter int, amount int);
insert into sales values (2021,1,100);
insert into sales values (2021,2,150);
insert into sales values (2021,3,200);
insert into sales values (2021,4,250);
insert into sales values (2022,1,300);
insert into sales values (2022,2,350);
insert into sales values (2022,3,400);
insert into sales values (2022,4,450);

select * from sales;
select year, quarter, sum(amount) as total_sales from sales group by year, quarter with rollup;
select year, quarter, avg(amount) as avg_sales from sales group by year, quarter with rollup;
select year, quarter, count(amount) as avg_sales from sales group by year, quarter with rollup order by avg_sales desc;
show create table sales;
select * from sales;
truncate sales;
drop table sales;




-- rollup with group by and having
drop table if exists sales;
create table sales (
year int,
quarter char(2),
product text,
amount decimal(10, 2)
);
insert into sales (year, quarter, product, amount) values
(2022, 'Q1', 'Product A', 1000.00),
(2022, 'Q1', 'Product B', 1500.00),
(2022, 'Q2', 'Product A', 2000.00),
(2022, 'Q2', 'Product C', 2500.00),
(2023, 'Q1', 'Product A', 3000.00),
(2023, 'Q1', 'Product B', 3500.00),
(2023, 'Q2', 'Product B', 4000.00),
(2023, 'Q2', 'Product C', 4500.00);
select
    year,
    quarter,
    null as product,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter with rollup;

select
    year,
    quarter,
    product,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    product with rollup;

select
    year,
    quarter,
    product,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    product with rollup
having
     (product is not null and quarter is not null) or
     (product is null and quarter is null);

select
    year,
    quarter,
    product,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    product with rollup
having
     (product = 'Product A') or (product = 'Product B');
drop table sales;




-- group by with rollup and grouping
drop table if exists orders;
drop table if exists order_items;
drop table if exists customers;
drop table if exists products;
drop table if exists categories;

create table orders (
order_id int primary key ,
customer_id int,
order_date date,
status enum('Pending', 'Shipped', 'Delivered', 'Cancelled')
);

insert into orders (order_id, customer_id, order_date, status) values
(1, 1, '2024-01-01', 'Delivered'),
(2, 2, '2024-01-02', 'Shipped'),
(3, 3, '2024-01-03', 'Delivered'),
(4, 1, '2024-01-04', 'Delivered');
select * from orders;

create table order_items (
item_id int primary key,
order_id int,
product_id int,
quantity int,
price decimal(10, 2)
);

insert into order_items (item_id, order_id, product_id, quantity, price) values
(1, 1, 101, 2, 19.99),
(2, 1, 102, 1, 29.99),
(3, 2, 103, 3, 9.99),
(4, 3, 104, 1, 49.99),
(5, 4, 101, 1, 19.99);
select * from order_items;

create table customers (
customer_id int primary key,
first_name varchar(50),
last_name varchar(50),
city char(50)
);

insert into customers (customer_id, first_name, last_name, city) values
(1, 'John', 'Doe', 'New York'),
(2, 'Jane', 'Smith', 'Los Angeles'),
(3, 'Alice', 'Johnson', 'Chicago');
select * from customers;

create table products (
product_id int primary key,
product_name varchar(100),
category_id int
);

insert into products (product_id, product_name, category_id) values
(101, 'Product A', 1),
(102, 'Product B', 1),
(103, 'Product C', 2),
(104, 'Product D', 2);
select * from products;

create table categories (
category_id int primary key,
category_name varchar(100)
);

insert into categories (category_id, category_name) values
(1, 'Electronics'),
(2, 'Books');
select * from categories;

-- @bvt:issue#20000
select
    year(o.order_date) as order_year,
    month(o.order_date) as order_month,
    c.city,
    cat.category_name,
    sum(oi.quantity * oi.price) as total_sales
from
    orders o
    join
    order_items oi on o.order_id = oi.order_id
    join
    customers c on o.customer_id = c.customer_id
    join
    products p on oi.product_id = p.product_id
    join
    categories cat on p.category_id = cat.category_id
where
    o.status = 'Delivered'
group by
    year(o.order_date),
    month(o.order_date),
    cat.category_name,
    c.city,
    c.customer_id,
    cat.category_id with rollup
order by
    order_year,
    order_month,
    c.city,
    cat.category_name;
-- @bvt:issue

-- @bvt:issue#19993
select
    year(o.order_date) as order_year,
    month(o.order_date) as order_month,
    c.city,
    cat.category_name,
    sum(oi.quantity * oi.price) as total_sales,
    grouping(year(o.order_date)) as year_grouping,
    grouping(month(o.order_date)) as month_grouping,
    grouping(c.city) as city_grouping,
    grouping(c.customer_id) as customer_grouping,
    grouping(cat.category_id) as category_grouping
from
    orders o
    join
    order_items oi on o.order_id = oi.order_id
    join
    customers c on o.customer_id = c.customer_id
    join
    products p on oi.product_id = p.product_id
    join
    categories cat on p.category_id = cat.category_id
where
    o.status = 'Delivered'
group by
    year(o.order_date),
    month(o.order_date),
    cat.category_name,
    c.city,
    c.customer_id,
    cat.category_id with rollup
order by
    order_year,
    order_month,
    c.city,
    cat.category_name;
-- @bvt:issue

drop table if exists orders;
drop table if exists order_items;
drop table if exists customers;
drop table if exists products;
drop table if exists categories;




-- dtype: bigint, decimal
drop table if exists uint_64;
create table uint_64(i bigint unsigned, j bigint unsigned, k decimal);
insert into uint_64 values (18446744073709551615, 2147483647, 123213.99898);
insert into uint_64 values (4294967295, 2147483647, 2);
insert into uint_64 values (18446744073709551615, 1, 2);
insert into uint_64 values (2147483647, 23289483, 123213.99898);
insert into uint_64 values (13289392, 2, 2);
insert into uint_64 values (18446744073709551615, 23289483, 1);
insert into uint_64 values (3824, 13289392, 123213.99898);
insert into uint_64 values (2438294, 1, 2);
insert into uint_64 values (3824, 13289392, 1);
select * from uint_64;
select
    i, j, sum(k) as total
from
    uint_64
group by
    i, j with rollup;

select
    i, j, sum(k) as total
from
    uint_64
group by
    j, i with rollup;

select
    i, j, sum(k) as total
from
    uint_64
group by
    j, i with rollup
having
    (i is not null and j is not null);
drop table uint_64;




-- multi-dimensional summary using rollup
drop table if exists sales;
create table sales (
year int,
quarter int,
region text,
amount decimal(10, 2)
);

insert into sales (year, quarter, region, amount) values
(2021, 1, 'North', 10000),
(2021, 1, 'South', 15000),
(2021, 2, 'North', 20000),
(2021, 2, 'South', 25000),
(2022, 1, 'North', 30000),
(2022, 1, 'South', 35000),
(2022, 2, 'North', 40000),
(2022, 2, 'South', 45000);

select
    year,
    quarter,
    region,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup
order by
    total_sales desc;

-- using having to distinguish summary rows and general rows
select
    if(grouping(year), 'Total', cast(year as char)) as year,
    if(grouping(quarter), 'Total', cast(quarter as char)) as quarter,
    if(grouping(region), 'Total', region) as region,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup;

select
    grouping(year) as year,
    grouping(quarter) as quarter,
    grouping(region) as region,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup;

select
    grouping(year) as year,
    grouping(quarter) as quarter,
    grouping(region) as region,
    count(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup;

select
    if(grouping(year), 'Total', cast(year as char)) as year,
    if(grouping(quarter), 'Total', cast(quarter as char)) as quarter,
    if(grouping(region), 'Total', region) as region,
    min(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup
order by
    total_sales desc;

select
    if(grouping(year), 'Total', cast(year as char)) as year,
    if(grouping(quarter), 'Total', cast(quarter as char)) as quarter,
    if(grouping(region), 'Total', region) as region,
    max(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup;

-- multi-level aggregation and sorting
select
    year,
    quarter,
    region,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup
order by total_sales desc;

-- alias
-- @bvt:issue#19998
select
    if(grouping(year), 'All years', year) as year,
    if(grouping(quarter), 'Quarter', quarter) as quarter,
    region,
    sum(amount) as total_sales
from
    sales
group by
    year,
    quarter,
    region with rollup
order by total_sales desc;
-- @bvt:issue
drop table sales;




-- rollup with group by and order by
drop table if exists sales_details;
create table sales_details (
id int auto_increment primary key,
sale_date date,
year int,
quarter varchar(2),
region char(50),
country varchar(50),
state varchar(50),
city varchar(50),
product_category varchar(50),
product_name varchar(50),
amount decimal(10, 2)
);

insert into sales_details (sale_date, year, quarter, region, country, state, city, product_category, product_name, amount) values
('2022-01-15', 2022, 'Q1', 'North America', 'USA', 'California', 'Los Angeles', 'Electronics', 'Laptop', 1000.00),
('2022-02-20', 2022, 'Q1', 'North America', 'USA', 'California', 'San Francisco', 'Electronics', 'Smartphone', 1500.00),
('2022-03-30', 2022, 'Q1', 'Europe', 'France', 'Ile-de-France', 'Paris', 'Furniture', 'Sofa', 2000.00),
('2023-09-10', 2023, 'Q3', 'asia', 'Japan', 'Tokyo', 'Tokyo', 'Electronics', 'Tablet', 4500.00),
('2023-09-10', 2023, 'Q3', 'asia', 'Japan', 'Tokyo', 'Tokyo', 'Electronics', 'Tablet', 4500.00);

select
    year,
    quarter,
    region,
    country,
    state,
    city,
    product_category,
    product_name,
    sum(amount) as total_sales,
    if(grouping(year), '9999', year) as sort_year,
    if(grouping(quarter), 'ZZ', quarter) as sort_quarter,
    if(grouping(region), 'ZZZ', quarter) as sort_region,
    if(grouping(country), 'ZZZZ', country) as sort_country,
    if(grouping(state), 'ZZZZZ', state) as sort_state,
    if(grouping(city), 'ZZZZZZ', city) as sort_city,
    if(grouping(product_category), 'ZZZZZZZ', product_category) as sort_product_category,
    if(grouping(product_name), 'ZZZZZZZZZZZZZZZZ', product_name) as sort_product_name
from
    sales_details
group by
    year,
    quarter,
    region,
    country,
    state,
    city,
    product_category,
    product_name with rollup
order by
    sort_year desc,
    sort_quarter asc,
    sort_region asc,
    sort_country asc,
    sort_state asc,
    sort_city asc,
    sort_product_category asc,
    sort_product_name asc;

drop table sales_details;


-- Test for issue #19998: IF with GROUPING should handle mixed string/numeric types
-- This tests that IF(GROUPING(...), 'string', number) returns varchar without errors
drop table if exists sales_mixed_types;
create table sales_mixed_types (
    year int,
    quarter int,
    region text,
    amount decimal(10, 2)
);

insert into sales_mixed_types (year, quarter, region, amount) values
(2021, 1, 'North', 10000),
(2021, 1, 'South', 15000),
(2021, 2, 'North', 20000),
(2021, 2, 'South', 25000),
(2022, 1, 'North', 30000),
(2022, 1, 'South', 35000),
(2022, 2, 'North', 40000),
(2022, 2, 'South', 45000);

-- Test 1: IF with string literal and numeric column (the original failing case)
select
    if(grouping(year), 'All years', year) AS year_label,
    if(grouping(quarter), 'All quarters', quarter) as quarter_label,
    region,
    sum(amount) as total_sales
from
    sales_mixed_types
group by
    year,
    quarter,
    region with rollup
order by year_label, quarter_label, region;

-- Test 2: Mixed types in different order (number first, string second)
select
    if(grouping(year) = 0, year, 'Grand Total') AS year_label,
    sum(amount) as total_sales
from
    sales_mixed_types
group by
    year with rollup
order by year_label;

-- Test 3: Multiple mixed-type IF expressions with GROUPING
select
    if(grouping(year), 'All', cast(year as char)) AS year_str,
    if(grouping(quarter), 'Total', quarter) as quarter_val,
    count(*) as cnt
from
    sales_mixed_types
group by
    year,
    quarter with rollup
order by year_str, quarter_val;

-- Test 4: Nested IF with GROUPING and mixed types
select
    year,
    if(grouping(region), if(grouping(year), 'Grand Total', 'Year Total'), region) as region_label,
    sum(amount) as total_sales
from
    sales_mixed_types
group by
    year,
    region with rollup
order by year, region_label;

-- Test 5: CASE expression with GROUPING (alternative to IF)
select
    case when grouping(year) = 1 then 'All years' else cast(year as char) end AS year_label,
    case when grouping(quarter) = 1 then 'All quarters' else cast(quarter as char) end as quarter_label,
    sum(amount) as total_sales
from
    sales_mixed_types
group by
    year,
    quarter with rollup
order by year_label, quarter_label;

-- Test 6: Verify return type is varchar (can concatenate with strings)
select
    concat('Year: ', if(grouping(year), 'Total', year)) AS year_label,
    sum(amount) as total_sales
from
    sales_mixed_types
group by
    year with rollup
order by year_label;

drop table sales_mixed_types;
drop database rollup_test;