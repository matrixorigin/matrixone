set enable_remap_hint = 1;
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- verify the basic rewriting functionality of a single table
drop database if exists hint_test;
create database hint_test;
use hint_test;
drop table if exists users;
create table users (
    id int primary key,
    name varchar(50),
    age int,
    city varchar(50)
);
insert into users values
(1, 'Alice', 25, 'Beijing'),
(2, 'Bob', 30, 'Shanghai'),
(3, 'Charlie', 35, 'Guangzhou'),
(4, 'David', 28, 'Shenzhen');

/*+ { "rewrites": { "hint_test.users": "SELECT * FROM hint_test.users WHERE age > 28" } } */ select * from hint_test.users;
select * from users where age > 28;
drop table users;



-- verify the basic rewriting functionality of a multi table
drop table if exists orders;
drop table if exists products;
create table orders (
    order_id int primary key,
    user_id int,
    amount decimal(10,2),
    status varchar(20)
);

create table products (
    product_id int primary key,
    product_name varchar(50),
    price decimal(10,2),
    category varchar(30)
);

insert into orders values
(1, 1, 100.00, 'completed'),
(2, 2, 200.00, 'pending'),
(3, 1, 150.00, 'completed'),
(4, 3, 300.00, 'cancelled');

insert into products values
(1, 'Laptop', 5000.00, 'Electronics'),
(2, 'Mouse', 50.00, 'Electronics'),
(3, 'Desk', 800.00, 'Furniture'),
(4, 'Chair', 600.00, 'Furniture');
/*+ { "rewrites": { "hint_test.orders": "SELECT * FROM orders WHERE status = 'completed'", "hint_test.products": "SELECT * FROM products WHERE category = 'Electronics'" } } */ select o.order_id, o.amount, p.product_name, p.price
from orders o, products p
where o.user_id = 1;
drop table orders;
drop table products;



-- the verification table can be mapped to the real table with the same name
drop table if exists users;
create table users (
    id int primary key,
    name varchar(50),
    age int,
    city varchar(50)
);
insert into users values
(1, 'Alice', 25, 'Beijing'),
(2, 'Bob', 30, 'Shanghai'),
(3, 'Charlie', 35, 'Guangzhou'),
(4, 'David', 28, 'Shenzhen');
/*+ { "rewrites": { "hint_test.users": "SELECT * FROM users WHERE city = 'Beijing'" } } */ select * from users;



-- verify that the rewrite rules support complex queries such as aggregation and grouping
-- @bvt:issue#22662
drop table if exists sales;
create table sales (
    sale_id int primary key,
    product_id int,
    quantity int,
    sale_date date
);

insert into sales values
(1, 1, 10, '2025-01-01'),
(2, 1, 15, '2025-01-02'),
(3, 2, 20, '2025-01-01'),
(4, 2, 25, '2025-01-03'),
(5, 3, 30, '2025-01-02');
/*+ { "rewrites": { "hint_test.sales_summary": "SELECT product_id, SUM(quantity) as total_quantity, COUNT(*) as sale_count FROM sales GROUP BY product_id" } } */ select * from hint_test.sales_summary where total_quantity > 20;
-- 期望结果：
-- product_id | total_quantity | sale_count
-- 1          | 25             | 2
-- 2          | 45             | 2
-- 3          | 30             | 1
drop table sales_summary;



-- the verification table can be rewritten as a reference view
drop view if exists active_users;
create view active_users as
select id, name, age, city
from users
where age between 25 and 35;
/*+ { "rewrites": { "hint_test.user_list": "SELECT * FROM active_users" } } */ select * from user_list where city = 'Shanghai';




-- verify that the rewrite rules support complex queries such as group by / multi group by
drop table if exists transactions;
create table transactions (
        trans_id int,
        user_id int,
        amount decimal(10,2),
        trans_date date,
        trans_type varchar(20)
);

insert into transactions values
(1, 1, 100.00, '2024-01-01', 'purchase'),
(2, 1, 200.00, '2024-01-01', 'purchase'),
(3, 2, 50.00, '2024-01-01', 'refund'),
(4, 2, 300.00, '2024-01-02', 'purchase'),
(5, 3, 150.00, '2024-01-02', 'purchase'),
(6, 1, 75.00, '2024-01-03', 'refund');
/*+ { "rewrites": { "hint_test.daily_summary": "SELECT trans_date, trans_type, COUNT(*) as trans_count, SUM(amount) as total_amount FROM transactions GROUP BY trans_date, trans_type" } } */ select trans_date, trans_type, total_amount
from daily_summary
where trans_type = 'purchase'
order by trans_date;

/*+ { "rewrites": { "hint_test.user_monthly_stats": "SELECT user_id, DATE_FORMAT(trans_date, '%Y-%m') as month, trans_type, COUNT(*) as count, SUM(amount) as total, AVG(amount) as avg_amount FROM transactions GROUP BY user_id, DATE_FORMAT(trans_date, '%Y-%m'), trans_type" } } */ select user_id, month, trans_type, total
from user_monthly_stats
where total > 100
order by user_id, month;




-- the having clause aggregation rewrite
drop table if exists customer_orders;
create table customer_orders (
        customer_id int,
        customer_name varchar(50),
        order_id int,
        order_amount decimal(10,2)
);
insert into customer_orders values
(1, 'Alice', 101, 500.00),
(1, 'Alice', 102, 300.00),
(1, 'Alice', 103, 200.00),
(2, 'Bob', 201, 100.00),
(2, 'Bob', 202, 150.00),
(3, 'Charlie', 301, 1000.00),
(3, 'Charlie', 302, 800.00),
(3, 'Charlie', 303, 1200.00);
/*+ { "rewrites": { "hint_test.high_value_customers": "SELECT customer_id, customer_name, COUNT(*) as order_count, SUM(order_amount) as total_spent, AVG(order_amount) as avg_order FROM customer_orders GROUP BY customer_id, customer_name HAVING SUM(order_amount) >= 1000" } } */ select customer_name, order_count, total_spent
from high_value_customers
order by total_spent desc;
drop table high_value_customers;




-- window function rewrite
drop table if exists employee_sales;
create table employee_sales (
        emp_id int,
        emp_name varchar(50),
        department varchar(30),
        monthly_sales double,
        sale_month date
);
insert into employee_sales values
(1, 'Alice', 'Sales', 10000, '2024-01-01'),
(2, 'Bob', 'Sales', 15000, '2024-01-01'),
(3, 'Charlie', 'Sales', 8000, '2024-01-01'),
(4, 'David', 'Marketing', 12000, '2024-01-01'),
(5, 'Eve', 'Marketing', 9000, '2024-01-01');
/*+ { "rewrites": { "hint_test.dept_rankings": "SELECT emp_id, emp_name, department, monthly_sales, RANK() OVER (PARTITION BY department ORDER BY monthly_sales DESC) as dept_rank, SUM(monthly_sales) OVER (PARTITION BY department) as dept_total FROM employee_sales" } } */ select emp_name, department, monthly_sales, dept_rank
from dept_rankings
where dept_rank <= 2
order by department, dept_rank;
drop table employee_sales;



-- subquery aggregation rewrite
drop table if exists sales;
create table sales (
    sale_id int primary key,
    product_id int,
    quantity int,
    sale_date date
);
insert into sales values
(1, 1, 10, '2025-01-01'),
(2, 1, 15, '2025-01-02'),
(3, 2, 20, '2025-01-01'),
(4, 2, 25, '2025-01-03'),
(5, 3, 30, '2025-01-02');
/*+ { "rewrites": { "hint_test.top_products": "SELECT product_id, product_name, category, (SELECT AVG(quantity) FROM sales s2 WHERE s2.category = s1.category) as category_avg_qty, quantity FROM sales s1 WHERE quantity > (SELECT AVG(quantity) FROM sales s3 WHERE s3.category = s1.category)" } } */ select product_name, category, quantity, category_avg_qty
from top_products
order by category, quantity desc;
drop table sales;



-- union aggregation rewrite
drop table if exists online_sales;
drop table if exists offline_sales;
create table online_sales (
    product varchar(50),
    amount decimal(10,2),
    sale_date date
);

create table offline_sales (
    product varchar(50),
    amount decimal(10,2),
    sale_date date
);

insert into online_sales values
('Laptop', 1200.00, '2024-01-01'),
('Mouse', 25.00, '2024-01-02');

insert into offline_sales values
('Laptop', 1100.00, '2024-01-01'),
('Keyboard', 80.00, '2024-01-03');
/*+ { "rewrites": { "hint_test.all_sales": "SELECT product, SUM(amount) as total_revenue, COUNT(*) as sale_count FROM (SELECT product, amount FROM online_sales UNION ALL SELECT product, amount FROM offline_sales) combined GROUP BY product" } } */ select product, total_revenue, sale_count
from all_sales
where total_revenue > 100
order by total_revenue desc;
drop table if exists online_sales;
drop table if exists offline_sales;




-- aggregation of complex CASE expressions
drop table if exists sales;
create table sales (
    product_id int primary key ,
    product_name varchar(50),
    category varchar(30),
    quantity int,
    price decimal(10,2),
    sale_date date
);
insert into sales values
(1, 'Laptop Pro', 'Electronics', 5, 1200.00, '2024-01-01'),
(2, 'Mouse', 'Electronics', 50, 25.00, '2024-01-02'),
(3, 'Keyboard', 'Electronics', 30, 80.00, '2024-01-03'),
(4, 'Monitor', 'Electronics', 20, 450.00, '2024-01-04'),
(5, 'Desk', 'Furniture', 10, 300.00, '2024-01-05'),
(6, 'Office Chair', 'Furniture', 15, 150.00, '2024-01-06'),
(7, 'Executive Chair', 'Furniture', 8, 1500.00, '2024-01-07'),
(8, 'Pen', 'Stationery', 200, 5.00, '2024-01-08'),
(9, 'Notebook', 'Stationery', 100, 15.00, '2024-01-09');
/*+ { "rewrites": { "hint_test.sales_segments": "SELECT category, SUM(CASE WHEN price >= 1000 THEN quantity ELSE 0 END) as high_price_qty, SUM(CASE WHEN price < 1000 AND price >= 100 THEN quantity ELSE 0 END) as mid_price_qty, SUM(CASE WHEN price < 100 THEN quantity ELSE 0 END) as low_price_qty, COUNT(DISTINCT product_id) as product_variety FROM sales GROUP BY category" } } */ select category, high_price_qty, mid_price_qty, low_price_qty, product_variety
from sales_segments
where product_variety >= 2
order by category;
drop table sales;
-- @bvt:issue



-- empty rule test
/*+ { "rewrites": {} } */ select * from users;
/*+ {} */ select * from users;




-- large-scale rewrite rule testing
/*+ { "rewrites": { "hint_test.t1": "SELECT * FROM users WHERE id = 1", "hint_test.t2": "SELECT * FROM users WHERE id = 2", "hint_test.t3": "SELECT * FROM users WHERE id = 3", "hint_test.t4": "SELECT * FROM users WHERE id = 4", "hint_test.t5": "SELECT * FROM orders WHERE order_id = 1", "hint_test.t6": "SELECT * FROM orders WHERE order_id = 2", "hint_test.t7": "SELECT * FROM products WHERE product_id = 1", "hint_test.t8": "SELECT * FROM products WHERE product_id = 2", "hint_test.t9": "SELECT * FROM sales WHERE sale_id = 1", "hint_test.t10": "SELECT * FROM sales WHERE sale_id = 2" } } */ select * from t1
union all
select * from t2;



-- special character processing
drop table if exists `user_data`;
create table `user-data` (
    id int,
    value varchar(50)
);

insert into `user-data` values (1, 'test');
/*+ { "rewrites": { "hint_test.user-data": "SELECT * FROM `user-data` WHERE id = 1" } } */ select * from `user-data`;
drop table `user-data`;




-- abnormal test: json format error test
-- missing closing parentheses
/*+ { "rewrites": { "hint_test.users": "SELECT * FROM users" } */ select * from users;
-- missing quotation marks
/*+ { rewrites: { "hint_test.users": "SELECT * FROM users" } } */ select * from users;



-- non-existent table reference test
/*+ { "rewrites": { "hint_test.test_table": "SELECT * FROM non_existing_table" } } */ select * from test_table;



-- recursive detection test
/*+ { "rewrites": { "hint_test.t1": "SELECT * FROM t2", "hint_test.t2": "SELECT * FROM users" } } */ select * from t1;
-- 期望结果：
-- t1 被重写为 "SELECT * FROM t2"
-- t2 不会再次被重写，直接查询真实的 t2 表
-- 如果 t2 表不存在，则报错



-- sql syntax error testing
/*+ { "rewrites": { "hint_test.users": "SELET * FORM users" } } */ select * from users;



--️ compatibility testing：test for different comment formats
-- format 1: /*+ */
/*+ { "rewrites": { "hint_test.users": "SELECT * FROM users WHERE age > 25" } } */ select * from users;

-- format 2: /*! */ not supported
/*! {
    "rewrites": {
        "hint_test.users": "SELECT * FROM users WHERE age > 25"
    }
} */
-- select * from users;



--️ compatibility testing：case sensitivity test
drop table if exists users;
create table Users (
    id int,
    name varchar(50)
);

insert into Users values (1, 'Test');
/*+ { "rewrites": { "hint_test.users": "SELECT * FROM Users WHERE id = 1", "hint_test.USERS": "SELECT * FROM Users WHERE id = 2" } } */ SELECT * FROM users;
SELECT * FROM USERS;
SELECT * FROM Users;



-- multiple hint comment tests
drop table if exists users;
create table users (
    id int primary key,
    name varchar(50),
    age int,
    city varchar(50)
);
insert into users values
(1, 'Alice', 25, 'Beijing'),
(2, 'Bob', 30, 'Shanghai'),
(3, 'Charlie', 35, 'Guangzhou'),
(4, 'David', 28, 'Shenzhen');
drop table if exists orders;
create table orders (
    order_id int primary key,
    user_id int,
    amount decimal(10,2),
    status varchar(20)
);
insert into orders values
(1, 1, 100.00, 'completed'),
(2, 2, 200.00, 'pending'),
(3, 1, 150.00, 'completed'),
(4, 3, 300.00, 'cancelled');
/*+ { "rewrites": { "hint_test.users": "SELECT * FROM users WHERE age > 25" } } */ /*+ { "rewrites": { "hint_test.orders": "SELECT * FROM orders WHERE status = 'completed'" } } */ select * from users, orders where users.id = orders.user_id;
drop database hint_test;



-- performance testing
-- @bvt:issue#22662
-- @session:id=1&user=acc01:test_account&password=111
set enable_remap_hint = 1;
drop database if exists acc01_test;
create database acc01_test;
use acc01_test;
drop table if exists large_table;
create table large_table (
    id int,
    data varchar(100)
);
insert into large_table select result, 2 from generate_series(1, 1000000) g;
/*+ { "rewrites": { "hint_test.large_table": "SELECT * FROM large_table WHERE id < 1000" } } */ select count(*) from large_table;
select count(*) from large_table where id < 1000;
drop database acc01_test;
set enable_remap_hint = 0;
-- @session
-- @bvt:issue
drop account acc01;
set enable_remap_hint = 0;
