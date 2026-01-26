drop database if exists arrayagg;
create database arrayagg;
use arrayagg;
CREATE TABLE employees (
    id INT,
    name VARCHAR(50),
    department VARCHAR(30),
    salary DECIMAL(10,2),
    hire_date DATE
);

INSERT INTO employees VALUES
(1, 'Alice Johnson', 'Engineering', 75000.00, '2020-03-15'),
(2, 'Bob Smith', 'Engineering', 68000.00, '2019-07-22'),
(3, 'Carol Davis', 'Engineering', 82000.00, '2018-11-08'),
(4, 'David Wilson', 'Marketing', 55000.00, '2021-01-10'),
(5, 'Eva Brown', 'Marketing', 62000.00, '2020-09-05'),
(6, 'Frank Miller', 'Sales', 48000.00, '2021-06-18'),
(7, 'Grace Lee', 'Sales', 51000.00, '2020-12-03');
SELECT JSON_ARRAYAGG(name) as all_employees
FROM employees;

SELECT
    department,
    JSON_ARRAYAGG(name) as department_employees
FROM employees
GROUP BY department;

SELECT
    department,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'id', id,
            'name', name,
            'salary', salary,
            'hire_date', hire_date
        )
    ) as employees_details
FROM employees
GROUP BY department;

SELECT
    department,
    JSON_ARRAYAGG(name) as employees_by_salary
FROM (
    SELECT
        department,
        name,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
    FROM employees
) ranked
GROUP BY department;

SELECT JSON_ARRAYAGG(department) as departments
FROM (
    SELECT DISTINCT department
    FROM employees
) unique_departments;

SELECT JSON_ARRAYAGG(department) as departments
FROM (
    SELECT department
    FROM employees
    GROUP BY department
) grouped_departments;

SELECT JSON_ARRAY(
    (SELECT GROUP_CONCAT(DISTINCT department SEPARATOR '","') FROM employees)
) as departments;

SELECT CONCAT('[',
    GROUP_CONCAT(
        JSON_OBJECT('name', department, 'count', dept_count)
        SEPARATOR ','
    ),
    ']') as department_stats
FROM (
    SELECT
        department,
        COUNT(*) as dept_count
    FROM employees
    GROUP BY department
    ORDER BY department
) dept_summary;

-- 部门员工的完整信息结构
SELECT
    JSON_OBJECT(
        'departments', JSON_OBJECTAGG(
            department,
            JSON_OBJECT(
                'employee_count', employee_count,
                'employees', employees,
                'avg_salary', avg_salary
            )
        )
    ) as company_structure
FROM (
    SELECT
        department,
        COUNT(*) as employee_count,
        JSON_ARRAYAGG(name) as employees,
        AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
) dept_summary;

-- 创建用户权限表
drop table if exists user_permissions;
CREATE TABLE user_permissions (
    user_id INT,
    permission VARCHAR(50)
);

INSERT INTO user_permissions VALUES
(1, 'read_users'),
(1, 'write_users'),
(1, 'read_reports'),
(1, 'admin_panel'),
(2, 'read_users'),
(2, 'read_reports'),
(3, 'read_users'),
(3, 'write_users'),
(3, 'read_reports'),
(3, 'write_reports');

SELECT
    user_id,
    JSON_ARRAYAGG(permission) as permissions
FROM user_permissions
GROUP BY user_id;


-- 创建产品标签表
drop table if exists product_tags;
CREATE TABLE product_tags (
    product_id INT,
    tag VARCHAR(30)
);

INSERT INTO product_tags VALUES
(1, 'electronics'),
(1, 'smartphone'),
(1, 'apple'),
(1, 'premium'),
(2, 'electronics'),
(2, 'laptop'),
(2, 'apple'),
(2, 'professional'),
(3, 'home'),
(3, 'kitchen'),
(3, 'appliance'),
(4, 'furniture'),
(4, 'office'),
(4, 'ergonomic');

-- 聚合每个产品的标签
SELECT
    product_id,
    JSON_ARRAYAGG(tag) as tags
FROM product_tags
GROUP BY product_id;


-- 创建订单商品表
CREATE TABLE order_items (
    order_id INT,
    product_name VARCHAR(50),
    quantity INT,
    price DECIMAL(10,2)
);

INSERT INTO order_items VALUES
(1001, 'iPhone 14', 1, 999.99),
(1001, 'iPhone Case', 1, 29.99),
(1001, 'Screen Protector', 2, 15.99),
(1002, 'MacBook Pro', 1, 1299.99),
(1002, 'USB-C Hub', 1, 79.99),
(1003, 'Coffee Mug', 3, 12.99),
(1003, 'Notebook', 5, 8.99);

-- 聚合每个订单的商品信息
SELECT
    order_id,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'product', product_name,
            'qty', quantity,
            'price', price,
            'subtotal', quantity * price
        )
    ) as order_items,
    SUM(quantity * price) as total_amount
FROM order_items
GROUP BY order_id;


-- 创建销售数据表
CREATE TABLE daily_sales (
    product_id INT,
    sale_date DATE,
    sales_amount DECIMAL(10,2)
);

INSERT INTO daily_sales VALUES
(1, '2024-01-01', 1500.00),
(1, '2024-01-02', 1200.00),
(1, '2024-01-03', 1800.00),
(1, '2024-01-04', 1350.00),
(2, '2024-01-01', 800.00),
(2, '2024-01-02', 950.00),
(2, '2024-01-03', 1100.00),
(2, '2024-01-04', 750.00);

-- 聚合每个产品的销售时间序列
SELECT
    product_id,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'date', sale_date,
            'amount', sales_amount
        )
    ) as sales_timeline
FROM (
    SELECT product_id, sale_date, sales_amount
    FROM daily_sales
    ORDER BY product_id, sale_date
) ordered_sales
GROUP BY product_id;




-- 创建角色权限表
CREATE TABLE role_permissions (
    role_name VARCHAR(30),
    permission VARCHAR(50)
);

CREATE TABLE user_roles (
    user_id INT,
    role_name VARCHAR(30)
);

INSERT INTO role_permissions VALUES
('admin', 'read_users'),
('admin', 'write_users'),
('admin', 'delete_users'),
('admin', 'admin_panel'),
('editor', 'read_users'),
('editor', 'write_reports'),
('viewer', 'read_users'),
('viewer', 'read_reports');

INSERT INTO user_roles VALUES
(1, 'admin'),
(2, 'editor'),
(2, 'viewer'),
(3, 'viewer');

-- 生成用户的完整权限结构
SELECT
    user_id,
    JSON_OBJECTAGG(
        role_name,
        permissions_array
    ) as user_permissions
FROM (
    SELECT
        ur.user_id,
        ur.role_name,
        JSON_ARRAYAGG(rp.permission) as permissions_array
    FROM user_roles ur
    JOIN role_permissions rp ON ur.role_name = rp.role_name
    GROUP BY ur.user_id, ur.role_name
) role_permissions_grouped
GROUP BY user_id;



drop table if exists products;
CREATE TABLE products (
    id INT,
    name VARCHAR(50),
    description TEXT,
    price DECIMAL(10,2),
    category VARCHAR(30)
);

INSERT INTO products VALUES
(1, 'iPhone 14', 'Latest Apple smartphone with advanced features', 999.99, 'Electronics'),
(2, 'MacBook Pro', NULL, 1299.99, 'Electronics'),  -- NULL 描述
(3, 'Coffee Mug', 'Ceramic mug perfect for morning coffee', 15.99, 'Home'),
(4, 'Desk Chair', NULL, 199.99, 'Furniture'),      -- NULL 描述
(5, 'Notebook', 'High-quality paper notebook for writing', 8.99, 'Office'),
(6, 'Wireless Mouse', NULL, 29.99, 'Electronics'), -- NULL 描述
(7, 'Table Lamp', 'Modern LED table lamp with adjustable brightness', 45.99, 'Home');

SELECT JSON_ARRAYAGG(description) as all_descriptions
FROM products;

SELECT JSON_ARRAYAGG(description) as non_null_descriptions
FROM products
WHERE description IS NOT NULL;

SELECT
    category,
    JSON_ARRAYAGG(name) as product_names,
    JSON_ARRAYAGG(description) as all_descriptions,
    JSON_ARRAYAGG(
        CASE
            WHEN description IS NOT NULL THEN description
            ELSE CONCAT('No description for ', name)
        END
    ) as descriptions_with_fallback
FROM products
GROUP BY category;

SELECT
    category,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'name', name,
            'description', description,
            'price', price
        )
    ) as products_with_descriptions
FROM products
WHERE description IS NOT NULL
GROUP BY category;

SELECT
    JSON_OBJECT(
        'category', category,
        'products', JSON_ARRAYAGG(
            JSON_OBJECT(
                'id', id,
                'name', name,
                'price', price,
                'description', COALESCE(description, ''),
                'hasDescription', description IS NOT NULL
            )
        )
    ) as category_data
FROM products
GROUP BY category;

SELECT
    category,
    CASE
        WHEN COUNT(description) > 0 THEN JSON_ARRAYAGG(description)
        ELSE JSON_ARRAY()
    END as descriptions
FROM products
WHERE description IS NOT NULL
GROUP BY category;

SELECT
    category,
    COUNT(*) as total_products,
    COUNT(description) as products_with_description,
    COUNT(*) - COUNT(description) as products_without_description,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'name', name,
            'has_description', description IS NOT NULL,
            'description', description
        )
    ) as product_details
FROM products
GROUP BY category;



-- 创建包含不同数据类型的表
drop table if exists product_attributes;
CREATE TABLE product_attributes (
    product_id INT,
    attribute_name VARCHAR(50),
    string_value VARCHAR(255),
    numeric_value DECIMAL(10,2),
    date_value DATE
);

INSERT INTO product_attributes VALUES
(1, 'color', 'Black', NULL, NULL),
(1, 'weight', NULL, 0.17, NULL),
(1, 'release_date', NULL, NULL, '2023-09-15'),
(1, 'warranty', '1 year', NULL, NULL),
(2, 'color', NULL, NULL, NULL),  -- 完全 NULL 的记录
(2, 'price_range', 'Premium', NULL, NULL);

-- 聚合时处理不同类型的 NULL 值
SELECT
    product_id,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'attribute', attribute_name,
            'value', COALESCE(
                string_value,
                CAST(numeric_value AS CHAR),
                CAST(date_value AS CHAR),
                'N/A'
            )
        )
    ) as attributes
FROM product_attributes
GROUP BY product_id;

SELECT
    category,
    JSON_ARRAYAGG(
        COALESCE(description, CONCAT('Default description for ', name))
    ) as descriptions_with_defaults
FROM products
GROUP BY category;

drop table if exists orders;
create table orders (
    order_id INT,
    product_id INT,
    quantity INT,
    notes TEXT
);

insert into orders values
(1001, 1, 2, 'Gift wrap requested'),
(1001, 3, 1, NULL),
(1001, 5, 3, 'Express delivery'),
(1002, 2, 1, NULL),
(1002, 4, 1, NULL);

SELECT
    o.order_id,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'product', p.name,
            'quantity', o.quantity,
            'notes', COALESCE(o.notes, 'No special notes'),
            'has_notes', o.notes IS NOT NULL
        )
    ) as order_items
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY o.order_id;
drop table orders;
drop table products;
drop database arrayagg;





