CREATE DATABASE IF NOT EXISTS moc4504;
USE moc4504;

-- #####################################################
--                Part 1: json_set
-- #####################################################

-- test table
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    info JSON
);

-- insert some simple data
INSERT INTO users (info) VALUES ('{"name": "John"}');

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+------------------+
-- | id | info             |
-- +----+------------------+
-- |  1 | {"name": "John"} |
-- +----+------------------+
-- 1 row in set (0.00 sec)

-- tc_001
-- insert new key:value
UPDATE users SET info = JSON_SET(info, '$.age', 30);
-- expected in mysql 8.0.31
-- mysql> UPDATE users SET info = JSON_SET(info, '$.age', 30);
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+-----------------------------+
-- | id | info                        |
-- +----+-----------------------------+
-- |  1 | {"age": 30, "name": "John"} |
-- +----+-----------------------------+
-- 1 row in set (0.00 sec)


-- tc_002
-- update existing key:value
UPDATE users SET info = JSON_SET(info, '$.age', 31) WHERE id = 1;
-- expected in mysql 8.0.31
-- mysql> UPDATE users SET info = JSON_SET(info, '$.age', 31) WHERE id = 1;
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+-----------------------------+
-- | id | info                        |
-- +----+-----------------------------+
-- |  1 | {"age": 31, "name": "John"} |
-- +----+-----------------------------+
-- 1 row in set (0.00 sec)


-- insert more complex data
INSERT INTO users (info) VALUES ('{"person": {"name": "Alice", "address": {"city": "New York"}}}');
-- expected in mysql 8.0.31
-- mysql> INSERT INTO users (info) VALUES ('{"person": {"name": "Alice", "address": {"city": "New York"}}}');
-- Query OK, 1 row affected (0.01 sec)

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+----------------------------------------------------------------+
-- | id | info                                                           |
-- +----+----------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                    |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "New York"}}} |
-- +----+----------------------------------------------------------------+
-- 2 rows in set (0.00 sec)

-- tc_003: update address city from NY to LA where user id is 2
UPDATE users SET info = JSON_SET(info, '$.person.address.city', 'Los Angeles') WHERE id = 2;
-- expected in mysql 8.0.31
-- mysql> UPDATE users SET info = JSON_SET(info, '$.person.address.city', 'Los Angeles') WHERE id = 2;
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+-------------------------------------------------------------------+
-- | id | info                                                              |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                       |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "Los Angeles"}}} |
-- +----+-------------------------------------------------------------------+
-- 2 rows in set (0.00 sec)

-- insert array data
INSERT INTO users (info) VALUES ('{"users": ["Cherry", "Davis", "Emma", "Francis"]}');
-- expected in mysql 8.0.31
-- mysql> INSERT INTO users (info) VALUES ('{"users": ["Cherry", "Davis", "Emma", "Francis"]}');
-- Query OK, 1 row affected (0.00 sec)

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+-------------------------------------------------------------------+
-- | id | info                                                              |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                       |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "Los Angeles"}}} |
-- |  3 | {"users": ["Cherry", "Davis", "Emma", "Francis"]}                 |
-- +----+-------------------------------------------------------------------+
-- 3 rows in set (0.01 sec)

-- tc_004: update user name to Devin with index 1
UPDATE users SET info = JSON_SET(info, '$.users[1]', 'Devin');
-- expected in mysql 8.0.31
-- mysql> UPDATE users SET info = JSON_SET(info, '$.users[1]', 'Devin');
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 3  Changed: 1  Warnings: 0

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM users;
-- +----+-------------------------------------------------------------------+
-- | id | info                                                              |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                       |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "Los Angeles"}}} |
-- |  3 | {"users": ["Cherry", "Devin", "Emma", "Francis"]}                 |
-- +----+-------------------------------------------------------------------+
-- 3 rows in set (0.00 sec)

INSERT INTO users VALUES (5, '{"name":"Eric","age":35,"hobby":"reading"}');

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> select * from users;
-- +----+-------------------------------------------------------------------+
-- | id | info                                                              |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                       |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "Los Angeles"}}} |
-- |  3 | {"users": ["Cherry", "Devin", "Emma", "Francis"]}                 |
-- |  5 | {"age": 35, "name": "Eric", "hobby": "reading"}                   |
-- +----+-------------------------------------------------------------------+
-- 4 rows in set (0.00 sec)


-- tc_005: update multiple keys
UPDATE users SET info = JSON_SET(info, '$.name', 'Bob', '$.hobby', 'writing') WHERE id = 5;
-- expected in mysql 8.0.31
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM users;
-- expected in mysql 8.0.31
-- mysql> select * from users;
-- +----+-------------------------------------------------------------------+
-- | id | info                                                              |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                       |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "Los Angeles"}}} |
-- |  3 | {"users": ["Cherry", "Devin", "Emma", "Francis"]}                 |
-- |  5 | {"age": 35, "name": "Bob", "hobby": "writing"}                    |
-- +----+-------------------------------------------------------------------+
-- 4 rows in set (0.00 sec)


-- tc_006: if user's age exists, update existing
SELECT JSON_SET(a.info, '$.age', 18) as user_id_1 FROM users as a WHERE a.id = 1;
-- expected in mysql 8.0.31
-- mysql> SELECT JSON_SET(a.info, '$.age', 18) as user_id_1 FROM users as a WHERE a.id = 1;
-- +-----------------------------+
-- | user_id_1                   |
-- +-----------------------------+
-- | {"age": 18, "name": "John"} |
-- +-----------------------------+
-- 1 row in set (0.00 sec)

-- tc_007: if user's gender does not exists, insert new
SELECT JSON_SET(a.info, '$.gender', 'Male') as user_id_1 FROM users as a WHERE a.id = 1;
-- expected in mysql 8.0.31
-- mysql> SELECT JSON_SET(a.info, '$.gender', 'Male') as user_id_1 FROM users as a WHERE a.id = 1;
-- +-----------------------------------------------+
-- | user_id_1                                     |
-- +-----------------------------------------------+
-- | {"age": 31, "name": "John", "gender": "Male"} |
-- +-----------------------------------------------+
-- 1 row in set (0.00 sec)


SET @json = '{"name":"Allen","age":14}';
-- expected in mysql 8.0.31
-- mysql> SET @json = '{"name":"Allen","age":14}';
-- Query OK, 0 rows affected (0.00 sec)

-- tc_008: insert new key:value
SELECT @json, JSON_SET(@json, '$.gender', 'Male');
-- mysql> SELECT @json, JSON_SET(@json, '$.gender', 'Male');
-- +----------------------------+-------------------------------------------------+
-- | @json                      | JSON_SET(@json, '$.gender', 'Male')             |
-- +----------------------------+-------------------------------------------------+
-- | {"name":"Allen","age":14}   | {"age": 14, "name": "Allen", "gender": "Male"}   |
-- +----------------------------+-------------------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_009: update existing key:value
SELECT @json, JSON_SET(@json, '$.age', 18);
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.age', 18);
-- +----------------------------+-------------------------------+
-- | @json                      | JSON_SET(@json, '$.age', 18)  |
-- +----------------------------+-------------------------------+
-- | {"name":"Allen","age":14}   | {"age": 18, "name": "Allen"}   |
-- +----------------------------+-------------------------------+
-- 1 row in set (0.00 sec)

-- tc_010: complex json
SET @json = '{"user":{"name":"Eric","age":20}}';
SELECT @json, JSON_SET(@json, '$.user.age', 22);
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.user.age', 22);
-- +-------------------------------------+-----------------------------------------+
-- | @json                               | JSON_SET(@json, '$.user.age', 22)       |
-- +-------------------------------------+-----------------------------------------+
-- | {"user":{"name":"Eric","age":20}}   | {"user": {"age": 22, "name": "Eric"}}   |
-- +-------------------------------------+-----------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_011: array update existing
SET @json = '{"fruits":["apple","banana","cherry"]}';
SELECT @json, JSON_SET(@json, '$.fruits[1]', 'pear');
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.fruits[1]', 'pear');
-- +----------------------------------------+-----------------------------------------+
-- | @json                                  | JSON_SET(@json, '$.fruits[1]', 'pear')  |
-- +----------------------------------------+-----------------------------------------+
-- | {"fruits":["apple","banana","cherry"]} | {"fruits": ["apple", "pear", "cherry"]} |
-- +----------------------------------------+-----------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_012: array insert new
SET @json = '{"numbers":[1,2,3]}';
SELECT @json, JSON_SET(@json, '$.numbers[3]', 4);
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.numbers[3]', 4);
-- +---------------------+------------------------------------+
-- | @json               | JSON_SET(@json, '$.numbers[3]', 4) |
-- +---------------------+------------------------------------+
-- | {"numbers":[1,2,3]} | {"numbers": [1, 2, 3, 4]}          |
-- +---------------------+------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_013: update multiple keys
SET @json = '{"name":"Henry","age":28,"city":"Beijing"}';
SELECT @json, JSON_SET(@json, '$.age', 30, '$.city', 'Shanghai');
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.age', 30, '$.city', 'Shanghai');
-- +---------------------------------------------+----------------------------------------------------+
-- | @json                                       | JSON_SET(@json, '$.age', 30, '$.city', 'Shanghai') |
-- +---------------------------------------------+----------------------------------------------------+
-- | {"name":"Henry","age":28,"city":"Beijing"}   | {"age": 30, "city": "Shanghai", "name": "Henry"}    |
-- +---------------------------------------------+----------------------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_014
SET @json = '{"a": 1,"b": [2, 3]}';
SELECT @json, JSON_SET(@json, '$.a', 10); 
-- expected in mysql 8.0.31
-- mysql> SELECT @json,  JSON_SET(@json, '$.a', 10); 
-- +----------------------+----------------------------+
-- | @json                | JSON_SET(@json, '$.a', 10) |
-- +----------------------+----------------------------+
-- | {"a": 1,"b": [2, 3]} | {"a": 10, "b": [2, 3]}     |
-- +----------------------+----------------------------+
-- 1 row in set (0.00 sec)

-- tc_015
SELECT @json, JSON_SET(@json, '$.c', '[true, false]');
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.c', '[true, false]');
-- +----------------------+---------------------------------------------+
-- | @json                | JSON_SET(@json, '$.c', '[true, false]')     |
-- +----------------------+---------------------------------------------+
-- | {"a": 1,"b": [2, 3]} | {"a": 1, "b": [2, 3], "c": "[true, false]"} |
-- +----------------------+---------------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_016
SELECT @json, JSON_SET(@json, '$.b[2]', 4) ;
-- expected in mysql 8.0.31
-- mysql> SELECT @json, JSON_SET(@json, '$.b[2]', 4) ;
-- +----------------------+------------------------------+
-- | @json                | JSON_SET(@json, '$.b[2]', 4) |
-- +----------------------+------------------------------+
-- | {"a": 1,"b": [2, 3]} | {"a": 1, "b": [2, 3, 4]}     |
-- +----------------------+------------------------------+
-- 1 row in set (0.00 sec)

-- #####################################################
--                Part 2: json_insert
-- #####################################################

DROP TABLE IF EXISTS products;
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    details JSON
);

INSERT INTO products (details) VALUES ('{"name": "Laptop"}');

SELECT * FROM products;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM products;
-- +----+--------------------+
-- | id | details            |
-- +----+--------------------+
-- |  1 | {"name": "Laptop"} |
-- +----+--------------------+
-- 1 row in set (0.00 sec)

-- tc_001: insert new key
UPDATE products SET details = JSON_INSERT(details, '$.price', 999.99);
-- expected in mysql 8.0.31
-- mysql> UPDATE products SET details = JSON_INSERT(details, '$.price', 999.99);
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM products;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM products;
-- +----+-------------------------------------+
-- | id | details                             |
-- +----+-------------------------------------+
-- |  1 | {"name": "Laptop", "price": 999.99} |
-- +----+-------------------------------------+
-- 1 row in set (0.00 sec)

INSERT INTO products (details) VALUES ('{"product_info": {"brand": "ABC", "features": []}}');
-- expected in mysql 8.0.31
-- mysql> INSERT INTO products (details) VALUES ('{"product_info": {"brand": "ABC", "features": []}}');
-- Query OK, 1 row affected (0.01 sec)

SELECT * FROM products;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM products;
-- +----+----------------------------------------------------+
-- | id | details                                            |
-- +----+----------------------------------------------------+
-- |  1 | {"name": "Laptop", "price": 999.99}                |
-- |  2 | {"product_info": {"brand": "ABC", "features": []}} |
-- +----+----------------------------------------------------+
-- 2 rows in set (0.00 sec)

-- tc_002: insert feature
UPDATE products SET details = JSON_INSERT(details, '$.product_info.features[0]', 'High - resolution display');
-- expected in mysql 8.0.31
-- UPDATE products SET details = JSON_INSERT(details, '$.product_info.features[0]', 'High - resolution display');
-- mysql> UPDATE products SET details = JSON_INSERT(details, '$.product_info.features[0]', 'High - resolution display');
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 2  Changed: 1  Warnings: 0

SELECT * FROM products;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM products;
-- +----+-------------------------------------------------------------------------------+
-- | id | details                                                                       |
-- +----+-------------------------------------------------------------------------------+
-- |  1 | {"name": "Laptop", "price": 999.99}                                           |
-- |  2 | {"product_info": {"brand": "ABC", "features": ["High - resolution display"]}} |
-- +----+-------------------------------------------------------------------------------+
-- 2 rows in set (0.00 sec)


DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    profile JSON
);

INSERT INTO customers (profile) VALUES ('{"name": "Customer1"}');

SELECT * FROM customers;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM customers;
-- +----+-----------------------+
-- | id | profile               |
-- +----+-----------------------+
-- |  1 | {"name": "Customer1"} |
-- +----+-----------------------+
-- 1 row in set (0.00 sec)

-- tc_003: insert complex data
UPDATE customers SET profile = JSON_INSERT(profile, '$.age', 30, '$.phone', '123 - 456 - 7890');
-- expected in mysql 8.0.31
-- mysql> UPDATE customers SET profile = JSON_INSERT(profile, '$.age', 30, '$.phone', '123 - 456 - 7890');
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0


SELECT * FROM customers;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM customers;
-- +----+---------------------------------------------------------------+
-- | id | profile                                                       |
-- +----+---------------------------------------------------------------+
-- |  1 | {"age": 30, "name": "Customer1", "phone": "123 - 456 - 7890"} |
-- +----+---------------------------------------------------------------+
-- 1 row in set (0.00 sec)

DROP TABLE IF EXISTS student;
CREATE TABLE student (
    id INT AUTO_INCREMENT PRIMARY KEY,
    json_data JSON
);

INSERT INTO student (json_data) VALUES ('{"name": "John"}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- +----+------------------+
-- | id | json_data        |
-- +----+------------------+
-- |  1 | {"name": "John"} |
-- +----+------------------+
-- 1 row in set (0.01 sec)

-- tc_004: insert new key:value 
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.age', 30)
WHERE id = 1;
-- expected in mysql 8.0.31
-- mysql> UPDATE student 
--     -> SET json_data = JSON_INSERT(json_data, '$.age', 30)
--     -> WHERE id = 1;
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 1;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 1;
-- +-----------------------------+
-- | json_data                   |
-- +-----------------------------+
-- | {"age": 30, "name": "John"} |
-- +-----------------------------+
-- 1 row in set (0.00 sec)

INSERT INTO student (json_data) VALUES ('{"person": {"name": "Alice"}}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM student;
-- +----+-------------------------------+
-- | id | json_data                     |
-- +----+-------------------------------+
-- |  1 | {"age": 30, "name": "John"}   |
-- |  2 | {"person": {"name": "Alice"}} |
-- +----+-------------------------------+
-- 2 rows in set (0.00 sec)

-- tc_002: update complext data
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.person.age', 25)
WHERE id = 2;
-- expected in mysql 8.0.31
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 2;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 2;
-- +------------------------------------------+
-- | json_data                                |
-- +------------------------------------------+
-- | {"person": {"age": 25, "name": "Alice"}} |
-- +------------------------------------------+
-- 1 row in set (0.00 sec)

INSERT INTO student (json_data) VALUES ('{"book": {"title": "Book1"}}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM student;
-- +----+------------------------------------------+
-- | id | json_data                                |
-- +----+------------------------------------------+
-- |  1 | {"age": 30, "name": "John"}              |
-- |  2 | {"person": {"age": 25, "name": "Alice"}} |
-- |  3 | {"book": {"title": "Book1"}}             |
-- +----+------------------------------------------+
-- 3 rows in set (0.00 sec)

-- tc_003: insert multiple keys:values
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.book.author', 'Author1', '$.book.price', 19.99)
WHERE id = 3;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM student;
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 3;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 3;
-- +-------------------------------------------------------------------+
-- | json_data                                                         |
-- +-------------------------------------------------------------------+
-- | {"book": {"price": 19.99, "title": "Book1", "author": "Author1"}} |
-- +-------------------------------------------------------------------+
-- 1 row in set (0.00 sec)


INSERT INTO student (json_data) VALUES ('{"fruits": ["apple", "banana"]}');

SELECT json_data FROM student;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student;
-- +-------------------------------------------------------------------+
-- | json_data                                                         |
-- +-------------------------------------------------------------------+
-- | {"age": 30, "name": "John"}                                       |
-- | {"person": {"age": 25, "name": "Alice"}}                          |
-- | {"book": {"price": 19.99, "title": "Book1", "author": "Author1"}} |
-- | {"fruits": ["apple", "banana"]}                                   |
-- +-------------------------------------------------------------------+
-- 4 rows in set (0.00 sec)

-- tc_004: insert new element
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.fruits[2]', 'cherry')
WHERE id = 4;
-- expected in mysql 8.0.31
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 4;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 4;
-- +-------------------------------------------+
-- | json_data                                 |
-- +-------------------------------------------+
-- | {"fruits": ["apple", "banana", "cherry"]} |
-- +-------------------------------------------+
-- 1 row in set (0.00 sec)


INSERT INTO student (json_data) VALUES ('{"numbers": [1, 3]}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM student;
-- +----+-------------------------------------------------------------------+
-- | id | json_data                                                         |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 30, "name": "John"}                                       |
-- |  2 | {"person": {"age": 25, "name": "Alice"}}                          |
-- |  3 | {"book": {"price": 19.99, "title": "Book1", "author": "Author1"}} |
-- |  4 | {"fruits": ["apple", "banana", "cherry"]}                         |
-- |  5 | {"numbers": [1, 3]}                                               |
-- +----+-------------------------------------------------------------------+
-- 5 rows in set (0.00 sec)

-- tc_005: insert new element
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.numbers[2]', 2)
WHERE id = 5;
-- expected in mysql 8.0.31
-- mysql> UPDATE student 
--     -> SET json_data = JSON_INSERT(json_data, '$.numbers[2]', 2)
--     -> WHERE id = 5;
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 5;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 5;
-- expected in mysql 8.0.31
-- +------------------------+
-- | json_data              |
-- +------------------------+
-- | {"numbers": [1, 3, 2]} |
-- +------------------------+
-- 1 row in set (0.00 sec)


INSERT INTO student (json_data) VALUES ('{"store": {"products": [{"name": "product1", "price": 10}]}}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- +----+-------------------------------------------------------------------+
-- | id | json_data                                                         |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 30, "name": "John"}                                       |
-- |  2 | {"person": {"age": 25, "name": "Alice"}}                          |
-- |  3 | {"book": {"price": 19.99, "title": "Book1", "author": "Author1"}} |
-- |  4 | {"fruits": ["apple", "banana", "cherry"]}                         |
-- |  5 | {"numbers": [1, 3, 2]}                                            |
-- |  6 | {"store": {"products": [{"name": "product1", "price": 10}]}}      |
-- +----+-------------------------------------------------------------------+
-- 6 rows in set (0.00 sec)

-- tc_006: insert new key:value
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.store.products[0].quantity', 5)
WHERE id = 6;
-- expected in mysql 8.0.31
-- mysql> UPDATE student 
--     -> SET json_data = JSON_INSERT(json_data, '$.store.products[0].quantity', 5)
--     -> WHERE id = 6;
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 6;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 6;
-- +-----------------------------------------------------------------------------+
-- | json_data                                                                   |
-- +-----------------------------------------------------------------------------+
-- | {"store": {"products": [{"name": "product1", "price": 10, "quantity": 5}]}} |
-- +-----------------------------------------------------------------------------+
-- 1 row in set (0.00 sec)

INSERT INTO student (json_data) VALUES ('{"user": {"name": "User1"}}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM student;
-- +----+-----------------------------------------------------------------------------+
-- | id | json_data                                                                   |
-- +----+-----------------------------------------------------------------------------+
-- |  1 | {"age": 30, "name": "John"}                                                 |
-- |  2 | {"person": {"age": 25, "name": "Alice"}}                                    |
-- |  3 | {"book": {"price": 19.99, "title": "Book1", "author": "Author1"}}           |
-- |  4 | {"fruits": ["apple", "banana", "cherry"]}                                   |
-- |  5 | {"numbers": [1, 3, 2]}                                                      |
-- |  6 | {"store": {"products": [{"name": "product1", "price": 10, "quantity": 5}]}} |
-- |  7 | {"user": {"name": "User1"}}                                                 |
-- +----+-----------------------------------------------------------------------------+
-- 7 rows in set (0.00 sec)

-- tc_007: insert NULL value
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.user.email', NULL)
WHERE id = 7;
-- expected in mysql 8.0.31
-- mysql> UPDATE student 
--     -> SET json_data = JSON_INSERT(json_data, '$.user.email', NULL)
--     -> WHERE id = 7;
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 7;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 7;
-- +--------------------------------------------+
-- | json_data                                  |
-- +--------------------------------------------+
-- | {"user": {"name": "User1", "email": null}} |
-- +--------------------------------------------+
-- 1 row in set (0.00 sec)

INSERT INTO student (json_data) VALUES ('{"category": "Electronics"}');

SELECT * FROM student;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM student;
-- +----+-----------------------------------------------------------------------------+
-- | id | json_data                                                                   |
-- +----+-----------------------------------------------------------------------------+
-- |  1 | {"age": 30, "name": "John"}                                                 |
-- |  2 | {"person": {"age": 25, "name": "Alice"}}                                    |
-- |  3 | {"book": {"price": 19.99, "title": "Book1", "author": "Author1"}}           |
-- |  4 | {"fruits": ["apple", "banana", "cherry"]}                                   |
-- |  5 | {"numbers": [1, 3, 2]}                                                      |
-- |  6 | {"store": {"products": [{"name": "product1", "price": 10, "quantity": 5}]}} |
-- |  7 | {"user": {"name": "User1", "email": null}}                                  |
-- |  8 | {"category": "Electronics"}                                                 |
-- +----+-----------------------------------------------------------------------------+
-- 8 rows in set (0.00 sec)

-- tc_008: insert multiple keys:values
UPDATE student 
SET json_data = JSON_INSERT(json_data, '$.product_details', '{"brand": "ABC", "model": "XYZ"}')
WHERE id = 8;
-- expected in mysql 8.0.31
-- mysql> UPDATE student 
--     -> SET json_data = JSON_INSERT(json_data, '$.product_details', '{"brand": "ABC", "model": "XYZ"}')
--     -> WHERE id = 8;
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT json_data FROM student WHERE id = 8;
-- expected in mysql 8.0.31
-- mysql> SELECT json_data FROM student WHERE id = 8;
-- +--------------------------------------------------------------------------------------------+
-- | json_data                                                                                  |
-- +--------------------------------------------------------------------------------------------+
-- | {"category": "Electronics", "product_details": "{\"brand\": \"ABC\", \"model\": \"XYZ\"}"} |
-- +--------------------------------------------------------------------------------------------+
-- 1 row in set (0.00 sec)


-- #####################################################
--                Part 3: json_update
-- #####################################################
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_info JSON
);

INSERT INTO employees (user_info) VALUES ('{"name": "John", "age": 30}');

SELECT * FROM employees;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM employees;
-- +----+-----------------------------+
-- | id | user_info                   |
-- +----+-----------------------------+
-- |  1 | {"age": 30, "name": "John"} |
-- +----+-----------------------------+
-- 1 row in set (0.00 sec)

-- tc_001
UPDATE employees SET user_info = JSON_REPLACE(user_info, '$.age', 31) WHERE id = 1;
-- expected in mysql 8.0.31
-- mysql> UPDATE employees SET user_info = JSON_REPLACE(user_info, '$.age', 31) WHERE id = 1;
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM employees;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM employees;
-- +----+-----------------------------+
-- | id | user_info                   |
-- +----+-----------------------------+
-- |  1 | {"age": 31, "name": "John"} |
-- +----+-----------------------------+
-- 1 row in set (0.00 sec)

INSERT INTO employees (user_info) VALUES ('{"person": {"name": "Alice", "address": {"city": "New York"}}}');

SELECT * FROM employees;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM employees;
-- +----+----------------------------------------------------------------+
-- | id | user_info                                                      |
-- +----+----------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                    |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "New York"}}} |
-- +----+----------------------------------------------------------------+
-- 2 rows in set (0.00 sec)

-- tc_002
UPDATE employees SET user_info = JSON_REPLACE(user_info, '$.person.address.city', 'Los Angeles') WHERE id = 2;
-- expected in mysql 8.0.31
-- mysql> UPDATE employees SET user_info = JSON_REPLACE(user_info, '$.person.address.city', 'Los Angeles') WHERE id = 2;
-- Query OK, 1 row affected (0.01 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM employees;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM employees;
-- +----+-------------------------------------------------------------------+
-- | id | user_info                                                         |
-- +----+-------------------------------------------------------------------+
-- |  1 | {"age": 31, "name": "John"}                                       |
-- |  2 | {"person": {"name": "Alice", "address": {"city": "Los Angeles"}}} |
-- +----+-------------------------------------------------------------------+
-- 2 rows in set (0.00 sec)


DROP TABLE IF EXISTS myproducts;
CREATE TABLE myproducts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_details JSON
);

INSERT INTO myproducts (product_details) VALUES ('{"size": "small", "color": "red", "weight": "light"}');

SELECT * FROM myproducts;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM myproducts;
-- +----+------------------------------------------------------+
-- | id | product_details                                      |
-- +----+------------------------------------------------------+
-- |  1 | {"size": "small", "color": "red", "weight": "light"} |
-- +----+------------------------------------------------------+
-- 1 row in set (0.00 sec)

-- tc_003
UPDATE myproducts SET product_details = JSON_REPLACE(product_details, '$.size', "medium", '$.color', "blue") WHERE id = 1;
-- expected in mysql 8.0.31
-- mysql> UPDATE myproducts SET product_details = JSON_REPLACE(product_details, '$.size', "medium", '$.color', "blue") WHERE id = 1;
-- Query OK, 1 row affected (0.04 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM myproducts;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM myproducts;
-- +----+--------------------------------------------------------+
-- | id | product_details                                        |
-- +----+--------------------------------------------------------+
-- |  1 | {"size": "medium", "color": "blue", "weight": "light"} |
-- +----+--------------------------------------------------------+
-- 1 row in set (0.00 sec)


DROP TABLE IF EXISTS inventory;
CREATE TABLE inventory (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_items JSON
);

INSERT INTO inventory (stock_items) VALUES ('{"items": ["apple", "banana"]}');

SELECT * FROM inventory;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM inventory;
-- +----+--------------------------------+
-- | id | stock_items                    |
-- +----+--------------------------------+
-- |  1 | {"items": ["apple", "banana"]} |
-- +----+--------------------------------+
-- 1 row in set (0.00 sec)

-- tc_004
UPDATE inventory SET stock_items = JSON_REPLACE(stock_items, '$.items[1]', "cherry") WHERE id = 1;

SELECT * FROM inventory;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM inventory;
-- +----+--------------------------------+
-- | id | stock_items                    |
-- +----+--------------------------------+
-- |  1 | {"items": ["apple", "cherry"]} |
-- +----+--------------------------------+
-- 1 row in set (0.01 sec)


INSERT INTO inventory (stock_items) VALUES ('{"items": ["1", "3", "5"]}');

SELECT * FROM inventory;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM inventory;
-- +----+--------------------------------+
-- | id | stock_items                    |
-- +----+--------------------------------+
-- |  1 | {"items": ["apple", "cherry"]} |
-- |  2 | {"items": ["1", "3", "5"]}     |
-- +----+--------------------------------+
-- 2 rows in set (0.00 sec)

-- tc_005
UPDATE inventory SET stock_items = JSON_REPLACE(stock_items, '$.items[0]', "2", '$.items[2]', "4") WHERE id = 2;
-- expected in mysql 8.0.31
-- mysql> UPDATE inventory SET stock_items = JSON_REPLACE(stock_items, '$.items[0]', "2", '$.items[2]', "4") WHERE id = 2;
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0

SELECT * FROM inventory;
-- expected in mysql 8.0.31
-- mysql> SELECT * FROM inventory;
-- +----+--------------------------------+
-- | id | stock_items                    |
-- +----+--------------------------------+
-- |  1 | {"items": ["apple", "cherry"]} |
-- |  2 | {"items": ["2", "3", "4"]}     |
-- +----+--------------------------------+
-- 2 rows in set (0.00 sec)

DROP DATABASE IF EXISTS moc4504;