SELECT JSON_REPLACE('{"fruits": ["apple", "banana", "cherry"]}', '$.fruits[1]', 'orange') AS result;
SELECT JSON_REPLACE('{"fruits": ["apple", "banana"]}', '$.fruits[2]', 'cherry') AS result;
SELECT JSON_REPLACE('{"fruits": [{"name": "apple"}, {"name": "banana"}]}', '$.fruits[1].color', 'yellow') AS result;
SELECT JSON_REPLACE('{"user": {"name": "John", "age": 30}}', '$.user.age', 31, '$.user.city', 'New York') AS result;
SELECT JSON_REPLACE('{"company": {"name": "Moonshot AI", "employees": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]}}', '$.company.employees[0].name', 'John Doe', '$.company.employees[1].department', 'HR') AS result;
CREATE TABLE users (
    id INT PRIMARY KEY,
    info JSON
);

INSERT INTO users (id, info) VALUES
(1, '{"name": "Alice", "age": 30, "email": "alice@example.com", "address": {"city": "New York", "zip": "10001"}}'),
(2, '{"name": "Bob", "age": 25, "email": "bob@example.com", "address": {"city": "Los Angeles", "zip": "90001"}}'),
(3, '{"name": "Charlie", "age": 28, "email": "charlie@example.com", "address": {"city": "Chicago", "zip": "60601"}, "skills": ["Java", "Python"]}');

SELECT * FROM users;

UPDATE users
SET info = JSON_REPLACE(info, '$.age', 31)
WHERE id = 1;
SELECT * FROM users;

UPDATE users
SET info = JSON_REPLACE(info, '$.address.city', 'San Francisco')
WHERE id = 1;
SELECT * FROM users;

UPDATE users
SET info = JSON_REPLACE(info, '$.skills[0]', 'JavaScript')
WHERE id = 3;
SELECT * FROM users;

UPDATE users
SET info = JSON_REPLACE(info, '$.age', 32, '$.address.city', 'San Francisco')
WHERE id = 1;
SELECT * FROM users;

drop table users;
