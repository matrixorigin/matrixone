SELECT JSON_SET('{"fruits": ["apple", "banana", "cherry"]}', '$.fruits[1]', 'orange') AS result;
SELECT JSON_SET('{"fruits": ["apple", "banana"]}', '$.fruits[2]', 'cherry') AS result;
SELECT JSON_SET('{"fruits": [{"name": "apple"}, {"name": "banana"}]}', '$.fruits[1].color', 'yellow') AS result;
SELECT JSON_SET('{"user": {"name": "John", "age": 30}}', '$.user.age', 31, '$.user.city', 'New York') AS result;
SELECT JSON_SET('{"company": {"name": "Moonshot AI", "employees": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]}}', '$.company.employees[0].name', 'John Doe', '$.company.employees[1].department', 'HR') AS result;
SELECT JSON_SET('{"fruits": ["apple", "banana", "cherry"]}') AS result;
SELECT JSON_SET('{"user": {"name": "John", "age": 30}}', '$.user.age', 31, '$.user.city') AS result;
SELECT JSON_SET(null, '$.fruits[1]', 'orange') AS result;
SELECT JSON_SET('{"fruits": ["apple", "banana", "cherry"]}', null, 'orange') AS result;
SELECT JSON_SET('{"fruits": ["apple", "banana", "cherry"]}', '$.fruits[1]', null) AS result;

-- JSON_SET values use the same typed conversion as JSON_OBJECT and JSON_ARRAY.
select json_type(json_extract(json_set('{}', '$.year', cast('2024' as year)), '$.year')), json_contains(json_set('{}', '$.year', cast('2024' as year)), '2024', '$.year'), json_contains(json_set('{}', '$.year', cast('2024' as year)), '"2024"', '$.year');
select json_unquote(json_extract(json_set('{}', '$.time', cast('04:05:06' as time(0))), '$.time')), json_unquote(json_extract(json_set('{}', '$.blob', cast(x'00ff' as blob)), '$.blob')), json_unquote(json_extract(json_set('{}', '$.bit', cast(b'1010' as bit(4))), '$.bit'));
select json_extract(json_set('{}', '$.g', st_geomfromtext('POINT(1 2)')), '$.g') = convert(st_geomfromtext('POINT(1 2)'), json);
set @json_set_year = '2024';
prepare json_set_typed from 'select json_set(''{}'', ''$.year'', cast(? as year))';
execute json_set_typed using @json_set_year;
deallocate prepare json_set_typed;

drop table if exists users;
CREATE TABLE users (
    id INT PRIMARY KEY,
    info JSON
);

INSERT INTO users (id, info) VALUES (1, '{"name": "Alice", "age": 30}');
INSERT INTO users (id, info) VALUES (2, '{"name": "Bob", "age": 25}');

SELECT * FROM users;

UPDATE users SET info = JSON_SET(info, '$.age', 31) WHERE id = 1;
SELECT * FROM users;

UPDATE users
SET info = JSON_SET(info, '$.phone', '123-456-7890')
WHERE id = 2;
SELECT * FROM users;

UPDATE users
SET info = JSON_SET(info, '$.age', 32, '$.address', '123 Main St')
WHERE id = 1;
SELECT * FROM users;

drop table users;

drop table if exists users;
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
SET info = JSON_SET(info, '$.address.city', 'San Francisco')
WHERE id = 1;
SELECT * FROM users;

UPDATE users
SET info = JSON_SET(info, '$.address.state', 'CA')
WHERE id = 2;
SELECT * FROM users;

UPDATE users
SET info = JSON_SET(info, '$.skills[0]', 'JavaScript')
WHERE id = 3;
SELECT * FROM users;

UPDATE users
SET info = JSON_SET(info, '$.skills[2]', 'SQL')
WHERE id = 3;
SELECT * FROM users;

UPDATE users
SET info = JSON_SET(info, '$.age', 32, '$.address.state', 'NY', '$.skills[3]', 'k8s')
WHERE id = 1;
SELECT * FROM users;

drop table users;

drop table if exists employees;
CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    info JSON
);

drop table if exists projects;
CREATE TABLE projects (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    members JSON
);

INSERT INTO employees (info) VALUES
('{"name": "John Doe", "age": 30, "department": "Engineering", "skills": ["Java", "Python", "SQL"]}'),
('{"name": "Jane Smith", "age": 25, "department": "Marketing", "skills": ["Social Media", "SEO", "Content Writing"]}');

INSERT INTO projects (name, members) VALUES
('Project A', '[1, 2]'),
('Project B', '[1]');


SELECT * FROM employees;
SELECT * FROM projects;

UPDATE employees
SET info = JSON_SET(info, '$.skills[3]', 'JavaScript')
WHERE id = 1;
SELECT * FROM employees;

UPDATE projects
SET members = JSON_SET(members, '$[2]', 3)
WHERE id = 1;
SELECT * FROM projects;

UPDATE employees
SET info = JSON_SET(info, '$.department.manager', 'Alice Johnson')
WHERE id = 2;
SELECT * FROM employees;

drop table employees;
drop table projects;
