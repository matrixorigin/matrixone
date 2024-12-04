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
SET info = JSON_INSERT(info, '$.phone', '123-456-7890')
WHERE id = 1;
SELECT * FROM users;

UPDATE users
SET info = JSON_INSERT(info, '$.address.state', 'CA')
WHERE id = 2;
SELECT * FROM users;

UPDATE users
SET info = JSON_INSERT(info, '$.skills[2]', 'SQL')
WHERE id = 3;
SELECT * FROM users;

UPDATE users
SET info = JSON_INSERT(info, '$.phone', '123-456-7890', '$.address.state', 'NY')
WHERE id = 1;
SELECT * FROM users;

drop table users;
