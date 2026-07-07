SELECT JSON_REMOVE('{"a":1,"b":2}', '$.a') AS result;
SELECT JSON_REMOVE('["a", ["b", "c"], "d"]', '$[1]') AS result;
SELECT JSON_REMOVE('{"a":{"b":1,"c":2},"d":3}', '$.a.b') AS result;
SELECT JSON_REMOVE('{"a":1}', '$.missing') AS result;
SELECT JSON_REMOVE('{"a":1}', '$.a.b') AS result;
SELECT JSON_REMOVE('{"a":1,"b":2}', '$.a[0]') AS result;
SELECT JSON_REMOVE('{"a":1,"b":2}', '$[0].a') AS result;
SELECT JSON_REMOVE('1', '$[0]') AS result;
SELECT JSON_REMOVE('["a","b","c","d"]', '$[1]', '$[1]') AS result;
SELECT JSON_REMOVE('{"a":1,"b":2,"c":3}', '$.a', '$.c') AS result;
SELECT JSON_REMOVE(NULL, '$.a') AS result;
SELECT JSON_REMOVE('{"a":1}', NULL) AS result;
SELECT JSON_REMOVE('{"a":1}', '$') AS result;
SELECT JSON_REMOVE('{"a":1}', '$.*') AS result;
SELECT JSON_REMOVE('{"a":1}', '$**.a') AS result;

DROP TABLE IF EXISTS json_remove_users;
CREATE TABLE json_remove_users (
    id INT PRIMARY KEY,
    info JSON
);

INSERT INTO json_remove_users (id, info) VALUES
(1, '{"name":"Alice","age":30,"email":"alice@example.com","skills":["Go","SQL","Python"]}'),
(2, '{"name":"Bob","age":25,"address":{"city":"Beijing","zip":"100000"}}');

SELECT * FROM json_remove_users ORDER BY id;

UPDATE json_remove_users
SET info = JSON_REMOVE(info, '$.email')
WHERE id = 1;
SELECT * FROM json_remove_users ORDER BY id;

UPDATE json_remove_users
SET info = JSON_REMOVE(info, '$.skills[1]')
WHERE id = 1;
SELECT * FROM json_remove_users ORDER BY id;

UPDATE json_remove_users
SET info = JSON_REMOVE(info, '$.address.zip', '$.missing')
WHERE id = 2;
SELECT * FROM json_remove_users ORDER BY id;

DROP TABLE json_remove_users;
