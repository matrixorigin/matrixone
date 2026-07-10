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
SELECT JSON_REMOVE('[1,2,3]', '$[0]') AS result;
SELECT JSON_REMOVE('[1,2,3]', '$[1]') AS result;
SELECT JSON_REMOVE('[1,2,3]', '$[2]') AS result;
SELECT JSON_REMOVE('[1,2,3]', '$[3]') AS result;
SELECT JSON_REMOVE('[{"a":true},{"b":false},{"c":null},{"a":null}]', '$[0].a', '$[2].c') AS result;
SELECT JSON_REMOVE('{"a":"foo","b":[true,{"c":123}]}', '$.b[1]') AS result;
SELECT JSON_REMOVE('{"a":"foo","b":[true,{"c":123,"d":456}]}', '$.b[1].c') AS result;
SELECT JSON_REMOVE('{"a":"foo","b":[true,{"c":123,"d":456}]}', '$.b[1].e') AS result;
SELECT JSON_REMOVE('{"a":{"b":1,"c":2}}', '$[0][0].a[0][0].b') AS result;
SELECT JSON_REMOVE('[{"a":[{"b":1,"c":2},123]},456]', '$[0][0].a[0][0].b') AS result;
SELECT JSON_REMOVE('123', '$.a') AS result;
SELECT JSON_REMOVE('null', '$[0]') AS result;
SELECT JSON_REMOVE('{"*":1,"a":2}', '$."*"') AS result;
SELECT JSON_REMOVE('{"last-updated":"x","a":1}', '$."last-updated"') AS result;
SELECT JSON_REMOVE(NULL, '$.a') AS result;
SELECT JSON_REMOVE('{"a":1}', NULL) AS result;
SELECT JSON_REMOVE('[1,{"a":true,"b":false,"c":null},5]', '$[1]', NULL) AS result;
SELECT JSON_REMOVE('{"a":1}', '$') AS result;
SELECT JSON_REMOVE('{"a":1}', '$.*') AS result;
SELECT JSON_REMOVE('{"a":1}', '$**.a') AS result;
SELECT JSON_REMOVE('[1,2,3]', '$[*]') AS result;
SELECT JSON_REMOVE('[1,2,3]', '$[2]', '$[*]') AS result;
SELECT JSON_REMOVE('[1,2,3]', '$[2]', '$**[2]') AS result;

DROP TABLE IF EXISTS json_remove_paths;
CREATE TABLE json_remove_paths (
    j JSON,
    p VARCHAR(20)
);

INSERT INTO json_remove_paths (j, p) VALUES
('{"a":1,"b":2,"c":3}', '$.a'),
('{"a":1,"b":2,"c":3}', '$.b'),
('{"a":1,"b":2,"c":3}', '$.c');

SELECT j, p, JSON_REMOVE(j, p) AS result FROM json_remove_paths ORDER BY p;
DROP TABLE json_remove_paths;

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
