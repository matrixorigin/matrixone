-- JSON_LENGTH function tests
SELECT JSON_LENGTH(NULL) AS result;
SELECT JSON_LENGTH('{}') AS result;
SELECT JSON_LENGTH('[]') AS result;
SELECT JSON_LENGTH('{"a":1,"b":2}') AS result;
SELECT JSON_LENGTH('[1,2,3]') AS result;
SELECT JSON_LENGTH('true') AS result;
SELECT JSON_LENGTH('false') AS result;
SELECT JSON_LENGTH('null') AS result;
SELECT JSON_LENGTH('"hello"') AS result;
SELECT JSON_LENGTH('42') AS result;
SELECT JSON_LENGTH('{"a":{"b":[1,2,3]}}', '$.a.b') AS result;
SELECT JSON_LENGTH('{"a":1}', '$.x') AS result;

-- Table with JSON column
DROP TABLE IF EXISTS t_json_len;
CREATE TABLE t_json_len (id INT PRIMARY KEY, payload JSON);
INSERT INTO t_json_len VALUES (1, '{"meta":{"level":{"nested":{"value":{"deep":"2924"}},"flag":true}}}');
SELECT id, JSON_LENGTH(payload) FROM t_json_len;
DROP TABLE t_json_len;
