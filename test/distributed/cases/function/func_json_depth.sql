-- JSON_DEPTH function tests
SELECT JSON_DEPTH(NULL) AS result;
SELECT JSON_DEPTH('null') AS result;
SELECT JSON_DEPTH('true') AS result;
SELECT JSON_DEPTH('42') AS result;
SELECT JSON_DEPTH('"hello"') AS result;
SELECT JSON_DEPTH('[]') AS result;
SELECT JSON_DEPTH('{}') AS result;
SELECT JSON_DEPTH('{"a":[1]}') AS result;
SELECT JSON_DEPTH('[{},[1,[2]]]') AS result;
SELECT JSON_DEPTH('[null,{"值":[false,"文字"]}]') AS result;
SELECT JSON_DEPTH(CAST('{"a":[1]}' AS JSON)) AS result;

DROP TABLE IF EXISTS t_json_depth;
CREATE TABLE t_json_depth (id INT PRIMARY KEY, doc VARCHAR(1024));
INSERT INTO t_json_depth VALUES (0, '{"a":[1]}'), (1, 'not-json'), (2, '{"a":{"b":1}}');
SELECT id, JSON_DEPTH(doc) FROM t_json_depth ORDER BY id;
DROP TABLE t_json_depth;

-- Invalid document and rejected source domains.
SELECT JSON_DEPTH('not-json') AS result;
SELECT JSON_DEPTH(1) AS result;
SELECT JSON_DEPTH(CAST('1' AS BINARY)) AS result;

-- 100 container levels are accepted (SQL depth 101); 101 are rejected.
SELECT JSON_DEPTH(CONCAT(REPEAT('{"a":', 100), '1', REPEAT('}', 100))) AS result;
SELECT JSON_DEPTH(CONCAT(REPEAT('{"a":', 101), '1', REPEAT('}', 101))) AS result;
