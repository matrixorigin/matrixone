SELECT JSON_ARRAY_APPEND('{"arr":[1,2]}', '$.arr', 3) AS result;
SELECT JSON_ARRAY_APPEND('["a",["b","c"],"d"]', '$[0]', 2) AS result;
SELECT JSON_ARRAY_APPEND('["a",["b","c"],"d"]', '$[1][0]', 3) AS result;
SELECT JSON_ARRAY_APPEND('{"a":1,"b":[2,3],"c":4}', '$.c', 'y') AS result;
SELECT JSON_ARRAY_APPEND('{"a":1}', '$', 'z') AS result;
SELECT JSON_ARRAY_APPEND('{"a":[1]}', '$.missing', 2) AS result;
SELECT JSON_ARRAY_APPEND('{"a":[]}', '$.a', 1, '$.a', 2) AS result;
SELECT JSON_ARRAY_APPEND('{"a":[]}', '$.a', CAST('{"x":1}' AS JSON)) AS result;
SELECT JSON_ARRAY_APPEND(NULL, '$.a', 1) AS result;
SELECT JSON_ARRAY_APPEND('{"a":[]}', NULL, 1) AS result;
SELECT JSON_ARRAY_APPEND('{"a":[]}', '$.a', NULL) AS result;
SELECT JSON_ARRAY_APPEND('{"a":[]}', '$.*', 1) AS result;

DROP TABLE IF EXISTS json_array_append_docs;
CREATE TABLE json_array_append_docs (
    id INT PRIMARY KEY,
    doc JSON
);
INSERT INTO json_array_append_docs VALUES (1, '{"arr":[1,2]}');
UPDATE json_array_append_docs
SET doc = JSON_ARRAY_APPEND(doc, '$.arr', 3)
WHERE id = 1;
SELECT * FROM json_array_append_docs;
DROP TABLE json_array_append_docs;
