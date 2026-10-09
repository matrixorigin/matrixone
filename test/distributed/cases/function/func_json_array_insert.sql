SELECT JSON_ARRAY_INSERT('{"a":[1,2]}', '$[0].a[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"a":[1,2]}', '$[last].a[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"o":{"a":[1,2]}}', '$.o[0].a[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT(CAST('{"a":[1,2]}' AS JSON), '$[0].a[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[{"a":[1,2]}]', '$[0].a[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"a":[1,2]}', '$.a[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1,2,3]}', '$.arr[1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[1,2]', '$[99]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[1,2,3]', '$[last]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[1,2,3]', '$[last-1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[1,2]', '$[last-5]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[]', '$[last]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1,2]}', '$.arr[1]', 9, '$.arr[2]', 8) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.missing[0]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"value":1}', '$.value[0]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr[0]', NULL) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr[0]', 9, '$.arr[0]', NULL) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr[0]', CAST('null' AS JSON)) AS result;
SELECT JSON_ARRAY_INSERT(NULL, '$[0]', 9) AS result;
SELECT JSON_ARRAY_INSERT('[1]', NULL, 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr[*]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr[0 to 1]', 9) AS result;
SELECT JSON_ARRAY_INSERT('{"arr":[1]}', '$.arr[0]', CAST('{"x":1}' AS JSON)) AS result;

DROP TABLE IF EXISTS json_array_insert_docs;
CREATE TABLE json_array_insert_docs (
    id INT PRIMARY KEY,
    doc JSON,
    value INT NULL
);
INSERT INTO json_array_insert_docs VALUES (1, '{"arr":[1,2]}', 9), (2, '{"arr":[1,2]}', NULL);
UPDATE json_array_insert_docs
SET doc = JSON_ARRAY_INSERT(doc, '$.arr[1]', 9)
WHERE id = 1;
SELECT id, doc FROM json_array_insert_docs ORDER BY id;
SELECT id, JSON_ARRAY_INSERT(doc, '$.arr[0]', value) AS inserted FROM json_array_insert_docs ORDER BY id;
DROP TABLE json_array_insert_docs;
