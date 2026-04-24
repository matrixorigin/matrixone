# Issue #23006, #23007: MySQL JSON -> and ->> operators
# -> equivalent to JSON_EXTRACT; ->> equivalent to JSON_UNQUOTE(JSON_EXTRACT(...))

SELECT j -> '$.key' AS result1 FROM (SELECT JSON_OBJECT('key', 'value') AS j) t;
SELECT j -> '$.b' AS result2 FROM (SELECT JSON_OBJECT('a', 1, 'b', 2) AS j) t;
SELECT j ->> '$.key' AS result3 FROM (SELECT JSON_OBJECT('key', 'value') AS j) t;
SELECT j ->> '$.b' AS result4 FROM (SELECT JSON_OBJECT('a', 1, 'b', 2) AS j) t;

SELECT j -> '$.c.d' AS nested FROM (SELECT JSON_OBJECT('a', 1, 'b', 2, 'c', JSON_OBJECT('d', 4)) AS j) t;
SELECT j ->> '$.c.d' AS nested_unquote FROM (SELECT JSON_OBJECT('a', 1, 'b', 2, 'c', JSON_OBJECT('d', 4)) AS j) t;

SELECT a -> '$[1]' AS elem1 FROM (SELECT JSON_ARRAY(10, 20, 30, 40) AS a) t;
SELECT a ->> '$[1]' AS elem1_unquote FROM (SELECT JSON_ARRAY(10, 20, 30, 40) AS a) t;

CREATE TABLE t_arr (id INT, a JSON);
INSERT INTO t_arr VALUES (1, '[3,10,5,17,44]');
INSERT INTO t_arr VALUES (2, '[3,10,5,17,[22,44,66]]');
INSERT INTO t_arr VALUES (3, '[1,2,3]');
SELECT a -> '$[4]' AS elem4 FROM t_arr WHERE id = 1;
SELECT a -> '$[4]' AS elem4_nested FROM t_arr WHERE id = 2;
SELECT a -> '$[4][1]' AS nested_idx FROM t_arr WHERE id = 2;
SELECT a -> '$[4][1]' AS missing_nested FROM t_arr WHERE id = 1;

SELECT j -> '$.b' AS missing_key FROM (SELECT JSON_OBJECT('a', 1) AS j) t;
SELECT a -> '$[10]' AS out_of_range FROM t_arr WHERE id = 3;

DROP TABLE t_arr;

CREATE TABLE jemp (c JSON, g INT);
INSERT INTO jemp VALUES ('{"id": "2", "name": "Wilma"}', 2);
INSERT INTO jemp VALUES ('{"id": "3", "name": "Barney"}', 3);
INSERT INTO jemp VALUES ('{"id": "4", "name": "Betty"}', 4);

SELECT c -> '$.id' AS id_json, c ->> '$.name' AS name_text FROM jemp ORDER BY g;
SELECT c, c -> '$.id', g FROM jemp WHERE (c -> '$.id') > 1 ORDER BY (c -> '$.name');
SELECT c ->> '$.name' AS name FROM jemp WHERE g > 2 ORDER BY c ->> '$.name';

DROP TABLE jemp;
