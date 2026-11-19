-- @case
-- @desc: UNION normalizes fixed BINARY widths and preserves temporal text (#30723, #30722)
-- @label:bvt

DROP DATABASE IF EXISTS union_width_regression;
CREATE DATABASE union_width_regression;
USE union_width_regression;

CREATE TABLE binary_source (a BINARY(1), b BINARY(4));
INSERT INTO binary_source VALUES ('a', 'a');
SELECT HEX(d) AS bytes, LENGTH(d) AS width
FROM (SELECT a AS d FROM binary_source UNION SELECT b FROM binary_source) AS u;
SELECT HEX(d) AS bytes, LENGTH(d) AS width
FROM (SELECT b AS d FROM binary_source UNION SELECT a FROM binary_source) AS u;
SELECT HEX(d) AS bytes, LENGTH(d) AS width
FROM (SELECT a AS d FROM binary_source UNION ALL SELECT b FROM binary_source) AS u
ORDER BY bytes, width;
CREATE TABLE binary_saved AS
SELECT d FROM (SELECT a AS d FROM binary_source UNION SELECT b FROM binary_source) AS u;
SELECT COUNT(*) AS rows_saved, HEX(MIN(d)) AS bytes FROM binary_saved;
SELECT HEX(d) AS bytes, LENGTH(d) AS width
FROM (SELECT NULL AS d UNION ALL SELECT a FROM binary_source UNION ALL SELECT b FROM binary_source) AS u
WHERE d IS NOT NULL ORDER BY bytes, width;
SELECT COUNT(*) AS rows_saved
FROM (SELECT CAST('a' AS VARBINARY(1)) AS d UNION SELECT CAST('a' AS VARBINARY(4))) AS u;
SELECT COUNT(*) AS rows_saved
FROM (SELECT CAST('a' AS CHAR(1)) AS d UNION SELECT CAST('a' AS CHAR(4))) AS u;

SELECT HEX(d) AS bytes, LENGTH(d) AS width
FROM (SELECT CAST('hello-long' AS BINARY) AS d UNION ALL SELECT CAST('x' AS BINARY(1))) AS u
ORDER BY bytes;
SELECT HEX(d) AS bytes, LENGTH(d) AS width
FROM (SELECT CAST('x' AS BINARY(1)) AS d UNION ALL SELECT CAST('hello-long' AS BINARY)) AS u
ORDER BY bytes;
SELECT COUNT(*) AS distinct_rows
FROM (SELECT CAST('hello-long' AS BINARY) AS d UNION SELECT CAST('h' AS BINARY(1))) AS u;
CREATE TABLE binary_unbounded_saved AS
SELECT d FROM (SELECT CAST('hello-long' AS BINARY) AS d UNION ALL SELECT CAST('x' AS BINARY(1))) AS u;
SELECT HEX(d) AS bytes, LENGTH(d) AS width FROM binary_unbounded_saved ORDER BY bytes;

SELECT d, LENGTH(d) AS width
FROM (SELECT '2026-01-01' AS d UNION ALL SELECT CAST('2026-02-02 10:20:30' AS DATETIME)) AS u
ORDER BY d;
SELECT d, LENGTH(d) AS width
FROM (SELECT 'x' AS d UNION ALL SELECT CAST('2026-02-02 10:20:30.123456' AS DATETIME(6))) AS u
ORDER BY d;
SELECT d, LENGTH(d) AS width
FROM (SELECT CAST('2026-02-02 10:20:30' AS DATETIME) AS d UNION ALL SELECT '2026-01-01') AS u
ORDER BY d;
SELECT d, LENGTH(d) AS width
FROM (SELECT 'start' AS d UNION ALL SELECT CAST('2026-02-02 10:20:30' AS DATETIME) UNION ALL SELECT 'end') AS u
ORDER BY d;
CREATE TABLE datetime_saved (d VARCHAR(30));
INSERT INTO datetime_saved
SELECT d FROM (SELECT '2026-01-01' AS d UNION ALL SELECT CAST('2026-02-02 10:20:30' AS DATETIME)) AS u;
SELECT d, LENGTH(d) AS width FROM datetime_saved ORDER BY d;
CREATE VIEW datetime_view AS
SELECT '2026-01-01' AS d UNION ALL SELECT CAST('2026-02-02 10:20:30' AS DATETIME);
SELECT d, LENGTH(d) AS width FROM datetime_view ORDER BY d;
SELECT data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'union_width_regression' AND table_name = 'datetime_view' AND column_name = 'd';

DROP DATABASE union_width_regression;
