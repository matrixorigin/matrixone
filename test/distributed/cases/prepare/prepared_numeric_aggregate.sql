-- @case
-- @desc:Prepared numeric functions infer stable parameter domains
-- @label:bvt

DROP DATABASE IF EXISTS prepared_numeric_aggregate;
CREATE DATABASE prepared_numeric_aggregate;
USE prepared_numeric_aggregate;

PREPARE p_sum FROM 'SELECT CAST(SUM(?) AS SIGNED) AS got';
SET @value = 2;
EXECUTE p_sum USING @value;
SET @value = '2';
EXECUTE p_sum USING @value;
SET @value = NULL;
EXECUTE p_sum USING @value;
SET @value = 'abc';
EXECUTE p_sum USING @value;
SET @value = 3;
EXECUTE p_sum USING @value;
DEALLOCATE PREPARE p_sum;

PREPARE p_avg FROM 'SELECT CAST(AVG(?) AS SIGNED) AS got';
SET @value = 4;
EXECUTE p_avg USING @value;
DEALLOCATE PREPARE p_avg;

PREPARE p_window FROM 'SELECT CAST(SUM(?) OVER () AS SIGNED) AS got';
SET @value = 5;
EXECUTE p_window USING @value;
DEALLOCATE PREPARE p_window;

CREATE TABLE ntile_input(id INT PRIMARY KEY, g INT);
INSERT INTO ntile_input VALUES (1, 1), (2, 1), (3, 1), (4, 2);
PREPARE p_ntile FROM 'SELECT id, NTILE(?) OVER (PARTITION BY g ORDER BY id) AS bucket FROM ntile_input ORDER BY id';
SET @value = 2;
EXECUTE p_ntile USING @value;
SET @value = NULL;
EXECUTE p_ntile USING @value;
DEALLOCATE PREPARE p_ntile;

PREPARE p_recursive FROM 'WITH RECURSIVE r(n) AS (SELECT ? UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT CAST(SUM(n) AS SIGNED) AS got FROM r';
SET @value = 1;
EXECUTE p_recursive USING @value;
DEALLOCATE PREPARE p_recursive;

CREATE TABLE count_input(id INT PRIMARY KEY);
INSERT INTO count_input SELECT result FROM generate_series(1, 10000) g;
PREPARE p_count_table FROM 'SELECT COUNT(?) AS got FROM count_input LIMIT 1';
SET @value = 'x';
EXECUTE p_count_table USING @value;
SET @value = NULL;
EXECUTE p_count_table USING @value;
DEALLOCATE PREPARE p_count_table;

DROP DATABASE prepared_numeric_aggregate;
