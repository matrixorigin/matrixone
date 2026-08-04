-- @case
-- @desc:Prepared SUM and AVG infer a stable numeric parameter domain
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

PREPARE p_recursive FROM 'WITH RECURSIVE r(n) AS (SELECT ? UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT CAST(SUM(n) AS SIGNED) AS got FROM r';
SET @value = 1;
EXECUTE p_recursive USING @value;
DEALLOCATE PREPARE p_recursive;

DROP DATABASE prepared_numeric_aggregate;
