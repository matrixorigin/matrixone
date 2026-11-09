-- DISTINCT does not change the value returned by MIN/MAX.
DROP DATABASE IF EXISTS distinct_minmax_values;
CREATE DATABASE distinct_minmax_values;
USE distinct_minmax_values;

CREATE TABLE values_by_group (g INT, c CHAR(8), v VARCHAR(8));
INSERT INTO values_by_group VALUES
    (1, NULL, 'a '), (1, NULL, 'z  '), (1, NULL, NULL),
    (2, 'q', NULL), (3, NULL, NULL), (4, NULL, '');

SET @saved_sql_mode = @@session.sql_mode;
SET SESSION sql_mode = '';
SELECT g,
       CONCAT('[', HEX(MIN(COALESCE(c, v))), ']') AS min_plain,
       CONCAT('[', HEX(MIN(DISTINCT COALESCE(c, v))), ']') AS min_distinct,
       CONCAT('[', HEX(MAX(COALESCE(c, v))), ']') AS max_plain,
       CONCAT('[', HEX(MAX(DISTINCT COALESCE(c, v))), ']') AS max_distinct
FROM values_by_group GROUP BY g ORDER BY g;
SELECT HEX(MIN(DISTINCT COALESCE(c, v))), HEX(MAX(DISTINCT COALESCE(c, v)))
FROM values_by_group WHERE g = -1;
SELECT HEX(MIN(DISTINCT v)), HEX(MAX(DISTINCT v)) FROM values_by_group WHERE g = 1;

PREPARE p FROM 'SELECT HEX(MIN(DISTINCT COALESCE(c, ?))), HEX(MAX(DISTINCT COALESCE(c, ?))) FROM values_by_group WHERE g = 1';
SET @arg = 'a ';
EXECUTE p USING @arg, @arg;
SET @arg = NULL;
EXECUTE p USING @arg, @arg;
DEALLOCATE PREPARE p;

SET SESSION sql_mode = @saved_sql_mode;
SELECT @@session.sql_mode = @saved_sql_mode AS sql_mode_restored;
DROP DATABASE distinct_minmax_values;
