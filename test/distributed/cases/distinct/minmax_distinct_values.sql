-- MIN/MAX discard DISTINCT before evaluating their arguments. They must keep
-- the original value, including trailing spaces inherited through COALESCE.
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
SELECT HEX(MIN(COALESCE(c, v))), HEX(MIN(DISTINCT COALESCE(c, v))),
       HEX(MAX(COALESCE(c, v))), HEX(MAX(DISTINCT COALESCE(c, v)))
FROM values_by_group WHERE 1 = 0;
SELECT HEX(MIN(v)), HEX(MIN(DISTINCT v)), HEX(MAX(v)), HEX(MAX(DISTINCT v))
FROM values_by_group WHERE g = 1;
SELECT COUNT(DISTINCT COALESCE(c, v), g) FROM values_by_group;

-- CHAR padding is part of the returned representative when this mode is set.
SET SESSION sql_mode = 'PAD_CHAR_TO_FULL_LENGTH';
SELECT HEX(MIN(COALESCE(c, v))), HEX(MIN(DISTINCT COALESCE(c, v))),
       HEX(MAX(COALESCE(c, v))), HEX(MAX(DISTINCT COALESCE(c, v)))
FROM values_by_group WHERE g = 2;
SET SESSION sql_mode = '';

-- The parameter participates in the aggregate value and changes type on reuse.
PREPARE p FROM 'SELECT HEX(MIN(DISTINCT COALESCE(c, ?))), HEX(MAX(DISTINCT COALESCE(c, ?))) FROM values_by_group WHERE g = 1';
SET @arg = 'a ';
EXECUTE p USING @arg, @arg;
SET @arg = CAST('a ' AS BINARY);
EXECUTE p USING @arg, @arg;
SET @arg = NULL;
EXECUTE p USING @arg, @arg;
SET @arg = 10;
EXECUTE p USING @arg, @arg;
SET @arg = 'a ';
EXECUTE p USING @arg, @arg;
DEALLOCATE PREPARE p;

-- DISTINCT elimination must not add evaluations of a volatile argument.
CREATE SEQUENCE s;
SELECT HEX(MIN(DISTINCT COALESCE(c, IF(NEXTVAL('s') <= 3, 'a ', 'z '))))
FROM values_by_group WHERE g = 1;
SELECT last_seq_num FROM s;
DROP SEQUENCE s;

-- MIN(E) and MIN(DISTINCT E) are separate expression occurrences.
CREATE SEQUENCE s;
SELECT LENGTH(MIN(COALESCE(c, IF(NEXTVAL('s') <= 3, 'a ', 'z ')))) AS plain_length,
       LENGTH(MIN(DISTINCT COALESCE(c, IF(NEXTVAL('s') <= 3, 'a ', 'z ')))) AS distinct_length
FROM values_by_group WHERE g = 1;
SELECT last_seq_num FROM s;
DROP SEQUENCE s;

SET SESSION sql_mode = @saved_sql_mode;
DROP DATABASE distinct_minmax_values;
