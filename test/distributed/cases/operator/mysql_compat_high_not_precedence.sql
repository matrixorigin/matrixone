-- @suite

-- @case
-- @desc: MySQL-compatible HIGH_NOT_PRECEDENCE changes NOT binding
-- @label:bvt

DROP DATABASE IF EXISTS mysql_compat_high_not_precedence;
CREATE DATABASE mysql_compat_high_not_precedence;
USE mysql_compat_high_not_precedence;

SET @old_sql_mode = @@session.sql_mode;

SET SESSION sql_mode = '';
SELECT
  NOT 1 BETWEEN 2 AND 3 AS default_not_between,
  NOT (1 BETWEEN 2 AND 3) AS parenthesized_not_between,
  (NOT 1) BETWEEN 2 AND 3 AS explicit_not_between,
  NOT 0 IN (0, 1) AS default_not_in,
  NOT (0 IN (0, 1)) AS parenthesized_not_in,
  (NOT 0) IN (0, 1) AS explicit_not_in;

SET SESSION sql_mode = 'HIGH_NOT_PRECEDENCE';
SELECT
  NOT 1 BETWEEN 2 AND 3 AS high_not_between,
  NOT (1 BETWEEN 2 AND 3) AS parenthesized_not_between,
  (NOT 1) BETWEEN 2 AND 3 AS explicit_not_between,
  NOT 0 IN (0, 1) AS high_not_in,
  NOT (0 IN (0, 1)) AS parenthesized_not_in,
  (NOT 0) IN (0, 1) AS explicit_not_in;

DROP TABLE IF EXISTS t_high_not_precedence;
CREATE TABLE t_high_not_precedence (
  id INT PRIMARY KEY,
  a INT
);
INSERT INTO t_high_not_precedence VALUES
  (1, 0),
  (2, 1),
  (3, 2),
  (4, 3),
  (5, 4);

SELECT id
FROM t_high_not_precedence
WHERE NOT a BETWEEN 2 AND 3
ORDER BY id;

SELECT id
FROM t_high_not_precedence
WHERE NOT a IN (0, 1)
ORDER BY id;

SET SESSION sql_mode = @old_sql_mode;
DROP DATABASE mysql_compat_high_not_precedence;
