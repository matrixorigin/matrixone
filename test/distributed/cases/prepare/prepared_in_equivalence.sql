-- @case
-- @desc: Prepared IN inside nested OR preserves all PK rows across executions
-- @label:bvt
-- Regression for #27503's underlying PK-filter failure, using a user table so
-- the detector does not depend on information_schema planner casts. IN and the
-- range arm cannot collapse into one atomic filter; the outer upper bound then
-- exercises the disjunct-container merge that previously dropped valid rows.

DROP DATABASE IF EXISTS prepared_in_equivalence;
CREATE DATABASE prepared_in_equivalence;
USE prepared_in_equivalence;

CREATE TABLE v (a VARCHAR(20) PRIMARY KEY);
INSERT INTO v VALUES (''), ('a'), ('b'), ('c'), ('z'), ('zz');

PREPARE prepared_nested_or FROM
  'SELECT a FROM v WHERE a <= ? AND (a IN (?,?,?) OR a > ?) ORDER BY a';

SET @upper = 'z';
SET @in1 = 'a';
SET @in2 = 'c';
SET @in3 = 'zz';
SET @lower = 'm';
EXECUTE prepared_nested_or USING @upper, @in1, @in2, @in3, @lower;

SET @upper = 'zz';
SET @in1 = 'b';
SET @in2 = 'zz';
SET @in3 = 'missing';
SET @lower = 'z';
EXECUTE prepared_nested_or USING @upper, @in1, @in2, @in3, @lower;

DEALLOCATE PREPARE prepared_nested_or;
DROP DATABASE prepared_in_equivalence;
