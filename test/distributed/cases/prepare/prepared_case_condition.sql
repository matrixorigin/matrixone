-- @case
-- @desc: Prepared CASE WHEN parameters preserve boolean execution semantics.
-- @label:bvt

DROP DATABASE IF EXISTS prepared_case_condition;
CREATE DATABASE prepared_case_condition;
USE prepared_case_condition;

PREPARE case_condition FROM 'SELECT CASE WHEN ? THEN 7 ELSE -7 END AS got';

SET @condition = TRUE;
EXECUTE case_condition USING @condition;

SET @condition = FALSE;
EXECUTE case_condition USING @condition;

SET @condition = NULL;
EXECUTE case_condition USING @condition;

SELECT CASE WHEN TRUE THEN 7 ELSE -7 END AS got;

DEALLOCATE PREPARE case_condition;
DROP DATABASE prepared_case_condition;
