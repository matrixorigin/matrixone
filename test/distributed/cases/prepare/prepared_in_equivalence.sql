-- @case
-- @desc: Prepared IN preserves the complete information_schema typed bag
-- @label:bvt
-- Regression for #27503. The literal query is a nearby path control; the
-- independently fixed expected rows are the five objects created below.

DROP DATABASE IF EXISTS prepared_in_equivalence;
CREATE DATABASE prepared_in_equivalence;
USE prepared_in_equivalence;

CREATE TABLE t001 (id INT PRIMARY KEY);
CREATE TABLE t002 (id INT PRIMARY KEY);
CREATE TABLE t003 (id INT PRIMARY KEY);
CREATE TABLE t004 (id INT PRIMARY KEY);
CREATE TABLE t005 (id INT PRIMARY KEY);

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'prepared_in_equivalence'
  AND table_name IN ('t001', 't002', 't003', 't004', 't005')
ORDER BY table_name;

SET @schema_name = 'prepared_in_equivalence';
SET @n1 = 't001';
SET @n2 = 't002';
SET @n3 = 't003';
SET @n4 = 't004';
SET @n5 = 't005';

PREPARE prepared_names FROM
  'SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name IN (?,?,?,?,?) ORDER BY table_name';
EXECUTE prepared_names USING @schema_name, @n1, @n2, @n3, @n4, @n5;
DEALLOCATE PREPARE prepared_names;

DROP DATABASE prepared_in_equivalence;
