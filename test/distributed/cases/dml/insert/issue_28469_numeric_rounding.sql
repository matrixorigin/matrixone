-- @suit
-- @case
-- @desc:issue #28469, preserve exact and approximate numeric rounding when INSERT assigns to integers
-- @label:bvt

DROP DATABASE IF EXISTS issue_28469;
CREATE DATABASE issue_28469;
USE issue_28469;
SET sql_mode = 'STRICT_TRANS_TABLES';

CREATE TABLE t_rounding (
    id INT PRIMARY KEY,
    value_int INT,
    value_bigint BIGINT
);

-- Exact numeric literals use half-away-from-zero; exponent literals are
-- approximate and use ties-to-even. Nearby non-ties guard against blanket tie handling.
INSERT INTO t_rounding VALUES
    (1, 2.5, 2.5),
    (2, -2.5, -2.5),
    (3, 3.5, 3.5),
    (4, -3.5, -3.5),
    (5, 2.5E0, 2.5E0),
    (6, -2.5E0, -2.5E0),
    (7, 3.5E0, 3.5E0),
    (8, -3.5E0, -3.5E0),
    (9, 2.49, 2.49),
    (10, -2.49, -2.49),
    (11, 2.51E0, 2.51E0),
    (12, -2.51E0, -2.51E0),
    (13, ((2.5)), ((9007199254740992.5000000000000001)));
SELECT * FROM t_rounding ORDER BY id;

-- String literals retain the existing strict string-to-integer assignment contract.
INSERT INTO t_rounding VALUES (14, '2.5', '-2.5');
SELECT COUNT(*) FROM t_rounding WHERE id = 14;

-- Exact numeric expressions may execute through FLOAT vectors, but retain
-- half-away-from-zero assignment semantics.
SET @exact_five = 5, @exact_two = 2;
INSERT INTO t_rounding VALUES (15, @exact_five / @exact_two, -@exact_five / @exact_two);
SELECT * FROM t_rounding WHERE id = 15;

-- Folding, projection and numeric wrappers retain the exact source domain.
CREATE TABLE src (x BIGINT);
INSERT INTO src VALUES (5);
CREATE TABLE dst (id INT PRIMARY KEY, v BIGINT);
INSERT INTO dst VALUES (1, 5 / 2);
INSERT INTO dst SELECT 2, x / 2 FROM src;
INSERT INTO dst SELECT 3, ABS(x / 2) FROM src;
INSERT INTO dst SELECT 4, -(x / 2) FROM src;
INSERT INTO dst SELECT 5, x / 2 + 0 FROM src;
INSERT INTO dst VALUES (6, 0);
UPDATE dst SET v = ABS(5 / 2) WHERE id = 6;
INSERT INTO dst SELECT 7, x / 2E0 FROM src;
INSERT INTO dst VALUES (8, ABS(5E0 / 2) + 0);
INSERT INTO dst SELECT 9, CAST(x / 2 AS DOUBLE) FROM src;
INSERT INTO dst SELECT 10, ABS(q) FROM (SELECT x / 2 AS q FROM src) s;
SELECT * FROM dst ORDER BY id;
CREATE TABLE quotient_source (result BIGINT);
INSERT INTO quotient_source VALUES (150), (250);
CREATE TABLE quotient_target (a BIGINT, b BIGINT, PRIMARY KEY(a,b));
INSERT INTO quotient_target SELECT result/100, result%100 FROM quotient_source;
SELECT * FROM quotient_target ORDER BY a,b;

-- ODKU consumes the already converted incoming VALUES row.
INSERT INTO t_rounding VALUES (20, 0, 0), (21, 0, 0);
INSERT INTO t_rounding VALUES (20, 2.5, -2.5), (21, 2.5E0, -2.5E0)
    ON DUPLICATE KEY UPDATE value_int = VALUES(value_int), value_bigint = VALUES(value_bigint);
SELECT * FROM t_rounding WHERE id IN (20, 21) ORDER BY id;

-- SQL PREPARE transports user variables as text but retains their logical numeric source type.
PREPARE insert_rounding FROM 'INSERT INTO t_rounding VALUES (?, ?, ?)';
SET @id = 30, @int_value = 2.5, @bigint_value = -2.5;
EXECUTE insert_rounding USING @id, @int_value, @bigint_value;
SET @id = 31, @int_value = CAST(2.5 AS DOUBLE), @bigint_value = CAST(-2.5 AS DOUBLE);
EXECUTE insert_rounding USING @id, @int_value, @bigint_value;
SET @id = 32, @int_value = CAST(3.5 AS DECIMAL(10, 1)), @bigint_value = CAST(-3.5 AS DECIMAL(10, 1));
EXECUTE insert_rounding USING @id, @int_value, @bigint_value;
SET @id = 33, @int_value = '2.5', @bigint_value = '-2.5';
EXECUTE insert_rounding USING @id, @int_value, @bigint_value;
SELECT * FROM t_rounding WHERE id BETWEEN 30 AND 33 ORDER BY id;
DEALLOCATE PREPARE insert_rounding;

-- Runtime specialization of nested expressions must leave the assignment cast
-- in charge of FLOAT-to-integer ties-to-even behavior.
CREATE TABLE t_nested_abs (value_bigint BIGINT);
CREATE TABLE t_nested_add (value_bigint BIGINT);
CREATE TABLE t_nested_explicit (id INT PRIMARY KEY, value_bigint BIGINT);
PREPARE insert_nested_abs FROM 'INSERT INTO t_nested_abs VALUES (ABS(?))';
PREPARE insert_nested_add FROM 'INSERT INTO t_nested_add VALUES (? + 0)';
PREPARE insert_nested_explicit FROM
    'INSERT INTO t_nested_explicit VALUES (?, ABS(CAST(? AS DOUBLE)))';
SET @nested_value = CAST(-2.5 AS DOUBLE);
EXECUTE insert_nested_abs USING @nested_value;
SET @nested_value = CAST(-2.5 AS DOUBLE);
EXECUTE insert_nested_add USING @nested_value;
SET @nested_id = 1, @nested_value = 2.5;
EXECUTE insert_nested_explicit USING @nested_id, @nested_value;
SELECT * FROM t_nested_abs;
SELECT * FROM t_nested_add;
SELECT * FROM t_nested_explicit;
DEALLOCATE PREPARE insert_nested_abs;
DEALLOCATE PREPARE insert_nested_add;
DEALLOCATE PREPARE insert_nested_explicit;

-- Negative approximate values must not wrap while assigning to unsigned
-- integers. Strict assignment rejects the row. Protocol-gated prepared IGNORE
-- and repeated COM_STMT execution are covered by the embedded integration test.
CREATE TABLE t_unsigned (id INT PRIMARY KEY, value_bigint BIGINT UNSIGNED);
INSERT INTO t_unsigned VALUES (1, -1E0);
SELECT COUNT(*) FROM t_unsigned WHERE id = 1;
PREPARE insert_unsigned FROM 'INSERT INTO t_unsigned VALUES (?, ?)';
SET @id = 4, @unsigned_value = CAST(-1 AS DOUBLE);
EXECUTE insert_unsigned USING @id, @unsigned_value;
SELECT COUNT(*) FROM t_unsigned WHERE id = 4;
DEALLOCATE PREPARE insert_unsigned;

DROP DATABASE issue_28469;
