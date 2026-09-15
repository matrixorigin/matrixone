-- @suit
-- @case
-- @desc:issue #28469, preserve exact and approximate numeric rounding when INSERT assigns to integers
-- @label:bvt

DROP DATABASE IF EXISTS issue_28469;
CREATE DATABASE issue_28469;
USE issue_28469;
SET sql_mode = 'STRICT_TRANS_TABLES';

-- Ordinary query metadata must not inherit integer-write binding.
SELECT 5/2 AS q;
CREATE TABLE select_contract AS SELECT 5/2 AS q;
SELECT q FROM select_contract;
PREPARE ordinary_division FROM 'SELECT ?/2 AS q';
SET @ordinary_value=5;
EXECUTE ordinary_division USING @ordinary_value;

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

-- Exact numeric expressions retain exact execution until the final
-- half-away-from-zero integer-assignment boundary.
SET @exact_five = 5, @exact_two = 2;
INSERT INTO t_rounding VALUES (15, @exact_five / @exact_two, -@exact_five / @exact_two);
SELECT * FROM t_rounding WHERE id = 15;

-- Folding, projection and numeric wrappers retain the exact source domain.
CREATE TABLE src (x BIGINT);
INSERT INTO src VALUES (5);
CREATE TABLE float_dst (v DOUBLE);
INSERT INTO float_dst SELECT x / 2 FROM src;
SELECT * FROM float_dst;
CREATE TABLE dst (id INT PRIMARY KEY, v BIGINT);
INSERT INTO dst SELECT 100, 1 + FLOOR(x/2) FROM src;
INSERT INTO dst SELECT 101, FLOOR(x/2) + 1 FROM src;
INSERT INTO dst SELECT 108, 10 DIV (x/2) FROM src;
INSERT INTO dst SELECT 109, 10 DIV FLOOR(x/2) FROM src;
INSERT INTO dst SELECT 114, CAST(10 AS UNSIGNED) DIV CAST(2 AS DECIMAL(65,0)) FROM src;
SELECT * FROM dst ORDER BY id;
DELETE FROM dst;
PREPARE div_root FROM 'INSERT INTO dst VALUES(110,10 DIV (?/2))';
SET @div_x=5E0;
EXECUTE div_root USING @div_x;
SELECT * FROM dst;
DELETE FROM dst;
SET @div_x='5';
EXECUTE div_root USING @div_x;
SELECT * FROM dst;
DELETE FROM dst;
DEALLOCATE PREPARE div_root;
PREPARE nested_div_root FROM 'INSERT INTO dst VALUES(112,ABS(10 DIV (?/2)))';
EXECUTE nested_div_root USING @div_x;
SELECT * FROM dst;
DELETE FROM dst;
DEALLOCATE PREPARE nested_div_root;

-- Planner-owned exact casts must not hide a constant zero divisor from
-- runtime strict-mode handling; the failed multi-row write is atomic.
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE strict_zero FROM 'INSERT INTO dst VALUES (102,10/0), (103,5/2)';
EXECUTE strict_zero;
SELECT COUNT(*) FROM dst;
DEALLOCATE PREPARE strict_zero;
PREPARE strict_nested_zero FROM 'INSERT INTO dst VALUES (104,10/(0/2)), (105,5/2)';
EXECUTE strict_nested_zero;
SELECT COUNT(*) FROM dst;
DEALLOCATE PREPARE strict_nested_zero;
PREPARE strict_floor_zero FROM 'INSERT INTO dst VALUES (106,10/FLOOR(1/2)), (107,5/2)';
EXECUTE strict_floor_zero;
SELECT COUNT(*) FROM dst;
DEALLOCATE PREPARE strict_floor_zero;
SET sql_mode = 'STRICT_TRANS_TABLES';
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
INSERT INTO dst SELECT 11, COALESCE(x / 2, 0) FROM src;
INSERT INTO dst SELECT 12, IF(TRUE, x / 2, 0) FROM src;
INSERT INTO dst SELECT 13, IFNULL(x / 2, 0) FROM src;
INSERT INTO dst SELECT 14, NULLIF(x / 2, 0) FROM src;
INSERT INTO dst SELECT 15, CASE WHEN TRUE THEN x / 2 ELSE 0 END FROM src;
INSERT INTO dst SELECT 16, ROUND(x / 2, 1) FROM src;
INSERT INTO dst SELECT 17, TRUNCATE(x / 2, 1) FROM src;
INSERT INTO dst SELECT 18, GREATEST(x / 2, 0) FROM src;
INSERT INTO dst SELECT 19, LEAST(x / 2, 3) FROM src;
-- Shared producers keep one physical type for every consumer.
CREATE TABLE shared_dst (i BIGINT, f DOUBLE);
INSERT INTO shared_dst SELECT q, q+0E0 FROM (SELECT x/2 AS q FROM src) s;
SELECT * FROM shared_dst;
-- Runtime source-domain changes rebind the shared window and all consumers.
DELETE FROM shared_dst;
PREPARE shared_division FROM 'INSERT INTO shared_dst SELECT q,q+0E0 FROM (SELECT SUM(?/2) OVER () AS q) s WHERE q>2.4';
SET @shared_value=5;
EXECUTE shared_division USING @shared_value;
SET @shared_value=5E0;
EXECUTE shared_division USING @shared_value;
SET @shared_value=5;
EXECUTE shared_division USING @shared_value;
SELECT * FROM shared_dst ORDER BY i,f;
DEALLOCATE PREPARE shared_division;
CREATE TABLE relational_dst (v BIGINT);
INSERT INTO relational_dst SELECT q FROM (SELECT x/2 AS q FROM src) s WHERE q>2.6;
SELECT COUNT(*) FROM relational_dst;
INSERT INTO relational_dst SELECT SUM(x/2) FROM src;
INSERT INTO relational_dst SELECT MIN(x/2) FROM src;
INSERT INTO relational_dst SELECT MAX(x/2) FROM src;
INSERT INTO relational_dst SELECT AVG(x/2) FROM src;
INSERT INTO relational_dst SELECT SUM(x/2) OVER () FROM src;
INSERT INTO relational_dst SELECT q FROM (SELECT x/2 AS q FROM src GROUP BY x/2) s;
SELECT * FROM relational_dst ORDER BY v;
DELETE FROM relational_dst;
PREPARE exact_division FROM 'INSERT INTO relational_dst VALUES (?/2)';
SET @division_value=5;
EXECUTE exact_division USING @division_value;
SET @division_value=CAST(5 AS DOUBLE);
EXECUTE exact_division USING @division_value;
SET @division_value=7;
EXECUTE exact_division USING @division_value;
SELECT * FROM relational_dst ORDER BY v;
DEALLOCATE PREPARE exact_division;
DELETE FROM relational_dst;
SET sql_mode = 'STRICT_TRANS_TABLES,NO_UNSIGNED_SUBTRACTION';
PREPARE unsigned_subtraction FROM
    'INSERT INTO relational_dst SELECT q FROM (SELECT ?/2 q) s WHERE ?-CAST(2 AS UNSIGNED)<0';
SET @unsigned_x=5,@unsigned_y=1;
EXECUTE unsigned_subtraction USING @unsigned_x,@unsigned_y;
SELECT * FROM relational_dst;
DEALLOCATE PREPARE unsigned_subtraction;
SET sql_mode = 'STRICT_TRANS_TABLES';
CREATE TABLE large_src (x BIGINT);
INSERT INTO large_src VALUES (9007199254740993);
INSERT INTO dst SELECT 20, x / 2 FROM large_src;
DELETE FROM large_src;
INSERT INTO large_src VALUES (9223372036854775807);
INSERT INTO dst SELECT 21, COALESCE(x / 1, 0) FROM large_src;
INSERT INTO dst VALUES (22, 9007199254740993 / 2);
INSERT INTO dst VALUES (23, 9223372036854775807 / 1);
-- A wide scaled intermediate must not reject a representable quotient.
INSERT INTO dst SELECT 24, x / CAST(1 AS DECIMAL(38,37)) FROM large_src;
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
SET sql_mode = '';
INSERT INTO t_unsigned VALUES (2, -1E0);
INSERT INTO t_unsigned VALUES (6, -5/2);
SET @id = 5, @unsigned_value = CAST(-1 AS DOUBLE);
EXECUTE insert_unsigned USING @id, @unsigned_value;
SELECT * FROM t_unsigned ORDER BY id;
DEALLOCATE PREPARE insert_unsigned;

SELECT 5/2 AS q;
SELECT q FROM select_contract;
EXECUTE ordinary_division USING @ordinary_value;
DEALLOCATE PREPARE ordinary_division;
DROP DATABASE issue_28469;
