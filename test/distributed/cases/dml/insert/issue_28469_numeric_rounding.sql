-- @suit
-- @case
-- @desc:issue #28469, preserve exact and approximate numeric rounding when INSERT assigns to integers
-- @label:bvt

DROP DATABASE IF EXISTS issue_28469;
CREATE DATABASE issue_28469;
USE issue_28469;

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

DROP DATABASE issue_28469;
