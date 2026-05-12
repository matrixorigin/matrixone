-- @suit
-- @case
-- @desc: insert / select for DECIMAL(M,D) columns with precision > 38 (Decimal256)
-- @label:bvt

DROP DATABASE IF EXISTS test_decimal256;
CREATE DATABASE test_decimal256;
USE test_decimal256;

CREATE TABLE t_decimal (
    id INT PRIMARY KEY,
    large_val DECIMAL(65, 10)
);

INSERT INTO t_decimal VALUES
(1, 12345678901234567890123456789012345678901234567890123.1234567890);

INSERT INTO t_decimal VALUES
(2, 99999999999999999999999999999999999999999999999999999.9999999999);

INSERT INTO t_decimal VALUES
(3, -12345678901234567890123456789012345678901234567890123.1234567890);

SELECT * FROM t_decimal ORDER BY id;

-- Exercise the operatorUnaryMinusDecimal256 overload in SELECT context
-- (the INSERT path only covers the planner side; this hits the executor).
SELECT id, -large_val AS neg FROM t_decimal ORDER BY id;

UPDATE t_decimal SET large_val = 11111111111111111111111111111111111111111111111111111.1111111111 WHERE id = 3;
SELECT id, large_val FROM t_decimal WHERE id = 3;

DELETE FROM t_decimal WHERE id = 2;
SELECT id FROM t_decimal ORDER BY id;

DROP DATABASE test_decimal256;
