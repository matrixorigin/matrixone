-- Exact integer and temporal division (/) returns DECIMAL and honors div_precision_increment.

DROP TABLE IF EXISTS t_int_div;
CREATE TABLE t_int_div (
    i8 TINYINT,
    i16 SMALLINT,
    i32 INT,
    i64 BIGINT,
    u8 TINYINT UNSIGNED,
    u64 BIGINT UNSIGNED,
    b BOOL,
    bt BIT(8),
    d DATE,
    y YEAR,
    tm TIME(6),
    dt DATETIME(6),
    ts TIMESTAMP(6),
    n_null INT
);
INSERT INTO t_int_div VALUES (1, 2, 3, -9223372036854775808, 3, 18446744073709551615, TRUE, b'00000101', '2024-01-02', 2024, '12:34:56.789000', '2024-01-02 12:34:56.789000', '2024-01-02 12:34:56.789000', NULL);

-- Basic literal division
SELECT 1/3, -1/3, 2/3, 10/4;
SELECT 1/0, NULL/0, 1/NULL, NULL/NULL;

-- Column-level NULL operands stay in DECIMAL domain
SELECT i32/n_null, n_null/i32, n_null/0 FROM t_int_div;

-- Metadata check via CTAS
DROP TABLE IF EXISTS t_int_div_ctas;
CREATE TABLE t_int_div_ctas AS SELECT 1/3 AS c1, i32/3 AS c2 FROM t_int_div;
SHOW CREATE TABLE t_int_div_ctas;
SELECT c1, c2 FROM t_int_div_ctas;
DROP TABLE t_int_div_ctas;

-- Exact domain types and extreme values
SELECT i8/i16, i32/u8, b/3, bt/3, d/3, y/3, y/0 FROM t_int_div;
SELECT tm/3, dt/3, ts/3 FROM t_int_div;
SELECT i64 / -1, u64 / 1 FROM t_int_div;

-- div_precision_increment = 0, 4, 10, 30 (including temporal DECIMAL256 promotion)
SET div_precision_increment = 0;
SELECT 1/3, i32/3 FROM t_int_div;

SET div_precision_increment = 10;
SELECT 1/3, i32/3 FROM t_int_div;

SET div_precision_increment = 30;
SELECT 1/3, i32/3, d/3, y/3, tm/3, dt/3, ts/3 FROM t_int_div;

-- Prepared statement honors div_precision_increment and parameter types
SET div_precision_increment = 4;
PREPARE p_int_div FROM 'SELECT ? / 3 AS r';
SET @a = 1;
EXECUTE p_int_div USING @a;
SET div_precision_increment = 10;
EXECUTE p_int_div USING @a;
SET @f = 1.0e0;
EXECUTE p_int_div USING @f;
SET @s = '1';
EXECUTE p_int_div USING @s;
DEALLOCATE PREPARE p_int_div;

PREPARE p_div_right FROM 'SELECT 1 / ? AS r';
SET @d = 3;
EXECUTE p_div_right USING @d;
SET div_precision_increment = 4;
EXECUTE p_div_right USING @d;
DEALLOCATE PREPARE p_div_right;

-- Approximate / string / JSON / INTEGER_DIV controls
SELECT 1 / 3.0e0, 1 / '3', CAST(1 AS JSON) / 3, 1 DIV 3, -7 DIV 3;

-- Strict DML division by zero
SET @old_sql_mode = @@sql_mode;
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
DROP TABLE IF EXISTS t_div_zero_strict;
CREATE TABLE t_div_zero_strict (a DECIMAL(10, 4));
INSERT INTO t_div_zero_strict SELECT 1/0;
DROP TABLE t_div_zero_strict;
SET sql_mode = @old_sql_mode;

DROP TABLE t_int_div;
