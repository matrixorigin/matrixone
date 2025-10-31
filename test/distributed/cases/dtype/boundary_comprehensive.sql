-- @suite
-- @case
-- @desc: Comprehensive boundary and overflow tests for all data types
-- @label: bvt

-- ========================================
-- PART 1: Numeric Type Boundaries and Overflow
-- ========================================

-- Test BIGINT boundaries
DROP TABLE IF EXISTS t_bigint;
CREATE TABLE t_bigint (
    id INT PRIMARY KEY AUTO_INCREMENT,
    val_signed BIGINT,
    val_unsigned BIGINT UNSIGNED
);

INSERT INTO t_bigint (val_signed, val_unsigned) VALUES (-9223372036854775808, 0);
INSERT INTO t_bigint (val_signed, val_unsigned) VALUES (0, 9223372036854775807);
INSERT INTO t_bigint (val_signed, val_unsigned) VALUES (9223372036854775807, 18446744073709551615);

SELECT * FROM t_bigint ORDER BY id;

-- Test overflow (should error)
-- @bvt:issue
INSERT INTO t_bigint (val_signed) VALUES (9223372036854775808);
-- @bvt:issue
-- @bvt:issue
INSERT INTO t_bigint (val_signed) VALUES (-9223372036854775809);
-- @bvt:issue

-- Test arithmetic overflow detection
SELECT val_signed + 1 FROM t_bigint WHERE val_signed = 9223372036854775807;
SELECT val_signed - 1 FROM t_bigint WHERE val_signed = -9223372036854775808;
SELECT val_signed * 2 FROM t_bigint WHERE val_signed = 9223372036854775807;

DROP TABLE t_bigint;


-- Test INT boundaries
DROP TABLE IF EXISTS t_int;
CREATE TABLE t_int (
    id INT PRIMARY KEY AUTO_INCREMENT,
    val_signed INT,
    val_unsigned INT UNSIGNED
);

INSERT INTO t_int (val_signed, val_unsigned) VALUES (-2147483648, 0);
INSERT INTO t_int (val_signed, val_unsigned) VALUES (2147483647, 4294967295);

SELECT * FROM t_int ORDER BY id;

-- Test overflow
-- @bvt:issue
INSERT INTO t_int (val_signed) VALUES (2147483648);
-- @bvt:issue
-- @bvt:issue
INSERT INTO t_int (val_unsigned) VALUES (4294967296);
-- @bvt:issue

DROP TABLE t_int;


-- Test TINYINT and SMALLINT
DROP TABLE IF EXISTS t_small_int;
CREATE TABLE t_small_int (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tiny TINYINT,
    small SMALLINT
);

INSERT INTO t_small_int (tiny, small) VALUES (-128, -32768);
INSERT INTO t_small_int (tiny, small) VALUES (127, 32767);

SELECT * FROM t_small_int ORDER BY id;

DROP TABLE t_small_int;


-- Test SUM aggregate overflow
DROP TABLE IF EXISTS t_sum_overflow;
CREATE TABLE t_sum_overflow (val BIGINT);

INSERT INTO t_sum_overflow VALUES (9223372036854775807);
INSERT INTO t_sum_overflow VALUES (9223372036854775807);
INSERT INTO t_sum_overflow VALUES (9223372036854775807);

-- Should error due to overflow
SELECT SUM(val) FROM t_sum_overflow;

-- Should work with DECIMAL cast
SELECT SUM(CAST(val AS DECIMAL(38,0))) FROM t_sum_overflow;

DROP TABLE t_sum_overflow;


-- ========================================
-- PART 2: Floating Point and Decimal Precision
-- ========================================

-- Test FLOAT vs DOUBLE precision
DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (
    id INT PRIMARY KEY AUTO_INCREMENT,
    val_float FLOAT,
    val_double DOUBLE
);

INSERT INTO t_float (val_float, val_double) VALUES (1.23456789, 1.23456789);
INSERT INTO t_float (val_float, val_double) VALUES (123456789.123456789, 123456789.123456789);

SELECT * FROM t_float ORDER BY id;

DROP TABLE t_float;


-- Test DECIMAL precision
DROP TABLE IF EXISTS t_decimal;
CREATE TABLE t_decimal (
    id INT PRIMARY KEY AUTO_INCREMENT,
    dec_5_2 DECIMAL(5,2),
    dec_20_10 DECIMAL(20,10)
);

INSERT INTO t_decimal (dec_5_2, dec_20_10) VALUES (999.99, 9999999999.9999999999);
INSERT INTO t_decimal (dec_5_2, dec_20_10) VALUES (-999.99, -9999999999.9999999999);

SELECT * FROM t_decimal ORDER BY id;

-- Test overflow
-- @bvt:issue
INSERT INTO t_decimal (dec_5_2) VALUES (1000.00);
-- @bvt:issue

-- Test rounding
INSERT INTO t_decimal (dec_5_2) VALUES (123.456);
SELECT * FROM t_decimal WHERE id = 3;

DROP TABLE t_decimal;


-- Test floating point comparison issues
DROP TABLE IF EXISTS t_float_cmp;
CREATE TABLE t_float_cmp (
    id INT PRIMARY KEY,
    val1 FLOAT,
    val2 FLOAT
);

INSERT INTO t_float_cmp VALUES (1, 0.1, 0.1);
INSERT INTO t_float_cmp VALUES (2, 0.1 + 0.1 + 0.1, 0.3);

SELECT id, val1, val2, val1 = val2 FROM t_float_cmp ORDER BY id;

DROP TABLE t_float_cmp;


-- ========================================
-- PART 3: String Boundaries and Truncation
-- ========================================

-- Test VARCHAR length boundaries
DROP TABLE IF EXISTS t_varchar;
CREATE TABLE t_varchar (
    id INT PRIMARY KEY AUTO_INCREMENT,
    short_str VARCHAR(10),
    long_str VARCHAR(1000)
);

INSERT INTO t_varchar (short_str) VALUES ('1234567890');
INSERT INTO t_varchar (short_str) VALUES ('12345');

-- Test truncation (should fail)
-- @bvt:issue
INSERT INTO t_varchar (short_str) VALUES ('12345678901');
-- @bvt:issue

-- Test empty string vs NULL
INSERT INTO t_varchar (short_str) VALUES ('');
INSERT INTO t_varchar (short_str) VALUES (NULL);

SELECT id, short_str, LENGTH(short_str) AS len, short_str IS NULL AS is_null
FROM t_varchar ORDER BY id;

-- Test multi-byte characters
INSERT INTO t_varchar (short_str) VALUES ('中文测试');
INSERT INTO t_varchar (short_str) VALUES ('🎉🎊');

SELECT id, short_str, LENGTH(short_str) AS bytes, CHAR_LENGTH(short_str) AS chars 
FROM t_varchar WHERE id > 5 ORDER BY id;

DROP TABLE t_varchar;


-- Test CHAR vs VARCHAR behavior
DROP TABLE IF EXISTS t_char_varchar;
CREATE TABLE t_char_varchar (
    id INT PRIMARY KEY AUTO_INCREMENT,
    char_col CHAR(10),
    varchar_col VARCHAR(10)
);

INSERT INTO t_char_varchar (char_col, varchar_col) VALUES ('test', 'test');
INSERT INTO t_char_varchar (char_col, varchar_col) VALUES ('test   ', 'test   ');

SELECT id, 
       CONCAT('[', char_col, ']') AS char_brackets,
       CONCAT('[', varchar_col, ']') AS varchar_brackets,
       LENGTH(char_col) AS char_len,
       LENGTH(varchar_col) AS varchar_len
FROM t_char_varchar ORDER BY id;

DROP TABLE t_char_varchar;


-- ========================================
-- PART 4: Date and Time Boundaries
-- ========================================

-- Test DATE boundaries
DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (
    id INT PRIMARY KEY AUTO_INCREMENT,
    dt DATE
);

-- Valid range: 1000-01-01 to 9999-12-31
INSERT INTO t_date (dt) VALUES ('1000-01-01');
INSERT INTO t_date (dt) VALUES ('1970-01-01');
INSERT INTO t_date (dt) VALUES ('2038-01-19');
INSERT INTO t_date (dt) VALUES ('9999-12-31');

SELECT * FROM t_date ORDER BY dt;

-- Test out of range (year < 1000)
-- @bvt:issue#date_lower_bound
-- INSERT INTO t_date (dt) VALUES ('0999-12-31');
-- @bvt:issue

-- Test out of range (year > 9999)
-- @bvt:issue
INSERT INTO t_date (dt) VALUES ('10000-01-01');
-- @bvt:issue

-- Test invalid dates
-- @bvt:issue
INSERT INTO t_date (dt) VALUES ('2024-02-30');
-- @bvt:issue
-- @bvt:issue
INSERT INTO t_date (dt) VALUES ('2024-13-01');
-- @bvt:issue

-- Test leap year
INSERT INTO t_date (dt) VALUES ('2024-02-29');
-- @bvt:issue
INSERT INTO t_date (dt) VALUES ('2023-02-29');
-- @bvt:issue
-- @bvt:issue
INSERT INTO t_date (dt) VALUES ('1900-02-29');
-- @bvt:issue

SELECT * FROM t_date WHERE YEAR(dt) IN (2000, 2023, 2024) ORDER BY dt;

DROP TABLE t_date;


-- Test DATETIME with precision
DROP TABLE IF EXISTS t_datetime;
CREATE TABLE t_datetime (
    id INT PRIMARY KEY AUTO_INCREMENT,
    dt0 DATETIME(0),
    dt6 DATETIME(6)
);

INSERT INTO t_datetime (dt0, dt6) VALUES 
    ('2024-01-01 12:34:56', '2024-01-01 12:34:56.789012');
INSERT INTO t_datetime (dt0, dt6) VALUES 
    ('2024-01-01 23:59:59.999', '2024-01-01 23:59:59.999999');

SELECT * FROM t_datetime ORDER BY id;

DROP TABLE t_datetime;


-- Test TIMESTAMP range (1970 to 2038)
DROP TABLE IF EXISTS t_timestamp;
CREATE TABLE t_timestamp (
    id INT PRIMARY KEY AUTO_INCREMENT,
    ts TIMESTAMP
);

INSERT INTO t_timestamp (ts) VALUES ('1970-01-01 00:00:01');
INSERT INTO t_timestamp (ts) VALUES ('2038-01-19 03:14:07');

SELECT * FROM t_timestamp ORDER BY ts;

-- Test out of range
-- @bvt:issue
INSERT INTO t_timestamp (ts) VALUES ('1969-12-31 23:59:59');
-- @bvt:issue
-- @bvt:issue
INSERT INTO t_timestamp (ts) VALUES ('2038-01-19 03:14:08');
-- @bvt:issue

DROP TABLE t_timestamp;


-- Test TIME range
DROP TABLE IF EXISTS t_time;
CREATE TABLE t_time (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tm TIME
);

INSERT INTO t_time (tm) VALUES ('-838:59:59');
INSERT INTO t_time (tm) VALUES ('00:00:00');
INSERT INTO t_time (tm) VALUES ('838:59:59');

SELECT * FROM t_time ORDER BY id;

-- @bvt:issue
INSERT INTO t_time (tm) VALUES ('839:00:00');
-- @bvt:issue

DROP TABLE t_time;


-- ========================================
-- PART 5: NULL vs Empty String vs Zero
-- ========================================

-- Test NULL, empty, and zero distinctions
DROP TABLE IF EXISTS t_null_test;
CREATE TABLE t_null_test (
    id INT PRIMARY KEY,
    str_col VARCHAR(50),
    int_col INT
);

INSERT INTO t_null_test VALUES (1, NULL, NULL);
INSERT INTO t_null_test VALUES (2, '', 0);
INSERT INTO t_null_test VALUES (3, '0', 0);

-- Test IS NULL vs equality
SELECT id, 
       str_col,
       str_col IS NULL AS is_null,
       str_col = '' AS is_empty
FROM t_null_test ORDER BY id;

-- Test NULL in comparisons
SELECT * FROM t_null_test WHERE int_col IS NULL;
SELECT * FROM t_null_test WHERE int_col = NULL;

-- Test NULL-safe equal operator (not yet implemented)
-- @bvt:issue#null_safe_equal
-- SELECT id, int_col <=> NULL AS null_safe_equals FROM t_null_test ORDER BY id;
-- @bvt:issue

DROP TABLE t_null_test;


-- Test NULL in arithmetic (results in NULL)
DROP TABLE IF EXISTS t_null_arith;
CREATE TABLE t_null_arith (
    id INT PRIMARY KEY,
    val INT
);

INSERT INTO t_null_arith VALUES (1, NULL);
INSERT INTO t_null_arith VALUES (2, 10);

SELECT id, val, val + 1 AS plus_one, val * 2 AS times_two FROM t_null_arith ORDER BY id;

DROP TABLE t_null_arith;


-- Test NULL in aggregates
DROP TABLE IF EXISTS t_null_agg;
CREATE TABLE t_null_agg (
    id INT PRIMARY KEY,
    category VARCHAR(10),
    value INT
);

INSERT INTO t_null_agg VALUES (1, 'A', 10);
INSERT INTO t_null_agg VALUES (2, 'A', NULL);
INSERT INTO t_null_agg VALUES (3, 'A', 20);
INSERT INTO t_null_agg VALUES (4, 'B', NULL);
INSERT INTO t_null_agg VALUES (5, 'B', NULL);

SELECT category,
       COUNT(*) AS count_all,
       COUNT(value) AS count_value,
       SUM(value) AS sum_value,
       AVG(value) AS avg_value
FROM t_null_agg
GROUP BY category
ORDER BY category;

DROP TABLE t_null_agg;


-- Test NULL in JOIN
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (id INT PRIMARY KEY, val INT);
CREATE TABLE t_right (id INT PRIMARY KEY, val INT);

INSERT INTO t_left VALUES (1, NULL), (2, 0), (3, 10);
INSERT INTO t_right VALUES (1, NULL), (2, 0), (3, 10);

-- NULLs don't match in regular JOIN
SELECT l.id, l.val, r.id, r.val
FROM t_left l JOIN t_right r ON l.val = r.val
ORDER BY l.id;

-- NULL-safe equal (not yet implemented)
-- @bvt:issue#null_safe_equal
-- SELECT l.id, l.val, r.id, r.val
-- FROM t_left l JOIN t_right r ON l.val <=> r.val
-- ORDER BY l.id;
-- @bvt:issue

DROP TABLE t_left;
DROP TABLE t_right;


-- ========================================
-- PART 6: Division and Modulo Edge Cases
-- ========================================

-- Test division by zero
DROP TABLE IF EXISTS t_div;
CREATE TABLE t_div (
    id INT PRIMARY KEY,
    numerator INT,
    denominator INT
);

INSERT INTO t_div VALUES (1, 100, 0);
INSERT INTO t_div VALUES (2, 10, 3);

-- Division by zero (should error)
-- @bvt:issue
SELECT numerator / denominator FROM t_div WHERE id = 1;
-- @bvt:issue

-- Normal division (integer division in SQL)
SELECT numerator / denominator FROM t_div WHERE id = 2;

DROP TABLE t_div;


-- Test modulo by zero
DROP TABLE IF EXISTS t_mod;
CREATE TABLE t_mod (
    id INT PRIMARY KEY,
    val INT,
    divisor INT
);

INSERT INTO t_mod VALUES (1, 100, 0);
INSERT INTO t_mod VALUES (2, 10, 3);

-- Modulo by zero
SELECT val % divisor FROM t_mod WHERE id = 1;

-- Normal modulo
SELECT val % divisor FROM t_mod WHERE id = 2;

DROP TABLE t_mod;


-- ========================================
-- PART 7: Type Conversion and Casting
-- ========================================

-- Test conversion overflow
DROP TABLE IF EXISTS t_conv;
CREATE TABLE t_conv (
    id INT PRIMARY KEY,
    big_val BIGINT,
    str_val VARCHAR(50)
);

INSERT INTO t_conv VALUES (1, 9223372036854775807, '9223372036854775807');
INSERT INTO t_conv VALUES (2, 0, '99999999999999999999');

-- Cast to smaller types
SELECT CAST(big_val AS INT) FROM t_conv WHERE id = 1;
SELECT CAST(big_val AS SMALLINT) FROM t_conv WHERE id = 1;

-- String to number conversion
SELECT CAST(str_val AS BIGINT) FROM t_conv WHERE id = 1;
SELECT CAST(str_val AS BIGINT) FROM t_conv WHERE id = 2;

DROP TABLE t_conv;


-- ========================================
-- PART 8: Comparison Edge Cases
-- ========================================

-- Test comparisons at boundaries
DROP TABLE IF EXISTS t_cmp;
CREATE TABLE t_cmp (
    id INT PRIMARY KEY,
    val BIGINT
);

INSERT INTO t_cmp VALUES (1, 9223372036854775807);
INSERT INTO t_cmp VALUES (2, -9223372036854775808);
INSERT INTO t_cmp VALUES (3, 0);

SELECT * FROM t_cmp WHERE val > 9223372036854775806;
SELECT * FROM t_cmp WHERE val < -9223372036854775807;
SELECT * FROM t_cmp WHERE val BETWEEN -1 AND 1;
SELECT * FROM t_cmp WHERE val IN (9223372036854775807, -9223372036854775808, 0);

DROP TABLE t_cmp;


-- ========================================
-- PART 9: String Edge Cases
-- ========================================

-- Test empty string vs NULL
DROP TABLE IF EXISTS t_str_empty;
CREATE TABLE t_str_empty (
    id INT PRIMARY KEY,
    str VARCHAR(50)
);

INSERT INTO t_str_empty VALUES (1, NULL);
INSERT INTO t_str_empty VALUES (2, '');
INSERT INTO t_str_empty VALUES (3, ' ');

SELECT id, str, 
       str IS NULL AS is_null,
       str = '' AS is_empty,
       LENGTH(str) AS len
FROM t_str_empty ORDER BY id;

-- Test CONCAT with NULL
SELECT id, CONCAT('prefix-', str, '-suffix') FROM t_str_empty ORDER BY id;

DROP TABLE t_str_empty;


-- Test special characters
DROP TABLE IF EXISTS t_special;
CREATE TABLE t_special (
    id INT PRIMARY KEY AUTO_INCREMENT,
    str VARCHAR(100)
);

INSERT INTO t_special (str) VALUES ('Line1\nLine2');
INSERT INTO t_special (str) VALUES ('Quote''s');
INSERT INTO t_special (str) VALUES ('Back\\Slash');

SELECT id, str, LENGTH(str) FROM t_special ORDER BY id;

DROP TABLE t_special;


-- ========================================
-- PART 10: Date Arithmetic and Functions
-- ========================================

-- Test date arithmetic at boundaries
DROP TABLE IF EXISTS t_date_arith;
CREATE TABLE t_date_arith (
    id INT PRIMARY KEY,
    dt DATE
);

INSERT INTO t_date_arith VALUES (1, '9999-12-31');
INSERT INTO t_date_arith VALUES (2, '1000-01-01');
INSERT INTO t_date_arith VALUES (3, '2024-02-29');

SELECT dt, DATE_ADD(dt, INTERVAL 1 DAY) FROM t_date_arith WHERE id = 1;
SELECT dt, DATE_SUB(dt, INTERVAL 1 DAY) FROM t_date_arith WHERE id = 2;
SELECT dt, DATE_ADD(dt, INTERVAL 1 MONTH) FROM t_date_arith WHERE id = 3;

DROP TABLE t_date_arith;


-- Test DATEDIFF
DROP TABLE IF EXISTS t_diff;
CREATE TABLE t_diff (
    id INT PRIMARY KEY,
    start_dt DATE,
    end_dt DATE
);

INSERT INTO t_diff VALUES (1, '1000-01-01', '9999-12-31');
INSERT INTO t_diff VALUES (2, '2024-01-01', '2024-12-31');

SELECT id, DATEDIFF(end_dt, start_dt) AS days FROM t_diff ORDER BY id;

DROP TABLE t_diff;


-- ========================================
-- PART 11: Constraint Edge Cases
-- ========================================

-- Test UNIQUE with NULL (multiple NULLs allowed)
DROP TABLE IF EXISTS t_unique;
CREATE TABLE t_unique (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE
);

INSERT INTO t_unique (code) VALUES (NULL);
INSERT INTO t_unique (code) VALUES (NULL);
INSERT INTO t_unique (code) VALUES ('A');

-- @bvt:issue
INSERT INTO t_unique (code) VALUES ('A');
-- @bvt:issue

SELECT * FROM t_unique ORDER BY id;

DROP TABLE t_unique;


-- Test NOT NULL with empty string
DROP TABLE IF EXISTS t_not_null;
CREATE TABLE t_not_null (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL
);

INSERT INTO t_not_null (name) VALUES ('');
INSERT INTO t_not_null (name) VALUES ('test');

-- @bvt:issue
INSERT INTO t_not_null (name) VALUES (NULL);
-- @bvt:issue

SELECT * FROM t_not_null ORDER BY id;

DROP TABLE t_not_null;


-- ========================================
-- PART 12: AUTO_INCREMENT Edge Cases
-- ========================================

-- Note: ALTER TABLE AUTO_INCREMENT not yet supported
-- @bvt:issue#auto_increment
-- DROP TABLE IF EXISTS t_auto;
-- CREATE TABLE t_auto (
--     id TINYINT AUTO_INCREMENT PRIMARY KEY,
--     val INT
-- );
-- 
-- INSERT INTO t_auto (val) VALUES (1);
-- ALTER TABLE t_auto AUTO_INCREMENT = 127;
-- INSERT INTO t_auto (val) VALUES (2);
-- SELECT * FROM t_auto ORDER BY id;
-- DROP TABLE t_auto;
-- @bvt:issue


-- ========================================
-- PART 13: Mixed Type Operations
-- ========================================

-- Test mixed type arithmetic
DROP TABLE IF EXISTS t_mixed;
CREATE TABLE t_mixed (
    id INT PRIMARY KEY,
    tiny TINYINT,
    normal INT,
    big BIGINT
);

INSERT INTO t_mixed VALUES (1, 127, 2147483647, 9223372036854775807);

SELECT tiny + normal FROM t_mixed WHERE id = 1;
SELECT normal + big FROM t_mixed WHERE id = 1;

DROP TABLE t_mixed;


-- ========================================
-- PART 14: DECIMAL vs FLOAT Calculations
-- ========================================

-- Test monetary calculations
DROP TABLE IF EXISTS t_money;
CREATE TABLE t_money (
    id INT PRIMARY KEY,
    amount_dec DECIMAL(15,2),
    amount_float FLOAT,
    quantity INT
);

INSERT INTO t_money VALUES (1, 0.01, 0.01, 100);
INSERT INTO t_money VALUES (2, 9.99, 9.99, 1000);

SELECT id, 
       amount_dec * quantity AS total_dec, 
       amount_float * quantity AS total_float
FROM t_money ORDER BY id;

SELECT SUM(amount_dec * quantity) AS sum_dec,
       SUM(amount_float * quantity) AS sum_float
FROM t_money;

DROP TABLE t_money;


-- ========================================
-- PART 15: Rounding and Precision
-- ========================================

-- Test ROUND, FLOOR, CEIL
DROP TABLE IF EXISTS t_round;
CREATE TABLE t_round (
    id INT PRIMARY KEY,
    val DECIMAL(10,5)
);

INSERT INTO t_round VALUES (1, 123.45678);
INSERT INTO t_round VALUES (2, -123.45678);

SELECT id, val, 
       ROUND(val, 2) AS round_2,
       FLOOR(val) AS floor_val,
       CEIL(val) AS ceil_val
FROM t_round ORDER BY id;

DROP TABLE t_round;

