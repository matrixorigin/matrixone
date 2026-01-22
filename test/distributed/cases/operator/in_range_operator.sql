-- @suit

-- @case
-- @desc:test for IN_RANGE operator
-- @label:bvt

-- Test with bool
SELECT IN_RANGE(TRUE, FALSE, TRUE, 0);
SELECT IN_RANGE(FALSE, TRUE, FALSE, 1);
SELECT IN_RANGE(TRUE, FALSE, TRUE, 1);
SELECT IN_RANGE(TRUE, FALSE, TRUE, 2);
SELECT IN_RANGE(TRUE, FALSE, TRUE, 3);

-- Test bool with NULL as first parameter
SELECT IN_RANGE(NULL, FALSE, TRUE, 0);
SELECT IN_RANGE(NULL, FALSE, TRUE, 1);
SELECT IN_RANGE(NULL, FALSE, TRUE, 2);
SELECT IN_RANGE(NULL, FALSE, TRUE, 3);

-- Test with tinyint
SELECT IN_RANGE(5, 1, 10, 0);
SELECT IN_RANGE(15, 1, 10, 0);
SELECT IN_RANGE(1, 1, 10, 1);
SELECT IN_RANGE(10, 1, 10, 2);
SELECT IN_RANGE(5, 1, 10, 3);

-- Test tinyint with NULL as first parameter
SELECT IN_RANGE(NULL, 1, 10, 0);
SELECT IN_RANGE(NULL, 1, 10, 1);
SELECT IN_RANGE(NULL, 1, 10, 2);
SELECT IN_RANGE(NULL, 1, 10, 3);

-- Test with unsigned tinyint
SELECT IN_RANGE(5, 1, 10, 0);
SELECT IN_RANGE(15, 1, 10, 0);
SELECT IN_RANGE(1, 1, 10, 1);
SELECT IN_RANGE(10, 1, 10, 2);
SELECT IN_RANGE(5, 1, 10, 3);

-- Test unsigned tinyint with NULL as first parameter
SELECT IN_RANGE(NULL, 1, 10, 0);
SELECT IN_RANGE(NULL, 1, 10, 1);
SELECT IN_RANGE(NULL, 1, 10, 2);
SELECT IN_RANGE(NULL, 1, 10, 3);

-- Test with smallint
SELECT IN_RANGE(50, 10, 100, 0);
SELECT IN_RANGE(150, 10, 100, 0);
SELECT IN_RANGE(10, 10, 100, 1);
SELECT IN_RANGE(100, 10, 100, 2);
SELECT IN_RANGE(50, 10, 100, 3);

-- Test smallint with NULL as first parameter
SELECT IN_RANGE(NULL, 10, 100, 0);
SELECT IN_RANGE(NULL, 10, 100, 1);
SELECT IN_RANGE(NULL, 10, 100, 2);
SELECT IN_RANGE(NULL, 10, 100, 3);

-- Test with unsigned smallint
SELECT IN_RANGE(50, 10, 100, 0);
SELECT IN_RANGE(150, 10, 100, 0);
SELECT IN_RANGE(10, 10, 100, 1);
SELECT IN_RANGE(100, 10, 100, 2);
SELECT IN_RANGE(50, 10, 100, 3);

-- Test unsigned smallint with NULL as first parameter
SELECT IN_RANGE(NULL, 10, 100, 0);
SELECT IN_RANGE(NULL, 10, 100, 1);
SELECT IN_RANGE(NULL, 10, 100, 2);
SELECT IN_RANGE(NULL, 10, 100, 3);

-- Test with int
SELECT IN_RANGE(5, 1, 10, 0);
SELECT IN_RANGE(15, 1, 10, 0);
SELECT IN_RANGE(1, 1, 10, 1);
SELECT IN_RANGE(10, 1, 10, 2);
SELECT IN_RANGE(5, 1, 10, 3);

-- Test int with NULL as first parameter
SELECT IN_RANGE(NULL, 1, 10, 0);
SELECT IN_RANGE(NULL, 1, 10, 1);
SELECT IN_RANGE(NULL, 1, 10, 2);
SELECT IN_RANGE(NULL, 1, 10, 3);

-- Test with unsigned int
SELECT IN_RANGE(5, 1, 10, 0);
SELECT IN_RANGE(15, 1, 10, 0);
SELECT IN_RANGE(1, 1, 10, 1);
SELECT IN_RANGE(10, 1, 10, 2);
SELECT IN_RANGE(5, 1, 10, 3);

-- Test unsigned int with NULL as first parameter
SELECT IN_RANGE(NULL, 1, 10, 0);
SELECT IN_RANGE(NULL, 1, 10, 1);
SELECT IN_RANGE(NULL, 1, 10, 2);
SELECT IN_RANGE(NULL, 1, 10, 3);

-- Test with bigint
SELECT IN_RANGE(500, 100, 1000, 0);
SELECT IN_RANGE(1500, 100, 1000, 0);
SELECT IN_RANGE(100, 100, 1000, 1);
SELECT IN_RANGE(1000, 100, 1000, 2);
SELECT IN_RANGE(500, 100, 1000, 3);

-- Test bigint with NULL as first parameter
SELECT IN_RANGE(NULL, 100, 1000, 0);
SELECT IN_RANGE(NULL, 100, 1000, 1);
SELECT IN_RANGE(NULL, 100, 1000, 2);
SELECT IN_RANGE(NULL, 100, 1000, 3);

-- Test with unsigned bigint
SELECT IN_RANGE(500, 100, 1000, 0);
SELECT IN_RANGE(1500, 100, 1000, 0);
SELECT IN_RANGE(100, 100, 1000, 1);
SELECT IN_RANGE(1000, 100, 1000, 2);
SELECT IN_RANGE(500, 100, 1000, 3);

-- Test unsigned bigint with NULL as first parameter
SELECT IN_RANGE(NULL, 100, 1000, 0);
SELECT IN_RANGE(NULL, 100, 1000, 1);
SELECT IN_RANGE(NULL, 100, 1000, 2);
SELECT IN_RANGE(NULL, 100, 1000, 3);

-- Test with float
SELECT IN_RANGE(1.5, 1.0, 2.0, 0);
SELECT IN_RANGE(2.5, 1.0, 2.0, 0);
SELECT IN_RANGE(1.0, 1.0, 2.0, 1);
SELECT IN_RANGE(2.0, 1.0, 2.0, 2);
SELECT IN_RANGE(1.5, 1.0, 2.0, 3);

-- Test float with NULL as first parameter
SELECT IN_RANGE(NULL, 1.0, 2.0, 0);
SELECT IN_RANGE(NULL, 1.0, 2.0, 1);
SELECT IN_RANGE(NULL, 1.0, 2.0, 2);
SELECT IN_RANGE(NULL, 1.0, 2.0, 3);

-- Test with decimal
SELECT IN_RANGE(10.50, 0.0, 15.0, 0);
SELECT IN_RANGE(20.50, 0.0, 15.0, 0);
SELECT IN_RANGE(0.0, 0.0, 15.0, 1);
SELECT IN_RANGE(15.0, 0.0, 15.0, 2);
SELECT IN_RANGE(10.50, 0.0, 15.0, 3);

-- Test decimal with NULL as first parameter
SELECT IN_RANGE(NULL, 0.0, 15.0, 0);
SELECT IN_RANGE(NULL, 0.0, 15.0, 1);
SELECT IN_RANGE(NULL, 0.0, 15.0, 2);
SELECT IN_RANGE(NULL, 0.0, 15.0, 3);

-- Test with date
SELECT IN_RANGE('2022-06-15', '2022-01-01', '2022-12-31', 0);
SELECT IN_RANGE('2021-12-31', '2022-01-01', '2022-12-31', 0);
SELECT IN_RANGE('2022-01-01', '2022-01-01', '2022-12-31', 1);
SELECT IN_RANGE('2022-12-31', '2022-01-01', '2022-12-31', 2);
SELECT IN_RANGE('2022-06-15', '2022-01-01', '2022-12-31', 3);

-- Test date with NULL as first parameter
SELECT IN_RANGE(NULL, '2022-01-01', '2022-12-31', 0);
SELECT IN_RANGE(NULL, '2022-01-01', '2022-12-31', 1);
SELECT IN_RANGE(NULL, '2022-01-01', '2022-12-31', 2);
SELECT IN_RANGE(NULL, '2022-01-01', '2022-12-31', 3);

-- Test with time
SELECT IN_RANGE('12:00:00', '10:00:00', '14:00:00', 0);
SELECT IN_RANGE('15:00:00', '10:00:00', '14:00:00', 0);
SELECT IN_RANGE('10:00:00', '10:00:00', '14:00:00', 1);
SELECT IN_RANGE('14:00:00', '10:00:00', '14:00:00', 2);
SELECT IN_RANGE('12:00:00', '10:00:00', '14:00:00', 3);

-- Test time with NULL as first parameter
SELECT IN_RANGE(NULL, '10:00:00', '14:00:00', 0);
SELECT IN_RANGE(NULL, '10:00:00', '14:00:00', 1);
SELECT IN_RANGE(NULL, '10:00:00', '14:00:00', 2);
SELECT IN_RANGE(NULL, '10:00:00', '14:00:00', 3);

-- Test with datetime
SELECT IN_RANGE('2022-06-15 12:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE('2023-01-01 00:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE('2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT IN_RANGE('2022-12-31 23:59:59', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT IN_RANGE('2022-06-15 12:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);

-- Test datetime with NULL as first parameter
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);

-- Test with timestamp
SELECT IN_RANGE('2022-06-15 12:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE('2023-01-01 00:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE('2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT IN_RANGE('2022-12-31 23:59:59', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT IN_RANGE('2022-06-15 12:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);

-- Test timestamp with NULL as first parameter
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT IN_RANGE(NULL, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);

-- Test with varchar (case sensitive)
SELECT IN_RANGE('hello', 'a', 'z', 0);
SELECT IN_RANGE('Hello', 'a', 'z', 0);
SELECT IN_RANGE('a', 'a', 'z', 1);
SELECT IN_RANGE('z', 'a', 'z', 2);
SELECT IN_RANGE('m', 'a', 'z', 3);

-- Test varchar with NULL as first parameter
SELECT IN_RANGE(NULL, 'a', 'z', 0);
SELECT IN_RANGE(NULL, 'a', 'z', 1);
SELECT IN_RANGE(NULL, 'a', 'z', 2);
SELECT IN_RANGE(NULL, 'a', 'z', 3);

-- Test with table columns for each type

-- Bool type
DROP TABLE IF EXISTS test_bool;
CREATE TABLE test_bool(
    bool_primary BOOL PRIMARY KEY,
    bool_normal BOOL
);
INSERT INTO test_bool VALUES(TRUE, TRUE), (FALSE, FALSE);
SELECT * FROM test_bool WHERE IN_RANGE(bool_primary, FALSE, TRUE, 0);
SELECT * FROM test_bool WHERE IN_RANGE(bool_primary, FALSE, TRUE, 1);
SELECT * FROM test_bool WHERE IN_RANGE(bool_primary, FALSE, TRUE, 2);
SELECT * FROM test_bool WHERE IN_RANGE(bool_primary, FALSE, TRUE, 3);
SELECT * FROM test_bool WHERE IN_RANGE(bool_normal, FALSE, TRUE, 0);
SELECT * FROM test_bool WHERE IN_RANGE(bool_normal, FALSE, TRUE, 1);
SELECT * FROM test_bool WHERE IN_RANGE(bool_normal, FALSE, TRUE, 2);
SELECT * FROM test_bool WHERE IN_RANGE(bool_normal, FALSE, TRUE, 3);
DROP TABLE IF EXISTS test_bool;

-- Tinyint type
DROP TABLE IF EXISTS test_tinyint;
CREATE TABLE test_tinyint(
    tinyint_primary TINYINT PRIMARY KEY,
    tinyint_normal TINYINT
);
INSERT INTO test_tinyint VALUES(5, 5), (15, 15), (1, NULL);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_primary, 1, 10, 0);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_primary, 1, 10, 1);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_primary, 1, 10, 2);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_primary, 1, 10, 3);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_normal, 1, 10, 0);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_normal, 1, 10, 1);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_normal, 1, 10, 2);
SELECT * FROM test_tinyint WHERE IN_RANGE(tinyint_normal, 1, 10, 3);
DROP TABLE IF EXISTS test_tinyint;

-- Unsigned Tinyint type
DROP TABLE IF EXISTS test_utinyint;
CREATE TABLE test_utinyint(
    utinyint_primary TINYINT UNSIGNED PRIMARY KEY,
    utinyint_normal TINYINT UNSIGNED
);
INSERT INTO test_utinyint VALUES(5, 5), (15, 15), (1, NULL);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_primary, 1, 10, 0);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_primary, 1, 10, 1);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_primary, 1, 10, 2);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_primary, 1, 10, 3);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_normal, 1, 10, 0);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_normal, 1, 10, 1);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_normal, 1, 10, 2);
SELECT * FROM test_utinyint WHERE IN_RANGE(utinyint_normal, 1, 10, 3);
DROP TABLE IF EXISTS test_utinyint;

-- Smallint type
DROP TABLE IF EXISTS test_smallint;
CREATE TABLE test_smallint(
    smallint_primary SMALLINT PRIMARY KEY,
    smallint_normal SMALLINT
);
INSERT INTO test_smallint VALUES(50, 50), (150, 150), (10, NULL);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_primary, 10, 100, 0);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_primary, 10, 100, 1);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_primary, 10, 100, 2);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_primary, 10, 100, 3);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_normal, 10, 100, 0);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_normal, 10, 100, 1);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_normal, 10, 100, 2);
SELECT * FROM test_smallint WHERE IN_RANGE(smallint_normal, 10, 100, 3);
DROP TABLE IF EXISTS test_smallint;

-- Unsigned Smallint type
DROP TABLE IF EXISTS test_usmallint;
CREATE TABLE test_usmallint(
    usmallint_primary SMALLINT UNSIGNED PRIMARY KEY,
    usmallint_normal SMALLINT UNSIGNED
);
INSERT INTO test_usmallint VALUES(50, 50), (150, 150), (10, NULL);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_primary, 10, 100, 0);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_primary, 10, 100, 1);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_primary, 10, 100, 2);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_primary, 10, 100, 3);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_normal, 10, 100, 0);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_normal, 10, 100, 1);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_normal, 10, 100, 2);
SELECT * FROM test_usmallint WHERE IN_RANGE(usmallint_normal, 10, 100, 3);
DROP TABLE IF EXISTS test_usmallint;

-- Int type
DROP TABLE IF EXISTS test_int;
CREATE TABLE test_int(
    int_primary INT PRIMARY KEY,
    int_normal INT
);
INSERT INTO test_int VALUES(5, 5), (15, 15), (1, NULL);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 0);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 1);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 2);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 3);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 0);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 1);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 2);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 3);
DROP TABLE IF EXISTS test_int;

-- Unsigned Int type
DROP TABLE IF EXISTS test_uint;
CREATE TABLE test_uint(
    uint_primary INT UNSIGNED PRIMARY KEY,
    uint_normal INT UNSIGNED
);
INSERT INTO test_uint VALUES(5, 5), (15, 15), (1, NULL);
SELECT * FROM test_uint WHERE IN_RANGE(uint_primary, 1, 10, 0);
SELECT * FROM test_uint WHERE IN_RANGE(uint_primary, 1, 10, 1);
SELECT * FROM test_uint WHERE IN_RANGE(uint_primary, 1, 10, 2);
SELECT * FROM test_uint WHERE IN_RANGE(uint_primary, 1, 10, 3);
SELECT * FROM test_uint WHERE IN_RANGE(uint_normal, 1, 10, 0);
SELECT * FROM test_uint WHERE IN_RANGE(uint_normal, 1, 10, 1);
SELECT * FROM test_uint WHERE IN_RANGE(uint_normal, 1, 10, 2);
SELECT * FROM test_uint WHERE IN_RANGE(uint_normal, 1, 10, 3);
DROP TABLE IF EXISTS test_uint;

-- Bigint type
DROP TABLE IF EXISTS test_bigint;
CREATE TABLE test_bigint(
    bigint_primary BIGINT PRIMARY KEY,
    bigint_normal BIGINT
);
INSERT INTO test_bigint VALUES(500, 500), (1500, 1500), (100, NULL);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_primary, 100, 1000, 0);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_primary, 100, 1000, 1);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_primary, 100, 1000, 2);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_primary, 100, 1000, 3);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_normal, 100, 1000, 0);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_normal, 100, 1000, 1);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_normal, 100, 1000, 2);
SELECT * FROM test_bigint WHERE IN_RANGE(bigint_normal, 100, 1000, 3);
DROP TABLE IF EXISTS test_bigint;

-- Unsigned Bigint type
DROP TABLE IF EXISTS test_ubigint;
CREATE TABLE test_ubigint(
    ubigint_primary BIGINT UNSIGNED PRIMARY KEY,
    ubigint_normal BIGINT UNSIGNED
);
INSERT INTO test_ubigint VALUES(500, 500), (1500, 1500), (100, NULL);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_primary, 100, 1000, 0);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_primary, 100, 1000, 1);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_primary, 100, 1000, 2);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_primary, 100, 1000, 3);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_normal, 100, 1000, 0);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_normal, 100, 1000, 1);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_normal, 100, 1000, 2);
SELECT * FROM test_ubigint WHERE IN_RANGE(ubigint_normal, 100, 1000, 3);
DROP TABLE IF EXISTS test_ubigint;

-- Float type
DROP TABLE IF EXISTS test_float;
CREATE TABLE test_float(
    float_primary FLOAT PRIMARY KEY,
    float_normal FLOAT
);
INSERT INTO test_float VALUES(1.5, 1.5), (2.5, 2.5), (1.1, NULL);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 0);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 1);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 2);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 3);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 0);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 1);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 2);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 3);
DROP TABLE IF EXISTS test_float;

-- Decimal type
DROP TABLE IF EXISTS test_decimal;
CREATE TABLE test_decimal(
    decimal_primary DECIMAL(10,2) PRIMARY KEY,
    decimal_normal DECIMAL(10,2)
);
INSERT INTO test_decimal VALUES(10.50, 10.50), (20.50, 20.50), (5.50, NULL);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 0);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 1);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 2);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 3);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 0);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 1);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 2);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 3);
DROP TABLE IF EXISTS test_decimal;

-- Date type
DROP TABLE IF EXISTS test_date;
CREATE TABLE test_date(
    date_primary DATE PRIMARY KEY,
    date_normal DATE
);
INSERT INTO test_date VALUES('2022-06-15', '2022-06-15'), ('2022-07-15', '2022-07-15'), ('2022-08-15', NULL);
SELECT * FROM test_date WHERE IN_RANGE(date_primary, '2022-01-01', '2022-12-31', 0);
SELECT * FROM test_date WHERE IN_RANGE(date_primary, '2022-01-01', '2022-12-31', 1);
SELECT * FROM test_date WHERE IN_RANGE(date_primary, '2022-01-01', '2022-12-31', 2);
SELECT * FROM test_date WHERE IN_RANGE(date_primary, '2022-01-01', '2022-12-31', 3);
SELECT * FROM test_date WHERE IN_RANGE(date_normal, '2022-01-01', '2022-12-31', 0);
SELECT * FROM test_date WHERE IN_RANGE(date_normal, '2022-01-01', '2022-12-31', 1);
SELECT * FROM test_date WHERE IN_RANGE(date_normal, '2022-01-01', '2022-12-31', 2);
SELECT * FROM test_date WHERE IN_RANGE(date_normal, '2022-01-01', '2022-12-31', 3);
DROP TABLE IF EXISTS test_date;

-- Time type
DROP TABLE IF EXISTS test_time;
CREATE TABLE test_time(
    time_primary TIME PRIMARY KEY,
    time_normal TIME
);
INSERT INTO test_time VALUES('12:00:00', '12:00:00'), ('13:00:00', '13:00:00'), ('15:00:00', NULL);
SELECT * FROM test_time WHERE IN_RANGE(time_primary, '10:00:00', '14:00:00', 0);
SELECT * FROM test_time WHERE IN_RANGE(time_primary, '10:00:00', '14:00:00', 1);
SELECT * FROM test_time WHERE IN_RANGE(time_primary, '10:00:00', '14:00:00', 2);
SELECT * FROM test_time WHERE IN_RANGE(time_primary, '10:00:00', '14:00:00', 3);
SELECT * FROM test_time WHERE IN_RANGE(time_normal, '10:00:00', '14:00:00', 0);
SELECT * FROM test_time WHERE IN_RANGE(time_normal, '10:00:00', '14:00:00', 1);
SELECT * FROM test_time WHERE IN_RANGE(time_normal, '10:00:00', '14:00:00', 2);
SELECT * FROM test_time WHERE IN_RANGE(time_normal, '10:00:00', '14:00:00', 3);
DROP TABLE IF EXISTS test_time;

-- Datetime type
DROP TABLE IF EXISTS test_datetime;
CREATE TABLE test_datetime(
    datetime_primary DATETIME PRIMARY KEY,
    datetime_normal DATETIME
);
INSERT INTO test_datetime VALUES('2022-06-15 12:00:00', '2022-06-15 12:00:00'), ('2022-07-15 13:00:00', '2022-07-15 13:00:00'), ('2022-08-15 15:00:00', NULL);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);
DROP TABLE IF EXISTS test_datetime;

-- Timestamp type
DROP TABLE IF EXISTS test_timestamp;
CREATE TABLE test_timestamp(
    timestamp_primary TIMESTAMP PRIMARY KEY,
    timestamp_normal TIMESTAMP
);
INSERT INTO test_timestamp VALUES('2022-06-15 12:00:00', '2022-06-15 12:00:00'), ('2022-07-15 13:00:00', '2022-07-15 13:00:00'), ('2022-08-15 15:00:00', NULL);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT * FROM test_timestamp WHERE IN_RANGE(timestamp_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);
DROP TABLE IF EXISTS test_timestamp;

-- Varchar type
DROP TABLE IF EXISTS test_varchar;
CREATE TABLE test_varchar(
    varchar_primary VARCHAR(50) PRIMARY KEY,
    varchar_normal VARCHAR(50)
);
INSERT INTO test_varchar VALUES('hello', 'hello'), ('world', 'world'), ('test', NULL);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 0);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 1);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 2);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 3);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 0);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 1);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 2);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 3);
DROP TABLE IF EXISTS test_varchar;

-- Large table tests with count(*) as output
-- Int type with 1000000 rows
DROP TABLE IF EXISTS test_large_int;
CREATE TABLE test_large_int(
    int_col INT PRIMARY KEY,
    int_data INT
);
INSERT INTO test_large_int SELECT *, * FROM generate_series(1, 1000000) t;
SELECT COUNT(*) FROM test_large_int WHERE IN_RANGE(int_col, 100000, 900000, 0);
SELECT COUNT(*) FROM test_large_int WHERE IN_RANGE(int_col, 100000, 900000, 1);
SELECT COUNT(*) FROM test_large_int WHERE IN_RANGE(int_col, 100000, 900000, 2);
SELECT COUNT(*) FROM test_large_int WHERE IN_RANGE(int_col, 100000, 900000, 3);
DROP TABLE IF EXISTS test_large_int;

-- Float type with 1000000 rows
DROP TABLE IF EXISTS test_large_float;
CREATE TABLE test_large_float(
    float_col FLOAT PRIMARY KEY,
    float_data FLOAT
);
INSERT INTO test_large_float SELECT *, * FROM generate_series(1, 1000000) t;
SELECT COUNT(*) FROM test_large_float WHERE IN_RANGE(float_col, 100000.0, 900000.0, 0);
SELECT COUNT(*) FROM test_large_float WHERE IN_RANGE(float_col, 100000.0, 900000.0, 1);
SELECT COUNT(*) FROM test_large_float WHERE IN_RANGE(float_col, 100000.0, 900000.0, 2);
SELECT COUNT(*) FROM test_large_float WHERE IN_RANGE(float_col, 100000.0, 900000.0, 3);
DROP TABLE IF EXISTS test_large_float;

-- Decimal type with 1000000 rows
DROP TABLE IF EXISTS test_large_decimal;
CREATE TABLE test_large_decimal(
    decimal_col DECIMAL(15,2) PRIMARY KEY,
    decimal_data DECIMAL(15,2)
);
INSERT INTO test_large_decimal SELECT *, * FROM generate_series(1, 1000000) t;
SELECT COUNT(*) FROM test_large_decimal WHERE IN_RANGE(decimal_col, 100000.00, 900000.00, 0);
SELECT COUNT(*) FROM test_large_decimal WHERE IN_RANGE(decimal_col, 100000.00, 900000.00, 1);
SELECT COUNT(*) FROM test_large_decimal WHERE IN_RANGE(decimal_col, 100000.00, 900000.00, 2);
SELECT COUNT(*) FROM test_large_decimal WHERE IN_RANGE(decimal_col, 100000.00, 900000.00, 3);
DROP TABLE IF EXISTS test_large_decimal;

-- Varchar type with 1000000 rows
DROP TABLE IF EXISTS test_large_varchar;
CREATE TABLE test_large_varchar(
    varchar_col VARCHAR(50) PRIMARY KEY,
    varchar_data VARCHAR(50)
);
INSERT INTO test_large_varchar SELECT *, * FROM generate_series(1, 1000000) t;
SELECT COUNT(*) FROM test_large_varchar WHERE IN_RANGE(varchar_col, '100000', '900000', 0);
SELECT COUNT(*) FROM test_large_varchar WHERE IN_RANGE(varchar_col, '100000', '900000', 1);
SELECT COUNT(*) FROM test_large_varchar WHERE IN_RANGE(varchar_col, '100000', '900000', 2);
SELECT COUNT(*) FROM test_large_varchar WHERE IN_RANGE(varchar_col, '100000', '900000', 3);
DROP TABLE IF EXISTS test_large_varchar;
