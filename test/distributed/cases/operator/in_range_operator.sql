-- @suit

-- @case
-- @desc:test for IN_RANGE operator
-- @label:bvt

-- Test basic IN_RANGE with inclusive bounds (flag=0)
SELECT IN_RANGE(2, 1, 3, 0), IN_RANGE(2, 3, 1, 0);
SELECT IN_RANGE(1, 2, 3, 0);
SELECT IN_RANGE('b', 'a', 'c', 0);
SELECT IN_RANGE(2, 2, '3', 0);

-- Test with bool
SELECT IN_RANGE(TRUE, FALSE, TRUE, 0);
SELECT IN_RANGE(FALSE, TRUE, FALSE, 1);
SELECT IN_RANGE(TRUE, FALSE, TRUE, 1);
SELECT IN_RANGE(TRUE, FALSE, TRUE, 2);
SELECT IN_RANGE(TRUE, FALSE, TRUE, 3);

-- Test with date
SELECT IN_RANGE('2022-06-15', '2022-01-01', '2022-12-31', 0);
SELECT IN_RANGE('2021-12-31', '2022-01-01', '2022-12-31', 0);
SELECT IN_RANGE('2022-01-01', '2022-01-01', '2022-12-31', 1);
SELECT IN_RANGE('2022-12-31', '2022-01-01', '2022-12-31', 2);
SELECT IN_RANGE('2022-06-15', '2022-01-01', '2022-12-31', 3);

-- Test with datetime
SELECT IN_RANGE('2022-06-15 12:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE('2023-01-01 00:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT IN_RANGE('2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT IN_RANGE('2022-12-31 23:59:59', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT IN_RANGE('2022-06-15 12:00:00', '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);

-- Test with time
SELECT IN_RANGE('12:00:00', '10:00:00', '14:00:00', 0);
SELECT IN_RANGE('15:00:00', '10:00:00', '14:00:00', 0);
SELECT IN_RANGE('10:00:00', '10:00:00', '14:00:00', 1);
SELECT IN_RANGE('14:00:00', '10:00:00', '14:00:00', 2);
SELECT IN_RANGE('12:00:00', '10:00:00', '14:00:00', 3);

-- Test with decimal
SELECT IN_RANGE(1.5, 1.0, 2.0, 0);
SELECT IN_RANGE(2.5, 1.0, 2.0, 0);
SELECT IN_RANGE(1.0, 1.0, 2.0, 1);
SELECT IN_RANGE(2.0, 1.0, 2.0, 2);
SELECT IN_RANGE(1.5, 1.0, 2.0, 3);

-- Test with varchar (case sensitive)
SELECT IN_RANGE('hello', 'a', 'z', 0);
SELECT IN_RANGE('Hello', 'a', 'z', 0);
SELECT IN_RANGE('a', 'a', 'z', 1);
SELECT IN_RANGE('z', 'a', 'z', 2);
SELECT IN_RANGE('m', 'a', 'z', 3);

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

-- Int type
DROP TABLE IF EXISTS test_int;
CREATE TABLE test_int(
    int_primary INT PRIMARY KEY,
    int_normal INT
);
INSERT INTO test_int VALUES(5, 5), (15, 15);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 0);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 1);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 2);
SELECT * FROM test_int WHERE IN_RANGE(int_primary, 1, 10, 3);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 0);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 1);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 2);
SELECT * FROM test_int WHERE IN_RANGE(int_normal, 1, 10, 3);
DROP TABLE IF EXISTS test_int;

-- Float type
DROP TABLE IF EXISTS test_float;
CREATE TABLE test_float(
    float_primary FLOAT PRIMARY KEY,
    float_normal FLOAT
);
INSERT INTO test_float VALUES(1.5, 1.5), (2.5, 2.5);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 0);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 1);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 2);
SELECT * FROM test_float WHERE IN_RANGE(float_primary, 1.0, 2.0, 3);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 0);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 1);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 2);
SELECT * FROM test_float WHERE IN_RANGE(float_normal, 1.0, 2.0, 3);
DROP TABLE IF EXISTS test_float;

-- Date type
DROP TABLE IF EXISTS test_date;
CREATE TABLE test_date(
    date_primary DATE PRIMARY KEY,
    date_normal DATE
);
INSERT INTO test_date VALUES('2022-06-15', '2022-06-15'), ('2022-07-15', '2022-07-15');
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
INSERT INTO test_time VALUES('12:00:00', '12:00:00'), ('13:00:00', '13:00:00');
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
INSERT INTO test_datetime VALUES('2022-06-15 12:00:00', '2022-06-15 12:00:00'), ('2022-07-15 13:00:00', '2022-07-15 13:00:00');
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_primary, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 0);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 1);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 2);
SELECT * FROM test_datetime WHERE IN_RANGE(datetime_normal, '2022-01-01 00:00:00', '2022-12-31 23:59:59', 3);
DROP TABLE IF EXISTS test_datetime;

-- Decimal type
DROP TABLE IF EXISTS test_decimal;
CREATE TABLE test_decimal(
    decimal_primary DECIMAL(10,2) PRIMARY KEY,
    decimal_normal DECIMAL(10,2)
);
INSERT INTO test_decimal VALUES(10.50, 10.50), (20.50, 20.50);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 0);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 1);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 2);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_primary, 0.0, 15.0, 3);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 0);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 1);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 2);
SELECT * FROM test_decimal WHERE IN_RANGE(decimal_normal, 0.0, 15.0, 3);
DROP TABLE IF EXISTS test_decimal;

-- Varchar type
DROP TABLE IF EXISTS test_varchar;
CREATE TABLE test_varchar(
    varchar_primary VARCHAR(50) PRIMARY KEY,
    varchar_normal VARCHAR(50)
);
INSERT INTO test_varchar VALUES('hello', 'hello'), ('world', 'world');
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 0);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 1);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 2);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_primary, 'a', 'z', 3);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 0);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 1);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 2);
SELECT * FROM test_varchar WHERE IN_RANGE(varchar_normal, 'a', 'z', 3);
DROP TABLE IF EXISTS test_varchar;
