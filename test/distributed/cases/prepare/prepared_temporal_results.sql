DROP DATABASE IF EXISTS prepared_temporal_results_test;
CREATE DATABASE prepared_temporal_results_test;
USE prepared_temporal_results_test;
SET @temporal_saved_tz = @@session.time_zone;
SET SESSION time_zone = '+00:00';

-- #30192: metadata follows the numeric source on each prepared execution.
PREPARE p_epoch FROM 'SELECT FROM_UNIXTIME(?) AS v';
SET @epoch = 1704067200;
-- @metacmp(true)
EXECUTE p_epoch USING @epoch;
SET @epoch = CAST(1704067200.123456 AS DECIMAL(16,6));
-- @metacmp(true)
EXECUTE p_epoch USING @epoch;
SET @epoch = 1704067200;
-- @metacmp(true)
EXECUTE p_epoch USING @epoch;
DEALLOCATE PREPARE p_epoch;
PREPARE p_epoch_ctas FROM 'CREATE TABLE epoch_copy AS SELECT FROM_UNIXTIME(?) AS v';
EXECUTE p_epoch_ctas USING @epoch;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'epoch_copy';
SELECT CAST(v AS CHAR) AS v FROM epoch_copy;
DROP TABLE epoch_copy;
SET @epoch = CAST(1704067200.123456 AS DECIMAL(16,6));
EXECUTE p_epoch_ctas USING @epoch;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'epoch_copy';
SELECT CAST(v AS CHAR) AS v FROM epoch_copy;
DEALLOCATE PREPARE p_epoch_ctas;

-- #30194: the dynamic first operand is temporal; literal string controls stay strings.
SET @d = '2024-01-31 12:34:56.123456';
PREPARE p_date FROM 'SELECT DATE_ADD(?, INTERVAL 1 HOUR) AS v';
-- @metacmp(true)
EXECUTE p_date USING @d;
DEALLOCATE PREPARE p_date;
PREPARE p_date_ctas FROM 'CREATE TABLE date_copy AS SELECT DATE_ADD(?, INTERVAL 1 HOUR) AS v';
EXECUTE p_date_ctas USING @d;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'date_copy';
SELECT CAST(v AS CHAR) AS v FROM date_copy;
DEALLOCATE PREPARE p_date_ctas;
-- @metacmp(true)
SELECT DATE_ADD('2024-01-31', INTERVAL 1 HOUR) AS literal_control;

PREPARE p_date_day FROM 'CREATE TABLE date_day_copy AS SELECT DATE_ADD(?, INTERVAL ? DAY) AS v';
SET @n = 1;
EXECUTE p_date_day USING @d, @n;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'date_day_copy';
SELECT CAST(v AS CHAR) AS v FROM date_day_copy;
DEALLOCATE PREPARE p_date_day;

-- Calendar-only result metadata also survives numeric compound counts.
PREPARE p_year_month FROM 'SELECT DATE_ADD(?, INTERVAL 1 YEAR_MONTH) AS added, DATE_SUB(?, INTERVAL 1 YEAR_MONTH) AS subtracted';
-- @metacmp(true)
EXECUTE p_year_month USING @d, @d;
DEALLOCATE PREPARE p_year_month;
PREPARE p_year_month_ctas FROM 'CREATE TABLE year_month_copy AS SELECT DATE_ADD(?, INTERVAL ? YEAR_MONTH) AS v';
SET @n = 1;
EXECUTE p_year_month_ctas USING @d, @n;
SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'year_month_copy';
SELECT CAST(v AS CHAR) AS v FROM year_month_copy;
DROP TABLE year_month_copy;
SET @n = '1';
EXECUTE p_year_month_ctas USING @d, @n;
SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'year_month_copy';
SELECT CAST(v AS CHAR) AS v FROM year_month_copy;
DEALLOCATE PREPARE p_year_month_ctas;

-- #30195: keep time-of-day and fractional input through CTAS and a count marker.
SET @d = '2024-01-31 12:34:56.000001';
SET @n = 1;
PREPARE p_ts_ctas FROM 'CREATE TABLE timestampadd_copy AS SELECT TIMESTAMPADD(MICROSECOND, ?, ?) AS v';
EXECUTE p_ts_ctas USING @n, @d;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'timestampadd_copy';
SELECT CAST(v AS CHAR) AS v FROM timestampadd_copy;
DROP TABLE timestampadd_copy;
SET @d = '2024-02-29 23:59:59.999999';
EXECUTE p_ts_ctas USING @n, @d;
SELECT CAST(v AS CHAR) AS v FROM timestampadd_copy;
DEALLOCATE PREPARE p_ts_ctas;
PREPARE p_ts_day FROM 'SELECT TIMESTAMPADD(DAY, ?, ?) AS v';
SET @d = '2024-01-31 12:34:56.123456';
-- @metacmp(true)
EXECUTE p_ts_day USING @n, @d;
DEALLOCATE PREPARE p_ts_day;
PREPARE p_explicit_ctas FROM 'CREATE TABLE explicit_copy (extra INT DEFAULT 1, v DATETIME(0)) AS SELECT TIMESTAMPADD(MICROSECOND, 1, ?) AS v';
EXECUTE p_explicit_ctas USING @d;
SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'explicit_copy' ORDER BY ordinal_position;
SELECT extra, CAST(v AS CHAR) AS v FROM explicit_copy;
DEALLOCATE PREPARE p_explicit_ctas;
CREATE TABLE temporal_insert_control (v DATETIME(6));
PREPARE p_ts_insert FROM 'INSERT INTO temporal_insert_control SELECT TIMESTAMPADD(MICROSECOND, 1, ?)';
EXECUTE p_ts_insert USING @d;
SELECT CAST(v AS CHAR) AS v FROM temporal_insert_control;
DEALLOCATE PREPARE p_ts_insert;

-- #30196: DATETIME materialization retains fractions and is stable across time zones.
CREATE TABLE timestamp_copy AS SELECT TIMESTAMP(CAST('2024-02-29 12:34:56.123456' AS DATETIME(6))) AS v;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'timestamp_copy';
SELECT CAST(v AS CHAR) AS v FROM timestamp_copy;
SET SESSION time_zone = '+08:00';
SELECT CAST(v AS CHAR) AS v FROM timestamp_copy;
SET SESSION time_zone = '+00:00';
-- @metacmp(true)
SELECT TIMESTAMP('2024-02-29 12:34:56.123') AS string_control, TIMESTAMP('2024-02-29', '12:34:56.123') AS pair_control;

-- @metacmp(true)
SELECT TIMESTAMP('invalid') AS invalid_value, TIMESTAMP(NULL) AS null_value;

-- TIMESTAMP literals/functions are DATETIME; mixed-domain predicates must stay equivalent.
CREATE TABLE timestamp_literal_copy AS SELECT TIMESTAMP '2024-02-29 12:34:56.123' AS v;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'timestamp_literal_copy';
SET SESSION time_zone = '+08:00';
CREATE TABLE timestamp_predicate_control (v TIMESTAMP(6) PRIMARY KEY);
INSERT INTO timestamp_predicate_control VALUES ('2024-01-01 00:20:00.123456'), ('2024-01-01 00:30:00.654321'), ('2024-01-01 00:40:00.123456');
SELECT CAST(v AS CHAR) AS v FROM timestamp_predicate_control WHERE v IN (TIMESTAMP '2024-01-01 00:20:00.123456', TIMESTAMP '2024-01-01 00:30:00.654321') ORDER BY v;
SELECT CAST(v AS CHAR) AS v FROM timestamp_predicate_control WHERE v = TIMESTAMP('2024-01-01 00:20:00.123456') OR v = TIMESTAMP('2024-01-01 00:30:00.654321') ORDER BY v;
-- A nonconstant unary expression survives PREPARE and catalog view replay.
CREATE TABLE timestamp_abi_control (v DATETIME(3));
INSERT INTO timestamp_abi_control VALUES ('2024-02-29 12:34:56.123');
PREPARE p_timestamp_column FROM 'SELECT CAST(TIMESTAMP(v) AS CHAR) AS v FROM timestamp_abi_control';
EXECUTE p_timestamp_column;
DEALLOCATE PREPARE p_timestamp_column;
CREATE VIEW timestamp_abi_view AS SELECT TIMESTAMP(v) AS v FROM timestamp_abi_control;
SELECT CAST(v AS CHAR) AS v FROM timestamp_abi_view;
DROP VIEW timestamp_abi_view;
DROP TABLE timestamp_abi_control;
SET SESSION time_zone = '+00:00';

-- #30197: calendar rollover yields a clock, genuine durations keep hours beyond 24.
PREPARE p_add FROM 'SELECT ADDTIME(?, ''02:00:00.000001'') AS v';
SET @d = '2024-02-29 23:30:00.123456';
-- @metacmp(true)
EXECUTE p_add USING @d;
SET @d = CAST('2024-02-29 23:30:00.123456' AS DATETIME(6));
-- @metacmp(true)
EXECUTE p_add USING @d;
SET @d = '23:30:00.123456';
-- @metacmp(true)
EXECUTE p_add USING @d;
SET @d = 'not-a-time';
EXECUTE p_add USING @d;
SET @d = NULL;
EXECUTE p_add USING @d;
SET @d = '2024-02-29 23:30:00.123456';
EXECUTE p_add USING @d;
DEALLOCATE PREPARE p_add;
PREPARE p_sub FROM 'SELECT SUBTIME(?, ''02:00:00.000001'') AS v';
SET @d = CAST('2024-02-29 00:30:00.123456' AS DATETIME(6));
-- @metacmp(true)
EXECUTE p_sub USING @d;
DEALLOCATE PREPARE p_sub;
PREPARE p_time_ctas FROM 'CREATE TABLE time_copy AS SELECT ADDTIME(?, ''02:00:00.000001'') AS v';
SET @d = CAST('2024-02-29 23:30:00.123456' AS DATETIME(6));
EXECUTE p_time_ctas USING @d;
SELECT column_type, datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'time_copy';
SELECT CAST(v AS CHAR) AS v FROM time_copy;
DEALLOCATE PREPARE p_time_ctas;
-- @metacmp(true)
SELECT ADDTIME(CAST('2024-02-29 23:30:00.123456' AS DATETIME(6)), '02:00:00.000001') AS explicit_control;

SET SESSION time_zone = @temporal_saved_tz;
DROP DATABASE prepared_temporal_results_test;
