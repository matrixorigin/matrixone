-- Test SUM aggregate function overflow

DROP TABLE IF EXISTS t_sum_overflow;
CREATE TABLE t_sum_overflow (
    id INT PRIMARY KEY AUTO_INCREMENT,
    val BIGINT
);

-- Insert boundary values that will overflow when summed
INSERT INTO t_sum_overflow (val) VALUES (9223372036854775807);  -- MAX_BIGINT
INSERT INTO t_sum_overflow (val) VALUES (9223372036854775807);
INSERT INTO t_sum_overflow (val) VALUES (9223372036854775807);

-- Test 1: SUM should detect overflow and error
SELECT SUM(val) AS total FROM t_sum_overflow;

-- Test 2: SUM with DECIMAL cast (should work)
SELECT SUM(CAST(val AS DECIMAL(38,0))) AS total_decimal FROM t_sum_overflow;

-- Test 3: Normal SUM (should work)
DELETE FROM t_sum_overflow;
INSERT INTO t_sum_overflow (val) VALUES (100);
INSERT INTO t_sum_overflow (val) VALUES (200);
INSERT INTO t_sum_overflow (val) VALUES (300);
SELECT SUM(val) AS total FROM t_sum_overflow;

-- Test 4: SUM with negative values
DELETE FROM t_sum_overflow;
INSERT INTO t_sum_overflow (val) VALUES (-9223372036854775808);  -- MIN_BIGINT
INSERT INTO t_sum_overflow (val) VALUES (-9223372036854775808);
INSERT INTO t_sum_overflow (val) VALUES (-9223372036854775808);
SELECT SUM(val) AS total FROM t_sum_overflow;

-- Test 5: Mixed positive and negative (should work)
DELETE FROM t_sum_overflow;
INSERT INTO t_sum_overflow (val) VALUES (9223372036854775807);
INSERT INTO t_sum_overflow (val) VALUES (-9223372036854775807);
SELECT SUM(val) AS total FROM t_sum_overflow;

-- Test 6: Large but not overflowing values
DELETE FROM t_sum_overflow;
INSERT INTO t_sum_overflow (val) VALUES (4611686018427387903);  -- Half of MAX
INSERT INTO t_sum_overflow (val) VALUES (4611686018427387903);
SELECT SUM(val) AS total FROM t_sum_overflow;

-- Test 7: Empty table
DELETE FROM t_sum_overflow;
SELECT SUM(val) AS total FROM t_sum_overflow;

-- Cleanup
DROP TABLE t_sum_overflow;

