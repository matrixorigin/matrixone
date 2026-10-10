-- String numeric context is shared by SUM and AVG, including their consumers.
DROP DATABASE IF EXISTS sum_avg_numeric;
CREATE DATABASE sum_avg_numeric;
USE sum_avg_numeric;
SET @sum_avg_saved_mode = @@sql_mode;
SET @sum_avg_saved_dop = @@max_dop;
SET @sum_avg_saved_hints = @@optimizer_hints;

CREATE TABLE numeric_strings (id INT PRIMARY KEY, g INT, s VARCHAR(32));
INSERT INTO numeric_strings VALUES
    (1, 1, '1'), (2, 1, '2.5'), (3, 1, ' 3 '), (4, 1, '-4e0'),
    (5, 1, '1.0'), (6, 2, '12x'), (7, 2, ''), (8, 2, 'abc'), (9, 3, NULL);
-- @metacmp(true)
SELECT SUM(s), AVG(s) FROM numeric_strings WHERE id <= 4;
SELECT SUM(s), AVG(s) FROM numeric_strings;
SELECT SUM(CAST(s AS DOUBLE)), AVG(CAST(s AS DOUBLE)) FROM numeric_strings;
SELECT SUM(s), AVG(s) FROM numeric_strings WHERE id < 0;
SELECT SUM(s), AVG(s) FROM numeric_strings WHERE g = 3;
-- Numeric DISTINCT must collapse '1' and '1.0' and the two nonnumeric zeros.
SELECT SUM(DISTINCT s), CAST(AVG(DISTINCT s) AS DECIMAL(16,6)) FROM numeric_strings;
SELECT g, SUM(s), AVG(s) FROM numeric_strings GROUP BY g ORDER BY g;
SELECT id,
       SUM(s) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum,
       CAST(AVG(s) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(12,4)) AS running_avg
FROM numeric_strings WHERE id <= 4 ORDER BY id;

-- The same CAST domain applies to the other character and binary storage types.
CREATE TABLE string_domains (c CHAR(8), t TEXT, b BINARY(1), vb VARBINARY(1), bl BLOB);
INSERT INTO string_domains VALUES ('1', '1', x'01', x'01', '1'), ('2.5', '2.5', x'02', x'02', '2.5');
SELECT SUM(c), AVG(c), SUM(t), AVG(t), SUM(bl), AVG(bl) FROM string_domains;
SELECT SUM(b) = SUM(CAST(b AS DOUBLE)) AS binary_sum_matches,
       AVG(b) = AVG(CAST(b AS DOUBLE)) AS binary_avg_matches,
       SUM(vb) = SUM(CAST(vb AS DOUBLE)) AS varbinary_sum_matches,
       AVG(vb) = AVG(CAST(vb AS DOUBLE)) AS varbinary_avg_matches
FROM string_domains;

-- Cached PREPARE retains the column's string domain across EXECUTEs.
PREPARE string_aggregate FROM 'SELECT SUM(s), AVG(s) FROM numeric_strings WHERE g = ?';
SET @sum_avg_group = 1;
EXECUTE string_aggregate USING @sum_avg_group;
SET @sum_avg_group = 2;
EXECUTE string_aggregate USING @sum_avg_group;
SET @sum_avg_group = 3;
EXECUTE string_aggregate USING @sum_avg_group;
DEALLOCATE PREPARE string_aggregate;
PREPARE string_marker FROM 'SELECT SUM(CAST(? AS CHAR)), AVG(CAST(? AS CHAR))';
SET @sum_avg_value = 1;
EXECUTE string_marker USING @sum_avg_value, @sum_avg_value;
SET @sum_avg_value = 2.5;
EXECUTE string_marker USING @sum_avg_value, @sum_avg_value;
SET @sum_avg_value = '12x';
EXECUTE string_marker USING @sum_avg_value, @sum_avg_value;
SET @sum_avg_value = NULL;
EXECUTE string_marker USING @sum_avg_value, @sum_avg_value;
DEALLOCATE PREPARE string_marker;

-- View and CTAS must preserve DOUBLE results and nullable groups.
CREATE VIEW string_totals AS SELECT g, SUM(s) AS total, AVG(s) AS mean FROM numeric_strings GROUP BY g;
SELECT * FROM string_totals ORDER BY g;
CREATE TABLE materialized_totals AS SELECT * FROM string_totals;
SELECT * FROM materialized_totals ORDER BY g;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = 'sum_avg_numeric' AND table_name = 'materialized_totals'
ORDER BY ordinal_position;

-- Force AP estimates so both one-worker and two-worker aggregation are exercised with few rows.
SET @@optimizer_hints = 'execType=2';
SET @@max_dop = 1;
SELECT g, SUM(s), AVG(s) FROM numeric_strings GROUP BY g ORDER BY g;
SET @@max_dop = 2;
SELECT g, SUM(s), AVG(s) FROM numeric_strings GROUP BY g ORDER BY g;
SELECT SUM(DISTINCT s), CAST(AVG(DISTINCT s) AS DECIMAL(16,6)) FROM numeric_strings;
SET @@max_dop = @sum_avg_saved_dop;
SET @@optimizer_hints = @sum_avg_saved_hints;

-- Native mode keeps existing strict CAST errors, without changing clean numeric strings.
SET @@sql_mode = CONCAT_WS(',', @sum_avg_saved_mode, 'MATRIXONE_NATIVE');
SELECT SUM(s), AVG(s) FROM numeric_strings WHERE id <= 4;
SELECT SUM(s) FROM numeric_strings WHERE id = 6;
SELECT AVG(s) FROM numeric_strings WHERE id = 6;
SET @@sql_mode = @sum_avg_saved_mode;
SELECT SUM(s), AVG(s) FROM numeric_strings WHERE id = 6;
DROP DATABASE sum_avg_numeric;
