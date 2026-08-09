-- @case
-- @desc:Prepared constant projections preserve grouped result cardinality
-- @label:bvt
-- @metacmp(false)

DROP DATABASE IF EXISTS prepared_projection_cardinality;
CREATE DATABASE prepared_projection_cardinality;
USE prepared_projection_cardinality;

CREATE TABLE metric_rows(bucket VARCHAR(20), value_col INT);
INSERT INTO metric_rows VALUES ('a', 10), ('b', 20), ('c', 30);
SET save_query_result = ON;

-- A parameter-backed constant is physically one value, but represents every
-- logical row in the grouped output batch.
PREPARE parameter_first FROM
  'SELECT ? AS projection_value, SUM(value_col) AS total FROM metric_rows GROUP BY bucket ORDER BY total';
SET @projection_value = 7;
EXECUTE parameter_first USING @projection_value;
SELECT * FROM result_scan(last_query_id()) AS saved_parameter_first;

-- Reuse the same statement and cover const-null broadcast semantics.
SET @projection_value = NULL;
EXECUTE parameter_first USING @projection_value;
SELECT * FROM result_scan(last_query_id()) AS saved_parameter_null;
DEALLOCATE PREPARE parameter_first;

-- Moving the same parameter away from the first column must not affect row
-- cardinality.
PREPARE parameter_second FROM
  'SELECT bucket, ? AS projection_value, SUM(value_col) AS total FROM metric_rows GROUP BY bucket ORDER BY total';
SET @projection_value = 11;
EXECUTE parameter_second USING @projection_value;
SELECT * FROM result_scan(last_query_id()) AS saved_parameter_second;
DEALLOCATE PREPARE parameter_second;
SET save_query_result = OFF;

-- Nearest non-prepared representation control.
SELECT 7 AS projection_value, SUM(value_col) AS total
FROM metric_rows GROUP BY bucket ORDER BY total;

-- Preserve the broadcast hashmap path fixed by #26843.
PREPARE parameter_group FROM
  'SELECT ? AS projection_value, SUM(value_col) AS total FROM metric_rows GROUP BY ?';
SET @projection_value = 13, @group_value = 17;
EXECUTE parameter_group USING @projection_value, @group_value;
DEALLOCATE PREPARE parameter_group;

DROP DATABASE prepared_projection_cardinality;
