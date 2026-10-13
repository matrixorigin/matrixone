-- @case
-- @desc: FIELD keeps prepared CASE return types across executions and relational consumers
-- @label:bvt

-- Text first execution.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Null first execution.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Binary first execution.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Numeric history uses the comparison consumer's conversion domain.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''0'', ''1'', ''2'', ''-1'') AS f';
SET @field_value = 1;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = '1.9tail';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary '-1tail';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = CAST(1.25 AS DECIMAL(10,2));
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- A numeric condition reparses both CASE producers together.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f1, FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f2';
SET @field_c1 = '1', @field_c2 = '1', @field_a = 'a', @field_b = _binary 'a';
EXECUTE p_field_history USING @field_c1, @field_a, @field_c2, @field_b;
SET @field_c2 = 1;
EXECUTE p_field_history USING @field_c1, @field_a, @field_c2, @field_b;
SET @field_c1 = 1, @field_b = 'a';
EXECUTE p_field_history USING @field_c1, @field_a, @field_c2, @field_b;
DEALLOCATE PREPARE p_field_history;

-- Materialized numeric history must reach the aggregate input.
PREPARE p_field_history FROM 'SELECT DISTINCT FIELD(MAX(CASE WHEN i=0 THEN ? ELSE ? END), ''9'', ''10'') AS f FROM (SELECT 0 i UNION ALL SELECT 1) d';
SET @field_a = 9, @field_b = 10;
EXECUTE p_field_history USING @field_a, @field_b;
SET @field_a = '9', @field_b = '10';
EXECUTE p_field_history USING @field_a, @field_b;
SET @field_a = _binary '9', @field_b = _binary '10';
EXECUTE p_field_history USING @field_a, @field_b;
DEALLOCATE PREPARE p_field_history;

-- Materialized numeric history must reach the aggregate input.
PREPARE p_field_history FROM 'SELECT DISTINCT FIELD(MAX(CASE WHEN i=0 THEN ? ELSE ? END) OVER (), ''9'', ''10'') AS f FROM (SELECT 0 i UNION ALL SELECT 1) d';
SET @field_a = 9, @field_b = 10;
EXECUTE p_field_history USING @field_a, @field_b;
SET @field_a = '9', @field_b = '10';
EXECUTE p_field_history USING @field_a, @field_b;
SET @field_a = _binary '9', @field_b = _binary '10';
EXECUTE p_field_history USING @field_a, @field_b;
DEALLOCATE PREPARE p_field_history;

-- NULL history survives a shared CTE producer.
PREPARE p_field_history FROM 'WITH d AS (SELECT CASE WHEN ? THEN ? ELSE NULL END x) SELECT FIELD(x, ''A'', ''a'') AS f1, FIELD(x, ''a'', ''A'') AS f2 FROM d';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Numeric string overflow reparses even when the source type is unchanged.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, 9223372036854775808, 18446744073709551615, 0) AS f';
SET @field_value = 1;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = '9223372036854775808';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = '18446744073709551615';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- CASE candidates also determine FIELD's numeric comparison domain.
PREPARE p_field_history FROM 'SELECT FIELD(''Z'', CASE WHEN ? THEN ? ELSE NULL END) AS f';
SET @field_value = 123;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;
PREPARE p_field_history FROM 'SELECT FIELD(''1.9'', CASE WHEN ? THEN ? ELSE NULL END) AS f';
SET @field_value = 123;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = '1.9tail';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Adjacent exact DECIMAL peers must stay distinct after a string transition.
PREPARE p_field_history FROM 'SELECT FIELD(CAST(''9007199254740992'' AS DECIMAL(30,0)), CASE WHEN ? THEN ? ELSE NULL END, CAST(''9007199254740992'' AS DECIMAL(30,0))) AS f';
SET @field_value = 9007199254740993;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = '9007199254740993';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary '9007199254740993';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;
