-- @case
-- @desc: FIELD keeps prepared CASE return types across executions and relational consumers
-- @label:bvt

DROP DATABASE IF EXISTS field_history_unrelated_ddl;
DROP DATABASE IF EXISTS field_history_dependencies;

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

-- String wrappers retain CASE type history.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN CONCAT(?) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Binary-first wrapped history keeps the reverse transition binary.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN CONCAT(?) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- CONCAT has a TEXT declaration even for a NULL-first marker.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN CONCAT(?) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- CONCAT_WS keeps parameter dependencies across all contributors.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN CONCAT_WS('''', ?) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- A scalar marker has a materialized TEXT declaration for NULL first.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN (SELECT ? LIMIT 1) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- A binary-first scalar projection retains its binary declaration.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN (SELECT ? LIMIT 1) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- A composite scalar projection must not inherit the bare marker declaration.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN (SELECT COALESCE(?, NULL) LIMIT 1) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- A bare parameter column has the same declaration through a CTE or derived table.
PREPARE p_field_history FROM 'WITH d AS (SELECT ? x) SELECT FIELD(CASE WHEN ? THEN x ELSE NULL END, ''A'', ''a'') AS f FROM d';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_value, @field_condition;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_value, @field_condition;
DEALLOCATE PREPARE p_field_history;
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN x ELSE NULL END, ''A'', ''a'') AS f FROM (SELECT ? x LIMIT 1) d';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Function dependencies do not depend on the return-domain inference whitelist.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ELT(1, ?) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Until reprepare, a string function uses its parameter declaration, not payload type.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ELT(1, ?) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = '1', @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_condition = 1;
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- NULLIF's NULL marker keeps its declaration; a NULL composite keeps its own domain.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN NULLIF(?, ''x'') ELSE NULL END, ''A'', ''a'') AS direct_f, FIELD(CASE WHEN ? THEN NULLIF(COALESCE(?, NULL), ''x'') ELSE NULL END, ''A'', ''a'') AS composite_f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- NULLIF resolves its return operand again when the statement is reprepared.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN NULLIF(?, ''x'') ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = '1', @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_condition = 1, @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Aggregate marker declarations survive scalar materialization, including NULL first.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN (SELECT MAX(?) LIMIT 1) ELSE NULL END, ''A'', ''a'') AS max_f, FIELD(CASE WHEN ? THEN (SELECT MIN(?) LIMIT 1) ELSE NULL END, ''A'', ''a'') AS min_f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- UNION resolves each branch's declaration before merging output domains.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN x ELSE NULL END, ''A'', ''a'') AS f FROM (SELECT ? x UNION ALL SELECT NULL) d ORDER BY f';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- String functions consume the declared domain of nested composite expressions.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ELT(1, COALESCE(?, NULL)) ELSE NULL END, ''A'', ''a'') AS f';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- A table dependency does not make an unrelated DDL a type reprepare.
-- ANY_VALUE preserves its direct value domain; a scalar output still exposes
-- the parameter's TEXT declaration at the relational consumption boundary.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ANY_VALUE(?) ELSE NULL END, ''A'', ''a'') AS direct_f, FIELD(CASE WHEN ? THEN (SELECT ANY_VALUE(?) LIMIT 1) ELSE NULL END, ''A'', ''a'') AS scalar_f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- NULLIF owns its operand declaration even when a NULL aggregate payload has
-- a numeric placeholder type. A numeric comparison peer stays numeric.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ANY_VALUE(NULLIF(?, ''x'')) ELSE NULL END, ''A'', ''a'') AS text_f, FIELD(CASE WHEN ? THEN ANY_VALUE(NULLIF(?, 1)) ELSE NULL END, ''A'', ''a'') AS numeric_f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition, @field_value, @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;

-- Window outputs consume bare marker declarations; string functions consume
-- the BLOB declaration of a parameter-dependent composite NULL.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN LAG(?,0) OVER () ELSE NULL END,''A'',''a'') AS lag_f, FIELD(CASE WHEN ? THEN LEAD(?,0) OVER () ELSE NULL END,''A'',''a'') AS lead_f, FIELD(CASE WHEN ? THEN CONCAT(COALESCE(?,NULL)) ELSE NULL END,''A'',''a'') AS concat_f, FIELD(CASE WHEN ? THEN LOWER(COALESCE(?,NULL)) ELSE NULL END,''A'',''a'') AS lower_f, FIELD(CASE WHEN ? THEN REVERSE(COALESCE(?,NULL)) ELSE NULL END,''A'',''a'') AS reverse_f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value;
SET @field_value = _binary 'a';
EXECUTE p_field_history USING @field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value,@field_condition,@field_value;
DEALLOCATE PREPARE p_field_history;

CREATE DATABASE field_history_dependencies;
CREATE TABLE field_history_dependencies.t(i INT);
INSERT INTO field_history_dependencies.t VALUES(1);
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f FROM field_history_dependencies.t';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
CREATE TABLE field_history_dependencies.unrelated(i INT);
EXECUTE p_field_history USING @field_condition, @field_value;
DROP TABLE field_history_dependencies.unrelated;
EXECUTE p_field_history USING @field_condition, @field_value;
-- A real captured-table schema change still starts a fresh type generation.
ALTER TABLE field_history_dependencies.t ADD COLUMN j INT;
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;
-- Rebinding to a temporary shadow changes dependency identity even though the
-- original persistent table's schema version did not change.
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f FROM field_history_dependencies.t';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
CREATE TEMPORARY TABLE field_history_dependencies.t(i INT);
INSERT INTO field_history_dependencies.t VALUES(2);
EXECUTE p_field_history USING @field_condition, @field_value;
DROP TEMPORARY TABLE field_history_dependencies.t;
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;
DROP DATABASE field_history_dependencies;

-- Plan invalidation preserves type history; restore the session settings.
-- Plan validity changes without a matching type dependency must retain FIELD
-- history, including a mode switch back to the original setting.
SET @field_old_sql_mode = @@sql_mode;
SET @field_old_div_precision = @@div_precision_increment;
SET SESSION sql_mode = '';
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f';
SET @field_condition = 1, @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY';
EXECUTE p_field_history USING @field_condition, @field_value;
SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION';
EXECUTE p_field_history USING @field_condition, @field_value;
SET SESSION sql_mode = '';
EXECUTE p_field_history USING @field_condition, @field_value;
SET SESSION div_precision_increment = 8;
EXECUTE p_field_history USING @field_condition, @field_value;
SET SESSION div_precision_increment = @field_old_div_precision;
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;
SET SESSION sql_mode = @field_old_sql_mode;

-- A mode rebuild must use runtime unsigned domains, not signed seed markers.
-- The reused FIELD query agrees with a fresh handle and a non-FIELD projection.
SET SESSION sql_mode = '';
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN 0-? ELSE NULL END,0,1) AS f';
SET @field_condition = 1, @field_unsigned = CAST(0 AS UNSIGNED);
EXECUTE p_field_history USING @field_condition,@field_unsigned;
SET SESSION sql_mode = 'NO_UNSIGNED_SUBTRACTION';
SET @field_unsigned = CAST(1 AS UNSIGNED);
EXECUTE p_field_history USING @field_condition,@field_unsigned;
DEALLOCATE PREPARE p_field_history;
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN 0-? ELSE NULL END,0,1) AS f';
EXECUTE p_field_history USING @field_condition,@field_unsigned;
DEALLOCATE PREPARE p_field_history;
PREPARE p_field_history FROM 'SELECT CASE WHEN ? THEN 0-? ELSE NULL END AS difference';
EXECUTE p_field_history USING @field_condition,@field_unsigned;
DEALLOCATE PREPARE p_field_history;

-- At maximum DECIMAL precision, changing the increment changes neither the
-- result declaration nor the value and must not start a new FIELD generation.
SET SESSION div_precision_increment = 4;
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END,''A'',''a'') AS f, CAST(? AS DECIMAL(65,30))/CAST(1 AS DECIMAL(65,30)) AS d';
SET @field_value = NULL, @field_decimal = 5;
EXECUTE p_field_history USING @field_condition,@field_value,@field_decimal;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition,@field_value,@field_decimal;
SET SESSION div_precision_increment = 8;
EXECUTE p_field_history USING @field_condition,@field_value,@field_decimal;
SET SESSION div_precision_increment = 0;
EXECUTE p_field_history USING @field_condition,@field_value,@field_decimal;
DEALLOCATE PREPARE p_field_history;
SET SESSION div_precision_increment = @field_old_div_precision;
SET SESSION sql_mode = @field_old_sql_mode;

SET @field_old_locale = @@lc_time_names;
SET @field_old_prefilter = @@enable_vector_prefilter_by_default;
SET @field_old_auto_mode = @@enable_vector_auto_mode_by_default;
SET lc_time_names = 'en_US';
SET enable_vector_prefilter_by_default = 0;
SET enable_vector_auto_mode_by_default = 0;
PREPARE p_field_history FROM 'SELECT FIELD(CASE WHEN ? THEN ? ELSE NULL END, ''A'', ''a'') AS f';
SET @field_value = NULL;
EXECUTE p_field_history USING @field_condition, @field_value;
SET @field_value = 'a';
EXECUTE p_field_history USING @field_condition, @field_value;
SET lc_time_names = 'fr_FR';
EXECUTE p_field_history USING @field_condition, @field_value;
SET enable_vector_prefilter_by_default = 1;
EXECUTE p_field_history USING @field_condition, @field_value;
SET enable_vector_auto_mode_by_default = 1;
EXECUTE p_field_history USING @field_condition, @field_value;
-- This statement has no table dependency. Unrelated DDL cannot reset its types.
CREATE DATABASE field_history_unrelated_ddl;
CREATE TABLE field_history_unrelated_ddl.t(i INT);
EXECUTE p_field_history USING @field_condition, @field_value;
DROP DATABASE field_history_unrelated_ddl;
EXECUTE p_field_history USING @field_condition, @field_value;
DEALLOCATE PREPARE p_field_history;
SET lc_time_names = @field_old_locale;
SET enable_vector_prefilter_by_default = @field_old_prefilter;
SET enable_vector_auto_mode_by_default = @field_old_auto_mode;
