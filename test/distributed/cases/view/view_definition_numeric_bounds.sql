-- @suite
-- @case
DROP DATABASE IF EXISTS view_definition_numeric_bounds;
CREATE DATABASE view_definition_numeric_bounds;
USE view_definition_numeric_bounds;
CREATE TABLE source_table (id INT);
CREATE VIEW valid_view AS SELECT CAST(id AS CHAR(20)) AS label FROM source_table;
DESC valid_view;
CREATE VIEW invalid_width AS SELECT CAST(1 AS CHAR(18446744073709551615));
CREATE VIEW invalid_sample AS SELECT SAMPLE(*, 18446744073709551615 ROWS) FROM source_table;
CREATE TABLE invalid_decimal (a DECIMAL(18446744073709551615, 2));
CREATE TABLE invalid_timestamp (a TIMESTAMP(18446744073709551615));
CREATE TABLE invalid_float (a FLOAT(18446744073709551615, 2));
CREATE VIEW invalid_result AS SELECT * FROM result_scan() AS missing_query;
-- Errors must leave the connection and the normal parser usable.
SELECT 18446744073709551615 AS unsigned_max;
INSERT INTO source_table VALUES (12);
SELECT * FROM valid_view;
SHOW TABLES;
DROP DATABASE view_definition_numeric_bounds;
-- @suite
