-- @suit
-- @case
-- @test issue 28603 IGNORE_SPACE controls whitespace-sensitive built-ins
-- @label:bvt

DROP DATABASE IF EXISTS issue_28603_ignore_space;
CREATE DATABASE issue_28603_ignore_space;
USE issue_28603_ignore_space;
CREATE TABLE src(a INT);
INSERT INTO src VALUES (10), (20);

SET @issue_28603_saved_sql_mode = @@session.sql_mode;
SET SESSION sql_mode = 'STRICT_TRANS_TABLES';

SELECT NOW () IS NOT NULL AS now_ok;
SELECT SUBSTRING ('abcdef', 2, 3) AS substring_ok;
SELECT COUNT (*) FROM src;
SELECT SUM (a) FROM src;
SELECT DATE_ADD ('2024-01-01', INTERVAL 1 DAY) AS date_add_ok;
SELECT ABS (-2) AS abs_ok, MOD (5, 2) AS mod_ok;

SET SESSION sql_mode = 'STRICT_TRANS_TABLES,IGNORE_SPACE';

SELECT NOW () IS NOT NULL AS now_ok;
SELECT SUBSTRING ('abcdef', 2, 3) AS substring_ok;
SELECT COUNT (*) = 2 AS count_ok FROM src;
SELECT SUM (a) = 30 AS sum_ok FROM src;
SELECT DATE_ADD ('2024-01-01', INTERVAL 1 DAY) = '2024-01-02' AS date_add_ok;
SELECT ABS (-2) AS abs_ok, MOD (5, 2) AS mod_ok;
CREATE TABLE issue_28603_ignore_space.count(a INT);
INSERT INTO issue_28603_ignore_space.count VALUES (30);
SELECT issue_28603_ignore_space.count.a AS qualified_count FROM issue_28603_ignore_space.count;
DROP TABLE issue_28603_ignore_space.count;

PREPARE issue_28603_p FROM 'SELECT COUNT (*) = 2 AS count_ok FROM src';
SET SESSION sql_mode = 'STRICT_TRANS_TABLES';
EXECUTE issue_28603_p;
DEALLOCATE PREPARE issue_28603_p;

SET SESSION sql_mode = 'STRICT_TRANS_TABLES,IGNORE_SPACE';
DROP VIEW IF EXISTS issue_28603_v;
CREATE VIEW issue_28603_v AS SELECT COUNT (*) = 2 AS count_ok FROM src;
SET SESSION sql_mode = 'STRICT_TRANS_TABLES';
SELECT * FROM issue_28603_v;
DROP VIEW issue_28603_v;

SET SESSION sql_mode = @issue_28603_saved_sql_mode;
DROP DATABASE issue_28603_ignore_space;
