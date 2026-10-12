DROP DATABASE IF EXISTS integer_assignment_rounding;
CREATE DATABASE integer_assignment_rounding;
USE integer_assignment_rounding;
CREATE TABLE dst(id INT PRIMARY KEY, v BIGINT);
-- Exact literals round half away from zero; approximate literals round ties to even.
INSERT INTO dst VALUES (1,2.5),(2,-2.5),(3,3.5),(4,-3.5);
SELECT * FROM dst ORDER BY id;
INSERT INTO dst VALUES (1,2.5E0),(2,-2.5E0),(3,3.5E0),(4,-3.5E0)
ON DUPLICATE KEY UPDATE v=VALUES(v);
SELECT * FROM dst ORDER BY id;
-- Parameter source types survive SQL text transport and repeated execution.
PREPARE p FROM 'INSERT INTO dst VALUES (5,?)';
SET @v=2.5;
EXECUTE p USING @v;
SELECT v FROM dst WHERE id=5;
DELETE FROM dst WHERE id=5;
SET @v=CAST(2.5 AS DOUBLE);
EXECUTE p USING @v;
SELECT v FROM dst WHERE id=5;
DELETE FROM dst WHERE id=5;
SET @v=CAST(-2.5 AS DECIMAL(5,1));
EXECUTE p USING @v;
SELECT v FROM dst WHERE id=5;
DELETE FROM dst WHERE id=5;
SET @v=7;
EXECUTE p USING @v;
SELECT v FROM dst WHERE id=5;
DEALLOCATE PREPARE p;
-- Existing typed assignments and explicit CAST remain independent controls.
INSERT INTO dst SELECT 6,CAST(2.5 AS DECIMAL(5,1));
UPDATE dst SET v=CAST(-2.5 AS DOUBLE) WHERE id=6;
SELECT v FROM dst WHERE id=6;
SELECT CAST(2.5 AS SIGNED),CAST(-2.5 AS SIGNED),CAST(2.5E0 AS SIGNED),CAST(-2.5E0 AS SIGNED);
-- Exact unsigned assignments round before destination range checks.
CREATE TABLE unsigned_dst(v BIGINT UNSIGNED);
INSERT INTO unsigned_dst VALUES (2.5),(CAST(2.5 AS DECIMAL(30,1))),(CAST(2.5 AS DECIMAL(65,1)));
SELECT v FROM unsigned_dst ORDER BY v;
CREATE TABLE tiny_unsigned_dst(v TINYINT UNSIGNED);
INSERT INTO tiny_unsigned_dst VALUES (2.5),(255.5);
SELECT COUNT(*) FROM tiny_unsigned_dst;
-- No-key ODKU uses the legacy VALUES binder; parentheses must not change types.
CREATE TABLE no_key_dst(v BIGINT);
INSERT INTO no_key_dst VALUES ((2.5)),((-2.5)) ON DUPLICATE KEY UPDATE v=VALUES(v);
SELECT v FROM no_key_dst ORDER BY v;
DROP DATABASE integer_assignment_rounding;
