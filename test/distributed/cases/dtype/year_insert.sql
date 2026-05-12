-- @suit
-- @case
-- @desc: insert / select for YEAR columns
-- @label:bvt
--
-- Note: mo-tester's JDBC driver renders YEAR as `YYYY-01-01` (java.sql.Date)
-- rather than MySQL's bare `YYYY`. This is a client-side display artifact, not
-- a server bug. The CAST(... AS UNSIGNED) assertions below lock down the
-- numeric semantics independently of that rendering. If a future mo-tester
-- version changes the YEAR display, update the `SELECT *` baseline in the
-- companion .result file accordingly.

DROP DATABASE IF EXISTS test_year;
CREATE DATABASE test_year;
USE test_year;

CREATE TABLE t_year (
    id INT PRIMARY KEY,
    birth_year YEAR,
    grad_year YEAR
);

INSERT INTO t_year VALUES (1, 2024, 2023);
INSERT INTO t_year VALUES (2, 99, 1999);
INSERT INTO t_year VALUES (3, 1901, 2155);

SELECT * FROM t_year ORDER BY id;

-- Cast the YEAR value to UNSIGNED so the check does not depend on
-- client-side YEAR-as-DATE rendering. This locks down the numeric
-- semantics the storage layer must preserve (2-digit year -> 1999,
-- boundary values 1901 / 2155).
SELECT id, CAST(birth_year AS UNSIGNED) AS by_num, CAST(grad_year AS UNSIGNED) AS gy_num
FROM t_year ORDER BY id;

-- ORDER BY on the YEAR column exercises the T_year arms in the planner
-- sort path as well as the Compare / InplaceSort code in the vector
-- layer. (WHERE-on-YEAR is deliberately omitted — 3.0-dev has no
-- comparison operator overload for T_year vs integer literals, and
-- fixing that is tracked separately.)
SELECT id, CAST(birth_year AS UNSIGNED) AS by_num
FROM t_year ORDER BY birth_year DESC;

UPDATE t_year SET grad_year = 2026 WHERE id = 1;
SELECT id, CAST(grad_year AS UNSIGNED) AS gy_num FROM t_year WHERE id = 1;

DELETE FROM t_year WHERE id = 3;
SELECT id FROM t_year ORDER BY id;

DROP DATABASE test_year;
