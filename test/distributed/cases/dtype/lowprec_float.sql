-- @suite
-- @case
-- @desc: scalar low-precision float types bf16/float16/float8/float4 (#20567)
-- @label:bvt

DROP DATABASE IF EXISTS lowprec_float;
CREATE DATABASE lowprec_float;
USE lowprec_float;

-- DDL: all four types in one table.
CREATE TABLE t (id INT PRIMARY KEY, a bf16, b float16, c float8, d float4);
INSERT INTO t VALUES (1, 1.5, 1.5, 1.5, 1.5), (2, -2.0, -2.0, -2.0, -2.0), (3, 0, 0, 0, 0), (4, NULL, NULL, NULL, NULL);
SELECT id, a, b, c, d FROM t ORDER BY id;

-- Values exactly representable in all four formats round-trip.
DROP TABLE IF EXISTS r;
CREATE TABLE r (id INT PRIMARY KEY, v float4);
INSERT INTO r VALUES (1, 0.5), (2, 1), (3, 1.5), (4, 2), (5, 3), (6, 4), (7, 6), (8, -6);
SELECT id, v FROM r ORDER BY id;

-- ORDER BY orders by float VALUE (negatives before positives), not raw bits.
SELECT id, a FROM t WHERE a IS NOT NULL ORDER BY a;
SELECT id, v FROM r ORDER BY v DESC;

-- Comparison predicates.
SELECT id FROM t WHERE a < 0 ORDER BY id;
SELECT id FROM r WHERE v >= 2 ORDER BY id;

-- Arithmetic widens to float (bf16 + bf16, mixed with int/float).
SELECT id, a + b AS ab, c * 2 AS c2, d - 1 AS dm FROM t WHERE id = 1;

-- Aggregates: SUM/AVG widen to double; MIN/MAX to float32.
SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM r;
SELECT COUNT(a), MIN(a), MAX(a) FROM t;

-- CAST both directions.
SELECT CAST(3.5 AS bf16) AS bf, CAST(2 AS float8) AS f8, CAST(1.5 AS float4) AS f4;
SELECT CAST(a AS DOUBLE) AS ad, CAST(c AS INT) AS ci FROM t WHERE id = 1;
SELECT CAST('4' AS float8) AS s8;

-- Out-of-range string cast is rejected (float4 max is 6, float8 max is 448).
SELECT CAST('7' AS float4);
SELECT CAST('1000' AS float8);

-- LOAD via INSERT ... SELECT round-trips through storage.
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (id INT PRIMARY KEY, a bf16, c float8);
INSERT INTO t2 SELECT id, a, c FROM t WHERE a IS NOT NULL;
SELECT id, a, c FROM t2 ORDER BY id;

DROP DATABASE lowprec_float;
