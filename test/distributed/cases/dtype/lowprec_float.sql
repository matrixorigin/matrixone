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

-- Out-of-range STRING cast is rejected (float4 max is 6, float8 max is 448).
SELECT CAST('7' AS float4);
SELECT CAST('1000' AS float8);

-- Out-of-range NUMERIC cast is rejected too (not silently saturated); non-finite floats
-- must never be persisted.
SELECT CAST(7 AS float4);
SELECT CAST(1000 AS float8);
SELECT CAST(70000 AS float16);
SELECT CAST(1e300 AS bf16);

-- Inf / NaN are rejected on cast (bf16/float16 would overflow to Inf; float8 has a NaN slot).
SELECT CAST('inf' AS float8);
SELECT CAST('nan' AS bf16);
SELECT CAST('-inf' AS float16);
-- Arithmetic that overflows float64 to Inf, then cast, is rejected.
SELECT CAST(1e308 * 100 AS bf16);

-- Arithmetic result overflowing the narrow range is rejected on cast-back.
SELECT CAST(6 * 2 AS float4);
DROP TABLE IF EXISTS t2_ovf;
CREATE TABLE t2_ovf (id INT PRIMARY KEY, a float4, b float4);
INSERT INTO t2_ovf VALUES (1, 3, 4);
SELECT CAST(a + b AS float4) FROM t2_ovf;

-- Subnormal handling: a value between the largest float8 subnormal (0.013671875) and the
-- smallest normal (0.015625) rounds UP to the smallest normal, not silently to zero.
SELECT CAST(0.015 AS float8);
-- Smallest float8 subnormal is 2^-9 = 0.001953125; a smaller magnitude underflows to 0.
SELECT CAST(0.001953125 AS float8), CAST(0.0001 AS float8);
-- float4 subnormal 0.5 round-trips.
SELECT CAST(0.5 AS float4), CAST(0.25 AS float4);

-- decimal256 -> low-precision float.
DROP TABLE IF EXISTS d256;
CREATE TABLE d256 (id INT PRIMARY KEY, v decimal(40,2));
INSERT INTO d256 VALUES (1, 2.00), (2, -3.50);
SELECT id, CAST(v AS bf16) AS b, CAST(v AS float8) AS c FROM d256 ORDER BY id;

-- LOAD via INSERT ... SELECT round-trips through storage.
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (id INT PRIMARY KEY, a bf16, c float8);
INSERT INTO t2 SELECT id, a, c FROM t WHERE a IS NOT NULL;
SELECT id, a, c FROM t2 ORDER BY id;

-- engine paths: top-k, window functions, peer groups, -0, widening
CREATE TABLE lp (id INT PRIMARY KEY, a bf16, b float16, c float8, d float4);
INSERT INTO lp VALUES (1, 1, 1, 1, 1), (2, 2, 2, 2, 2), (3, 0, 0, 0, 0), (4, -0.1, -0.1, -0.1, -0.1), (5, NULL, NULL, NULL, NULL), (6, 1.5, 1.5, 1.5, 1.5);
SELECT * FROM lp ORDER BY id LIMIT 3;
SELECT id, a FROM lp ORDER BY a DESC, id LIMIT 3;
SELECT id, RANK() OVER (ORDER BY b), LAG(c) OVER (ORDER BY id), LAST_VALUE(d) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM lp ORDER BY id;
SELECT id, SUM(id) OVER (PARTITION BY d ORDER BY id) FROM lp ORDER BY id;
SELECT id, SUM(id) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM lp ORDER BY id;
-- -0.1 rounds to -0 in float4, which groups with 0
SELECT d, COUNT(*) FROM lp WHERE id IN (3, 4) GROUP BY d;
SELECT COUNT(DISTINCT d), COUNT(DISTINCT a) FROM lp WHERE id IN (3, 4);
SELECT ANY_VALUE(a), MEDIAN(b), STDDEV(c), GROUP_CONCAT(d ORDER BY id) FROM lp WHERE id < 3;
SELECT JSON_ARRAYAGG(a), JSON_OBJECTAGG('k', b) FROM lp WHERE id = 2;
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY d) FROM lp;
SELECT COALESCE(a, 0), IFNULL(b, 1), GREATEST(c, 1), IF(d > 1, 'y', 'n'), JSON_OBJECT('k', a) FROM lp ORDER BY id;
SELECT a FROM lp INTERSECT SELECT a FROM lp WHERE id < 3 ORDER BY 1;
SET @v = (SELECT b FROM lp WHERE id = 2);
SELECT @v, @v + 1;
INSERT INTO lp VALUES (6, 1.5, 1.5, 1.5, 1.5) ON DUPLICATE KEY UPDATE a = VALUES(a);
SELECT ROW_COUNT();
UPDATE lp SET a = 3 WHERE id = 6;
SELECT id, a FROM lp WHERE id = 6;
-- a numeric literal compares in the column's precision, rounded as the stored value was
-- (-0.1 is stored as -0.100097656 in bf16); a literal outside the type's range compares
-- as a wider float
SELECT COUNT(*) FROM lp WHERE a = -0.1;
SELECT COUNT(*) FROM lp WHERE a = CAST(-0.1 AS bf16);
SELECT id FROM lp WHERE b IN (-0.1, 1.5) ORDER BY id;
SELECT id FROM lp WHERE c < 0.1 ORDER BY id;
SELECT COUNT(*) FROM lp WHERE c < 1000;
SELECT id FROM lp WHERE b IN (CAST(1 AS float16), CAST(2 AS float16)) ORDER BY id;
SELECT id FROM lp WHERE c >= CAST(1 AS float8) AND d <> CAST(2 AS float4) ORDER BY id;
SELECT id FROM lp WHERE a <=> CAST(NULL AS bf16);
DROP TABLE lp;

-- low-precision floats cannot be key parts
CREATE TABLE kp (k bf16 PRIMARY KEY, v INT);
CREATE TABLE ku (id INT PRIMARY KEY, k float16 UNIQUE KEY);
CREATE TABLE kc (k float4, j INT, PRIMARY KEY (k, j));
CREATE TABLE ki (id INT PRIMARY KEY, k float8, KEY ik (k));
CREATE TABLE kb (id INT, k float8) CLUSTER BY (k);
CREATE INDEX ia ON t2 (a);

-- a value wider than float32 rounds once: just above a tie rounds up
SELECT cast('1.0039062500001' AS bf16), cast(1.0039062500001 AS bf16), cast(cast(1.0039062500001 AS double) AS bf16);
SELECT cast('1.00048828125001' AS float16), cast('1.0625000001' AS float8), cast('1.2500000001' AS float4);
-- text with more digits than float64, decimals and integers above 2^53 also round once
SELECT cast('1.00390625000000000001' AS bf16), cast(1.00390625000000000001 AS bf16), cast(1157425104234217473 AS bf16);
SELECT cast('[1.0039062500001]' AS vecbf16(1)), cast('[1.00048828125001]' AS vecf16(1)), cast(cast('[1.0039062500001]' AS vecf64(1)) AS vecbf16(1));
-- digits beyond float64 and beyond any fixed working precision decide a tie: 1.0625 is the
-- float8 midpoint of 1 and 1.125
SELECT cast('1.062500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001' AS float8), cast('-1.062500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001' AS float8), cast('1.06250000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' AS float8);
-- an out-of-range error names the value given
SELECT cast('448.0000001' AS float8);
SELECT cast('1e39' AS bf16);
CREATE TABLE tr (b bf16);
INSERT INTO tr VALUES ('1.0039062500001'), (1.0039062500001);
SELECT b FROM tr;
-- a numeral text literal rounds to the column type, as a numeric literal does
CREATE TABLE tx (e float8, f bf16, h float16);
INSERT INTO tx VALUES ('0.3', '0.1', '0.1');
SELECT count(*) FROM tx WHERE e = '0.3';
SELECT count(*) FROM tx WHERE f = '0.1';
SELECT count(*) FROM tx WHERE h = '0.1';
SELECT count(*) FROM tx WHERE e BETWEEN '0.3' AND '0.3';
SELECT count(*) FROM tx WHERE e IN ('0.3', '7');
SELECT count(*) FROM tx WHERE e NOT IN ('0.3', '7');
SELECT count(*) FROM tx WHERE e IN ('abc', '7');
-- a prepared IN list with one value outside the type's range
CREATE TABLE tw (id INT, f bf16, e float8);
INSERT INTO tw VALUES (1, 0.99, 0.3), (2, 1.5, 2), (3, 1e30, 4);
SELECT count(*) FROM tw WHERE f IN (0.99, 1e39);
PREPARE pw FROM 'SELECT count(*) FROM tw WHERE f IN (?, ?)';
PREPARE nw FROM 'SELECT count(*) FROM tw WHERE f NOT IN (?, ?)';
PREPARE ew FROM 'SELECT count(*) FROM tw WHERE e IN (?, ?)';
SET @x = 0.99;
SET @y = 1e39;
EXECUTE pw USING @x, @y;
EXECUTE nw USING @x, @y;
SET @a = 0.3;
SET @b = 1000;
EXECUTE ew USING @a, @b;
DEALLOCATE PREPARE pw;
DEALLOCATE PREPARE nw;
DEALLOCATE PREPARE ew;

-- prepared parameters round to the column type when the value is in range
CREATE TABLE pp (id INT, b bf16, h float16, e float8, q float4);
INSERT INTO pp VALUES (1, 0.3, 0.3, 0.3, 0.5), (2, 1.5, 1.5, 1.5, 1.5);
PREPARE pb FROM 'SELECT id FROM pp WHERE b IN (?, 9) ORDER BY id';
PREPARE ph FROM 'SELECT id FROM pp WHERE h IN (?, 9) ORDER BY id';
PREPARE pe FROM 'SELECT id FROM pp WHERE e IN (?, 9) ORDER BY id';
PREPARE pq FROM 'SELECT id FROM pp WHERE q = ? ORDER BY id';
PREPARE nb FROM 'SELECT id FROM pp WHERE b NOT IN (?, 9) ORDER BY id';
PREPARE eb FROM 'SELECT id FROM pp WHERE b = ? ORDER BY id';
PREPARE lb FROM 'SELECT id FROM pp WHERE b <= ? ORDER BY id';
SET @v = 0.3;
EXECUTE pb USING @v;
EXECUTE ph USING @v;
EXECUTE pe USING @v;
EXECUTE nb USING @v;
EXECUTE eb USING @v;
EXECUTE lb USING @v;
SET @v = '0.3';
EXECUTE pb USING @v;
EXECUTE eb USING @v;
SET @v = 0.5;
EXECUTE pq USING @v;
SET @v = 1.5;
EXECUTE pb USING @v;
EXECUTE nb USING @v;
SET @v = 1e30;
EXECUTE pb USING @v;
EXECUTE eb USING @v;
EXECUTE lb USING @v;
DEALLOCATE PREPARE pb;
DEALLOCATE PREPARE ph;
DEALLOCATE PREPARE pe;
DEALLOCATE PREPARE pq;
DEALLOCATE PREPARE nb;
DEALLOCATE PREPARE eb;
DEALLOCATE PREPARE lb;

-- NOT IN, !=, <=> and BETWEEN round literals to the column type as = and IN do
CREATE TABLE cmp (a bf16, q float4, h float16);
INSERT INTO cmp VALUES (1.5, 1.1, 1.1);
SELECT a = 1.501, a IN (1.501), a <> 1.501, a != 1.501, a NOT IN (1.501), a NOT IN (1.501, 7), a <=> 1.501, a BETWEEN 1.501 AND 2 FROM cmp;
SELECT h BETWEEN 1.1 AND 2, h >= 1.1 AND h <= 2 FROM cmp;
-- literals rounding to the same value are equal constants
SELECT COUNT(*) FROM cmp WHERE q = 1.1 AND q = 1;
SELECT COUNT(*) FROM cmp WHERE q = 1.1 AND q <> 1;
-- integer and decimal user variables round as literals
SET @i = 1, @d = 1.1, @s = '0x1p0';
PREPARE pi FROM 'SELECT COUNT(*) FROM cmp WHERE q = ?';
EXECUTE pi USING @i;
PREPARE pl FROM 'SELECT COUNT(*) FROM cmp WHERE q IN (?, ?)';
EXECUTE pl USING @d, @i;
EXECUTE pi USING @s;
DEALLOCATE PREPARE pi;
DEALLOCATE PREPARE pl;
-- IF/CASE/COALESCE keep the type; a multi-table UPDATE stores the values
CREATE TABLE m1 (id INT PRIMARY KEY, a float16, b float8, c float4, d bf16);
INSERT INTO m1 VALUES (1, 1.5, 1.5, 1.5, 1.5);
CREATE TABLE m2 (k INT, a float16, b float8, c float4, d bf16);
INSERT INTO m2 VALUES (1, 3.5, 3.5, 3, 3.5);
UPDATE m1 JOIN m2 ON m1.id = m2.k SET m1.a = m2.a, m1.b = m2.b, m1.c = m2.c, m1.d = m2.d;
SELECT * FROM m1;
SELECT IF(a > 1, a, b), COALESCE(NULL, d), IFNULL(d, a), CASE WHEN a > 0 THEN d END, NULLIF(d, 1) FROM m1;
SELECT IF(d, 'y', 'n'), INTERVAL(d, 1, 4), CONV(b, 10, 2) FROM m1;
SELECT BIT_AND(d), BIT_OR(c) FROM m1;
-- data branch diff and merge see a conflicting bf16 value
CREATE TABLE bx (id INT PRIMARY KEY, v bf16, w INT);
CREATE TABLE by2 (id INT PRIMARY KEY, v bf16, w INT);
INSERT INTO bx VALUES (1, 1.5, 1), (2, 2.5, 2);
INSERT INTO by2 VALUES (1, 3.5, 1), (2, 2.5, 2);
DATA BRANCH DIFF by2 AGAINST bx;
DATA BRANCH MERGE by2 INTO bx;

-- a CASE branch casts only the rows it selects
CREATE TABLE sel (a INT, s VARCHAR(20));
INSERT INTO sel VALUES (1, '1.5'), (0, 'invalid');
SELECT a, CASE WHEN a = 1 THEN CAST(s AS bf16) END, CASE WHEN a = 1 THEN CAST(s AS float8) END FROM sel ORDER BY a;

-- two flushed objects with disjoint ranges: zonemap pruning by IN lists, constants and the
-- ORDER BY ... LIMIT top value keeps every matching row
CREATE TABLE zf (id INT, b bf16, h float16, e float8, f float4);
INSERT INTO zf VALUES (1, -6, -6, -6, -6), (2, -3, -3, -3, -3), (3, -1, -1, -1, -1);
-- @ignore:0
SELECT mo_ctl('dn', 'flush', 'lowprec_float.zf');
INSERT INTO zf VALUES (4, 1, 1, 1, 1), (5, 3, 3, 3, 3), (6, 6, 6, 6, 6);
-- @ignore:0
SELECT mo_ctl('dn', 'flush', 'lowprec_float.zf');
SELECT id FROM zf WHERE b IN (-3, 3) ORDER BY id;
SELECT id FROM zf WHERE h IN (-1, 6) ORDER BY id;
SELECT id FROM zf WHERE e IN (-6, 1) ORDER BY id;
SELECT id FROM zf WHERE f IN (4, 6) ORDER BY id;
SELECT id FROM zf WHERE e IN (10, 20) ORDER BY id;
SELECT id FROM zf WHERE b = -1;
SELECT id FROM zf WHERE h > 2 ORDER BY id;
SELECT id FROM zf WHERE f < -2 ORDER BY id;
SELECT id, e FROM zf ORDER BY e DESC LIMIT 2;
SELECT id, b FROM zf ORDER BY b LIMIT 2;
SELECT id, f FROM zf ORDER BY f LIMIT 1;
DROP TABLE zf;

-- a low-precision argument is read as its own value: uuid swap flag, interval value
SELECT hex(uuid_to_bin('6ccd780c-baba-1026-9564-5b8c656024db', cast(1 AS bf16))), hex(uuid_to_bin('6ccd780c-baba-1026-9564-5b8c656024db', cast(0 AS float8)));
SELECT date_add('2020-01-01 00:00:00', INTERVAL cast(1.5 AS bf16) SECOND), date_sub('2020-01-01 00:00:00', INTERVAL cast(0.5 AS float4) SECOND);
SELECT date_add(DATE '2020-01-01', INTERVAL cast(1.5 AS float8) DAY), '2020-01-01 00:00:00' + INTERVAL cast(1.5 AS float16) SECOND;
SELECT json_row(cast(1.5 AS bf16), cast(-2 AS float16), cast(448 AS float8), cast(6 AS float4), cast(NULL AS bf16));
-- arbitrary bits are not a stored low-precision value
SELECT bit_cast(cast(x'803f' AS varbinary(2)) AS bf16), bit_cast(cast(x'0000803f' AS varbinary(4)) AS float);

-- a literal or parameter is in range by the value CAST rounds it to (to odd, from the exact
-- source); one just outside float4 (6) or float8 (448) keeps the wide comparison
CREATE TABLE fp (id INT, f float4, g float8);
INSERT INTO fp VALUES (1, 6, 448), (2, -6, -448);
SELECT count(*) FROM fp WHERE f <= 6.0000001;
SELECT count(*) FROM fp WHERE f >= -6.0000001;
SELECT count(*) FROM fp WHERE g <= 448.000001;
SELECT count(*) FROM fp WHERE g >= -448.000001;
SELECT count(*) FROM fp WHERE f <= '6.0000001';
SELECT count(*) FROM fp WHERE f BETWEEN -6.0000001 AND 6.0000001;
SELECT count(*) FROM fp WHERE f IN (6.0000001, 6, -6);
SELECT count(*) FROM fp WHERE f NOT IN (6.0000001);
SELECT count(*) FROM fp WHERE f <= 6.0000000000000000000000001;
SELECT count(*) FROM fp WHERE f <= '6.0000000000000000000000001';
SELECT count(*) FROM fp WHERE f < 6.0000001 AND f > 5.9999999;
SELECT count(*) FROM fp WHERE f <= 6.1;
SELECT count(*) FROM fp WHERE g <= 449;
SELECT count(*) FROM fp WHERE f <= 6;
SELECT count(*) FROM fp WHERE f = 6;
SELECT count(*) FROM fp WHERE CAST(f AS double) <= 6.0000001;
SET @p = 6.0000001;
PREPARE s FROM 'SELECT count(*) FROM fp WHERE f <= ?';
EXECUTE s USING @p;
SET @p = '6.0000000000000000000000001';
EXECUTE s USING @p;
SET @p = 6;
EXECUTE s USING @p;
DEALLOCATE PREPARE s;
SET @q = -448.000001;
PREPARE s2 FROM 'SELECT count(*) FROM fp WHERE g >= ?';
EXECUTE s2 USING @q;
DEALLOCATE PREPARE s2;
-- CAST and writes still reject a value outside the type
SELECT CAST(6.0000001 AS float4);
INSERT INTO fp VALUES (3, 6.0000001, 0);
SELECT count(*) FROM fp;

-- SAMPLE ... ROWS replaces pooled rows of every low-precision type
CREATE TABLE sp (id INT, b bf16, h float16, e float8, q float4);
INSERT INTO sp SELECT result, result % 7, result % 5, result % 3, result % 2 FROM generate_series(1, 50) g;
SELECT count(*) FROM (SELECT sample(b, 5 rows) FROM sp) x;
SELECT count(*) FROM (SELECT sample(*, 5 rows) FROM sp) x;
SELECT count(*) FROM (SELECT sample(q, 3 rows) FROM sp) x;
-- a RANGE bound would round to the column type: not a RANGE partition column;
-- KEY, HASH and LIST compare by equality and are accepted
CREATE TABLE pr1 (id INT, e float8) PARTITION BY RANGE COLUMNS(e) (PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (100));
CREATE TABLE pr2 (id INT, b bf16) PARTITION BY RANGE(b) (PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (300));
CREATE TABLE pr3 (id INT, h float16) PARTITION BY RANGE((h)) (PARTITION p0 VALUES LESS THAN (0));
CREATE TABLE pr4 (id INT, e float8) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20));
INSERT INTO pr4 VALUES (5, 99), (15, -3);
SELECT count(*) FROM pr4;
CREATE TABLE pk1 (id INT, e float8) PARTITION BY KEY(e) PARTITIONS 2;
INSERT INTO pk1 VALUES (1, -3), (2, 99), (3, -3);
SELECT count(*) FROM pk1 WHERE e = -3;
CREATE TABLE pl1 (id INT, e float8) PARTITION BY LIST COLUMNS(e) (PARTITION p0 VALUES IN (-3, 0), PARTITION p1 VALUES IN (2.5, 99));
INSERT INTO pl1 VALUES (1, -3), (2, 0), (3, 2.5), (4, 99);
SELECT count(*) FROM pl1 WHERE e = 99;

DROP DATABASE lowprec_float;
