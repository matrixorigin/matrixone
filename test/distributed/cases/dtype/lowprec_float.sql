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
-- only a column narrows a parameter, as for a literal
SELECT CAST(1.1 AS FLOAT4) = 1.1;
PREPARE xq FROM 'SELECT CAST(1.1 AS FLOAT4) = ?';
SET @v = 1.1;
EXECUTE xq USING @v;
DEALLOCATE PREPARE xq;
-- a numeral beyond DECIMAL(38) narrows by its exact value
SELECT id FROM pp WHERE b = 0.30000000000000000000000000000000000000001;
SELECT id FROM pp WHERE b = CAST(0.30000000000000000000000000000000000000001 AS BF16);

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
SELECT a, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL cast(1.5 AS bf16) DAY PRECEDING AND CURRENT ROW) c FROM (SELECT 1 a, DATE '2020-01-01' d UNION ALL SELECT 2, DATE '2020-01-02') t ORDER BY a;
SELECT a, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL cast(1 AS float4) DAY PRECEDING AND CURRENT ROW) c FROM (SELECT 1 a, DATE '2020-01-01' d UNION ALL SELECT 2, DATE '2020-01-03') t ORDER BY a;
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

-- float16 (±65504) and bf16: a value just above the maximum rounds back to it under
-- round-to-nearest-even, and is still outside the type: CAST rejects it and a comparison
-- with it stays wide
CREATE TABLE fh (id INT, k float16, b bf16);
INSERT INTO fh VALUES (1, 65504, 3.38e38), (2, -65504, -3.38e38);
SELECT count(*) FROM fh WHERE k < 65505;
SELECT count(*) FROM fh WHERE k = 65505;
SELECT count(*) FROM fh WHERE k > -65505;
SELECT count(*) FROM fh WHERE k <= 65504;
SELECT count(*) FROM fh WHERE k IN (65505, 65504);
SELECT count(*) FROM fh WHERE b < 3.39e38;
SELECT count(*) FROM fh WHERE b > -3.39e38;
SET @h = 65505;
PREPARE s3 FROM 'SELECT count(*) FROM fh WHERE k < ?';
EXECUTE s3 USING @h;
SET @h = 65504;
EXECUTE s3 USING @h;
DEALLOCATE PREPARE s3;
SELECT CAST(65504 AS float16), CAST(-65504 AS float16);
SELECT CAST(65505 AS float16);
SELECT CAST(-65505 AS float16);
SELECT CAST('65504.5' AS float16);
SELECT CAST(3.39e38 AS bf16);
-- a bf16 is checked by its float32 value: its printed maximum loads back
SELECT CAST(CAST('3.3895314e+38' AS bf16) AS char), CAST(CAST('-3.3895314e+38' AS bf16) AS char);
SELECT CAST('3.3895316e+38' AS bf16);
-- the printed maximum also matches the stored value in a comparison, as a literal and as a
-- prepared parameter
CREATE TABLE bmax (id int, f bf16);
INSERT INTO bmax VALUES (1, 3.3895314e38), (2, -3.3895314e38), (3, 1);
SELECT id FROM bmax WHERE f = 3.3895314e38;
SELECT id FROM bmax WHERE f = -3.3895314e38;
SELECT id FROM bmax WHERE f <=> 3.3895314e38;
SELECT id FROM bmax WHERE f = '3.3895314e+38';
SELECT id FROM bmax WHERE f IN (3.3895314e38, -3.3895314e38) ORDER BY id;
SELECT count(*) FROM bmax WHERE f = 3.3895316e38;
PREPARE bm FROM 'SELECT id FROM bmax WHERE f = ?';
SET @bm = 3.3895314e38;
EXECUTE bm USING @bm;
SET @bm = -3.3895314e38;
EXECUTE bm USING @bm;
SET @bm = '3.3895314e+38';
EXECUTE bm USING @bm;
SET @bm = '-3.3895314e+38';
EXECUTE bm USING @bm;
DEALLOCATE PREPARE bm;
INSERT INTO fh VALUES (3, 65505, 0);
SELECT count(*) FROM fh;
-- the same bound for vecf16 and vecbf16 elements, from text and from a wider vector
SELECT CAST('[65504,-65504]' AS vecf16(2));
SELECT CAST('[65505]' AS vecf16(1));
SELECT CAST(CAST('[65505]' AS vecf32(1)) AS vecf16(1));
SELECT CAST('[3.39e38]' AS vecbf16(1));
SELECT CAST(CAST('[-3.39e38]' AS vecf32(1)) AS vecbf16(1));

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

-- TIMESTAMPADD SECOND widens a narrow float count to float32
SELECT TIMESTAMPADD(SECOND, CAST(1.5 AS bf16), '2020-01-01 00:00:00'), TIMESTAMPADD(SECOND, CAST(2.25 AS float16), '2020-01-01 00:00:00'), TIMESTAMPADD(SECOND, CAST(1.5 AS float8), '2020-01-01 00:00:00'), TIMESTAMPADD(SECOND, CAST(1.5 AS float4), '2020-01-01 00:00:00');

-- ADD COLUMN ... NOT NULL without a default backfills 0 on a populated table
CREATE TABLE addnn (id INT PRIMARY KEY, x INT);
INSERT INTO addnn VALUES (1, 10), (2, 20);
ALTER TABLE addnn ADD COLUMN b bf16 NOT NULL, ALGORITHM=COPY;
ALTER TABLE addnn ADD COLUMN h float16 NOT NULL, ALGORITHM=COPY;
ALTER TABLE addnn ADD COLUMN e float8 NOT NULL, ALGORITHM=COPY;
ALTER TABLE addnn ADD COLUMN q float4 NOT NULL, ALGORITHM=COPY;
ALTER TABLE addnn ADD COLUMN b2 bf16 NOT NULL, ADD COLUMN q2 float4 NOT NULL;
SELECT * FROM addnn ORDER BY id;
DROP TABLE addnn;

-- JSON compared with a number keeps its category, as for float: a JSON string is not the number
CREATE TABLE jn (id INT, d JSON, f FLOAT, b bf16, h float16, e float8, q float4);
INSERT INTO jn VALUES (1, '1.5', 1.5, 1.5, 1.5, 1.5, 1.5), (2, '"1.5"', 1.5, 1.5, 1.5, 1.5, 1.5), (3, '0.3', 0.3, 0.3, 0.3, 0.3, 0.5), (4, '-2', -2, -2, -2, -2, -2);
SELECT id, d = f, d = b, d = h, d = e, d = q, b = d, d <=> q, d < b, d >= e FROM jn ORDER BY id;
SELECT id FROM jn WHERE d = CAST(1.5 AS bf16) ORDER BY id;
SELECT id FROM jn WHERE d = CAST(1.5 AS FLOAT) ORDER BY id;
-- a JSON number outside the type's finite range is an error, as CAST rejects it
SELECT id FROM jn WHERE CAST('7' AS JSON) = q;
DROP TABLE jn;

-- a user variable holding a narrow float computes in DOUBLE, as for FLOAT
CREATE TABLE uv (id INT, a bf16, f FLOAT);
INSERT INTO uv VALUES (1, 1.5, 1.5);
SET @y = CAST(1.5 AS bf16);
SET @yf = CAST(1.5 AS FLOAT);
SELECT @y + 1, @y + 0.25, @y * 2, @yf + 1, @yf + 0.25, @yf * 2;
SET @z = CAST(0.5 AS float8);
SELECT id FROM uv WHERE a = @z + 1;
SELECT id FROM uv WHERE f = @z + 1;
DROP TABLE uv;

-- a constant expression compared with the column is folded and rounded as a literal is
CREATE TABLE ce (a bf16, h float16, q float4, f FLOAT);
INSERT INTO ce VALUES (0.1, 0.1, 1, 0.1);
SELECT COUNT(*) FROM ce WHERE a = ABS(-0.1);
SELECT COUNT(*) FROM ce WHERE h = ABS(-0.1);
SELECT COUNT(*) FROM ce WHERE q IN (ABS(-1.1));
SELECT COUNT(*) FROM ce WHERE f = ABS(-0.1);
DROP TABLE ce;

-- hex and bit literals are unsigned integers, as for FLOAT
SELECT CAST(0x05 AS bf16), CAST(x'01' AS float4), CAST(b'101' AS float8), CAST(0x05 AS FLOAT);
SELECT CAST(0x10 AS float4);
CREATE TABLE hx (a bf16 DEFAULT 0x02, q float8);
INSERT INTO hx (q) VALUES (0x03);
SELECT * FROM hx;
DROP TABLE hx;

-- JSON, YEAR and ENUM convert as they convert to FLOAT
SELECT CAST(CAST('1.5' AS JSON) AS bf16), CAST(CAST(2024 AS YEAR) AS bf16), CAST(CAST('1.5' AS JSON) AS FLOAT);
CREATE TABLE src (e ENUM('a','b','c'), j JSON, y YEAR);
INSERT INTO src VALUES ('c', '2.5', 2024);
SELECT CAST(e AS float4), CAST(j AS float8), CAST(y AS float16), CAST(e AS FLOAT) FROM src;
CREATE TABLE dst (b bf16, q float4);
INSERT INTO dst SELECT j, e FROM src;
SELECT * FROM dst;
SELECT CAST(CAST('7' AS JSON) AS float4);
DROP TABLE src;
DROP TABLE dst;

-- text that is not a number converts to its numeric prefix with a warning, as for FLOAT;
-- a prefix outside the type's finite range is an error
SET sql_mode = '';
CREATE TABLE txt (id INT, b bf16, f FLOAT);
INSERT INTO txt VALUES (1, 'abc', 'abc');
INSERT INTO txt VALUES (2, '1abc', '1abc');
INSERT INTO txt VALUES (3, '', '');
INSERT IGNORE INTO txt VALUES (4, ' 2.5x', ' 2.5x');
SELECT * FROM txt ORDER BY id;
INSERT INTO txt VALUES (5, '1e39x', 1);
DROP TABLE txt;
SET sql_mode = DEFAULT;

-- serial keeps the stored bits; serial_extract restores them; the encoding orders as the values
SELECT HEX(SERIAL(CAST(1.5 AS bf16), CAST(-2 AS float16), CAST(-0.5 AS float8), CAST(3 AS float4)));
SELECT SERIAL_EXTRACT(SERIAL(CAST(1.5 AS bf16), CAST(-2 AS float16), CAST(-0.5 AS float8), CAST(3 AS float4)), 0 AS bf16),
       SERIAL_EXTRACT(SERIAL(CAST(1.5 AS bf16), CAST(-2 AS float16), CAST(-0.5 AS float8), CAST(3 AS float4)), 1 AS float16),
       SERIAL_EXTRACT(SERIAL(CAST(1.5 AS bf16), CAST(-2 AS float16), CAST(-0.5 AS float8), CAST(3 AS float4)), 2 AS float8),
       SERIAL_EXTRACT(SERIAL(CAST(1.5 AS bf16), CAST(-2 AS float16), CAST(-0.5 AS float8), CAST(3 AS float4)), 3 AS float4);
SELECT SERIAL_FULL(CAST(1.5 AS bf16), NULL) IS NULL, SERIAL(CAST(1.5 AS bf16), NULL) IS NULL;
CREATE TABLE so (v float4);
INSERT INTO so VALUES (6), (-1.5), (0.5), (-6), (0), (4), (-0.5), (1);
SELECT v FROM so ORDER BY SERIAL(v);
DROP TABLE so;

-- NUMERIC_PRECISION is the significand bits
CREATE TABLE np (a bf16, b float16, c float8, d float4, f FLOAT);
SELECT column_name, numeric_precision FROM information_schema.columns WHERE table_schema = 'lowprec_float' AND table_name = 'np' ORDER BY ordinal_position;
DROP TABLE np;

-- text converts as FLOAT does: radix literals are their integer value, other text its decimal prefix
SELECT CAST('0x10' AS FLOAT), CAST('0x10' AS bf16), CAST('-0x10' AS float16), CAST('0b101' AS float8), CAST('0b11' AS float4), CAST('0o7' AS bf16);
SELECT CAST('inf' AS FLOAT), CAST('inf' AS bf16), CAST('-inf' AS float16), CAST('nan' AS float8), CAST('nan' AS float4);
SELECT CAST('1.5abc' AS FLOAT), CAST('1.5abc' AS bf16), CAST('1.5abc' AS float16), CAST('1.5abc' AS float8), CAST('1.5abc' AS float4);
SELECT CAST('0x1.1p0' AS bf16);

-- truncated text in an assignment: an error in strict mode, a warning with IGNORE or without strict mode
CREATE TABLE ta (id INT PRIMARY KEY, f FLOAT, a bf16, b float16, c float8, d float4);
SET sql_mode = 'STRICT_TRANS_TABLES';
INSERT INTO ta (id, f) VALUES (1, '1.5abc');
INSERT INTO ta (id, a) VALUES (1, '1.5abc');
INSERT INTO ta (id, b) VALUES (1, '1.5abc');
INSERT INTO ta (id, c) VALUES (1, '1.5abc');
INSERT INTO ta (id, d) VALUES (1, '1.5abc');
INSERT IGNORE INTO ta VALUES (1, '1.5abc', '1.5abc', '1.5abc', '1.5abc', '1.5abc');
SET sql_mode = '';
INSERT INTO ta VALUES (2, '2.5x', '2.5x', '2.5x', '2.5x', '2.5x');
SET sql_mode = DEFAULT;
SELECT * FROM ta ORDER BY id;
DROP TABLE ta;

-- indicator and local-rank window functions read the values as FLOAT does
CREATE TABLE tiw (id INT PRIMARY KEY, f FLOAT, a bf16, b float16, c float8, d float4);
INSERT INTO tiw VALUES (1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2), (3, 3, 3, 3, 3, 3), (4, 4, 4, 4, 4, 4), (5, 1.5, 1.5, 1.5, 1.5, 1.5);
SELECT id, sma(f, 2) OVER (ORDER BY id), sma(a, 2) OVER (ORDER BY id), sma(b, 2) OVER (ORDER BY id), sma(c, 2) OVER (ORDER BY id), sma(d, 2) OVER (ORDER BY id) FROM tiw ORDER BY id;
SELECT id, mmaxDrawdown(f, 3) OVER (ORDER BY id), mmaxDrawdown(a, 3) OVER (ORDER BY id), mmaxDrawdown(d, 3) OVER (ORDER BY id), cumpositivestreak(c) OVER (ORDER BY id) FROM tiw ORDER BY id;
SELECT id, mrank(f, 3) OVER (ORDER BY id), mrank(a, 3) OVER (ORDER BY id), mrank(c, 3) OVER (ORDER BY id), cumrank(b) OVER (ORDER BY id), cumrank(d) OVER (ORDER BY id) FROM tiw ORDER BY id;
SELECT quantileExactWeighted(f, id, 0.5), quantileExactWeighted(a, id, 0.5), quantileExactWeighted(c, id, 0.5), quantileExactWeighted(d, id, 0.5) FROM tiw;
DROP TABLE tiw;

DROP DATABASE lowprec_float;
