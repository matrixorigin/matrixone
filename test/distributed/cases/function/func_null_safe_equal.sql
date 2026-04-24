CREATE DATABASE IF NOT EXISTS test_null_safe;
USE test_null_safe;

SELECT 1 <=> 1;
SELECT 1 <=> 0;
SELECT 1 <=> NULL;
SELECT NULL <=> 1;
SELECT NULL <=> NULL;
SELECT 'a' <=> 'a';
SELECT 'a' <=> 'b';
SELECT 'a' <=> NULL;
SELECT NULL <=> 'a';

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
    id INT,
    val INT
);

INSERT INTO t1 VALUES (1, 1), (2, 0), (3, NULL);

SELECT id, val, val <=> 1 AS eq_1, val <=> 0 AS eq_0, val <=> NULL AS eq_null FROM t1 ORDER BY id;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (
    id INT,
    val VARCHAR(20)
);

INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, NULL);

SELECT id, val, val <=> 'a' AS eq_a, val <=> 'b' AS eq_b, val <=> NULL AS eq_null FROM t2 ORDER BY id;

SELECT 1.0 <=> 1;
SELECT 1.0 <=> 1.1;
SELECT 1.0 <=> NULL;
SELECT 1 <=> '1';
SELECT '1' <=> 1;

SELECT CAST(1.10 AS DECIMAL(10,2)) <=> CAST(1.1 AS DECIMAL(10,1)) AS dec_eq_1;
SELECT CAST(1.10 AS DECIMAL(10,2)) <=> CAST(1.11 AS DECIMAL(10,2)) AS dec_eq_2;
SELECT CAST(NULL AS DECIMAL(10,2)) <=> CAST(NULL AS DECIMAL(10,2)) AS dec_eq_3;
SELECT CAST(1.00 AS DECIMAL(10,2)) <=> CAST(NULL AS DECIMAL(10,2)) AS dec_eq_4;

SELECT (1, NULL) <=> (1, NULL);
SELECT (1, 1) <=> (1, NULL);
SELECT (1, 2) <=> (1, 3);

SELECT (val <=> NULL) IS NULL FROM t1 ORDER BY id;

SELECT 1 as result WHERE NULL <=> NULL;
SELECT 1 as result WHERE 1 <=> NULL;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP DATABASE test_null_safe;
