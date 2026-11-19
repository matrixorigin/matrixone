-- Arithmetic existence keys preserve NULLs, outer occurrences and inner witnesses.
DROP DATABASE IF EXISTS expression_exists_bvt;
CREATE DATABASE expression_exists_bvt;
USE expression_exists_bvt;
CREATE TABLE ot (seq INT PRIMARY KEY, k INT, g INT);
CREATE TABLE it (k INT, g INT);
INSERT INTO ot VALUES (1,1,1),(2,2,1),(3,2,1),(4,4,2),(5,NULL,1),(6,8,3),(7,-2147483648,4);
INSERT INTO it VALUES (0,1),(1,1),(1,1),(3,2),(NULL,1);

SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) ORDER BY o.seq;
SELECT seq FROM ot WHERE k IN (SELECT k+1 FROM it) ORDER BY seq;
SELECT DISTINCT o.seq FROM ot o JOIN it i ON i.k=o.k-1 ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k+1=o.k) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k*10+i.g=o.k*10+o.g) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 AND i.g=o.g) ORDER BY o.seq;
SELECT o.seq, EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) AS matched,
       NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) AS unmatched FROM ot o ORDER BY o.seq;
SELECT o.seq, EXISTS (SELECT 1 FROM it i WHERE i.k=o.k*4294967296) AS matched FROM ot o ORDER BY o.seq;

-- Hash each existence branch without duplicating outer occurrences or NULL witnesses.
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1) ORDER BY o.seq;
SELECT o.seq, EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1) AS matched,
       (EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) OR EXISTS (SELECT 1 FROM it i WHERE i.k=o.k+1)) AS reference_match
FROM ot o ORDER BY o.seq;
SELECT o.seq, EXISTS (SELECT 1 FROM it i WHERE i.k=o.k OR i.k=o.g) AS matched,
       NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k OR i.k=o.g) AS unmatched FROM ot o ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k+1=o.k OR i.k-1=o.k) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k>0 AND (i.k=o.k-1 OR i.k=o.k+1)) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1 OR i.g=o.g) ORDER BY o.seq;
-- A non-equality branch must keep its residual semantics.
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k>o.k) ORDER BY o.seq;
-- NOT IN must retain its distinct UNKNOWN result for a genuinely NULL inner key.
SELECT seq FROM ot WHERE k NOT IN (SELECT k+1 FROM it) ORDER BY seq;

DELETE FROM it;
SELECT o.seq, EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1) AS matched,
       NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1) AS unmatched FROM ot o ORDER BY o.seq;
SELECT o.seq, EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) AS matched,
       NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) AS unmatched FROM ot o ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) ORDER BY o.seq;
SELECT o.seq FROM ot o WHERE NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1) ORDER BY o.seq;

-- An empty inner input must not eagerly evaluate a fallible outer BIGINT key.
CREATE TABLE wide_ot (k BIGINT);
INSERT INTO wide_ot VALUES (-9223372036854775808);
SELECT o.k FROM wide_ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1);
SELECT o.k FROM wide_ot o WHERE NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1);
SELECT o.k FROM wide_ot o WHERE EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1);
SELECT o.k FROM wide_ot o WHERE NOT EXISTS (SELECT 1 FROM it i WHERE i.k=o.k-1 OR i.k=o.k+1);
-- An inner projection can evaluate even with an empty outer input. Its error
-- must agree with the control that disables subquery predicate optimization.
DELETE FROM ot;
SELECT EXISTS (SELECT 1 FROM wide_ot i WHERE i.k-1=o.k) FROM ot o;
SET @saved_expression_exists_hints = @@optimizer_hints;
SET optimizer_hints = 'subqueryPredicatePlanning=1';
SELECT EXISTS (SELECT 1 FROM wide_ot i WHERE i.k-1=o.k) FROM ot o;
SET optimizer_hints = @saved_expression_exists_hints;
DROP DATABASE expression_exists_bvt;
