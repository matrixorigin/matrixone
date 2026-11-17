-- @suite
-- @case
-- @desc: ENUM and SET string comparisons use their declared collation (issue #31364)
-- @label:bvt
DROP DATABASE IF EXISTS issue_31364;
CREATE DATABASE issue_31364 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE issue_31364;

CREATE TABLE en2 (id INT, e ENUM('Red','GREEN'));
INSERT INTO en2 VALUES (1,'Red'), (2,'GREEN'), (3,NULL);
SELECT id, e FROM en2 WHERE e='RED';
SELECT COUNT(*) FROM en2 WHERE e='red';
SELECT COUNT(*) FROM en2 WHERE e='green';
SELECT COUNT(*) FROM en2 WHERE e='RED ';
SELECT id FROM en2 WHERE e IN ('red','green') ORDER BY id;
SELECT id FROM en2 WHERE e IN ('RED',2) ORDER BY id;
SELECT id FROM en2 WHERE e IN (1,2) ORDER BY id;
SELECT id FROM en2 WHERE e NOT IN ('red','green') ORDER BY id;
SELECT id FROM en2 WHERE e != 'RED' ORDER BY id;
SELECT id FROM en2 WHERE e <=> 'RED' ORDER BY id;
SELECT id FROM en2 WHERE e <=> NULL ORDER BY id;
SELECT id FROM en2 WHERE e LIKE 'r%' ORDER BY id;
SELECT COUNT(*) FROM en2 WHERE e='missing';
SELECT COUNT(*) FROM en2 WHERE e IS NULL;
SELECT id FROM en2 WHERE e=1 ORDER BY id;
SELECT id FROM (SELECT id, e FROM en2) derived WHERE e='RED' ORDER BY id;
SELECT (SELECT e FROM en2 WHERE id=1) = 'RED';
SELECT (SELECT e FROM en2 WHERE id=3) <=> NULL;
SELECT 'RED' IN (SELECT e FROM en2);
SELECT 'RED' NOT IN (SELECT e FROM en2);
SELECT 'missing' IN (SELECT e FROM en2);
SELECT 'missing' NOT IN (SELECT e FROM en2);
SELECT COUNT(*) FROM en2 WHERE e COLLATE utf8mb4_bin='RED';
SELECT COUNT(*) FROM en2 WHERE e COLLATE utf8mb4_general_ci='RED';
INSERT INTO en2 VALUES (4,'RED');
SELECT id, e FROM en2 WHERE id=4;
DELETE FROM en2 WHERE id=4;
INSERT INTO en2 VALUES (4,2);
SELECT id, e FROM en2 WHERE id=4;
DELETE FROM en2 WHERE id=4;

CREATE TABLE st1 (id INT, s SET('a','b','c','d'));
INSERT INTO st1 VALUES (1,'a'), (2,'a,b'), (3,'b,c,d'), (4,'d'), (5,NULL), (6,'');
SELECT id, s FROM st1 WHERE s='A,B';
SELECT id FROM st1 WHERE s IN ('A,B','D') ORDER BY id;
SELECT id FROM st1 WHERE s IN ('A,B',4) ORDER BY id;
SELECT id FROM st1 WHERE s IN (1,3) ORDER BY id;
SELECT id FROM st1 WHERE s NOT IN ('A,B','D') ORDER BY id;
SELECT id FROM st1 WHERE s != 'A,B' ORDER BY id;
SELECT id FROM st1 WHERE s <=> 'A,B' ORDER BY id;
SELECT id FROM st1 WHERE s <=> NULL ORDER BY id;
SELECT id FROM st1 WHERE s LIKE 'A,%' ORDER BY id;
SELECT COUNT(*) FROM st1 WHERE s='B,A';
SELECT COUNT(*) FROM st1 WHERE s='missing';
SELECT COUNT(*) FROM st1 WHERE s IS NULL;
SELECT id FROM st1 WHERE s='' ORDER BY id;
SELECT id FROM st1 WHERE s=3 ORDER BY id;
SELECT (SELECT s FROM st1 WHERE id=2) = 'A,B';
SELECT (SELECT s FROM st1 WHERE id=5) <=> NULL;
SELECT 'A,B' IN (SELECT s FROM st1);
SELECT 'A,B' NOT IN (SELECT s FROM st1);
SELECT 'missing' IN (SELECT s FROM st1);
SELECT 'missing' NOT IN (SELECT s FROM st1);
INSERT INTO st1 VALUES (7,'A,B');
SELECT id, s FROM st1 WHERE id=7;
DELETE FROM st1 WHERE id=7;

CREATE TABLE en_bin (id INT, e ENUM('Red','GREEN') COLLATE utf8mb4_bin);
INSERT INTO en_bin VALUES (1,'Red'), (2,'GREEN'), (3,NULL);
SELECT COUNT(*) FROM en_bin WHERE e='RED';
SELECT COUNT(*) FROM en_bin WHERE e='Red';
SELECT COUNT(*) FROM en_bin WHERE e IN ('RED','green');
SELECT COUNT(*) FROM en_bin WHERE e IN ('Red','GREEN');
SELECT COUNT(*) FROM en_bin WHERE e COLLATE utf8mb4_general_ci='RED';
SELECT COUNT(*) FROM en_bin WHERE e LIKE 'R%';
SELECT COUNT(*) FROM en_bin WHERE e LIKE 'Red%';
SELECT id FROM en_bin WHERE e=1 ORDER BY id;
SELECT (SELECT e FROM en_bin WHERE id=1) = 'RED';
SELECT (SELECT e FROM en_bin WHERE id=1) = 'Red';
SELECT (SELECT e FROM en_bin WHERE id=3) <=> NULL;
SELECT 'RED' IN (SELECT e FROM en_bin WHERE id IN (1,2));
SELECT 'RED' NOT IN (SELECT e FROM en_bin WHERE id IN (1,2));

CREATE TABLE st_bin (id INT, s SET('a','b','c','d') COLLATE utf8mb4_bin);
INSERT INTO st_bin VALUES (1,'a'), (2,'a,b'), (3,'b,c,d'), (4,'d'), (5,NULL), (6,'');
SELECT COUNT(*) FROM st_bin WHERE s='A,B';
SELECT COUNT(*) FROM st_bin WHERE s='a,b';
SELECT COUNT(*) FROM st_bin WHERE s IN ('A,B','D');
SELECT COUNT(*) FROM st_bin WHERE s IN ('a,b','d');
SELECT COUNT(*) FROM st_bin WHERE s COLLATE utf8mb4_general_ci='A,B';
SELECT COUNT(*) FROM st_bin WHERE s LIKE 'A,%';
SELECT COUNT(*) FROM st_bin WHERE s LIKE 'a,%';
SELECT id FROM st_bin WHERE s=3 ORDER BY id;
SELECT (SELECT s FROM st_bin WHERE id=2) = 'A,B';
SELECT (SELECT s FROM st_bin WHERE id=2) = 'a,b';
SELECT (SELECT s FROM st_bin WHERE id=5) <=> NULL;
SELECT 'A,B' IN (SELECT s FROM st_bin WHERE id IN (1,2));
SELECT 'A,B' NOT IN (SELECT s FROM st_bin WHERE id IN (1,2));

SELECT ((SELECT e FROM en2 WHERE id=1) COLLATE utf8mb4_general_ci) = 'RED';
SELECT (d.e COLLATE utf8mb4_general_ci) = 'RED' FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT ((SELECT e FROM en_bin WHERE id=1) COLLATE utf8mb4_general_ci) = 'RED';
SELECT (d.e COLLATE utf8mb4_general_ci) = 'RED' FROM (SELECT e FROM en_bin WHERE id=1) d;
SELECT ((SELECT e FROM en2 WHERE id=1) COLLATE utf8mb4_bin) = 'RED';
SELECT d.e IN (SELECT 'RED') FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT d.e NOT IN (SELECT 'RED') FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT d.e IN (SELECT r.v FROM (SELECT 'RED' AS v) r) FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT d.e NOT IN (SELECT r.v FROM (SELECT 'RED' AS v) r) FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT d.e IN (SELECT NULL) FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT d.e NOT IN (SELECT NULL) FROM (SELECT e FROM en2 WHERE id=1) d;
SELECT d.e IN (SELECT 'RED') FROM (SELECT e FROM en_bin WHERE id=1) d;
SELECT d.e IN (SELECT 'Red') FROM (SELECT e FROM en_bin WHERE id=1) d;

SELECT ((SELECT s FROM st1 WHERE id=2) COLLATE utf8mb4_general_ci) = 'A,B';
SELECT (d.s COLLATE utf8mb4_general_ci) = 'A,B' FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT ((SELECT s FROM st_bin WHERE id=2) COLLATE utf8mb4_general_ci) = 'A,B';
SELECT (d.s COLLATE utf8mb4_general_ci) = 'A,B' FROM (SELECT s FROM st_bin WHERE id=2) d;
SELECT ((SELECT s FROM st1 WHERE id=2) COLLATE utf8mb4_bin) = 'A,B';
SELECT d.s IN (SELECT 'A,B') FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT d.s NOT IN (SELECT 'A,B') FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT d.s IN (SELECT r.v FROM (SELECT 'A,B' AS v) r) FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT d.s NOT IN (SELECT r.v FROM (SELECT 'A,B' AS v) r) FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT d.s IN (SELECT NULL) FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT d.s NOT IN (SELECT NULL) FROM (SELECT s FROM st1 WHERE id=2) d;
SELECT d.s IN (SELECT 'A,B') FROM (SELECT s FROM st_bin WHERE id=2) d;
SELECT d.s IN (SELECT 'a,b') FROM (SELECT s FROM st_bin WHERE id=2) d;

SELECT (SELECT e COLLATE utf8mb4_general_ci FROM en2 WHERE id=1) = 'RED';
SELECT d.e = 'RED' FROM (SELECT e COLLATE utf8mb4_general_ci AS e FROM en2 WHERE id=1) d;
SELECT 'RED' IN (SELECT e COLLATE utf8mb4_general_ci FROM en2 WHERE id IN (1,2));
SELECT 'RED' NOT IN (SELECT e COLLATE utf8mb4_general_ci FROM en2 WHERE id IN (1,2));
SELECT (SELECT e COLLATE utf8mb4_general_ci FROM en_bin WHERE id=1) = 'RED';
SELECT d.e = 'RED' FROM (SELECT e COLLATE utf8mb4_general_ci AS e FROM en_bin WHERE id=1) d;
SELECT 'RED' IN (SELECT e COLLATE utf8mb4_general_ci FROM en_bin WHERE id IN (1,2));
SELECT 'RED' NOT IN (SELECT e COLLATE utf8mb4_general_ci FROM en_bin WHERE id IN (1,2));
SELECT (SELECT e COLLATE utf8mb4_bin FROM en2 WHERE id=1) = 'RED';
SELECT d.e = 'RED' FROM (SELECT e COLLATE utf8mb4_bin AS e FROM en2 WHERE id=1) d;

SELECT (SELECT s COLLATE utf8mb4_general_ci FROM st1 WHERE id=2) = 'A,B';
SELECT d.s = 'A,B' FROM (SELECT s COLLATE utf8mb4_general_ci AS s FROM st1 WHERE id=2) d;
SELECT 'A,B' IN (SELECT s COLLATE utf8mb4_general_ci FROM st1 WHERE id IN (1,2));
SELECT 'A,B' NOT IN (SELECT s COLLATE utf8mb4_general_ci FROM st1 WHERE id IN (1,2));
SELECT (SELECT s COLLATE utf8mb4_general_ci FROM st_bin WHERE id=2) = 'A,B';
SELECT d.s = 'A,B' FROM (SELECT s COLLATE utf8mb4_general_ci AS s FROM st_bin WHERE id=2) d;
SELECT 'A,B' IN (SELECT s COLLATE utf8mb4_general_ci FROM st_bin WHERE id IN (1,2));
SELECT 'A,B' NOT IN (SELECT s COLLATE utf8mb4_general_ci FROM st_bin WHERE id IN (1,2));
SELECT (SELECT s COLLATE utf8mb4_bin FROM st1 WHERE id=2) = 'A,B';
SELECT d.s = 'A,B' FROM (SELECT s COLLATE utf8mb4_bin AS s FROM st1 WHERE id=2) d;

SELECT n.id FROM en2 n WHERE EXISTS (SELECT 1 WHERE n.e='RED') ORDER BY n.id;
SELECT d.id FROM (SELECT id, e FROM en2) d WHERE EXISTS (SELECT 1 WHERE d.e='RED') ORDER BY d.id;
SELECT d.id FROM (SELECT id, e FROM en2) d WHERE EXISTS (SELECT 1 WHERE d.e<=>NULL) ORDER BY d.id;
SELECT d.id FROM (SELECT id, e FROM en2) d WHERE EXISTS (SELECT 1 FROM st1 WHERE EXISTS (SELECT 1 WHERE d.e='RED')) ORDER BY d.id;
SELECT d.id FROM (SELECT id, e FROM en_bin) d WHERE EXISTS (SELECT 1 WHERE d.e='RED') ORDER BY d.id;
SELECT d.id FROM (SELECT id, e FROM en_bin) d WHERE EXISTS (SELECT 1 WHERE d.e='Red') ORDER BY d.id;

SELECT n.id FROM st1 n WHERE EXISTS (SELECT 1 WHERE n.s='A,B') ORDER BY n.id;
SELECT d.id FROM (SELECT id, s FROM st1) d WHERE EXISTS (SELECT 1 WHERE d.s='A,B') ORDER BY d.id;
SELECT d.id FROM (SELECT id, s FROM st1) d WHERE EXISTS (SELECT 1 WHERE d.s<=>NULL) ORDER BY d.id;
SELECT d.id FROM (SELECT id, s FROM st1) d WHERE EXISTS (SELECT 1 FROM en2 WHERE EXISTS (SELECT 1 WHERE d.s='A,B')) ORDER BY d.id;
SELECT d.id FROM (SELECT id, s FROM st_bin) d WHERE EXISTS (SELECT 1 WHERE d.s='A,B') ORDER BY d.id;
SELECT d.id FROM (SELECT id, s FROM st_bin) d WHERE EXISTS (SELECT 1 WHERE d.s='a,b') ORDER BY d.id;

WITH d AS (SELECT e COLLATE utf8mb4_general_ci AS e, RAND() AS r FROM en2 WHERE id=1) SELECT COUNT(*) FROM d WHERE e='RED';
WITH d AS (SELECT s COLLATE utf8mb4_general_ci AS s, RAND() AS r FROM st1 WHERE id=2) SELECT COUNT(*) FROM d WHERE s='A,B';
WITH d AS (SELECT e COLLATE utf8mb4_general_ci AS e, RAND() AS r FROM en_bin WHERE id=1) SELECT COUNT(*) FROM d WHERE e='RED';
WITH d AS (SELECT s COLLATE utf8mb4_general_ci AS s, RAND() AS r FROM st_bin WHERE id=2) SELECT COUNT(*) FROM d WHERE s='A,B';
WITH d AS (SELECT e COLLATE utf8mb4_bin AS e, RAND() AS r FROM en2 WHERE id=1) SELECT COUNT(*) FROM d WHERE e='RED';
WITH d AS (SELECT s COLLATE utf8mb4_bin AS s, RAND() AS r FROM st1 WHERE id=2) SELECT COUNT(*) FROM d WHERE s='A,B';
WITH d AS (SELECT CAST(e AS CHAR) COLLATE utf8mb4_general_ci AS e, RAND() AS r FROM en2 WHERE id=1) SELECT COUNT(*) FROM d WHERE e='RED';
WITH d AS (SELECT CAST(s AS CHAR) COLLATE utf8mb4_general_ci AS s, RAND() AS r FROM st1 WHERE id=2) SELECT COUNT(*) FROM d WHERE s='A,B';

SELECT COUNT(*) FROM (SELECT e COLLATE utf8mb4_general_ci AS e FROM en2 WHERE id=1 UNION ALL SELECT NULL) d WHERE e='RED';
SELECT COUNT(*) FROM (SELECT s COLLATE utf8mb4_general_ci AS s FROM st1 WHERE id=2 UNION ALL SELECT NULL) d WHERE s='A,B';
SELECT COUNT(*) FROM (SELECT e COLLATE utf8mb4_general_ci AS e FROM en_bin WHERE id=1 UNION ALL SELECT NULL) d WHERE e='RED';
SELECT COUNT(*) FROM (SELECT s COLLATE utf8mb4_general_ci AS s FROM st_bin WHERE id=2 UNION ALL SELECT NULL) d WHERE s='A,B';
SELECT COUNT(*) FROM (SELECT e COLLATE utf8mb4_bin AS e FROM en2 WHERE id=1 UNION ALL SELECT NULL) d WHERE e='RED';
SELECT COUNT(*) FROM (SELECT s COLLATE utf8mb4_bin AS s FROM st1 WHERE id=2 UNION ALL SELECT NULL) d WHERE s='A,B';
SELECT COUNT(*) FROM (SELECT CAST(e AS CHAR) COLLATE utf8mb4_general_ci AS e FROM en2 WHERE id=1 UNION ALL SELECT NULL) d WHERE e='RED';
SELECT COUNT(*) FROM (SELECT CAST(s AS CHAR) COLLATE utf8mb4_general_ci AS s FROM st1 WHERE id=2 UNION ALL SELECT NULL) d WHERE s='A,B';
SELECT COUNT(*) FROM (SELECT e COLLATE utf8mb4_general_ci AS e FROM en2 WHERE id=1 UNION ALL SELECT 'GREEN') d WHERE e='RED';
SELECT COUNT(*) FROM (SELECT e FROM en2 WHERE id=1 UNION ALL SELECT e FROM en2 WHERE id=1) d WHERE e='RED';
SELECT COUNT(*) FROM (SELECT s FROM st1 WHERE id=2 UNION ALL SELECT s FROM st1 WHERE id=2) d WHERE s='A,B';

SELECT (e COLLATE utf8mb4_general_ci)='RED' FROM en2 WHERE id=1 GROUP BY e COLLATE utf8mb4_general_ci;
SELECT (s COLLATE utf8mb4_general_ci)='A,B' FROM st1 WHERE id=2 GROUP BY s COLLATE utf8mb4_general_ci;
SELECT (e COLLATE utf8mb4_general_ci)='RED' FROM en_bin WHERE id=1 GROUP BY e COLLATE utf8mb4_general_ci;
SELECT (s COLLATE utf8mb4_general_ci)='A,B' FROM st_bin WHERE id=2 GROUP BY s COLLATE utf8mb4_general_ci;
SELECT (e COLLATE utf8mb4_bin)='RED' FROM en2 WHERE id=1 GROUP BY e COLLATE utf8mb4_bin;
SELECT (s COLLATE utf8mb4_bin)='A,B' FROM st1 WHERE id=2 GROUP BY s COLLATE utf8mb4_bin;
SELECT (CAST(e AS CHAR) COLLATE utf8mb4_general_ci)='RED' FROM en2 WHERE id=1 GROUP BY CAST(e AS CHAR) COLLATE utf8mb4_general_ci;
SELECT (CAST(s AS CHAR) COLLATE utf8mb4_general_ci)='A,B' FROM st1 WHERE id=2 GROUP BY CAST(s AS CHAR) COLLATE utf8mb4_general_ci;

CREATE TABLE special_unicode (
    id INT,
    e ENUM('Red','GREEN') COLLATE utf8mb4_unicode_ci,
    s SET('a','b','c','d') COLLATE utf8mb4_unicode_ci
);
INSERT INTO special_unicode VALUES (1,'Red','a,b'), (2,'GREEN','d'), (3,NULL,NULL);
SELECT id FROM special_unicode WHERE e='RED' ORDER BY id;
SELECT id FROM special_unicode WHERE s='A,B' ORDER BY id;
SELECT id FROM special_unicode WHERE e IN ('red','green') ORDER BY id;
SELECT id FROM special_unicode WHERE s IN ('A,B','D') ORDER BY id;

DROP DATABASE issue_31364;
