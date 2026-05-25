-- =============================================================
-- FULL [OUTER] JOIN functional test cases (part 1: core semantics)
--
-- Companion files:
--   fulljoin_types.sql        -- per-data-type join-key coverage
--   fulljoin_constraints.sql  -- PK / UK / FK / NOT NULL / DEFAULT / CHECK /
--                                AUTO_INCREMENT / generated / ON UPDATE
--   fulljoin_tables.sql       -- table kinds: temporary / partitioned / view /
--                                cluster / external / fulltext- and vector-
--                                indexed tables
--
-- Each non-trivial case is paired with the MySQL-style
-- LEFT JOIN UNION RIGHT JOIN rewrite so results can be
-- cross-checked against the standard semantics.
-- =============================================================

drop database if exists fulljoin_db;
create database fulljoin_db;
use fulljoin_db;

-- -------------------------------------------------------------
-- 1. Basic equi-join on int / varchar with NULLs on both sides
-- -------------------------------------------------------------
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (S1 INT, S2 varchar(10));
CREATE TABLE t2 (S1 INT, S2 varchar(10));
INSERT INTO t1 VALUES (1,'aaa'),(2,'bbb'),(3,'ccc'),(4,NULL),(5,'eee'),(6,'fff'),(NULL,'aaa'),(9,'bbb'),(10,'ccc'),(11,'ddd'),(12,'abc'),(NULL,NULL);
INSERT INTO t2 VALUES (11,'aaa'),(12,'bbb'),(13,'ccc'),(14,NULL),(1,'aaa'),(2,'bbb'),(3,'ccc'),(4,'ddd'),(NULL,'abc'),(NULL,NULL);

-- 1.1 FULL OUTER JOIN on S1 (int)
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1 ORDER BY t1.S1, t2.S1, t1.S2, t2.S2;

-- 1.2 FULL JOIN synonym (no OUTER keyword)
SELECT * FROM t1 FULL JOIN t2 ON t1.S1 = t2.S1 ORDER BY t1.S1, t2.S1, t1.S2, t2.S2;

-- 1.3 FULL OUTER JOIN on S2 (varchar)
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.S2 = t2.S2 ORDER BY t1.S1, t2.S1, t1.S2, t2.S2;

-- 1.4 FULL OUTER JOIN ≡ LEFT UNION RIGHT
SELECT * FROM t1 LEFT  JOIN t2 ON t1.S1 = t2.S1
UNION
SELECT * FROM t1 RIGHT JOIN t2 ON t1.S1 = t2.S1
ORDER BY 1,3,2,4;

-- -------------------------------------------------------------
-- 2. Commutativity: swap sides
-- -------------------------------------------------------------
SELECT t1.S1 a, t1.S2 b, t2.S1 c, t2.S2 d FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1 ORDER BY a,c,b,d;
SELECT t1.S1 a, t1.S2 b, t2.S1 c, t2.S2 d FROM t2 FULL OUTER JOIN t1 ON t1.S1 = t2.S1 ORDER BY a,c,b,d;

-- -------------------------------------------------------------
-- 3. Empty-table corners
-- -------------------------------------------------------------
drop table if exists t3;
CREATE TABLE t3 (S1 INT, S2 varchar(10));

-- 3.1 Right side empty
SELECT * FROM t1 FULL OUTER JOIN t3 ON t1.S1 = t3.S1 ORDER BY t1.S1, t1.S2;

-- 3.2 Left side empty
SELECT * FROM t3 FULL OUTER JOIN t1 ON t1.S1 = t3.S1 ORDER BY t1.S1, t1.S2;

-- 3.3 Both sides empty
drop table if exists t4;
CREATE TABLE t4 (S1 INT, S2 varchar(10));
SELECT * FROM t3 FULL OUTER JOIN t4 ON t3.S1 = t4.S1;

-- -------------------------------------------------------------
-- 4. No row matches at all
-- -------------------------------------------------------------
drop table if exists a_nomatch;
drop table if exists b_nomatch;
create table a_nomatch(x int, y varchar(10));
create table b_nomatch(x int, y varchar(10));
insert into a_nomatch values (1,'a'),(2,'b'),(3,'c');
insert into b_nomatch values (10,'x'),(20,'y'),(30,'z');
SELECT * FROM a_nomatch FULL OUTER JOIN b_nomatch ON a_nomatch.x = b_nomatch.x ORDER BY a_nomatch.x, b_nomatch.x;

-- -------------------------------------------------------------
-- 5. Every row matches exactly once (equivalent to INNER here)
-- -------------------------------------------------------------
drop table if exists a_allmatch;
drop table if exists b_allmatch;
create table a_allmatch(x int primary key, y varchar(10));
create table b_allmatch(x int primary key, y varchar(10));
insert into a_allmatch values (1,'a'),(2,'b'),(3,'c');
insert into b_allmatch values (1,'A'),(2,'B'),(3,'C');
SELECT * FROM a_allmatch FULL OUTER JOIN b_allmatch ON a_allmatch.x = b_allmatch.x ORDER BY a_allmatch.x;

-- -------------------------------------------------------------
-- 6. Duplicates on both sides -> cartesian per key + unmatched
-- -------------------------------------------------------------
drop table if exists a_dup;
drop table if exists b_dup;
create table a_dup(k int, v varchar(10));
create table b_dup(k int, v varchar(10));
insert into a_dup values (1,'a1'),(1,'a2'),(2,'a3'),(4,'a4');
insert into b_dup values (1,'b1'),(1,'b2'),(3,'b3'),(4,'b4');
SELECT * FROM a_dup FULL OUTER JOIN b_dup ON a_dup.k = b_dup.k ORDER BY a_dup.k, a_dup.v, b_dup.k, b_dup.v;

-- -------------------------------------------------------------
-- 7. Multi-column ON condition
-- -------------------------------------------------------------
drop table if exists orders;
drop table if exists payments;
create table orders  (order_id int, user_id int, amount decimal(10,2));
create table payments(order_id int, user_id int, paid   decimal(10,2));
insert into orders   values (1,100,50.00),(2,100,25.00),(3,200,10.00),(4,300,5.00);
insert into payments values (1,100,50.00),(2,100,20.00),(5,400,8.00);
SELECT o.order_id, o.user_id, o.amount, p.order_id, p.user_id, p.paid
FROM orders o FULL OUTER JOIN payments p
  ON o.order_id = p.order_id AND o.user_id = p.user_id
ORDER BY o.order_id, p.order_id;

-- -------------------------------------------------------------
-- 8. USING clause (coalesced join column)
-- -------------------------------------------------------------
drop table if exists u1;
drop table if exists u2;
create table u1(id int, name varchar(10));
create table u2(id int, score int);
insert into u1 values (1,'alice'),(2,'bob'),(3,'carol');
insert into u2 values (2,90),(3,85),(4,70);
SELECT * FROM u1 FULL OUTER JOIN u2 USING (id) ORDER BY id;
SELECT id, name, score FROM u1 FULL OUTER JOIN u2 USING (id) ORDER BY id;

-- -------------------------------------------------------------
-- 9. Non-equi ON condition
-- -------------------------------------------------------------
drop table if exists r1;
drop table if exists r2;
create table r1(x int);
create table r2(y int);
insert into r1 values (1),(5),(10);
insert into r2 values (3),(6),(12);
SELECT * FROM r1 FULL OUTER JOIN r2 ON r1.x < r2.y ORDER BY r1.x, r2.y;

-- -------------------------------------------------------------
-- 10. WHERE after FULL OUTER JOIN (predicate-pushdown traps)
-- -------------------------------------------------------------
-- 10.1 anti-left (rows only in t1)
SELECT t1.S1, t1.S2 FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1
WHERE t2.S1 IS NULL ORDER BY t1.S1, t1.S2;

-- 10.2 anti-right (rows only in t2)
SELECT t2.S1, t2.S2 FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1
WHERE t1.S1 IS NULL ORDER BY t2.S1, t2.S2;

-- 10.3 unmatched on either side (symmetric difference)
SELECT t1.S1, t1.S2, t2.S1, t2.S2
FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1
WHERE t1.S1 IS NULL OR t2.S1 IS NULL
ORDER BY t1.S1, t2.S1, t1.S2, t2.S2;

-- 10.4 predicate on a nullable column must not be pushed into FULL side
SELECT t1.S1, t2.S1 FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1
WHERE t2.S1 > 5 ORDER BY t1.S1, t2.S1;

-- -------------------------------------------------------------
-- 11. COALESCE of join keys produces a merged key
-- -------------------------------------------------------------
SELECT COALESCE(t1.S1, t2.S1) AS k, t1.S2 AS left_s2, t2.S2 AS right_s2
FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1
ORDER BY k, left_s2, right_s2;

-- -------------------------------------------------------------
-- 12. Aggregation over FULL OUTER JOIN result
-- -------------------------------------------------------------
SELECT COUNT(*) AS total_rows,
       COUNT(t1.S1) AS left_side_rows,
       COUNT(t2.S1) AS right_side_rows,
       SUM(CASE WHEN t1.S1 IS NULL THEN 1 ELSE 0 END) AS right_only,
       SUM(CASE WHEN t2.S1 IS NULL THEN 1 ELSE 0 END) AS left_only
FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1;

SELECT COALESCE(t1.S1, t2.S1) AS k, COUNT(*) AS c
FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1
GROUP BY COALESCE(t1.S1, t2.S1)
ORDER BY k;

-- -------------------------------------------------------------
-- 13. Derived tables on both sides
-- -------------------------------------------------------------
SELECT * FROM
  (SELECT S1, S2 FROM t1 WHERE S1 IS NOT NULL) l
FULL OUTER JOIN
  (SELECT S1, S2 FROM t2 WHERE S1 IS NOT NULL) r
ON l.S1 = r.S1
ORDER BY l.S1, r.S1, l.S2, r.S2;

-- -------------------------------------------------------------
-- 14. Chained FULL OUTER JOIN across three tables
-- -------------------------------------------------------------
drop table if exists tA;
drop table if exists tB;
drop table if exists tC;
create table tA(id int, va varchar(5));
create table tB(id int, vb varchar(5));
create table tC(id int, vc varchar(5));
insert into tA values (1,'a1'),(2,'a2'),(3,'a3');
insert into tB values (2,'b2'),(3,'b3'),(4,'b4');
insert into tC values (3,'c3'),(4,'c4'),(5,'c5');
SELECT COALESCE(tA.id, tB.id, tC.id) AS id, va, vb, vc
FROM tA FULL OUTER JOIN tB ON tA.id = tB.id
        FULL OUTER JOIN tC ON COALESCE(tA.id, tB.id) = tC.id
ORDER BY id;

-- -------------------------------------------------------------
-- 15. Self FULL OUTER JOIN
-- -------------------------------------------------------------
drop table if exists emp;
create table emp(id int, mgr_id int, name varchar(10));
insert into emp values (1,NULL,'root'),(2,1,'alice'),(3,1,'bob'),(4,2,'carol'),(5,NULL,'orphan');
SELECT e.id, e.name, m.id AS mgr_id, m.name AS mgr_name
FROM emp e FULL OUTER JOIN emp m ON e.mgr_id = m.id
ORDER BY e.id, m.id;

-- -------------------------------------------------------------
-- 16. Mixed join types: INNER -> FULL, FULL -> LEFT
-- -------------------------------------------------------------
SELECT tA.id AS a_id, tB.id AS b_id, tC.id AS c_id
FROM tA INNER JOIN tB ON tA.id = tB.id
        FULL OUTER JOIN tC ON tB.id = tC.id
ORDER BY a_id, b_id, c_id;

SELECT tA.id AS a_id, tB.id AS b_id, tC.id AS c_id
FROM tA FULL OUTER JOIN tB ON tA.id = tB.id
        LEFT JOIN tC ON tB.id = tC.id
ORDER BY a_id, b_id, c_id;

-- -------------------------------------------------------------
-- 17. Key type coverage
-- -------------------------------------------------------------

-- 17.1 decimal key
drop table if exists dec_a;
drop table if exists dec_b;
create table dec_a(k decimal(10,2), v varchar(5));
create table dec_b(k decimal(10,2), v varchar(5));
insert into dec_a values (1.10,'a'),(2.20,'b'),(3.30,'c'),(NULL,'n');
insert into dec_b values (2.20,'B'),(3.30,'C'),(4.40,'D'),(NULL,'N');
SELECT * FROM dec_a FULL OUTER JOIN dec_b ON dec_a.k = dec_b.k ORDER BY dec_a.k, dec_b.k;

-- 17.2 date key
drop table if exists dt_a;
drop table if exists dt_b;
create table dt_a(d date, v varchar(5));
create table dt_b(d date, v varchar(5));
insert into dt_a values ('2024-01-01','a'),('2024-02-02','b'),('2024-03-03','c');
insert into dt_b values ('2024-02-02','B'),('2024-03-03','C'),('2024-04-04','D');
SELECT * FROM dt_a FULL OUTER JOIN dt_b ON dt_a.d = dt_b.d ORDER BY dt_a.d, dt_b.d;

-- 17.3 varchar vs char (trailing-space semantics)
drop table if exists str_a;
drop table if exists str_b;
create table str_a(k varchar(10), v int);
create table str_b(k char(10),    v int);
insert into str_a values ('abc',1),('def',2),('ghi',3);
insert into str_b values ('abc',10),('def',20),('xyz',30);
SELECT * FROM str_a FULL OUTER JOIN str_b ON str_a.k = str_b.k ORDER BY str_a.k, str_b.k;

-- -------------------------------------------------------------
-- 18. NULL-key semantics with = vs <=>
-- -------------------------------------------------------------
drop table if exists n1;
drop table if exists n2;
create table n1(x int);
create table n2(x int);
insert into n1 values (1),(NULL),(2),(NULL);
insert into n2 values (1),(NULL),(3),(NULL);

-- 18.1 = : NULL does not match NULL, so every NULL stays unmatched
SELECT * FROM n1 FULL OUTER JOIN n2 ON n1.x = n2.x ORDER BY n1.x, n2.x;

-- 18.2 <=> : NULL matches NULL -> cartesian product on NULL keys
SELECT * FROM n1 FULL OUTER JOIN n2 ON n1.x <=> n2.x ORDER BY n1.x, n2.x;

-- -------------------------------------------------------------
-- 19. EXPLAIN
-- -------------------------------------------------------------
-- @separator:table
explain SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1;
-- @separator:table
explain SELECT COALESCE(t1.S1, t2.S1) FROM t1 FULL OUTER JOIN t2 ON t1.S1 = t2.S1;

-- -------------------------------------------------------------
-- 20. Row-count invariant on a larger dataset
--     |FULL| = |left-only| + |matched| + |right-only|
-- -------------------------------------------------------------
drop table if exists big_a;
drop table if exists big_b;
create table big_a(k int primary key, v int);
create table big_b(k int primary key, v int);
insert into big_a select result, result*10 from generate_series(1,1000) g;
insert into big_b select result, result*20 from generate_series(500,1500) g;

SELECT
  (SELECT COUNT(*) FROM big_a FULL OUTER JOIN big_b ON big_a.k = big_b.k) AS full_cnt,
  (SELECT COUNT(*) FROM big_a LEFT  JOIN big_b ON big_a.k = big_b.k WHERE big_b.k IS NULL) AS left_only,
  (SELECT COUNT(*) FROM big_a INNER JOIN big_b ON big_a.k = big_b.k) AS matched,
  (SELECT COUNT(*) FROM big_a RIGHT JOIN big_b ON big_a.k = big_b.k WHERE big_a.k IS NULL) AS right_only;

-- -------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------
drop database if exists fulljoin_db;
