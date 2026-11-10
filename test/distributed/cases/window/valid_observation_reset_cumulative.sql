-- @suite

-- @case
-- @desc: Documented migration equivalents for valid-observation windows and
-- reset-aware cumulative state; entry-name availability vs semantic coverage (#29948)
-- @label:bvt

-- Issue #29948 requests DolphinDB-style valid-observation-count windows and
-- boundary/reset-aware cumulative operators (entry names movingWindowIndex,
-- dynamicGroupCumsum, dynamicGroupCumcount) or documented migration equivalents.
-- Design + staged-SQL equivalents: docs/design/20261024-valid-observation-windows-reset-cumulative.md
--
-- The three entry names remain unsupported (expected errors below, error 20105);
-- that availability gap is asserted separately from the mathematical rewrites,
-- exactly as the issue's acceptance split requires. The staged-SQL equivalents
-- below implement, on unmodified current SQL:
--   I1 last-K-valid window: the frame of row r is exactly the last K ordinal
--      positions i <= r with v_i IS NOT NULL; NULL when that set is empty;
--      one output row per input row; row order and source columns preserved.
--   I2 reset-aware cumulative: a new run starts iff the reset predicate is
--      TRUE or it is the partition's first row; the boundary row enters its
--      new run (current-row inclusion); a NULL value emits the current state
--      without mutating it (carry); an all-NULL run prefix emits NULL.
--   I3 one total-order ordinal rid := ROW_NUMBER() OVER (PARTITION BY p
--      ORDER BY o, <tie-breaks>) is used in every stage with explicit ROWS
--      framing on cumulative stages, so ties never merge runs or frames.
--
-- EXPLAIN cond-shapes executed on the unmodified base during implementation
-- (transcripts in the handler evidence directory; asserted values below, not
-- plan text): the band join's cond is the NULL-safe partition-equality OR plus
-- the two BETWEEN inequalities with NO equality conjunct, so compile.go:7178-7193
-- selects LoopJoin (accepted quadratic-comparison worst case, O(N*V), migration
-- profile only); the join-back cond carries the pure j.rid = t.rid equality plus
-- the partition-equality OR residual, so it is hash-join eligible. With P
-- partitions sharing one rid value the duplicate-key residual costs up to P^2
-- comparisons (operator.go:2902; hashjoin/join.go:921-1006); the many-partition
-- check at the end exercises the all-singleton case (every rid = 1) and asserts
-- exact results and row cardinality.

drop database if exists valid_observation_reset_cumulative;
create database valid_observation_reset_cumulative;
use valid_observation_reset_cumulative;

-- The issue's verified fixture: v carries a NULL over ids 1 and 5; flag rows
-- mark reset boundaries; tick is the session/regime key.
create table issue_fixture (id int primary key, tick int, v double, flag int);
insert into issue_fixture values (1,0,null,1),(2,0,2,0),(3,1,4,1),(4,3,4,1),(5,3,null,0),(6,8,-1,1);

-- Named/API availability stays visible separately: expected error 20105 each.
select movingWindowIndex(CAST(1 AS DOUBLE),3) result;
select dynamicGroupCumsum(CAST(1 AS DOUBLE),1) result;
select dynamicGroupCumcount(CAST(1 AS DOUBLE),1) result;

-- Negative control: a physical two-row frame must NOT reproduce the
-- valid-observation profile (differs at ids 5,6: 4 and -1, not 8 and 3).
select id, sum(v) over (order by id rows between 1 preceding and current row) as phys_result
from issue_fixture order by id;

-- Valid-observation window ("last K valid"), K=2, on the persisted issue fixture.
-- Expected by id: NULL,2,6,8,8,3 (the issue's verified last-two-valid profile;
-- id 5 carries the frame across its own NULL row).
WITH t AS (
  SELECT id AS o, 1 AS p, v, id FROM issue_fixture),
sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Reset-aware cumulative, P-flag form (explicit reset flag).
-- Expected by id: NULL,2,4,4,4,-1 (the issue's verified reset-on-flag profile).
WITH t AS (
  SELECT id AS o, 1 AS p, v, flag, id FROM issue_fixture),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Reset-aware cumulative, P-key form (derived consecutive-key boundary, k := tick).
-- Expected by id: NULL,2,4,4,4,-1 (coincides: tick boundaries equal flag rows here).
WITH t AS (
  SELECT id AS o, 1 AS p, v, tick AS k, id FROM issue_fixture),
sequenced AS (
  SELECT o, p, v, k, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, v, k, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, v, k, id, rid,
         SUM(CASE WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, v, k,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- The declared P-key predicate and the template's CASE indicator must agree
-- across every three-valued-logic transition. Expected columns by tag:
-- declared_pred and indicator both 1,0,1,1,1,1.
SELECT tag, prev_k, k,
       ((prev_k = k) IS NOT TRUE) AS declared_pred,
       CASE WHEN prev_k = k THEN 0 ELSE 1 END AS indicator
FROM (
  SELECT 1 AS tag, CAST(NULL AS SIGNED) AS prev_k, 9 AS k UNION ALL
  SELECT 2, 9, 9 UNION ALL
  SELECT 3, 9, 8 UNION ALL
  SELECT 4, 9, CAST(NULL AS SIGNED) UNION ALL
  SELECT 5, CAST(NULL AS SIGNED), CAST(NULL AS SIGNED) UNION ALL
  SELECT 6, CAST(NULL AS SIGNED), 9
) t
ORDER BY tag;

-- Valid-window across three partitions (p=1 dense, p=NULL, p=2 all-NULL), K=2.
-- Partitions are independent; NULL partition keys form one partition.
-- Expected by id: 1->10, 2->10, 3->30, 4->50, 5->NULL, 6->7, 7->NULL, 8->NULL.
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 1 AS o, 1 AS p, CAST(10 AS DOUBLE) AS v UNION ALL
        SELECT 2, 2, 1, CAST(NULL AS DOUBLE) UNION ALL
        SELECT 3, 3, 1, CAST(20 AS DOUBLE) UNION ALL
        SELECT 4, 4, 1, CAST(30 AS DOUBLE) UNION ALL
        SELECT 5, 5, CAST(NULL AS SIGNED), CAST(NULL AS DOUBLE) UNION ALL
        SELECT 6, 6, CAST(NULL AS SIGNED), CAST(7 AS DOUBLE) UNION ALL
        SELECT 7, 7, 2, CAST(NULL AS DOUBLE) UNION ALL
        SELECT 8, 8, 2, CAST(NULL AS DOUBLE)) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Valid-window on a NULL-partition single row, K=2. Expected: 5 (not NULL).
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 1 AS o, CAST(NULL AS SIGNED) AS p, CAST(5 AS DOUBLE) AS v) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Two-row tie counterexample: duplicate order values must be resolved by rid,
-- so the last-1-valid of each row is itself. Expected by id: 5,7.
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 5 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v UNION ALL
        SELECT 2, 5, 1, CAST(7 AS DOUBLE)) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (1 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Discriminator D1 (consecutive true flags, unchanged keys): flag form must
-- reset each row. Expected by id: 5,7.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 1 AS flag UNION ALL
  SELECT 2, 2, 1, CAST(7 AS DOUBLE), 1),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Discriminator D1 under the key form (keys 9,9 equal): no key boundary, so the
-- rows must share one run. Expected by id: 5,12.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 9 AS k UNION ALL
  SELECT 2, 2, 1, CAST(7 AS DOUBLE), 9),
sequenced AS (
  SELECT o, p, v, k, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, v, k, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, v, k, id, rid,
         SUM(CASE WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, v, k,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Discriminator D2 (key changes without a reset flag): flag form must keep one
-- run. Expected by id: 5,12.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 0 AS flag UNION ALL
  SELECT 2, 2, 1, CAST(7 AS DOUBLE), 0),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Discriminator D2 under the key form (keys 1,2 differ): the change must open a
-- new run. Expected by id: 5,7.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 1 AS k UNION ALL
  SELECT 2, 2, 1, CAST(7 AS DOUBLE), 2),
sequenced AS (
  SELECT o, p, v, k, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, v, k, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, v, k, id, rid,
         SUM(CASE WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, v, k,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Tied-order consecutive-reset counterexample (ids 1,2 share o=5), flag form:
-- flags [1,1] require separate runs, asserted by source id. Expected: 5,7.
WITH t AS (
  SELECT 1 AS id, 5 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 1 AS flag UNION ALL
  SELECT 2, 5, 1, CAST(7 AS DOUBLE), 1),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Same tied fixture under the key form with distinct keys 9,8: both rows open
-- their own run, asserted by source id. Expected: 5,7.
WITH t AS (
  SELECT 1 AS id, 5 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 9 AS k UNION ALL
  SELECT 2, 5, 1, CAST(7 AS DOUBLE), 8),
sequenced AS (
  SELECT o, p, v, k, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, v, k, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, v, k, id, rid,
         SUM(CASE WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, v, k,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Composed predicate (flag OR key change) on tied order values, both directions:
-- peers (1,2) share o=5 with no boundary and must merge; peers (3,4) share o=7
-- with a reset at id 3 and must split. Expected by id: 5,12,11,24.
WITH t AS (
  SELECT 1 AS id, 5 AS o, 1 AS p, 0 AS flag, 9 AS k, CAST(5 AS DOUBLE) AS v UNION ALL
  SELECT 2, 5, 1, 0, 9, CAST(7 AS DOUBLE) UNION ALL
  SELECT 3, 7, 1, 1, 9, CAST(11 AS DOUBLE) UNION ALL
  SELECT 4, 7, 1, 0, 9, CAST(13 AS DOUBLE)),
sequenced AS (
  SELECT o, p, flag, k, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, flag, k, v, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, flag, k, v, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, flag, k, v,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Composed predicate with NULL flag fall-through and NULL-key boundaries.
-- Expected by id: 5,12,11,13,17,19.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, 0 AS flag, 1 AS k, CAST(5 AS DOUBLE) AS v UNION ALL
  SELECT 2, 2, 1, 0, 1, CAST(7 AS DOUBLE) UNION ALL
  SELECT 3, 3, 1, 1, 1, CAST(11 AS DOUBLE) UNION ALL
  SELECT 4, 4, 1, 0, 2, CAST(13 AS DOUBLE) UNION ALL
  SELECT 5, 5, 1, 0, CAST(NULL AS SIGNED), CAST(17 AS DOUBLE) UNION ALL
  SELECT 6, 6, 1, CAST(NULL AS SIGNED), CAST(NULL AS SIGNED), CAST(19 AS DOUBLE)),
sequenced AS (
  SELECT o, p, flag, k, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, flag, k, v, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, flag, k, v, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, flag, k, v,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- NULL flags never reset; a NULL value carries; an all-NULL run prefix emits
-- NULL. flag=[0,0,NULL,0], v=[NULL,5,NULL,7]. Expected by id: NULL,5,5,12.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(NULL AS DOUBLE) AS v, 0 AS flag UNION ALL
  SELECT 2, 2, 1, CAST(5 AS DOUBLE), 0 UNION ALL
  SELECT 3, 3, 1, CAST(NULL AS DOUBLE), CAST(NULL AS SIGNED) UNION ALL
  SELECT 4, 4, 1, CAST(7 AS DOUBLE), 0),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- NULL partition keys group natively; under the P-key form each NULL-key row
-- opens its own run (IS NOT TRUE), adjacent to a normal keyed run.
-- (o,p,k,v): (1,NULL,5,2),(2,NULL,NULL,3),(3,NULL,NULL,4),(4,NULL,5,1),(5,9,5,10).
-- Expected by o: 2,3,4,1,10.
WITH t AS (
  SELECT 1 AS id, 1 AS o, CAST(NULL AS SIGNED) AS p, 5 AS k, CAST(2 AS DOUBLE) AS v UNION ALL
  SELECT 2, 2, CAST(NULL AS SIGNED), CAST(NULL AS SIGNED), CAST(3 AS DOUBLE) UNION ALL
  SELECT 3, 3, CAST(NULL AS SIGNED), CAST(NULL AS SIGNED), CAST(4 AS DOUBLE) UNION ALL
  SELECT 4, 4, CAST(NULL AS SIGNED), 5, CAST(1 AS DOUBLE) UNION ALL
  SELECT 5, 5, 9, 5, CAST(10 AS DOUBLE)),
sequenced AS (
  SELECT o, p, v, k, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, v, k, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, v, k, id, rid,
         SUM(CASE WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, v, k,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Consecutive resets: flags [1,1,1,0] restart the first three rows; the last
-- joins the third run. Expected by id: 2,3,4,9.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(2 AS DOUBLE) AS v, 1 AS flag UNION ALL
  SELECT 2, 2, 1, CAST(3 AS DOUBLE), 1 UNION ALL
  SELECT 3, 3, 1, CAST(4 AS DOUBLE), 1 UNION ALL
  SELECT 4, 4, 1, CAST(5 AS DOUBLE), 0),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Empty input: both templates emit zero rows (one output row per input row).
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 0 AS flag
  WHERE 1 = 0),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v WHERE 1 = 0) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Single-row boundaries: one valid row sums to itself; one all-NULL row (a run
-- prefix with no valid value yet) emits NULL.
WITH t AS (SELECT 1 AS id, 1 AS o, 1 AS p, CAST(5 AS DOUBLE) AS v, 0 AS flag),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

WITH t AS (SELECT 1 AS id, 1 AS o, 1 AS p, CAST(NULL AS DOUBLE) AS v, 1 AS flag),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Numeric boundaries: negative and large-magnitude DOUBLE values.
-- Reset form expects by id: -15000000000, 2499985000000000, 2499984575000000.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, CAST(-1.5E10 AS DOUBLE) AS v, 0 AS flag UNION ALL
  SELECT 2, 2, 1, CAST(2.5E15 AS DOUBLE), 0 UNION ALL
  SELECT 3, 3, 1, CAST(-4.25E8 AS DOUBLE), 0),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Same DOUBLE fixture, valid-window K=2.
-- Expected by id: -15000000000, 2499985000000000, 2499999575000000.
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 1 AS o, 1 AS p, CAST(-1.5E10 AS DOUBLE) AS v UNION ALL
        SELECT 2, 2, 1, CAST(2.5E15 AS DOUBLE) UNION ALL
        SELECT 3, 3, 1, CAST(-4.25E8 AS DOUBLE)) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Type boundary: an integer value column. Reset form expects by id: 5,12,11.
WITH t AS (
  SELECT 1 AS id, 1 AS o, 1 AS p, 5 AS v, 0 AS flag UNION ALL
  SELECT 2, 2, 1, 7, 0 UNION ALL
  SELECT 3, 3, 1, 11, 1),
sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;

-- Integer value column with NULL, valid-window K=2. Expected by id: 5,5,12.
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 1 AS o, 1 AS p, 5 AS v UNION ALL
        SELECT 2, 2, 1, CAST(NULL AS SIGNED) UNION ALL
        SELECT 3, 3, 1, 7) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

-- Many-partition join-back check: nine singleton partitions (p=1..8, p=NULL),
-- so every join-back hash key collides at rid = 1 and the partition-equality
-- residual must still pick exactly one match. Expected by id:
-- 10,20,NULL,40,NULL,60,70,80,90; nine rows in, nine rows out.
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM (SELECT 1 AS id, 1 AS o, 1 AS p, CAST(10 AS DOUBLE) AS v UNION ALL
        SELECT 2, 1, 2, CAST(20 AS DOUBLE) UNION ALL
        SELECT 3, 1, 3, CAST(NULL AS DOUBLE) UNION ALL
        SELECT 4, 1, 4, CAST(40 AS DOUBLE) UNION ALL
        SELECT 5, 1, 5, CAST(NULL AS DOUBLE) UNION ALL
        SELECT 6, 1, 6, CAST(60 AS DOUBLE) UNION ALL
        SELECT 7, 1, 7, CAST(70 AS DOUBLE) UNION ALL
        SELECT 8, 1, 8, CAST(80 AS DOUBLE) UNION ALL
        SELECT 9, 1, CAST(NULL AS SIGNED), CAST(90 AS DOUBLE)) t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (2 - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;

drop database valid_observation_reset_cumulative;
