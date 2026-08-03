-- @suite
-- @setup
drop database if exists test_nested_correlated_scalar;
create database test_nested_correlated_scalar;
use test_nested_correlated_scalar;
create table j_dim  (id int);
create table j_fact (id int, dim_id int, ts int, val double);
insert into j_dim values (1), (2), (3);
insert into j_fact values
    (1, 1, 10, 100),
    (2, 1, 20, 200),
    (3, 2, 20, 220),
    (4, 2, 30, 300),
    (5, 99, 0, 900);

-- @case
-- @desc: scalar aggregate nested inside another scalar aggregate may correlate two levels up
-- @label:bvt
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (SELECT MAX(y.ts)
                         FROM j_fact y
                        WHERE y.dim_id = a.id)
       ) AS latest
FROM j_dim a
ORDER BY a.id;

-- approx_count returns 0 for empty input. Until the deep decorrelation can
-- synthesize that missing aggregate row, keep this shape on the NYI path
-- instead of silently turning 0 into NULL. id=3 has no matching y rows while
-- x contains ts=0, so an unsafe rewrite would return NULL instead of 900.
-- @pattern
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (SELECT APPROX_COUNT(y.ts)
                         FROM j_fact y
                        WHERE y.dim_id = a.id)
       ) AS latest
FROM j_dim a
WHERE a.id = 3;

-- Even a NULL-on-empty inner aggregate is unsafe when its complete consuming
-- expression observes that NULL. For id=3, MAX(y.ts) is NULL and COALESCE
-- produces 0 for each of the five x rows, so SQL COUNT returns 5. The current
-- deep rewrite cannot synthesize the missing per-key aggregate row and must
-- keep this shape on the NYI path instead of returning 0.
-- @pattern
SELECT a.id,
       (SELECT COUNT(COALESCE(
                   (SELECT MAX(y.ts)
                      FROM j_fact y
                     WHERE y.dim_id = a.id),
                   0))
          FROM j_fact x) AS actual
  FROM j_dim a
 WHERE a.id = 3;

-- The same missing-key case is unsafe when COALESCE feeds the enclosing
-- filter. SQL semantics match x.ts=0 and return 900; dropping the x row would
-- incorrectly expose NULL, so this shape also remains NYI.
-- @pattern
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = COALESCE(
                   (SELECT MAX(y.ts)
                      FROM j_fact y
                     WHERE y.dim_id = a.id),
                   0)) AS latest
  FROM j_dim a
 WHERE a.id = 3;

-- A NULL-observing projection inside the deep scalar is unsafe too. For id=3,
-- SQL evaluates COALESCE(MAX(y.ts), 0) to 0 and matches the x.ts=0 row, yielding
-- 900. The grouped rewrite has no y.dim_id=3 row on which to run COALESCE, so
-- it would expose NULL and incorrectly drop that x row. Keep this shape NYI.
-- @pattern
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (
               SELECT COALESCE(MAX(y.ts), 0)
                 FROM j_fact y
                WHERE y.dim_id = a.id)) AS latest
  FROM j_dim a
 WHERE a.id = 3;

-- LIMIT 1 is redundant for an implicit scalar aggregate when evaluated once
-- per outer row. After decorrelation adds dim_id to GROUP BY, however, leaving
-- the limit on the grouped plan would keep only one correlation key globally.
-- Reject the shape until LIMIT can be rewritten per key. This query spans two
-- matching outer keys and one missing key so a global limit cannot hide.
-- @pattern
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (
               SELECT MAX(y.ts)
                 FROM j_fact y
                WHERE y.dim_id = a.id
                LIMIT 1)) AS latest
  FROM j_dim a
 ORDER BY a.id;

-- OFFSET 1 removes the sole implicit aggregate row independently for every
-- outer key. A global offset after grouping would instead skip only one key
-- and expose another, so this topology must remain NYI too.
-- @pattern
SELECT a.id,
       (SELECT MAX(x.val)
          FROM j_fact x
         WHERE x.ts = (
               SELECT MAX(y.ts)
                 FROM j_fact y
                WHERE y.dim_id = a.id
                LIMIT 1 OFFSET 1)) AS latest
  FROM j_dim a
 ORDER BY a.id;

-- @teardown
drop database test_nested_correlated_scalar;
