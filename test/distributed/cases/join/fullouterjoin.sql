-- =====================================================================
-- FULL OUTER JOIN tests
-- Covers Phase 1 (broadcast hash equi), Phase 2 (shuffle hash equi),
-- Phase 3 (spill), and Phase 4 (loopjoin non-equi). Each section is
-- annotated with the path it primarily exercises.
-- =====================================================================
drop database if exists fojdb;
create database fojdb;
use fojdb;

-- ---------------------------------------------------------------------
-- 1. Basic semantics: matched, unmatched-left, unmatched-right
-- ---------------------------------------------------------------------
drop table if exists l;
drop table if exists r;
create table l (k int, lv varchar(8));
create table r (k int, rv varchar(8));
insert into l values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(null,'lN');
insert into r values (3,'C'),(4,'D'),(5,'E'),(6,'F'),(null,'rN');

-- Symmetric coverage: matched (3,4), unmatched-left (1,2,nullKey),
-- unmatched-right (5,6,nullKey). NULL keys never match.
select l.k as lk, lv, r.k as rk, rv
  from l full outer join r on l.k = r.k
  order by lk, rk, lv, rv;

-- Counts cross-check
select count(*) as total,
       sum(case when l.k is not null and r.k is not null then 1 else 0 end) as matched,
       sum(case when l.k is null then 1 else 0 end) as unmatched_right_or_lk_null,
       sum(case when r.k is null then 1 else 0 end) as unmatched_left_or_rk_null
  from l full outer join r on l.k = r.k;

-- WHERE on the left side filters out unmatched-right rows (l.k IS NULL)
select l.k as lk, r.k as rk
  from l full outer join r on l.k = r.k
  where l.k is not null
  order by lk;

-- ---------------------------------------------------------------------
-- 2. Empty corners
-- ---------------------------------------------------------------------
drop table if exists e;
create table e (k int);
-- empty FOJ non-empty: every right row appears with null left
select l.k as lk, e.k as ek from e full outer join l on e.k = l.k order by lk;
-- non-empty FOJ empty: every left row appears with null right
select l.k as lk, e.k as ek from l full outer join e on l.k = e.k order by lk;
-- empty FOJ empty
select * from e a full outer join e b on a.k = b.k;

-- ---------------------------------------------------------------------
-- 3. Composite keys + duplicates (Cartesian within matched group)
-- ---------------------------------------------------------------------
drop table if exists ld;
drop table if exists rd;
create table ld (k1 int, k2 int, v int);
create table rd (k1 int, k2 int, v int);
insert into ld values (1,1,10),(1,1,11),(2,2,20),(3,3,30);
insert into rd values (1,1,100),(1,1,101),(2,2,200),(4,4,400);
-- Matched (1,1) yields 4 rows (2x2), (2,2) yields 1, unmatched (3,3) and (4,4) each yield 1
select ld.k1, ld.k2, ld.v, rd.v
  from ld full outer join rd on ld.k1=rd.k1 and ld.k2=rd.k2
  order by coalesce(ld.k1,rd.k1), coalesce(ld.k2,rd.k2), ld.v, rd.v;

-- ---------------------------------------------------------------------
-- 4. Multi-column projection + expressions referencing both sides
-- ---------------------------------------------------------------------
select coalesce(l.k, r.k) as key_,
       case when l.k is null then 'R-only'
            when r.k is null then 'L-only'
            else 'both' end as origin,
       lv, rv
  from l full outer join r on l.k = r.k
  order by key_, origin, lv, rv;

-- ---------------------------------------------------------------------
-- 5. Nested in subquery + aggregation
-- ---------------------------------------------------------------------
select origin, count(*) as c
  from (select case when l.k is null then 'R-only'
                    when r.k is null then 'L-only'
                    else 'both' end as origin
        from l full outer join r on l.k = r.k) t
  group by origin order by origin;

-- ---------------------------------------------------------------------
-- 6. ON predicate with extra build-side restriction
-- (extra predicate is part of join, NOT a WHERE filter)
-- ---------------------------------------------------------------------
select l.k as lk, r.k as rk, rv
  from l full outer join r on l.k = r.k and r.rv <> 'C'
  order by lk, rk, rv;

-- ---------------------------------------------------------------------
-- 7. Larger data — exercise multi-batch probe + finalize
-- ---------------------------------------------------------------------
drop table if exists big_l;
drop table if exists big_r;
create table big_l (k int);
create table big_r (k int);
insert into big_l select * from generate_series(1, 5000) g;
insert into big_r select * from generate_series(2500, 7500) g;
-- Expect: matched 2501, unmatched-left 2499, unmatched-right 2500 → total 7500
select count(*) as total,
       sum(case when big_l.k is not null and big_r.k is not null then 1 else 0 end) as matched,
       sum(case when big_l.k is null then 1 else 0 end) as unmatched_right,
       sum(case when big_r.k is null then 1 else 0 end) as unmatched_left
  from big_l full outer join big_r on big_l.k = big_r.k;

-- ---------------------------------------------------------------------
-- 8. FULL JOIN as alias for FULL OUTER JOIN
-- ---------------------------------------------------------------------
select count(*) from l full join r on l.k = r.k;

-- ---------------------------------------------------------------------
-- 9. Non-equi FULL OUTER JOIN — Phase 4 (loopjoin)
-- ---------------------------------------------------------------------
-- Inequality on the small table: every l matched (its k > some r.k); checks
-- unmatched-build emission via Finalize.
select count(*) as total,
       sum(case when l.k is not null and r.k is not null then 1 else 0 end) as matched,
       sum(case when l.k is null then 1 else 0 end) as unmatched_build_or_lk_null,
       sum(case when r.k is null then 1 else 0 end) as unmatched_probe_or_rk_null
  from l full outer join r on l.k > r.k;

-- Inequality with empty build: every probe row null-padded right.
select l.k as lk, e.k as ek
  from l full outer join e on l.k > e.k
  order by lk;

-- Inequality with empty probe: every build row null-padded left via Finalize.
select e.k as ek, l.k as lk
  from e full outer join l on e.k > l.k
  order by lk;


-- ---------------------------------------------------------------------
-- 10. FULL OUTER JOIN ... USING(col): merged column must coalesce both
--    sides (issue #24247). Null-padded rows must surface the value from
--    the non-padded side.
-- ---------------------------------------------------------------------
drop table if exists u1;
drop table if exists u2;
drop table if exists u3;
create table u1(id int, v varchar(4));
create table u2(id int, v varchar(4));
create table u3(id int, v varchar(4));
insert into u1 values (1,'a'),(2,'b'),(3,'c');
insert into u2 values (2,'B'),(3,'C'),(4,'D');
insert into u3 values (3,'x'),(4,'y'),(5,'z');

-- Bare USING column is COALESCE(left.id, right.id): never NULL on a row
-- that exists on either side.
select id from u1 full outer join u2 using(id) order by id;

-- SELECT * exposes the merged column once and then each side's other cols.
select * from u1 full outer join u2 using(id) order by id;

-- WHERE on the merged column resolves to coalesce; predicate must hit
-- right-only and left-only rows.
select id from u1 full outer join u2 using(id) where id = 4;
select id from u1 full outer join u2 using(id) where id = 1;

-- GROUP BY / aggregate over the merged column.
select id, count(*) from u1 full outer join u2 using(id) group by id order by id;

-- Nested FOJ ... USING: inner merged id propagates as coalesce into the
-- outer USING comparison, so id=4 matches across all three tables once.
select id from (u1 full outer join u2 using(id)) full outer join u3 using(id)
  order by id;

-- Qualified column references keep the per-side value (NULL on padded side).
select u1.id as l_id, u2.id as r_id
  from u1 full outer join u2 using(id)
  order by l_id, r_id;

-- INNER JOIN ... USING(col): merged column resolves to the chosen
-- (left) side, NOT a coalesce. This exercises the non-OUTER USING path.
select id from u1 inner join u2 using(id) order by id;

-- Nested: (FOJ ... USING) INNER JOIN ... USING(col). The outer INNER USING
-- inherits the inner FOJ's coalesce list, so id resolves through the FOJ
-- arms and the equality predicate compares against u3.id.
select id from (u1 full outer join u2 using(id)) inner join u3 using(id)
  order by id;

-- LEFT JOIN ... USING(col) on top of FOJ ... USING preserves the inner
-- coalesce list on the chosen left side.
select id from (u1 full outer join u2 using(id)) left join u3 using(id)
  order by id;

-- Star-expand on nested FOJ-USING: merged column appears once, via coalesce.
select * from (u1 full outer join u2 using(id)) full outer join u3 using(id)
  order by id;

-- Sibling FOJ-USING subtrees joined at the same level: each subtree's
-- merged id must coalesce its OWN arms, not inherit the sibling's. The
-- bind-context-wide outerUsingCols map is shared, so per-NameTuple arms
-- on the binding tree are required for SELECT * to resolve correctly.
-- u3 here doubles as the right subtree's left arm; values arranged so
-- left.id ranges over {1,2,3,4} and right.id ranges over {3,4,5,6}.
drop table if exists s1;
drop table if exists s2;
create table s1(id int);
create table s2(id int);
insert into s1 values (3),(4),(6);
insert into s2 values (4),(5),(6);
select * from (u1 full outer join u2 using(id)),
              (s1 full outer join s2 using(id))
  order by 1, 2, 3, 4;
drop table s1;
drop table s2;

drop table u1;
drop table u2;
drop table u3;

-- ---------------------------------------------------------------------
-- 11. NATURAL FULL [OUTER] JOIN (issue #24250). Grammar must reduce both
--    `natural full outer join` and `natural full join` to FULL OUTER,
--    not silently to RIGHT.
-- ---------------------------------------------------------------------
drop table if exists n1;
drop table if exists n2;
create table n1(id int, v varchar(4));
create table n2(id int, v varchar(4));
insert into n1 values (1,'a'),(2,'b');
insert into n2 values (2,'B'),(3,'C');

select id from n1 natural full outer join n2 order by id;
select id from n1 natural full join n2 order by id;
select * from n1 natural full outer join n2 order by id, v;

drop table n1;
drop table n2;

-- ---------------------------------------------------------------------
-- 12. ORDER BY multi-key over FULL OUTER JOIN with NULLs in keys.
--    Regression for #24248: secondary sort key must work within
--    partitions formed when the primary key has adjacent NULLs (from
--    null-padded rows).
-- ---------------------------------------------------------------------
create table fj_o1(s int, v varchar(5));
create table fj_o2(s int, v varchar(5));
insert into fj_o1 values (1,'a'),(5,'b'),(NULL,'x');
insert into fj_o2 values (13,'c'),(14,NULL);
select fj_o1.s, fj_o1.v, fj_o2.s, fj_o2.v
  from fj_o1 full outer join fj_o2 on fj_o1.s = fj_o2.s
  order by fj_o1.s, fj_o2.s, fj_o1.v, fj_o2.v;
select fj_o1.s, fj_o2.s
  from fj_o1 full outer join fj_o2 on fj_o1.s = fj_o2.s
  order by fj_o1.s, fj_o2.s desc;
drop table fj_o1;
drop table fj_o2;

drop database fojdb;
