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

-- Section 8: ORDER BY multi-key over FULL OUTER JOIN with NULLs in keys.
-- Regression for #24248: secondary sort key must work within partitions
-- formed when the primary key has adjacent NULLs (from null-padded rows).
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

drop database fojdb;
