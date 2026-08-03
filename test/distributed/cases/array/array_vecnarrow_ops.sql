-- narrow vector types (vecbf16/vecf16/vecint8): comparison operators, ordering/
-- grouping/aggregates, distance functions, and the arithmetic rule (elementwise
-- arithmetic errors -- must CAST to vecf32 first). Small integers are exact in all
-- three narrow formats, so results are deterministic.
drop database if exists nvops;
create database nvops;
use nvops;

-- ===== vecbf16 =====
create table b(a int, v vecbf16(3));
insert into b values (1,'[1,2,3]'),(2,'[4,5,6]'),(3,'[1,2,3]'),(4,'[7,8,9]');
-- comparison operators
select a, v = cast('[1,2,3]' as vecbf16(3)) as eq, v != cast('[1,2,3]' as vecbf16(3)) as ne, v < cast('[4,5,6]' as vecbf16(3)) as lt, v > cast('[1,2,3]' as vecbf16(3)) as gt, v <= cast('[1,2,3]' as vecbf16(3)) as le, v >= cast('[4,5,6]' as vecbf16(3)) as ge from b order by a;
-- ordering / distinct / group by / aggregates (all use comparison)
select v from b order by v, a;
select distinct v from b order by v;
select v, count(*) as c from b group by v order by v;
-- where filter by comparison
select a from b where v = cast('[1,2,3]' as vecbf16(3)) order by a;
select a from b where v < cast('[4,5,6]' as vecbf16(3)) order by a;
-- distance functions
select a, round(l2_distance(v, cast('[1,2,3]' as vecbf16(3))),4) as l2, round(l2_distance_sq(v, cast('[1,2,3]' as vecbf16(3))),4) as l2sq, round(inner_product(v, cast('[1,2,3]' as vecbf16(3))),4) as ip, round(cosine_distance(v, cast('[1,2,3]' as vecbf16(3))),4) as cd, round(cosine_similarity(v, cast('[1,2,3]' as vecbf16(3))),4) as cs from b order by a;
-- arithmetic is NOT supported on narrow types directly
select v + v from b;
select v - v from b;
select v * 2 from b;
-- ... but works after an explicit CAST to vecf32
select a, cast(v as vecf32(3)) + cast('[1,1,1]' as vecf32(3)) as r from b order by a;

-- ===== vecf16 =====
create table f(a int, v vecf16(3));
insert into f values (1,'[1,2,3]'),(2,'[4,5,6]'),(3,'[1,2,3]');
select a, v = cast('[1,2,3]' as vecf16(3)) as eq, v < cast('[4,5,6]' as vecf16(3)) as lt from f order by a;
select distinct v from f order by v;
select v, count(*) as c from f group by v order by v;
select a, round(l2_distance_sq(v, cast('[1,2,3]' as vecf16(3))),4) as l2sq, round(inner_product(v, cast('[1,2,3]' as vecf16(3))),4) as ip from f order by a;
select v * 2 from f;

-- ===== vecint8 =====
create table i(a int, v vecint8(3));
insert into i values (1,'[1,2,3]'),(2,'[4,5,6]'),(3,'[1,2,3]');
select a, v = cast('[1,2,3]' as vecint8(3)) as eq, v < cast('[4,5,6]' as vecint8(3)) as lt from i order by a;
select distinct v from i order by v;
select v, count(*) as c from i group by v order by v;
select a, round(l2_distance_sq(v, cast('[1,2,3]' as vecint8(3))),4) as l2sq, round(inner_product(v, cast('[1,2,3]' as vecint8(3))),4) as ip from i order by a;
select v + v from i;

drop database nvops;
