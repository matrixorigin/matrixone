-- vecbf16 / vecf16 / vecint8 narrow vector column types
-- Scope (per design): distance functions + casts + storage only.
-- Elementwise arithmetic (+ - * / sqrt abs summation subvector) is NOT
-- supported on the narrow types and must require an explicit CAST to vecf32.

-- pre
drop database if exists vecnarrowdb;
create database vecnarrowdb;
use vecnarrowdb;
drop table if exists nvec;

-- standard: one column of each new type
create table nvec(a int, bf vecbf16(3), f16 vecf16(3), i8 vecint8(3));
desc nvec;
show create table nvec;
insert into nvec values(1, "[1,2,3]", "[1,2,3]", "[1,2,3]");
insert into nvec values(2, "[4,5,6]", "[4,5,6]", "[4,5,6]");
select * from nvec;

-- int8: a string literal must be an integer in [-128,127]; boundary values OK.
drop table if exists i8t;
create table i8t(a int, v vecint8(4));
insert into i8t values(1, "[127,-128,0,5]");
select * from i8t;
-- out-of-range and non-integer string literals error (no silent round/clamp)
insert into i8t values(2, "[200,-200,0,0]");
insert into i8t values(3, "[1.4,2.6,0,0]");
-- rounding/clamping IS available, but only via the vecf32 -> vecint8 CAST path
select cast(cast("[1.6,200,-3.5,-200]" as vecf32(4)) as vecint8(4));

-- string -> narrow casts
select cast("[1,2,3]" as vecbf16(3));
select cast("[1,2,3]" as vecf16(3));
select cast("[1,2,3]" as vecint8(3));
-- non-integer string literal -> vecint8 errors (strict)
select cast("[1.4,2.6,-3.5]" as vecint8(3));

-- narrow -> vecf32 / vecf64 casts (the explicit widening path)
select cast(bf as vecf32(3)), cast(f16 as vecf32(3)), cast(i8 as vecf32(3)) from nvec order by a;
select cast(bf as vecf64(3)) from nvec order by a;

-- vecf32 -> narrow casts
select cast(cast("[1,2,3]" as vecf32(3)) as vecbf16(3));
select cast(cast("[1,2,3]" as vecf32(3)) as vecf16(3));
select cast(cast("[1.6,2.4,-3.5]" as vecf32(3)) as vecint8(3));

-- narrow -> narrow casts
select cast(bf as vecf16(3)), cast(f16 as vecint8(3)), cast(i8 as vecbf16(3)) from nvec order by a;

-- distance functions on bf16
select l2_distance(bf, "[1,2,3]") from nvec order by a;
select l2_distance_sq(bf, "[1,2,3]") from nvec order by a;
select inner_product(bf, "[1,2,3]") from nvec order by a;
select cosine_distance(bf, "[1,2,3]") from nvec order by a;
select cosine_similarity(bf, "[1,2,3]") from nvec order by a;
select normalize_l2(bf) from nvec order by a;

-- distance functions on f16
select l2_distance(f16, "[1,2,3]") from nvec order by a;
select inner_product(f16, "[1,2,3]") from nvec order by a;
select cosine_distance(f16, "[1,2,3]") from nvec order by a;
select normalize_l2(f16) from nvec order by a;

-- distance functions on int8
select l2_distance(i8, "[1,2,3]") from nvec order by a;
select inner_product(i8, "[1,2,3]") from nvec order by a;
select cosine_distance(i8, "[1,2,3]") from nvec order by a;

-- distance between two narrow columns of the same type
select l2_distance(bf, cast("[4,5,6]" as vecbf16(3))) from nvec order by a;
select l2_distance(i8, cast("[4,5,6]" as vecint8(3))) from nvec order by a;

-- top-K (ORDER BY distance + LIMIT)
select a FROM nvec ORDER BY l2_distance(bf, '[1,2,3]') LIMIT 5;
select a FROM nvec ORDER BY cosine_distance(f16, '[1,2,3]') LIMIT 5;
select a FROM nvec ORDER BY inner_product(i8, '[1,2,3]') LIMIT 5;

-- filtering / equality / ordering
select * from nvec where i8 = "[1,2,3]";
select * from nvec order by bf desc;
select distinct v from i8t order by v;

-- negative: arithmetic is not allowed on narrow types (must CAST to vecf32 first)
select bf + bf from nvec;
select bf - bf from nvec;
select bf * bf from nvec;
select sqrt(bf) from nvec;
select abs(i8) from nvec;
select summation(f16) from nvec;
-- arithmetic IS allowed after an explicit cast to vecf32
select cast(bf as vecf32(3)) + cast(bf as vecf32(3)) from nvec order by a;

-- post
drop database if exists vecnarrowdb;
