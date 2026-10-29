-- vecbf16 / vecf16 / vecint8 narrow vector column types
-- Scope (per design): distance functions, casts, storage, and the reductions and
-- slicing below. Elementwise arithmetic (+ - * / sqrt abs) is NOT supported on the narrow
-- types and must require an explicit CAST to vecf32. summation, l1_norm and
-- l2_norm take them directly and return DOUBLE; subvector keeps their type.

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
-- arithmetic IS allowed after an explicit cast to vecf32
select cast(bf as vecf32(3)) + cast(bf as vecf32(3)) from nvec order by a;

-- summation / l1_norm / l2_norm / subvector on the narrow types (#31666)
drop table if exists nfn;
create table nfn(a int, bf vecbf16(3), f16 vecf16(3), i8 vecint8(3));
insert into nfn values(1, "[3,4,0]", "[3,4,0]", "[3,4,0]");
insert into nfn values(2, "[-1.5,2,0.25]", "[-1.5,2,0.25]", "[-128,127,0]");
insert into nfn values(3, null, null, null);
select a, summation(bf), l1_norm(bf), l2_norm(bf) from nfn order by a;
select a, summation(f16), l1_norm(f16), l2_norm(f16) from nfn order by a;
select a, summation(i8), l1_norm(i8), l2_norm(i8) from nfn order by a;
select a, subvector(bf, 2), subvector(f16, -1), subvector(i8, 1, 2) from nfn order by a;
select a, subvector(i8, 0), subvector(i8, 4, 1), subvector(i8, -2, 1) from nfn order by a;
-- the results match the explicit vecf32 cast
select count(*) from nfn where summation(bf) = summation(cast(bf as vecf32(3))) and l2_norm(f16) = l2_norm(cast(f16 as vecf32(3))) and l1_norm(i8) = l1_norm(cast(i8 as vecf32(3)));
-- subvector keeps the narrow type
create table nfn_sub as select subvector(bf, 1, 2) as s_bf, subvector(f16, 2) as s_f16, subvector(i8, -1) as s_i8 from nfn where a = 1;
desc nfn_sub;
drop table nfn_sub;
drop table nfn;

-- narrow vector columns cannot be a primary key (inline declaration)
create table pkbf(a int, v vecbf16(3) primary key);
create table pkf16(a int, v vecf16(3) primary key);
create table pki8(a int, v vecint8(3) primary key);

-- post
drop database if exists vecnarrowdb;
