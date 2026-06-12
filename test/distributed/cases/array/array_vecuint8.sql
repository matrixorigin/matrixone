-- vecuint8 narrow vector column type (unsigned 8-bit, [0,255]).
-- Mirrors array_vecnarrow for the int8 sibling; scope (per design): distance
-- functions + casts + storage only. Elementwise arithmetic is NOT supported and
-- must go through an explicit CAST to vecf32.

drop database if exists vecu8db;
create database vecu8db;
use vecu8db;

-- column type: create / desc / show create / insert / select
create table u8t(a int, v vecuint8(4));
desc u8t;
show create table u8t;
insert into u8t values(1, "[0,1,2,3]");
insert into u8t values(2, "[255,254,0,128]");
insert into u8t values(3, "[10,20,30,40]");
select * from u8t order by a;

-- strict string parse: an integer in [0,255]; boundary values OK.
-- out-of-range and non-integer literals error (no silent round/clamp).
insert into u8t values(4, "[300,0,0,0]");
insert into u8t values(5, "[-1,0,0,0]");
insert into u8t values(6, "[1.4,0,0,0]");

-- rounding/clamping IS available, but only via the vecf32 -> vecuint8 CAST path
select cast(cast("[1.6,300,-5,200]" as vecf32(4)) as vecuint8(4));

-- string -> vecuint8 cast (strict)
select cast("[1,2,3]" as vecuint8(3));
select cast("[1.4,2.6,-3.5]" as vecuint8(3));

-- vecuint8 -> vecf32 / vecf64 (explicit widening)
select cast(v as vecf32(4)), cast(v as vecf64(4)) from u8t order by a;

-- vecuint8 <-> other narrow casts
select cast(cast("[1,2,3]" as vecf32(3)) as vecuint8(3));
select cast(cast("[1,2,3]" as vecuint8(3)) as vecint8(3));
select cast(cast("[1,2,3]" as vecuint8(3)) as vecbf16(3));

-- distance functions on vecuint8
select a, l2_distance(v, "[0,1,2,3]") from u8t order by a;
select a, l2_distance_sq(v, "[0,1,2,3]") from u8t order by a;
select a, inner_product(v, "[1,1,1,1]") from u8t order by a;
select a, cosine_distance(v, "[0,1,2,3]") from u8t order by a;
select a, cosine_similarity(v, "[0,1,2,3]") from u8t order by a;
select normalize_l2(v) from u8t order by a;

-- distance between two vecuint8 values
select a, l2_distance(v, cast("[10,20,30,40]" as vecuint8(4))) from u8t order by a;

-- top-K (ORDER BY distance + LIMIT)
select a from u8t order by l2_distance(v, '[0,1,2,3]') limit 3;
select a from u8t order by inner_product(v, '[1,1,1,1]') limit 3;

-- filtering / equality / ordering / distinct
select a from u8t where v = "[10,20,30,40]";
select * from u8t order by v desc;
select distinct v from u8t order by v;

-- negative: arithmetic is not allowed on vecuint8 (must CAST to vecf32 first)
select v + v from u8t;
select v * v from u8t;
select abs(v) from u8t;
select summation(v) from u8t;
-- arithmetic IS allowed after an explicit cast to vecf32
select cast(v as vecf32(4)) + cast(v as vecf32(4)) from u8t order by a;

-- vecuint8_from_base64: decode raw little-endian uint8 bytes. This is the builtin
-- the ivf search re-rank emits for the (quantized) query vector. A constant
-- argument constant-folds, so this is a direct regression for the elemSize
-- divide-by-zero panic that hid when the uint8 case was missing from the decoder.
select vecuint8_from_base64('ChQeKA==');
select vecuint8_from_base64('AP+AAQ==');

drop database if exists vecu8db;
