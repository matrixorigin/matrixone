-- #29084: vector conversion functions must not produce or persist non-finite (NaN/Inf) elements.
-- Vector-to-vector CAST narrowing and VEC*_FROM_BASE64 decode both bypassed the finite check that
-- the text cast and direct insert already enforce, letting Infinity/NaN reach VECF32/VECF64 columns
-- (and HNSW). These now fail with the same "vector element cannot be NaN or Inf" error.
drop database if exists vec_nonfinite;
create database vec_nonfinite;
use vec_nonfinite;

-- Control: direct text-to-target cast already rejects non-finite (unchanged behavior).
select cast('[1e300,-1e300]' as vecf32(2));
select cast('[inf]' as vecf32(1));
select cast('[nan]' as vecf32(1));

-- Bypass 1: narrowing CAST through a legal intermediate must reject the overflow, not return Inf.
select cast(cast('[1e300,-1e300]' as vecf64(2)) as vecf32(2));
select cast(cast('[1e10,-1e10]' as vecf32(2)) as vecf16(2));
select cast(cast('[1e300,-1e300]' as vecf64(2)) as vecbf16(2));

-- Bypass 2: raw IEEE-754 base64 decode must reject NaN/Inf for every float element type.
select vecf32_from_base64('AACAfw==');
select vecf32_from_base64('AADAfw==');
select vecf64_from_base64('AAAAAAAA8H8=');
select vecf64_from_base64('AAAAAAAA+H8=');
select vecf16_from_base64('AHw=');
select vecf16_from_base64('AH4=');
select vecbf16_from_base64('gH8=');
select vecbf16_from_base64('wH8=');

-- Persistence paths must all fail (nothing non-finite lands in a column).
create table src(id bigint primary key, v vecf64(2));
insert into src values(1,'[1e300,-1e300]'),(2,'[3e38,-3e38]');

create table inserted(id bigint primary key, v vecf32(2));
insert into inserted select id, cast(v as vecf32(2)) from src;
select count(*) from inserted;

create table ctas as select id, cast(v as vecf32(2)) v from src;

create table altered like src;
insert into altered select * from src;
alter table altered modify column v vecf32(2);

-- Generated column over the base64 decoder: update to +Inf and insert NaN must both fail.
create table b64_generated(
  id bigint primary key,
  payload varchar(32),
  v vecf32(1) as (vecf32_from_base64(payload)) stored
);
insert into b64_generated(id,payload) values(1,'AAAAQA==');
update b64_generated set payload='AACAfw==' where id=1;
insert into b64_generated(id,payload) values(2,'AADAfw==');
select id, v from b64_generated order by id;

-- Finite controls: narrowing within range, a finite base64 payload, and a finite VECF64 value that
-- overflows float32 (1e300) but is valid in double -- the check runs in native precision, so it must
-- decode, not falsely reject.
select cast(cast('[3e38,-3e38]' as vecf64(2)) as vecf32(2)) finite_narrow;
select vecf32_from_base64('AAAAQA==') finite_b64_f32;
select vecf64_from_base64('nHUAiDzkN34=') finite_big_f64;

drop database vec_nonfinite;
