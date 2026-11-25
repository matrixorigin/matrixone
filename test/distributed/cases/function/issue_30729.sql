-- @suite
-- #30729 unsized CAST(v AS BINARY) inside IF/IFNULL/CASE/COALESCE next to an
-- untyped NULL keeps the cast bytes unchanged (VARCHAR(20) 'hello-long' is 10
-- bytes, HEX 68656C6C6F2D6C6F6E67) instead of widening to BINARY(65535) and
-- NUL-padding. Explicit BINARY(n) keeps fixed truncate/pad. Boundary rows
-- cover empty, embedded-NUL (unhex 410042) and UTF-8 payloads. Values are
-- hand-derived from MySQL 8.3 byte semantics and frozen in
-- out/preparation2/oracle-30729.json.
-- @setup
drop table if exists src;
drop table if exists out_if;
create table src(id int primary key, v varchar(20));
insert into src values (1, 'hello-long'), (0, 'zero-row'), (2, ''), (3, '你好'), (4, unhex('410042'));

-- @case
-- @desc: issue SQL shape; per-byte boundaries over real column rows
-- @label:bvt
select length(if(id=0, null, cast(v as binary))) as len_then_null from src order by id;
select hex(if(id=0, null, cast(v as binary))) as hex_then_null from src order by id;

-- @case
-- @desc: NULL branch order swapped keeps bytes on the selected rows
-- @label:bvt
select length(if(id=0, cast(v as binary), null)) as len_else_null from src order by id;
select hex(if(id=0, cast(v as binary), null)) as hex_else_null from src where id=0;

-- @case
-- @desc: direct unsized cast control, length and full hex per row
-- @label:bvt
select length(cast(v as binary)) as len_direct, hex(cast(v as binary)) as hex_direct from src order by id;

-- @case
-- @desc: ifnull/case/coalesce siblings unchanged next to untyped NULL
-- @label:bvt
select length(ifnull(cast(v as binary), null)) as len_ifnull from src order by id;
select length(case when id=0 then null else cast(v as binary) end) as len_case from src order by id;
select length(coalesce(cast(v as binary), null)) as len_coalesce from src order by id;

-- @case
-- @desc: two unsized binary branches
-- @label:bvt
select length(if(id=0, cast(v as binary), cast(v as binary))) as len_two from src order by id;

-- @case
-- @desc: explicit BINARY(3) truncation control (length and full hex)
-- @label:bvt
select length(if(id=0, null, cast(v as binary(3)))) as len_b3 from src order by id;
select hex(if(id=0, null, cast(v as binary(3)))) as hex_b3 from src order by id;

-- @case
-- @desc: explicit BINARY(7) truncate control
-- @label:bvt
select length(if(id=0, null, cast(v as binary(7)))) as len_b7 from src order by id;
select hex(if(id=0, null, cast(v as binary(7)))) as hex_b7 from src order by id;

-- @case
-- @desc: explicit BINARY(20) pad control, full 40-char hex proves the NUL tail
-- @label:bvt
select length(if(id=0, null, cast(v as binary(20)))) as len_b20 from src order by id;
select hex(if(id=0, null, cast(v as binary(20)))) as hex_b20 from src order by id;

-- @case
-- @desc: mixed explicit/unsized branches keep the BINARY(n) branch contract
-- @label:bvt
select length(if(id=0, cast(v as binary(3)), cast(v as binary))) as len_mixed from src order by id;

-- @case
-- @desc: CTAS keeps materialized bytes; readback ordered by length,hex with NULL first
-- @label:bvt
create table out_if as select if(id=0, null, cast(v as binary)) as b from src;
select length(b) as ctas_len, hex(b) as ctas_hex from out_if order by length(b), hex(b);
select count(*) as ctas_null_count from out_if where b is null;

-- @case
-- @desc: prepared handle reused for three different parameters
-- @label:bvt
prepare s from 'select length(if(id=0, null, cast(v as binary))) from src where id = ?';
set @a = 1;
execute s using @a;
set @a = 0;
execute s using @a;
set @a = 2;
execute s using @a;
deallocate prepare s;

-- @setup
drop table if exists out_if;
drop table if exists src;
