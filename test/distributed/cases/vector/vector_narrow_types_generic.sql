-- Generic SQL surface over the four narrow vector types (vecbf16/vecf16/
-- vecint8/vecuint8). These are NOT index operations — they are the ordinary
-- type-dispatch paths (zonemaps, aggregates, JSON, sample, window fill) that
-- enumerated only vecf32/vecf64, so a narrow vector column either errored or
-- panicked the CN.
--
-- Deliberately CPU-only and outside gpu_cases/: today's narrow-vector coverage
-- lives in the GPU suite, which reads 0% on non-GPU CI — which is how this class
-- of gap reached review in the first place.
drop database if exists narrowgen;
create database narrowgen;
use narrowgen;

create table nv(
    a int primary key,
    bf vecbf16(3),
    f16 vecf16(3),
    i8 vecint8(3),
    u8 vecuint8(3)
);
insert into nv values
    (1, '[1,2,3]',  '[1,2,3]',  '[1,2,3]',  '[1,2,3]'),
    (2, '[4,5,6]',  '[4,5,6]',  '[4,5,6]',  '[4,5,6]'),
    (3, '[-1,0,7]', '[-1,0,7]', '[-1,0,7]', '[10,128,255]');

-- ZM getValue: panicked the CN for a table that merely CONTAINS a narrow vector
-- column, even when the query targeted a different column entirely. The flush is
-- required — zonemaps come from persisted objects, so without it the query never
-- reaches getValue and returns null for every column (including plain int).
-- Expected: the int column yields a real max; every VECTOR column yields empty,
-- vecf32 included, because array zonemaps are never inited. Parity, not a panic.
select mo_ctl('dn', 'flush', 'narrowgen.nv');
select mo_table_col_max('narrowgen', 'nv', 'a');
select mo_table_col_max('narrowgen', 'nv', 'bf');
select mo_table_col_max('narrowgen', 'nv', 'i8');
select mo_table_col_max('narrowgen', 'nv', 'u8');

-- group_concat: failed with "unsupported type for group_concat payload".
-- Vectors concat as their raw storage bytes (this is also what vecf32 does), so
-- hex() keeps the expected output textual and stable.
select hex(group_concat(i8 order by a)) from nv;
select hex(group_concat(u8 order by a)) from nv;
select hex(group_concat(bf order by a)) from nv where a = 1;
select hex(group_concat(f16 order by a)) from nv where a = 1;

-- json_arrayagg: failed with "unsupported type for json aggregate".
-- int8 keeps sign; uint8 must not wrap 128/255 negative.
select json_arrayagg(i8) from nv where a = 3;
select json_arrayagg(u8) from nv where a = 3;
select json_arrayagg(bf) from nv where a = 1;
select json_arrayagg(f16) from nv where a = 1;

-- sample: "unsupported type for sample pool" when replacement touched the column.
select count(*) from (select sample(i8, 2 rows) from nv) t;
select count(*) from (select sample(u8, 2 rows) from nv) t;
select count(*) from (select sample(bf, 2 rows) from nv) t;

-- COALESCE / LEAST / GREATEST: the overload tables and the supported-OID gate
-- listed only vecf32/vecf64, so these failed with "invalid argument function
-- ..., bad value [VECINT8]" while the identical call on vecf32 worked.
select coalesce(i8, '[0,0,0]') from nv where a = 1;
select coalesce(bf, '[0,0,0]') from nv where a = 1;
select least(i8, i8) from nv where a = 1;
select greatest(u8, u8) from nv where a = 3;
select least(f16, f16) from nv where a = 1;

-- serial / serial_extract: composite-key packing rejected the narrow types, so
-- any internal key build including such a column failed.
select hex(serial(i8, a)) from nv where a = 1;
select serial_extract(serial(i8, a), 0 as vecint8(3)) from nv where a = 1;
select serial_extract(serial(bf, a), 0 as vecbf16(3)) from nv where a = 1;

-- json_row / json_array / json_object. json_array needed TWO fixes: the value
-- conversion AND jsonConstructorSupportsType, a separate type-check gate that
-- rejected the column before the conversion ever ran.
select json_row(i8) from nv where a = 1;
select json_array(i8) from nv where a = 3;
select json_array(u8) from nv where a = 3;
select json_object('k', bf) from nv where a = 1;

-- ALTER ADD ... NOT NULL on a non-empty table: synthesized `null` for the
-- backfill — invalid for a NOT NULL column — where vecf32 produced a zero
-- vector. Now uses IsArrayRelate so all six vector types agree.
create table alt(a int);
insert into alt values (1);
alter table alt add column v vecint8(3) not null;
select v from alt;
alter table alt add column w vecbf16(3) not null;
select w from alt;

-- NOTE on FILL: appendValue/setValue in pkg/sql/colexec/fill also enumerated
-- only vecf32/vecf64 and panicked the CN on the four narrow types. That fix is
-- covered by TestFillNarrowVectorValues (a unit test) rather than here: max()
-- and min() reject vector types outright, and the window forms that do carry a
-- vector through (any_value) did not produce empty windows to fill in any setup
-- tried, so a SQL case would assert a parser/aggregate error instead of
-- exercising fill — a green test proving nothing.

drop database narrowgen;
