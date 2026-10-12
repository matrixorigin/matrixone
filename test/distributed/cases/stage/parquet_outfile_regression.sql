-- Regression coverage for Parquet SELECT INTO OUTFILE.
drop database if exists parquet_outfile_regression;
create database parquet_outfile_regression;
use parquet_outfile_regression;

drop stage if exists parquet_outfile_stage;
create stage parquet_outfile_stage URL='file://$resources/into_outfile/parquet_outfile_regression';

-- A single executor batch must still honor a small split target and preserve every row.
create table split_src (id int, payload varchar(600));
insert into split_src
select result, repeat('x', 512) from generate_series(1, 32) g;

-- A failed CSV export must not poison the next Parquet export on this connection.
select id, payload from split_src
into outfile 'stage://parquet_outfile_stage/failed.csv'
format 'csv' splitsize '1' header 'true';

select id, payload from split_src order by id
into outfile 'stage://parquet_outfile_stage/split_%d.parquet'
format 'parquet' splitsize '8k';
select count(*) > 1 as split_ok
from stage_list('stage://parquet_outfile_stage/split_*.parquet');
create table split_copy (id int, payload varchar(600));
load data infile {'filepath'='stage://parquet_outfile_stage/split_*.parquet', 'format'='parquet'} into table split_copy;
select count(*), min(id), max(id), min(length(payload)), max(length(payload)) from split_copy;

-- An empty result still creates a readable Parquet file.
create table empty_src (id int, payload varchar(20));
select * from empty_src into outfile 'stage://parquet_outfile_stage/empty.parquet' format 'parquet';
create table empty_copy (id int, payload varchar(20));
load data infile {'filepath'='stage://parquet_outfile_stage/empty.parquet', 'format'='parquet'} into table empty_copy;
select count(*) from empty_copy;

-- Documented scalar and vector types must survive Parquet export and LOAD DATA.
create table typed_src (
    id bigint unsigned,
    bits bit(8),
    tags set('a', 'b', 'c'),
    v32 vecf32(2),
    v64 vecf64(2),
    year_val year,
    ts timestamp
);
set @parquet_old_time_zone = @@time_zone;
set time_zone = '+08:00';
insert into typed_src values (18446744073709551615, b'10101010', 'a,b', '[1.25,2.5]', '[3.5,4.75]', 2024, '2024-01-01 00:00:00');
select * from typed_src into outfile 'stage://parquet_outfile_stage/typed.parquet' format 'parquet';
set time_zone = '-05:00';
create table typed_copy (
    id bigint unsigned,
    bits bit(8),
    tags set('a', 'b', 'c'),
    v32 vecf32(2),
    v64 vecf64(2),
    year_val year,
    ts timestamp
);
load data infile {'filepath'='stage://parquet_outfile_stage/typed.parquet', 'format'='parquet'} into table typed_copy;
select id, cast(bits as unsigned), tags, vector_dims(v32), vector_dims(v64), year_val, unix_timestamp(ts) from typed_copy;
set time_zone = @parquet_old_time_zone;

-- SET member whitespace is lexical data, including combinations, empty and NULL.
create table lexical_set_src (id int, tags set(' a', 'a'));
insert into lexical_set_src values (1, ' a'), (2, 'a'), (3, ' a,a'), (4, ''), (5, NULL);
select * from lexical_set_src order by id into outfile 'stage://parquet_outfile_stage/lexical_set.parquet' format 'parquet';
create table lexical_set_copy (id int, tags set(' a', 'a'));
load data infile {'filepath'='stage://parquet_outfile_stage/lexical_set.parquet', 'format'='parquet'} into table lexical_set_copy;
select id, cast(tags as unsigned) as bitmap from lexical_set_copy order by id;

-- Spatial metadata, link values, and every vector element type survive typed LOAD.
create table extended_src (
    id int,
    g point srid 4326,
    g32 point32 srid 4326,
    line linestring srid 4326,
    link datalink,
    bf vecbf16(3),
    f16 vecf16(3),
    i8 vecint8(3),
    u8 vecuint8(3),
    f32 vecf32(3),
    f64 vecf64(3),
    y year
);
insert into extended_src values
(1, st_geomfromtext('POINT(1.0000001192092896 -2.5)',4326),
    st_geomfromtext('POINT(1.0000001192092896 -2.5)',4326),
    st_geomfromtext('LINESTRING(1.25 -2.5,3.75 4.5)',4326),
    'file:///does-not-exist%20file?offset=2&size=3',
    '[1.5,-2.5,0]', '[1.5,-2.5,65504]', '[-128,0,127]', '[0,128,255]',
    '[1,-2.5,3]', '[0.5,-1.25,4]', 1901),
(2, null, null, null, null, null, null, null, null, null, null, null);
select * from extended_src order by id into outfile 'stage://parquet_outfile_stage/extended.parquet' format 'parquet';
create table extended_copy like extended_src;
load data infile {'filepath'='stage://parquet_outfile_stage/extended.parquet', 'format'='parquet'} into table extended_copy;
select t.id,
    (st_aswkb(s.g) <=> st_aswkb(t.g)) and (st_srid(s.g) <=> st_srid(t.g))
    and (st_aswkb(s.g32) <=> st_aswkb(t.g32)) and (st_srid(s.g32) <=> st_srid(t.g32))
    and (st_aswkb(s.line) <=> st_aswkb(t.line)) and (st_srid(s.line) <=> st_srid(t.line))
    and (cast(s.link as varchar) <=> cast(t.link as varchar))
    and (s.bf <=> t.bf) and (s.f16 <=> t.f16) and (s.i8 <=> t.i8) and (s.u8 <=> t.u8)
    and (s.f32 <=> t.f32) and (s.f64 <=> t.f64) and (s.y <=> t.y) as roundtrip_ok
from extended_src s join extended_copy t on s.id=t.id order by t.id;
select id, st_astext(g), st_srid(g), st_astext(g32), st_srid(g32), st_astext(line), cast(link as varchar), bf, f16, i8, u8 from extended_copy order by id;

-- Direct Parquet LOAD must enforce SRID and subtype, then recover on the same session.
select id,g from extended_src order by id into outfile 'stage://parquet_outfile_stage/point.parquet' format 'parquet';
create table wrong_srid (id int, g point srid 0);
load data infile {'filepath'='stage://parquet_outfile_stage/point.parquet', 'format'='parquet'} into table wrong_srid;
select count(*) from wrong_srid;
create table wrong_subtype (id int, g linestring srid 4326);
load data infile {'filepath'='stage://parquet_outfile_stage/point.parquet', 'format'='parquet'} into table wrong_subtype;
select count(*) from wrong_subtype;
create table point_copy (id int, g point srid 4326);
load data infile {'filepath'='stage://parquet_outfile_stage/point.parquet', 'format'='parquet'} into table point_copy;
select id, st_astext(g), st_srid(g) from point_copy order by id;

-- Duplicate aliases must fail instead of silently dropping a Parquet column.
select id as duplicate_name, id as duplicate_name from split_src
into outfile 'stage://parquet_outfile_stage/duplicate.parquet' format 'parquet';

remove files from stage if exists 'stage://parquet_outfile_stage/*';
drop table split_copy;
drop table split_src;
drop table empty_copy;
drop table empty_src;
drop table typed_copy;
drop table typed_src;
drop table lexical_set_copy;
drop table lexical_set_src;
drop table extended_copy;
drop table extended_src;
drop table wrong_srid;
drop table wrong_subtype;
drop table point_copy;
drop stage parquet_outfile_stage;
drop database parquet_outfile_regression;
