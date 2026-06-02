-- GIS: GEOMETRY32 (float32-coordinate) type — storage, functions, and casts.

drop database if exists geo32;
create database geo32;
use geo32;

-- DDL: generic geometry32 and a subtype alias.
drop table if exists t32;
create table t32(id int, g geometry32, p point32);
drop table t32;

-- Storage round-trip: values are stored as float32 WKB, rendered as WKT.
drop table if exists g32;
create table g32(id int, g geometry32);
insert into g32 values (1, st_geomfromtext('POINT(1 2)'));
insert into g32 values (2, st_geomfromtext('LINESTRING(0 0,3 4)'));
insert into g32 values (3, st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))'));
select id, st_astext(g) as wkt from g32 order by id;
-- A 2D point stored as float32 WKB occupies 13 bytes (vs 21 for float64).
select length(g) as f32_point_bytes from g32 where id = 1;
select id, st_geometrytype(g) as gtype from g32 order by id;

-- Spatial functions read GEOMETRY32 correctly (computation is float64 internally).
select st_x(g) as x, st_y(g) as y from g32 where id = 1;
select st_area(g) as area from g32 where id = 3;
select st_length(g) as len from g32 where id = 2;
select st_astext(st_centroid(g)) as centroid from g32 where id = 3;
drop table g32;

-- Casts between geometry and geometry32 preserve the geometry (to float32
-- precision).
select st_astext(cast(st_geomfromtext('POINT(1.5 2.25)') as geometry32)) as to32;
select st_astext(cast(cast(st_geomfromtext('POINT(1.5 2.25)') as geometry32) as geometry)) as roundtrip;
select st_astext(cast('LINESTRING(0 0,1 1)' as geometry32)) as text_to_32;

drop database geo32;
