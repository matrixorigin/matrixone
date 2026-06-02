-- GIS: GEOMETRY32 (float32-coordinate) type — storage, functions, and casts.

drop database if exists geo32;
create database geo32;
use geo32;

-- DDL: generic geometry32 and a subtype alias; SHOW CREATE renders the 32 family.
drop table if exists t32;
create table t32(id int, g geometry32, p point32, l linestring32);
show create table t32;
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

-- GEOMETRY32 precision propagation: coordinate accessors return float32, and
-- geometry-returning functions stay GEOMETRY32 (float32 WKB, shorter bytes).
drop table if exists g32p;
create table g32p(id int, g geometry32);
insert into g32p values
  (1, st_geomfromtext('POINT(1.5 2.5)')),
  (2, st_geomfromtext('LINESTRING(0 0, 3 4)')),
  (3, st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'));

-- ST_X / ST_Y on a POINT32 yield float32 values.
select st_x(g) as x, st_y(g) as y from g32p where id = 1;

-- Geometry-returning functions preserve GEOMETRY32: a float32-WKB point is 13
-- bytes, a float64-WKB point is 21. ST_Centroid / ST_StartPoint of a
-- GEOMETRY32 column stay float32 (13 bytes).
select length(st_centroid(g)) as centroid_bytes from g32p where id = 3;
select length(st_startpoint(g)) as start_bytes from g32p where id = 2;
select st_astext(st_swapxy(g)) as swapped from g32p where id = 1;
select st_astext(st_envelope(g)) as env from g32p where id = 2;

-- ST_ConvexHull / ST_Buffer over GEOMETRY32 also stay float32.
select st_astext(st_convexhull(g)) as hull from g32p where id = 3;
select length(st_buffer(g, 1)) > 0 as buffered from g32p where id = 1;

-- Measures over GEOMETRY32 return float32.
select st_length(g) as len from g32p where id = 2;
select st_area(g) as area from g32p where id = 3;

-- Distance between two GEOMETRY32 values returns float32.
select st_distance(a.g, b.g) as dist from g32p a, g32p b where a.id = 1 and b.id = 1;
drop table g32p;

drop database geo32;
