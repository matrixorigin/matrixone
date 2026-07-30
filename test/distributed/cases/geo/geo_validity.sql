-- GIS: geometry validity / simplicity predicates (ST_IsSimple, ST_IsRing, ST_IsValid).

-- ST_IsSimple: a geometry with no anomalous self-intersection.
select st_issimple(st_geomfromtext('POINT(1 2)')) as simple_point;
select st_issimple(st_geomfromtext('LINESTRING(0 0,1 0,2 0)')) as simple_line;
select st_issimple(st_geomfromtext('LINESTRING(0 0,2 0,1 1,0 0)')) as simple_closed_triangle;
select st_issimple(st_geomfromtext('LINESTRING(0 0,2 2,0 2,2 0)')) as not_simple_crossing;
select st_issimple(st_geomfromtext('LINESTRING(0 0,1 0,0 0)')) as not_simple_backtrack;
select st_issimple(st_geomfromtext(null)) as simple_null;

-- ST_IsRing: a closed and simple linestring.
select st_isring(st_geomfromtext('LINESTRING(0 0,2 0,1 1,0 0)')) as ring_yes;
select st_isring(st_geomfromtext('LINESTRING(0 0,1 0,2 0)')) as ring_not_closed;
select st_isring(st_geomfromtext('LINESTRING(0 0,1 0,0 0)')) as ring_closed_not_simple;
select st_isring(st_geomfromtext(null)) as ring_null;
-- ST_IsRing requires a LINESTRING.
-- @regex("geometry is not a LINESTRING",true)
select st_isring(st_geomfromtext('POINT(1 2)'));

-- ST_IsValid: structural validity.
select st_isvalid(st_geomfromtext('POINT(1 2)')) as valid_point;
select st_isvalid(st_geomfromtext('LINESTRING(0 0,1 1)')) as valid_line;
select st_isvalid(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))')) as valid_polygon;
select st_isvalid(st_geomfromtext('POLYGON((0 0,4 4,4 0,0 4,0 0))')) as invalid_bowtie;
select st_isvalid(st_geomfromtext('POLYGON((0 0,6 0,6 6,0 6,0 0),(1 1,2 1,2 2,1 2,1 1))')) as valid_with_hole;
select st_isvalid(st_geomfromtext('POLYGON((0 0,6 0,6 6,0 6,0 0),(0 1,2 1,2 2,0 2,0 1))')) as invalid_hole_touches;
select st_isvalid(st_geomfromtext('GEOMETRYCOLLECTION EMPTY')) as valid_empty_collection;
select st_isvalid(st_geomfromtext(null)) as valid_null;

-- Over a stored column.
drop database if exists geo_valid;
create database geo_valid;
use geo_valid;
drop table if exists rings;
create table rings(id int, g geometry);
insert into rings values
  (1, st_geomfromtext('LINESTRING(0 0,2 0,1 1,0 0)')),
  (2, st_geomfromtext('LINESTRING(0 0,1 0,2 0)'));
select id, st_isring(g) as is_ring, st_issimple(g) as is_simple from rings order by id;
drop table rings;
drop database geo_valid;
