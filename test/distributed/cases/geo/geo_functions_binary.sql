-- GIS: binary spatial functions — distance and relationship predicates (Cartesian / SRID 0).

-- Distance.
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(3 4)')) as d_pp;
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('LINESTRING(1 0,1 1)')) as d_pl;
select st_distance(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))'), st_geomfromtext('POLYGON((5 0,7 0,7 2,5 2,5 0))')) as d_polypoly;
select st_distance(st_geomfromtext('POINT(5 5)'), st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))')) as d_inside;

-- Containment.
select st_contains(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'), st_geomfromtext('POINT(2 2)')) as contains_yes;
select st_contains(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'), st_geomfromtext('POINT(9 9)')) as contains_no;
select st_within(st_geomfromtext('POINT(2 2)'), st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))')) as within_yes;
select st_covers(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'), st_geomfromtext('POINT(0 2)')) as covers_boundary;
select st_coveredby(st_geomfromtext('POINT(0 2)'), st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))')) as coveredby;

-- Intersection / disjointness.
select st_intersects(st_geomfromtext('LINESTRING(0 0,2 2)'), st_geomfromtext('LINESTRING(0 2,2 0)')) as intersects_yes;
select st_intersects(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(5 5)')) as intersects_no;
select st_disjoint(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(5 5)')) as disjoint_yes;
select st_disjoint(st_geomfromtext('LINESTRING(0 0,2 2)'), st_geomfromtext('POINT(1 1)')) as disjoint_no;

-- Touches / crosses / overlaps.
select st_touches(st_geomfromtext('LINESTRING(0 0,2 0)'), st_geomfromtext('LINESTRING(2 0,4 0)')) as touches;
select st_crosses(st_geomfromtext('LINESTRING(-1 1,3 1)'), st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))')) as crosses;
select st_overlaps(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))'), st_geomfromtext('POLYGON((1 1,3 1,3 3,1 3,1 1))')) as overlaps;

-- Equality (order-independent).
select st_equals(st_geomfromtext('POINT(1 1)'), st_geomfromtext('POINT(1 1)')) as equals_yes;
select st_equals(st_geomfromtext('POINT(1 1)'), st_geomfromtext('POINT(2 2)')) as equals_no;
select st_equals(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))'), st_geomfromtext('POLYGON((2 0,2 2,0 2,0 0,2 0))')) as equals_polygon;

-- Predicates over stored columns.
drop database if exists geo_bin;
create database geo_bin;
use geo_bin;
drop table if exists shapes;
create table shapes(id int, g geometry);
insert into shapes values
  (1, st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))')),
  (2, st_geomfromtext('POINT(2 2)')),
  (3, st_geomfromtext('POINT(9 9)'));
select a.id as poly, b.id as pt
  from shapes a join shapes b
  on st_contains(a.g, b.g) and a.id = 1 and b.id <> 1
  order by b.id;
drop table shapes;
drop database geo_bin;
