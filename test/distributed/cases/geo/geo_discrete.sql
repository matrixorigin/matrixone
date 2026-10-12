-- GIS: discrete curve distances (ST_FrechetDistance, ST_HausdorffDistance).
-- Planar (Cartesian) distance over geometry vertices.

-- Two parallel lines 1 unit apart.
select st_hausdorffdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), st_geomfromtext('LINESTRING(0 1, 10 1)')) as hd_parallel;
select st_frechetdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), st_geomfromtext('LINESTRING(0 1, 10 1)')) as fd_parallel;

-- Identical geometries -> 0.
select st_hausdorffdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), st_geomfromtext('LINESTRING(0 0, 10 0)')) as hd_same;
select st_frechetdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), st_geomfromtext('LINESTRING(0 0, 10 0)')) as fd_same;

-- A diverging endpoint raises the Fréchet distance to the offset (5).
select st_frechetdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), st_geomfromtext('LINESTRING(0 0, 10 5)')) as fd_diverge;

-- Works for point sets too.
select st_hausdorffdistance(st_geomfromtext('MULTIPOINT(0 0, 0 3)'), st_geomfromtext('MULTIPOINT(4 0, 4 3)')) as hd_points;

-- Hausdorff distance is directed from the first geometry to the second. The
-- Fréchet result is an independent symmetric-sequence control and is unchanged.
select st_hausdorffdistance(st_geomfromtext('LINESTRING(0 0,1 4,4 4)'), st_geomfromtext('LINESTRING(0 0,4 0,4 4)')) as hd_forward,
       st_hausdorffdistance(st_geomfromtext('LINESTRING(0 0,4 0,4 4)'), st_geomfromtext('LINESTRING(0 0,1 4,4 4)')) as hd_reverse,
       st_hausdorffdistance(st_geomfromtext('LINESTRING(0 0,0 5,5 5)'), st_geomfromtext('LINESTRING(0 1,0 6,3 3,5 6)')) as hd_manual,
       st_frechetdistance(st_geomfromtext('LINESTRING(0 0,0 5,5 5)'), st_geomfromtext('LINESTRING(0 1,0 6,3 3,5 6)')) as fd_control,
       st_hausdorffdistance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('MULTIPOINT(0 0,3 4)')) as hd_point_to_multipoint,
       st_hausdorffdistance(st_geomfromtext('MULTIPOINT(0 0,3 4)'), st_geomfromtext('POINT(0 0)')) as hd_multipoint_to_point;

-- Exercise both SQL overloads on table vectors (float64 GEOMETRY and float32 GEOMETRY32).
drop table if exists hausdorff_pairs;
create table hausdorff_pairs(id int, g geometry, g32 geometry32);
insert into hausdorff_pairs values
  (1, st_geomfromtext('LINESTRING(0 0,1 4,4 4)'), st_geomfromtext('LINESTRING(0 0,1 4,4 4)')),
  (2, st_geomfromtext('LINESTRING(0 0,4 0,4 4)'), st_geomfromtext('LINESTRING(0 0,4 0,4 4)'));
select st_hausdorffdistance(a.g, b.g) as hd_forward,
       st_hausdorffdistance(b.g, a.g) as hd_reverse,
       st_hausdorffdistance(a.g32, b.g32) as hd32_forward,
       st_hausdorffdistance(b.g32, a.g32) as hd32_reverse
from hausdorff_pairs a, hausdorff_pairs b where a.id = 1 and b.id = 2;
drop table hausdorff_pairs;
