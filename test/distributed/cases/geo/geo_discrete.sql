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
