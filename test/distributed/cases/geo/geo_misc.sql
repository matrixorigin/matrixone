-- GIS: point accessors and misc functions
-- (ST_Longitude, ST_Latitude, ST_SwapXY, ST_Validate, ST_MakeEnvelope, ST_Distance_Sphere).

-- Longitude/Latitude of a point.
select st_longitude(st_geomfromtext('POINT(3 4)')) as lon;
select st_latitude(st_geomfromtext('POINT(3 4)')) as lat;

-- ST_SwapXY swaps coordinates.
select st_astext(st_swapxy(st_geomfromtext('POINT(1 2)'))) as swap_point;
select st_astext(st_swapxy(st_geomfromtext('LINESTRING(0 1,2 3)'))) as swap_line;
select st_astext(st_swapxy(st_geomfromtext('POLYGON((0 0,4 0,4 2,0 0))'))) as swap_poly;

-- ST_Validate: valid geometry passes through, invalid yields NULL.
select st_astext(st_validate(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'))) as valid;
select st_astext(st_validate(st_geomfromtext('POLYGON((0 0,4 4,4 0,0 4,0 0))'))) as invalid_bowtie;

-- ST_MakeEnvelope builds the bounding rectangle of two corner points.
select st_astext(st_makeenvelope(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(2 3)'))) as envelope;
select st_astext(st_makeenvelope(st_geomfromtext('POINT(5 7)'), st_geomfromtext('POINT(1 2)'))) as envelope_unordered;

-- ST_Distance_Sphere: great-circle meters (one equatorial degree ~111195 m).
select st_distance_sphere(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 0)')) as one_degree_m;
