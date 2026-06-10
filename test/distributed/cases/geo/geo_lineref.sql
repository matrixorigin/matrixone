-- GIS: linear referencing (ST_LineInterpolatePoint, ST_LineInterpolatePoints, ST_PointAtDistance).
-- Planar (Cartesian) interpolation along a linestring.

-- Midpoint and a fraction spanning two segments (total length 20, 75% -> (10,5)).
select st_astext(st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0)'), 0.5)) as mid;
select st_astext(st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0, 10 10)'), 0.75)) as quarter3;
select st_astext(st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0)'), 0)) as start_pt;
select st_astext(st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0)'), 1)) as end_pt;

-- Points at regular intervals; 1.0 yields just the endpoint.
select st_astext(st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0, 10 0)'), 0.25)) as quarters;
select st_astext(st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0, 10 0)'), 1.0)) as just_end;

-- Point at an absolute distance along the line.
select st_astext(st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), 3)) as at3;
select st_astext(st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0, 10 10)'), 15)) as at15;

-- Distance beyond the line length is rejected.
-- @regex("out of range",true)
select st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), 99);

-- Fraction outside (0,1] is rejected for ST_LineInterpolatePoints.
-- @regex("fraction",true)
select st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0, 10 0)'), 0);

-- Non-linestring input is rejected.
-- @regex("not a LINESTRING",true)
select st_lineinterpolatepoint(st_geomfromtext('POINT(1 1)'), 0.5);
