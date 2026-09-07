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

-- Non-finite fractions and distances are rejected before interpolation.
-- @regex("ST_LineInterpolatePoint: fraction must be finite",true)
select st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('NaN' as double));
-- @regex("ST_LineInterpolatePoint: fraction must be finite",true)
select st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('Inf' as double));
-- @regex("ST_LineInterpolatePoint: fraction must be finite",true)
select st_lineinterpolatepoint(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('-Inf' as double));
-- @regex("ST_LineInterpolatePoints: fraction must be finite",true)
select st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('NaN' as double));
-- @regex("ST_LineInterpolatePoints: fraction must be finite",true)
select st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('Inf' as double));
-- @regex("ST_LineInterpolatePoints: fraction must be finite",true)
select st_lineinterpolatepoints(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('-Inf' as double));
-- @regex("ST_PointAtDistance: distance must be finite",true)
select st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('NaN' as double));
-- @regex("ST_PointAtDistance: distance must be finite",true)
select st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('Inf' as double));
-- @regex("ST_PointAtDistance: distance must be finite",true)
select st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('-Inf' as double));

-- Exercise finite, numeric-NULL, geometry-NULL and NaN column paths with
-- deterministic row selection. Entry and exit cleanup keep the case rerunnable.
drop table if exists geo_lineref_params_28188;
create temporary table geo_lineref_params_28188(id int primary key, g geometry, fraction double, distance double);
insert into geo_lineref_params_28188 values (1, st_geomfromtext('LINESTRING(0 0, 10 0)'), 0.5, 3);
insert into geo_lineref_params_28188 values (2, st_geomfromtext('LINESTRING(0 0, 10 0)'), null, null);
insert into geo_lineref_params_28188 values (3, null, 0.5, 3);
insert into geo_lineref_params_28188 values (4, st_geomfromtext('LINESTRING(0 0, 10 0)'), cast('NaN' as double), cast('NaN' as double));
select id, st_astext(st_lineinterpolatepoint(g, fraction)) as interp_point, st_astext(st_lineinterpolatepoints(g, fraction)) as interp_points, st_astext(st_pointatdistance(g, distance)) as at_distance from geo_lineref_params_28188 where id in (1, 2, 3) order by id;
-- @regex("ST_LineInterpolatePoint: fraction must be finite",true)
select st_lineinterpolatepoint(g, fraction) from geo_lineref_params_28188 where id = 4;
-- @regex("ST_LineInterpolatePoints: fraction must be finite",true)
select st_lineinterpolatepoints(g, fraction) from geo_lineref_params_28188 where id = 4;
-- @regex("ST_PointAtDistance: distance must be finite",true)
select st_pointatdistance(g, distance) from geo_lineref_params_28188 where id = 4;
drop table if exists geo_lineref_params_28188;

-- The session remains usable after the rejected inputs.
select st_astext(st_pointatdistance(st_geomfromtext('LINESTRING(0 0, 10 0)'), 3)) as finite_after_errors;

-- Non-linestring input is rejected.
-- @regex("not a LINESTRING",true)
select st_lineinterpolatepoint(st_geomfromtext('POINT(1 1)'), 0.5);
