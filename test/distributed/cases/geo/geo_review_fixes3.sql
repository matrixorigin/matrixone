-- Regression coverage for the fourth-pass review fix (PR #24777):
-- ST_Length / ST_Area / ST_Distance now reject any SRID other than 0
-- (Cartesian) and 4326 (geodetic) instead of silently falling back to unitless
-- Cartesian math. The gate applies to both the explicit +SRID argument and the
-- SRID carried in the operand's type Width.

drop database if exists geo_review3;
create database geo_review3;
use geo_review3;

-- Supported SRIDs still compute: 0 -> Cartesian, 4326 -> geodetic (meters).
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 0) as area_planar;
select st_length(st_geomfromtext('LINESTRING(0 0,3 4)'), 0) as len_planar;
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(3 4)'), 0) as dist_planar;
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 4326) > 0 as area_geodetic_ok;

-- An unsupported explicit SRID (e.g. the projected system 3857) errors instead
-- of returning a meaningless unitless Cartesian result.
-- @regex("unsupported SRID 3857",true)
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 3857);
-- @regex("unsupported SRID 3857",true)
select st_length(st_geomfromtext('LINESTRING(0 0,3 4)'), 3857);
-- @regex("unsupported SRID 3857",true)
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(3 4)'), 3857);

-- The same gate applies when the unsupported SRID is carried in the operand's
-- type Width rather than passed explicitly.
-- @regex("unsupported SRID 3857",true)
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))', 3857));
-- @regex("unsupported SRID 3857",true)
select st_length(st_geomfromtext('LINESTRING(0 0,3 4)', 3857));
-- @regex("unsupported SRID 3857",true)
select st_distance(st_geomfromtext('POINT(0 0)', 3857), st_geomfromtext('POINT(3 4)', 3857));

drop database geo_review3;
