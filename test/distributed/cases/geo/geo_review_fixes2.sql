-- Regression coverage for the third-pass review fixes (PR #24777):
-- ST_Distance_Sphere input constraints, explicit-SRID argument range checks,
-- half-NaN WKB rejection, and large-coordinate overlay (no int64 overflow).

drop database if exists geo_review2;
create database geo_review2;
use geo_review2;

-- ST_Distance_Sphere accepts only POINT / MULTIPOINT, requires matching SRIDs,
-- and validates longitude/latitude ranges (the sphere kernel reads X/Y as
-- degrees regardless of SRID).
select st_distance_sphere(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 0)')) as one_degree_m;
select st_distance_sphere(st_geomfromtext('MULTIPOINT(0 0,2 2)'), st_geomfromtext('POINT(1 0)')) as mp_ok;
-- @regex("only supports POINT and MULTIPOINT",true)
select st_distance_sphere(st_geomfromtext('LINESTRING(0 0,1 1)'), st_geomfromtext('POINT(1 0)'));
-- @regex("longitude .* out of range",true)
select st_distance_sphere(st_geomfromtext('POINT(200 0)'), st_geomfromtext('POINT(1 0)'));
-- @regex("latitude .* out of range",true)
select st_distance_sphere(st_geomfromtext('POINT(0 100)'), st_geomfromtext('POINT(1 0)'));
-- @regex("different srids",true)
select st_distance_sphere(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 0));

-- An explicit SRID argument is range-checked before it is narrowed to uint32, so
-- a negative or oversized SRID errors instead of silently wrapping (e.g. -1 ->
-- 4294967295) into the wrong coordinate-system kernel.
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(3 4)'), 0) as dist_srid0_ok;
-- @regex("SRID value -1 is out of range",true)
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(3 4)'), -1);
-- @regex("SRID value -1 is out of range",true)
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), -1);
-- @regex("SRID value 5000000000 is out of range",true)
select st_length(st_geomfromtext('LINESTRING(0 0,3 4)'), 5000000000);

-- Only the (NaN, NaN) pattern is an empty point; a half-NaN point is malformed
-- WKB and is rejected rather than yielding a NaN-tainted geometry.
select st_astext(st_geomfromwkb(unhex('0101000000000000000000F87F000000000000F87F'))) as empty_pt_ok;
-- @regex("invalid",true)
select st_astext(st_geomfromwkb(unhex('0101000000000000000000F87F000000000000F03F')));

-- Overlay snap-rounding no longer overflows int64 for large coordinates
-- (a coordinate magnitude above ~9.22e9 used to corrupt the result).
select st_astext(st_intersection(
    st_geomfromtext('POLYGON((0 0,2e10 0,2e10 2e10,0 2e10,0 0))'),
    st_geomfromtext('POLYGON((1e10 1e10,3e10 1e10,3e10 3e10,1e10 3e10,1e10 1e10))'))) as big_overlay;

drop database geo_review2;
