-- GIS: geodetic (SRID 4326) measures return meters / square meters, whereas
-- SRID 0 measures are Cartesian (unitless).

-- Length: one degree of arc on the equator is ~111195 m geodesically, but 1.0
-- in Cartesian units.
select st_length(st_geomfromtext('LINESTRING(0 0,1 0)', 4326)) as geodesic_len_m;
select st_length(st_geomfromtext('LINESTRING(0 0,1 0)')) as cartesian_len;

-- Distance: meters for SRID 4326, units for SRID 0.
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326)) as geodesic_dist_m;
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(3 4)')) as cartesian_dist;

-- Area: square meters for SRID 4326, square units for SRID 0.
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))', 4326)) as geodesic_area_m2;
select st_area(st_geomfromtext('POLYGON((0 0,3 0,3 4,0 4,0 0))')) as cartesian_area;

-- Antimeridian-local polygons must select the two-degree patch, not its
-- 358-degree complement. The distance probes verify the same interior choice.
select st_area(st_geomfromtext('POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))', 4326)) as antimeridian_area_m2;
select st_distance(st_geomfromtext('POINT(180 0)', 4326), st_geomfromtext('POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))', 4326)) as antimeridian_local_distance_m;
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))', 4326)) as antimeridian_greenwich_distance_m;

-- The +SRID overload forces the coordinate system regardless of the type SRID.
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 4326) as forced_geodesic_m2;
select st_length(st_geomfromtext('LINESTRING(0 0,1 0)'), 4326) as forced_geodesic_len_m;
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 0)'), 4326) as forced_geodesic_dist_m;
select st_area(st_geomfromtext('POLYGON((0 0,3 0,3 4,0 4,0 0))', 4326), 0) as forced_cartesian_area;

-- MySQL-compatible length-unit overloads.
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'kilometre') as geodesic_distance_km;
select st_frechetdistance(st_geomfromtext('LINESTRING(0 0,1 0)', 4326), st_geomfromtext('LINESTRING(0 1,1 1)', 4326), 'kilometre') as geodesic_frechet_km;
select st_hausdorffdistance(st_geomfromtext('LINESTRING(0 0,1 0)', 4326), st_geomfromtext('LINESTRING(0 1,1 1)', 4326), 'kilometre') as geodesic_hausdorff_km;
-- Exercise the authoritative MySQL unit names, case/accent-insensitive
-- lookup, and an independently scaled meter oracle. Abbreviations such as
-- "km" are intentionally not accepted as aliases.
select round(st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'US SURVEY FOOT') * 0.30480060960121924, 6) as us_survey_foot_m, round(st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'Statute mile') * 1609.344, 6) as statute_mile_m, round(st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'fathom') * 1.8288, 6) as fathom_m, round(st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'chain') * 20.1168, 6) as chain_m, round(st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'link') * 0.201168, 6) as link_m, round(st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'MÈTRE'), 6) as accented_metre_m;
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 0)', 4326), 'km');

-- S2 normalizes finite out-of-range coordinates. SRID-4326 measurement
-- functions must reject them before entering the S2 kernels; validate both
-- operands and every coordinate, even when distance could return early.
select st_distance(st_geomfromtext('POINT(181 0)', 4326), st_geomfromtext('POINT(0 0)', 4326));
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(0 91)', 4326));
select st_distance(st_geomfromtext('MULTIPOINT((0 0),(181 0))', 4326), st_geomfromtext('POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))', 4326));
select st_length(st_geomfromtext('LINESTRING(0 0,181 0)', 4326));
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 91,0 91,0 0))', 4326));

-- The explicit-SRID overload selects the effective coordinate system. These
-- must reject invalid coordinates for 4326, but preserve SRID-0 Cartesian
-- behavior even when the geometry value itself is typed as 4326.
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(181 0)'), 4326);
select st_length(st_geomfromtext('LINESTRING(0 0,0 91)'), 4326);
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 91,0 91,0 0))'), 4326);
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(181 0)')) as cartesian_distance_out_of_range;
select st_length(st_geomfromtext('LINESTRING(0 0,181 0)')) as cartesian_length_out_of_range;
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 91,0 91,0 0))')) as cartesian_area_out_of_range;
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(181 0)', 4326), 0) as forced_cartesian_distance_out_of_range;
select st_length(st_geomfromtext('LINESTRING(0 0,181 0)', 4326), 0) as forced_cartesian_length_out_of_range;
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 91,0 91,0 0))', 4326), 0) as forced_cartesian_area_out_of_range;

-- A GEOGRAPHY column (generic geometry defaulting to SRID 4326) computes
-- geodesically.
drop database if exists geo_geodetic;
create database geo_geodetic;
use geo_geodetic;
drop table if exists places;
create table places(id int, g geography, g32 geography32);
insert into places values (1, st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))', 4326), cast('POLYGON((0 0,1 0,1 1,0 1,0 0))' as geography32)), (2, st_geomfromtext('POINT(181 0)', 4326), cast('POINT(181 0)' as geography32)), (3, st_geomfromtext('LINESTRING(0 0,0 91)', 4326), cast('LINESTRING(0 0,0 91)' as geography32)), (4, st_geomfromtext('POLYGON((0 0,1 0,1 91,0 91,0 0))', 4326), cast('POLYGON((0 0,1 0,1 91,0 91,0 0))' as geography32)), (5, st_geomfromtext('POINT(180 90)', 4326), cast('POINT(180 90)' as geography32));
-- Reusing a prepared plan around an error must revalidate each execution and
-- must not poison a later valid row.
prepare geo_measure_reuse from 'select st_distance(g, g) as distance_m from places where id = ?';
set @geo_measure_id = 1;
execute geo_measure_reuse using @geo_measure_id;
set @geo_measure_id = 2;
execute geo_measure_reuse using @geo_measure_id;
set @geo_measure_id = 5;
execute geo_measure_reuse using @geo_measure_id;
deallocate prepare geo_measure_reuse;
-- Re-specialize one prepared plan across string, numeric, string and NULL
-- runtime domains instead of retaining the first overload choice.
prepare geo_distance_overload from 'select st_distance(st_geomfromtext(?,4326), st_geomfromtext(?,4326), ?) as distance_m';
set @geo_distance_left = 'POINT(0 0)';
set @geo_distance_right = 'POINT(1 0)';
set @geo_distance_arg = 'kilometre';
execute geo_distance_overload using @geo_distance_left, @geo_distance_right, @geo_distance_arg;
set @geo_distance_arg = 4326;
execute geo_distance_overload using @geo_distance_left, @geo_distance_right, @geo_distance_arg;
set @geo_distance_arg = 'metre';
execute geo_distance_overload using @geo_distance_left, @geo_distance_right, @geo_distance_arg;
set @geo_distance_arg = NULL;
execute geo_distance_overload using @geo_distance_left, @geo_distance_right, @geo_distance_arg;
deallocate prepare geo_distance_overload;
select id, st_srid(g) as srid, st_area(g) as area_m2 from places where id = 1;
select st_distance(g, g) from places where id = 2;
select st_length(g) from places where id = 3;
select st_area(g) from places where id = 4;
select st_distance(g32, g32) from places where id = 2;
select st_length(g32) from places where id = 3;
select st_area(g32) from places where id = 4;
select st_distance(g, g) as inclusive_geodetic_boundary from places where id = 5;
select st_distance(g32, g32) as inclusive_geodetic_boundary32 from places where id = 5;
select st_distance(g32, g32, 'kilometre') as geometry32_distance_km from places where id = 1;
drop table places;
drop database geo_geodetic;
