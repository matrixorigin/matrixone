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

-- The +SRID overload forces the coordinate system regardless of the type SRID.
select st_area(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 4326) as forced_geodesic_m2;
select st_length(st_geomfromtext('LINESTRING(0 0,1 0)'), 4326) as forced_geodesic_len_m;
select st_distance(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 0)'), 4326) as forced_geodesic_dist_m;
select st_area(st_geomfromtext('POLYGON((0 0,3 0,3 4,0 4,0 0))', 4326), 0) as forced_cartesian_area;

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
select id, st_srid(g) as srid, st_area(g) as area_m2 from places where id = 1;
select st_distance(g, g) from places where id = 2;
select st_length(g) from places where id = 3;
select st_area(g) from places where id = 4;
select st_distance(g32, g32) from places where id = 2;
select st_length(g32) from places where id = 3;
select st_area(g32) from places where id = 4;
select st_distance(g, g) as inclusive_geodetic_boundary from places where id = 5;
select st_distance(g32, g32) as inclusive_geodetic_boundary32 from places where id = 5;
drop table places;
drop database geo_geodetic;
