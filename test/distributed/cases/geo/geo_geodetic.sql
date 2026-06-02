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

-- A GEOGRAPHY column (generic geometry defaulting to SRID 4326) computes
-- geodesically.
drop database if exists geo_geodetic;
create database geo_geodetic;
use geo_geodetic;
drop table if exists places;
create table places(id int, g geography);
insert into places values (1, st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))', 4326));
select id, st_srid(g) as srid, st_area(g) as area_m2 from places order by id;
drop table places;
drop database geo_geodetic;
