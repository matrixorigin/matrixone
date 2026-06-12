-- GIS: SRID lives in the column/expression type (not the WKB payload).

-- ST_GeomFromText with an explicit SRID, read back by ST_SRID.
select st_srid(st_geomfromtext('POINT(1 2)', 4326)) as srid_4326;
select st_srid(st_geomfromtext('POINT(1 2)', 0)) as srid_0;
-- No SRID argument -> SRID 0.
select st_srid(st_geomfromtext('POINT(1 2)')) as srid_default;

-- SRID propagates through derived geometries (carried by the result type).
select st_srid(st_centroid(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))', 4326))) as srid_centroid;
select st_srid(st_boundary(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))', 4326))) as srid_boundary;
select st_srid(st_envelope(st_geomfromtext('LINESTRING(0 0,1 1)', 4326))) as srid_envelope;
select st_srid(st_startpoint(st_geomfromtext('LINESTRING(7 8,9 10)', 4326))) as srid_startpoint;
select st_srid(st_geometryn(st_geomfromtext('MULTIPOINT(1 1,2 2)', 4326), 1)) as srid_geometryn;

-- The geometry itself round-trips regardless of SRID.
select st_astext(st_geomfromtext('POINT(1 2)', 4326)) as wkt_with_srid;

-- A SRID column records its declared SRID and stores matching-SRID geometries.
drop database if exists geo_srid;
create database geo_srid;
use geo_srid;
drop table if exists gs;
create table gs(g point srid 4326);
show create table gs;
insert into gs values (st_geomfromtext('POINT(1 1)', 4326));
insert into gs values (st_geomfromtext('POINT(2 2)', 4326));
select st_astext(g) as wkt, st_srid(g) as srid from gs order by 1;
-- A geometry with a different SRID is rejected.
-- @regex("does not match",true)
insert into gs values (st_geomfromtext('POINT(3 3)', 0));
-- A geometry with no SRID is rejected for a SRID-constrained column.
-- @regex("does not match",true)
insert into gs values (st_geomfromtext('POINT(3 3)'));
drop table gs;

-- A plain geometry column has SRID 0.
drop table if exists gp;
create table gp(g geometry);
insert into gp values (st_geomfromtext('POINT(1 1)'));
select st_srid(g) as plain_srid from gp;
drop table gp;
drop database geo_srid;

-- Binary spatial functions reject operands with different SRIDs.
-- @regex("different srids",true)
select st_distance(st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(3 4)', 0));
-- @regex("different srids",true)
select st_contains(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))', 4326), st_geomfromtext('POINT(2 2)', 0));
