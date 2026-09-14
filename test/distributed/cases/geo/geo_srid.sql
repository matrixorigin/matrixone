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

-- Overlay, discrete-distance, MBR, envelope, and collect evaluators use the same
-- SRID admission contract as Distance/Contains. These are typed column inputs
-- so the execution boundary is exercised rather than only constant folding.
create database geo_srid_checks;
use geo_srid_checks;
create table inputs (poly_4326 geometry srid 4326, poly_0 geometry, line_4326 geometry srid 4326, line_0 geometry, point_4326 geometry srid 4326, point_0 geometry);
insert into inputs values (st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))', 4326), st_geomfromtext('POLYGON((1 1,3 1,3 3,1 3,1 1))', 0), st_geomfromtext('LINESTRING(0 0,1 1)', 4326), st_geomfromtext('LINESTRING(0 0,1 1)', 0), st_geomfromtext('POINT(0 0)', 4326), st_geomfromtext('POINT(1 1)', 0));
-- @regex("different srids",true)
select st_union(poly_4326, poly_0) from inputs;
-- @regex("different srids",true)
select st_intersection(poly_4326, poly_0) from inputs;
-- @regex("different srids",true)
select st_difference(poly_4326, poly_0) from inputs;
-- @regex("different srids",true)
select st_symdifference(poly_4326, poly_0) from inputs;
-- @regex("different srids",true)
select st_frechetdistance(line_4326, line_0) from inputs;
-- @regex("different srids",true)
select st_hausdorffdistance(line_4326, line_0) from inputs;
-- @regex("different srids",true)
select mbrcontains(poly_4326, point_0) from inputs;
-- @regex("different srids",true)
select mbrcovers(poly_4326, point_0) from inputs;
-- @regex("different srids",true)
select mbrwithin(point_4326, poly_0) from inputs;
-- @regex("different srids",true)
select mbrcoveredby(point_4326, poly_0) from inputs;
-- @regex("different srids",true)
select mbrdisjoint(point_4326, point_0) from inputs;
-- @regex("different srids",true)
select mbrintersects(point_4326, point_0) from inputs;
-- @regex("different srids",true)
select mbrequals(point_4326, point_0) from inputs;
-- @regex("different srids",true)
select mbroverlaps(poly_4326, poly_0) from inputs;
-- @regex("different srids",true)
select mbrtouches(point_4326, point_0) from inputs;
-- @regex("different srids",true)
select st_makeenvelope(point_4326, point_0) from inputs;
-- @regex("different srids",true)
select st_collect(point_4326, point_0) from inputs;

-- Equal effective SRIDs remain valid; constructors retain their established
-- output SRID metadata (Collect propagates input SRID; MakeEnvelope returns 0).
select st_srid(st_collect(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 1)', 0))) as collect_zero_srid;
select st_srid(st_collect(point_4326, st_geomfromtext('POINT(1 1)', 4326))) as collect_4326_srid from inputs;
select st_srid(st_union(poly_4326, st_geomfromtext('POLYGON((2 0,3 0,3 1,2 1,2 0))', 4326))) as union_4326_srid from inputs;
select st_srid(st_makeenvelope(point_4326, st_geomfromtext('POINT(1 1)', 4326))) as envelope_constructor_srid from inputs;
create table null_inputs (left_4326 geometry srid 4326, right_0 geometry);
insert into null_inputs values (NULL, st_geomfromtext('POINT(1 1)', 0));
select st_collect(left_4326, right_0) as collect_null from null_inputs;
drop database geo_srid_checks;
