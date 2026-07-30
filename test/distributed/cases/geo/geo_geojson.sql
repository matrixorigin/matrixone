-- GIS: GeoJSON I/O (ST_AsGeoJSON, ST_GeomFromGeoJSON).

-- Export each geometry kind to GeoJSON.
select st_asgeojson(st_geomfromtext('POINT(1 2)')) as pt;
select st_asgeojson(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) as ls;
select st_asgeojson(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')) as poly;
select st_asgeojson(st_geomfromtext('MULTIPOINT(0 0, 1 1)')) as mpt;
select st_asgeojson(st_geomfromtext('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))')) as gc;

-- Coordinate rounding via maxdecimaldigits.
select st_asgeojson(st_geomfromtext('POINT(1.23456 2.34567)')) as full_prec;
select st_asgeojson(st_geomfromtext('POINT(1.23456 2.34567)'), 2) as two_digits;

-- Import GeoJSON back to a geometry; round-trips through WKT.
select st_astext(st_geomfromgeojson('{"type":"Point","coordinates":[3,4]}')) as pt;
select st_astext(st_geomfromgeojson('{"type":"LineString","coordinates":[[0,0],[1,1],[2,2]]}')) as ls;
select st_astext(st_geomfromgeojson('{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}')) as poly;

-- Default SRID is 4326; explicit SRID override is honored.
select st_srid(st_geomfromgeojson('{"type":"Point","coordinates":[3,4]}')) as default_srid;
select st_srid(st_geomfromgeojson('{"type":"Point","coordinates":[3,4]}', 0)) as srid0;

-- Round-trip: geometry -> GeoJSON -> geometry.
select st_astext(st_geomfromgeojson(st_asgeojson(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')))) as rt;

-- Invalid GeoJSON is rejected.
-- @regex("invalid GeoJSON",true)
select st_geomfromgeojson('{"type":"Point"}');

drop table if exists gj_t;
create table gj_t(id int, g geometry);
insert into gj_t values (1, st_geomfromgeojson('{"type":"Point","coordinates":[10,20]}'));
select id, st_astext(g) from gj_t;
drop table gj_t;
