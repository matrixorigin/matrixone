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

-- High precision must preserve finite coordinates and always produce valid JSON.
select json_valid(st_asgeojson(st_geomfromtext('POINT(2 -2)'), 308)) as scale_overflow_valid, json_valid(st_asgeojson(st_geomfromtext('POINT(1.23456789 0)'), 309)) as infinite_scale_valid, json_valid(st_asgeojson(st_geomfromtext('POINT(1.23456789 0)'), 1000)) as large_precision_valid, json_valid(st_asgeojson(st_geomfromtext('POINT(1.23456789 0)'), 4294967295)) as max_precision_valid;
select st_asgeojson(st_geomfromtext('POINT(1.23456789 0)'), 309) as digits_309, st_asgeojson(st_geomfromtext('POINT(1.23456789 0)'), 1000) as digits_1000;

-- The documented maximum is accepted; out-of-range signed values fail cleanly.
-- @regex("maxdecimaldigits must be between 0 and 4294967295",true)
select st_asgeojson(st_geomfromtext('POINT(1 2)'), -1);
-- @regex("maxdecimaldigits must be between 0 and 4294967295",true)
select st_asgeojson(st_geomfromtext('POINT(1 2)'), 4294967296);
-- @regex("maxdecimaldigits must be between 0 and 4294967295",true)
select st_asgeojson(st_geomfromtext('POINT(1 2)'), 9223372036854775807);
select st_asgeojson(st_geomfromtext(null), -1) as null_geometry, st_asgeojson(st_geomfromtext('POINT(1 2)'), null) as null_precision;

-- Column and prepared-parameter paths share the same bounded validation.
drop table if exists gj_precision_t;
create table gj_precision_t(id int, g geometry, digits bigint);
insert into gj_precision_t values (1, st_geomfromtext('POINT(2 -2)'), 308), (2, st_geomfromtext('POINT(1.23456789 0)'), 1000), (3, st_geomfromtext('POINT(1.23456789 0)'), 4294967295);
select id, json_valid(st_asgeojson(g, digits)) as valid_json from gj_precision_t order by id;
insert into gj_precision_t values (4, st_geomfromtext('POINT(1 2)'), -1);
-- @regex("maxdecimaldigits must be between 0 and 4294967295",true)
select id, st_asgeojson(g, digits) from gj_precision_t order by id;
select 1 as after_invalid_precision;
drop table gj_precision_t;
prepare gj_precision_p from 'select json_valid(st_asgeojson(st_geomfromtext(''POINT(1.23456789 0)''), ?)) as valid_json';
set @gj_precision = 1000;
execute gj_precision_p using @gj_precision;
deallocate prepare gj_precision_p;

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
