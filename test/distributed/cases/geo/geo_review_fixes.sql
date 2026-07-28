-- Regression coverage for the second-pass review fixes (PR #24777):
-- SRID storage bounds, non-constant SRID rejection, geometry->text casts,
-- GEOMETRY32 function overloads, and GeoJSON 3D/empty-point handling.

drop database if exists geo_review;
create database geo_review;
use geo_review;

-- SRID is stored in the type's int32 Width; the largest storable SRID is
-- 2147483646. Larger values are rejected rather than silently collapsing to
-- the "undefined" sentinel (e.g. 4294967295+1 would wrap to 0).
select st_srid(st_geomfromtext('POINT(1 1)', 2147483646)) as srid_max_ok;
-- @regex("SRID should be between 0 and 2147483646",true)
select st_geomfromtext('POINT(1 1)', 2147483647);
-- @regex("SRID should be between 0 and 2147483646",true)
select st_geomfromtext('POINT(1 1)', 4294967295);

-- A non-constant SRID cannot be carried in the type and is rejected instead of
-- being silently dropped.
-- @regex("SRID argument of a geometry constructor must be a constant",true)
select st_srid(st_geomfromtext('POINT(1 1)', cast(4326 as bigint)));

-- CAST(geometry AS text) renders WKT; CAST AS varbinary keeps the raw WKB.
select cast(st_geomfromtext('POINT(1 2)') as char) as as_char;
select cast(st_geomfromtext('LINESTRING(0 0,1 1)') as text) as as_text;
select length(cast(st_geomfromtext('POINT(1 2)') as varbinary)) as raw_bytes;

-- GEOMETRY32 resolves the structural / predicate function overloads.
drop table if exists g32fn;
create table g32fn(g geometry32);
insert into g32fn values (st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'));
select st_isempty(g) e, st_srid(g) s, st_dimension(g) d, st_iscollection(g) c,
       st_numgeometries(g) n, st_numinteriorrings(g) ir from g32fn;
select st_asgeojson(g) as gj from g32fn;
drop table g32fn;

-- GeoJSON is 2D only: 3D positions are rejected instead of being truncated.
-- @regex("must have exactly 2 coordinates",true)
select st_astext(st_geomfromgeojson('{"type":"Point","coordinates":[1,2,3]}'));
-- The GeoJSON SRID argument enforces the same upper bound.
-- @regex("SRID should be between 0 and 2147483646",true)
select st_astext(st_geomfromgeojson('{"type":"Point","coordinates":[1,2]}', 5000000000));

-- An empty point inside a MULTIPOINT serializes as [] (not a fabricated [0,0]).
select st_asgeojson(st_collect(st_geomfromtext('POINT EMPTY'), st_geomfromtext('POINT(1 1)'))) as gj_empty_member;

drop database geo_review;
