-- GIS: WKT/WKB I/O round-trips for every geometry kind.
-- Geometry is stored as WKB; ST_AsText renders canonical WKT.

-- ST_GeomFromText -> ST_AsText round-trips, all seven kinds + EMPTY.
select st_astext(st_geomfromtext('POINT(1 2)')) as point;
select st_astext(st_geomfromtext('LINESTRING(0 0,1 1,2 3)')) as linestring;
select st_astext(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))')) as polygon;
select st_astext(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))')) as polygon_hole;
select st_astext(st_geomfromtext('MULTIPOINT(1 1,2 2)')) as multipoint;
select st_astext(st_geomfromtext('MULTILINESTRING((0 0,1 1),(2 2,3 3))')) as multilinestring;
select st_astext(st_geomfromtext('MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))')) as multipolygon;
select st_astext(st_geomfromtext('GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))')) as collection;
select st_astext(st_geomfromtext('POINT EMPTY')) as point_empty;
select st_astext(st_geomfromtext('GEOMETRYCOLLECTION EMPTY')) as collection_empty;

-- Numeric formats: negatives, decimals, scientific.
select st_astext(st_geomfromtext('POINT(-1.5 2.25)')) as decimals;
select st_astext(st_geomfromtext('POINT(1e2 -3.5e-1)')) as scientific;

-- Casting text to geometry and back.
select st_astext(cast('POINT(7 8)' as geometry)) as cast_point;
select st_astext(cast('POLYGON((0 0,1 0,1 1,0 1,0 0))' as geometry)) as cast_polygon;

-- ST_AsWKT is a synonym for ST_AsText.
select st_aswkt(st_geomfromtext('POINT(1 2)')) as aswkt_point;

-- A geometry-typed column renders directly as WKT.
select st_geomfromtext('POINT(7 8)') as direct_geom;

-- WKB binary I/O: ST_GeomFromWKB(ST_AsWKB(g)) round-trips.
select st_astext(st_geomfromwkb(st_aswkb(st_geomfromtext('POINT(1 2)')))) as wkb_point;
select st_astext(st_geomfromwkb(st_aswkb(st_geomfromtext('LINESTRING(0 0,1 1,2 3)')))) as wkb_line;
select st_astext(st_geomfromwkb(st_aswkb(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))')))) as wkb_polygon;
-- st_asbinary / st_geomfrombinary are synonyms.
select st_astext(st_geomfrombinary(st_asbinary(st_geomfromtext('MULTIPOINT(1 1,2 2)')))) as wkb_multipoint;

-- NULL handling.
select st_astext(st_geomfromtext(null)) as null_text;
select st_astext(cast(null as geometry)) as null_cast;

-- Invalid input is rejected.
-- @regex("invalid geometry payload",true)
select st_astext(cast('NOT A GEOMETRY' as geometry));
-- @regex("invalid geometry payload",true)
select st_astext(cast('POINT(1' as geometry));
-- @regex("invalid geometry payload",true)
select st_astext(cast('POINT(NaN 1)' as geometry));
-- @regex("invalid geometry type",true)
select st_astext(st_geomfromtext('CIRCLE(0 0,1)'));
