-- GIS: typed geometry constructors from WKT and WKB (MySQL parity).

-- From text, one per subtype.
select st_astext(st_pointfromtext('POINT(1 2)')) as p;
select st_astext(st_linefromtext('LINESTRING(0 0,1 1)')) as l;
select st_astext(st_polyfromtext('POLYGON((0 0,1 0,1 1,0 0))')) as poly;
select st_astext(st_mpointfromtext('MULTIPOINT(1 1,2 2)')) as mp;
select st_astext(st_mlinefromtext('MULTILINESTRING((0 0,1 1),(2 2,3 3))')) as ml;
select st_astext(st_mpolyfromtext('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))')) as mpoly;
select st_astext(st_geomcollfromtext('GEOMETRYCOLLECTION(POINT(1 1))')) as gc;

-- Long-name synonyms.
select st_astext(st_linestringfromtext('LINESTRING(0 0,2 2)')) as l2;
select st_astext(st_polygonfromtext('POLYGON((0 0,1 0,1 1,0 0))')) as poly2;
select st_astext(st_geometryfromtext('POINT(3 4)')) as gft;

-- Subtype mismatch is rejected.
-- @regex("geometry is not a POINT",true)
select st_pointfromtext('LINESTRING(0 0,1 1)');
-- @regex("geometry is not a POLYGON",true)
select st_polyfromtext('POINT(1 2)');

-- From WKB (round-trip via ST_AsWKB).
select st_astext(st_pointfromwkb(st_aswkb(st_geomfromtext('POINT(5 6)')))) as wp;
select st_astext(st_polyfromwkb(st_aswkb(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 0))')))) as wpoly;
select st_astext(st_geomfromwkb(st_aswkb(st_geomfromtext('LINESTRING(0 0,1 1)')))) as wg;
-- @regex("geometry is not a POINT",true)
select st_pointfromwkb(st_aswkb(st_geomfromtext('LINESTRING(0 0,1 1)')));

-- ST_NumInteriorRing (singular synonym of ST_NumInteriorRings).
select st_numinteriorring(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 1))')) as nir;
