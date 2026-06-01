-- GIS: unary spatial functions — accessors, measures, and derived geometries.

-- Type / structure accessors.
select st_geometrytype(st_geomfromtext('POINT(1 2)')) as t_point;
select st_geometrytype(st_geomfromtext('LINESTRING(0 0,1 1)')) as t_line;
select st_geometrytype(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 0))')) as t_poly;
select st_geometrytype(st_geomfromtext('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))')) as t_mpoly;

select st_dimension(st_geomfromtext('POINT(1 2)')) as d_point;
select st_dimension(st_geomfromtext('LINESTRING(0 0,1 1)')) as d_line;
select st_dimension(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 0))')) as d_poly;

select st_isempty(st_geomfromtext('POINT EMPTY')) as empty_yes;
select st_isempty(st_geomfromtext('POINT(1 2)')) as empty_no;
select st_iscollection(st_geomfromtext('MULTIPOINT(1 1,2 2)')) as is_coll;
select st_iscollection(st_geomfromtext('POINT(1 1)')) as not_coll;

-- Point coordinates.
select st_x(st_geomfromtext('POINT(3 4)')) as x;
select st_y(st_geomfromtext('POINT(3 4)')) as y;

-- LineString accessors.
select st_numpoints(st_geomfromtext('LINESTRING(0 0,1 1,2 2)')) as n_points;
select st_isclosed(st_geomfromtext('LINESTRING(0 0,1 0,1 1,0 0)')) as closed_yes;
select st_isclosed(st_geomfromtext('LINESTRING(0 0,1 1)')) as closed_no;
select st_astext(st_startpoint(st_geomfromtext('LINESTRING(7 8,9 10,11 12)'))) as start_pt;
select st_astext(st_endpoint(st_geomfromtext('LINESTRING(7 8,9 10,11 12)'))) as end_pt;
select st_astext(st_pointn(st_geomfromtext('LINESTRING(7 8,9 10,11 12)'), 2)) as point_n;

-- Polygon accessors.
select st_astext(st_exteriorring(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'))) as ext_ring;
select st_numinteriorrings(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2))')) as n_rings;
select st_astext(st_interiorringn(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2))'), 1)) as int_ring;

-- Collection accessors.
select st_numgeometries(st_geomfromtext('MULTIPOINT(1 1,2 2,3 3)')) as n_geoms;
select st_astext(st_geometryn(st_geomfromtext('MULTIPOINT(1 1,2 2,3 3)'), 2)) as geom_n;

-- Measures (Cartesian / SRID 0).
select st_area(st_geomfromtext('POLYGON((0 0,3 0,3 4,0 4,0 0))')) as rect_area;
select st_area(st_geomfromtext('POLYGON((0 0,20 0,20 20,0 20,0 0),(5 5,15 5,15 15,5 15,5 5))')) as hole_area;
select st_area(st_geomfromtext('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,4 2,4 4,2 4,2 2)))')) as mpoly_area;
select st_length(st_geomfromtext('LINESTRING(0 0,3 4)')) as line_len;
select st_length(st_geomfromtext('MULTILINESTRING((0 0,0 1),(0 0,1 0))')) as mline_len;

-- Derived geometries (rendered as WKT).
select st_astext(st_centroid(st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))'))) as centroid;
select st_astext(st_centroid(st_geomfromtext('MULTIPOINT(0 0,2 0,2 2,0 2)'))) as centroid_mp;
select st_astext(st_envelope(st_geomfromtext('LINESTRING(1 2,3 4,0 5)'))) as envelope;
select st_astext(st_boundary(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'))) as boundary;
select st_astext(st_pointonsurface(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'))) as pos;

-- Type errors are rejected.
-- @regex("geometry is not a POLYGON or MULTIPOLYGON",true)
select st_area(st_geomfromtext('POINT(1 1)'));
-- @regex("geometry is not a LINESTRING",true)
select st_length(st_geomfromtext('POINT(1 1)'));
