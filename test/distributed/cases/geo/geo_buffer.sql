-- GIS: ST_Buffer (planar Minkowski-sum buffer via polygon union).
-- Areas are approximate (the disc is polygon-approximated), so checks use ranges.

-- Point buffer ~ pi*r^2. r=2 -> ~12.57; the default 32-gon is slightly under.
-- @separator:table
select st_area(st_buffer(st_geomfromtext('POINT(0 0)'), 2)) between 12.4 and 12.6 as point_buf_ok;

-- Higher segments-per-quarter gets closer to a true circle.
select st_area(st_buffer(st_geomfromtext('POINT(0 0)'), 2, 32)) between 12.55 and 12.57 as fine_buf_ok;

-- Line buffer ~ 2*r*len + pi*r^2. len=10, r=1 -> ~23.14.
select st_area(st_buffer(st_geomfromtext('LINESTRING(0 0, 10 0)'), 1)) between 23.0 and 23.2 as line_buf_ok;

-- Polygon buffer grows the area: 10x10 square, r=1 -> ~143.14.
select st_area(st_buffer(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))'), 1)) between 142.9 and 143.3 as poly_buf_ok;

-- The result is areal.
select st_geometrytype(st_buffer(st_geomfromtext('POINT(0 0)'), 1)) as buf_type;

-- Zero distance returns the geometry unchanged.
select st_astext(st_buffer(st_geomfromtext('POINT(3 4)'), 0)) as zero_buf;

-- Negative distance is rejected.
-- @regex("negative distance",true)
select st_buffer(st_geomfromtext('POINT(0 0)'), -1);
