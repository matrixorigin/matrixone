-- GIS: constructive operations (ST_ConvexHull, ST_Simplify, ST_Collect).

-- Convex hull: interior point dropped, collinear -> line, single -> point.
select st_astext(st_convexhull(st_geomfromtext('MULTIPOINT(0 0, 4 0, 4 4, 0 4, 2 2)'))) as hull_poly;
select st_astext(st_convexhull(st_geomfromtext('MULTIPOINT(0 0, 1 1, 2 2)'))) as hull_line;
select st_astext(st_convexhull(st_geomfromtext('MULTIPOINT(5 5, 5 5)'))) as hull_point;
select st_astext(st_convexhull(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))'))) as hull_from_poly;

-- Simplify: nearly-straight middle vertex removed; real bend kept.
select st_astext(st_simplify(st_geomfromtext('LINESTRING(0 0, 5 0.0001, 10 0)'), 0.001)) as simp_flat;
select st_astext(st_simplify(st_geomfromtext('LINESTRING(0 0, 5 5, 10 0)'), 0.001)) as simp_bend;
select st_astext(st_simplify(st_geomfromtext('POINT(1 2)'), 1)) as simp_point;

-- Collect: same kind -> multi; mixed -> collection; flattened.
select st_astext(st_collect(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 1)'))) as collect_pts;
select st_astext(st_collect(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), st_geomfromtext('POLYGON((2 2,3 2,3 3,2 3,2 2))'))) as collect_polys;
select st_astext(st_collect(st_geomfromtext('POINT(0 0)'), st_geomfromtext('LINESTRING(0 0, 1 1)'))) as collect_mixed;

-- SRID is preserved through constructive ops on a typed column.
drop table if exists cons_t;
create table cons_t(id int, g geometry srid 4326);
insert into cons_t values (1, st_geomfromtext('LINESTRING(0 0, 1 0, 2 0, 3 0)', 4326));
select st_srid(st_simplify(g, 0.5)) as simp_srid, st_srid(st_convexhull(g)) as hull_srid from cons_t;
drop table cons_t;
