-- #28196: SRID 4326 topology and polygon overlay must evaluate a bounded
-- spherical region in one common local frame when an edge crosses the
-- antimeridian. The SQL layer supplies the SRID from the typed column; the
-- WKB payload itself remains SRID-free.

drop database if exists geo_antimeridian;
create database geo_antimeridian;
use geo_antimeridian;

drop table if exists antimeridian_pairs;
create table antimeridian_pairs(
    id int primary key,
    g geometry srid 4326
);
insert into antimeridian_pairs values
    (1, st_geomfromtext('POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))', 4326)),
    (2, st_geomfromtext('POLYGON((179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5,179.5 -0.5))', 4326)),
    (3, st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))', 4326)),
    (4, st_geomfromtext('POLYGON((0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5))', 4326));

-- Typed-column/JOIN path: outer contains inner, and the inner does not
-- overlap it as a peer region.
select
    st_contains(o.g, i.g),
    st_within(i.g, o.g),
    st_intersects(o.g, i.g),
    st_disjoint(o.g, i.g),
    st_touches(o.g, i.g),
    st_crosses(o.g, i.g),
    st_overlaps(o.g, i.g),
    st_equals(o.g, i.g),
    st_covers(o.g, i.g),
    st_coveredby(i.g, o.g)
from antimeridian_pairs o join antimeridian_pairs i on o.id = 1 and i.id = 2;

-- Swapping operands must select the same longitude branch.
select
    st_contains(i.g, o.g),
    st_within(o.g, i.g),
    st_intersects(i.g, o.g),
    st_disjoint(i.g, o.g),
    st_touches(i.g, o.g),
    st_crosses(i.g, o.g),
    st_overlaps(i.g, o.g),
    st_equals(i.g, o.g),
    st_covers(i.g, o.g),
    st_coveredby(o.g, i.g)
from antimeridian_pairs o join antimeridian_pairs i on o.id = 1 and i.id = 2;

select
    st_astext(st_intersection(o.g, i.g)) as intersection,
    st_astext(st_union(o.g, i.g)) as union_result,
    st_astext(st_difference(o.g, i.g)) as difference_result,
    st_astext(st_symdifference(o.g, i.g)) as xor_result
from antimeridian_pairs o join antimeridian_pairs i on o.id = 1 and i.id = 2;

-- Geodetic area must remain the small local patch after overlay.
select
    st_area(st_intersection(o.g, i.g)) as intersection_area_m2,
    st_area(st_union(o.g, i.g)) as union_area_m2,
    st_area(st_difference(o.g, i.g)) as difference_area_m2
from antimeridian_pairs o join antimeridian_pairs i on o.id = 1 and i.id = 2;

-- Ordinary Cartesian-looking WGS84 regions remain unchanged.
select st_contains(o.g, i.g), st_intersects(o.g, i.g), st_overlaps(o.g, i.g)
from antimeridian_pairs o join antimeridian_pairs i on o.id = 3 and i.id = 4;

-- High-latitude but non-polar local regions are still handled by the same
-- bounded spherical projection.
select st_contains(
    st_geomfromtext('POLYGON((179 80,-179 80,-179 81,179 81,179 80))', 4326),
    st_geomfromtext('POLYGON((179.5 80.2,-179.5 80.2,-179.5 80.8,179.5 80.8,179.5 80.2))', 4326)
);

-- A polar vertex is outside the local planar applicability domain and must be
-- rejected explicitly rather than silently using Cartesian semantics.
-- @regex("SRID 4326 topology does not support geometry vertices at the poles",true)
select st_intersects(
    st_geomfromtext('POLYGON((0 85,10 85,10 90,0 90,0 85))', 4326),
    st_geomfromtext('POINT(5 85)', 4326)
);

-- GEOMETRY32 overlay overload must use the same topology adapter and preserve
-- float32 output. Boolean topology predicates do not currently expose a
-- GEOMETRY32 SQL overload, so the predicate coverage above uses GEOMETRY.
drop table if exists antimeridian_geometry32;
create table antimeridian_geometry32(id int primary key, g geography32);
insert into antimeridian_geometry32 values
    (1, cast('POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))' as geography32)),
    (2, cast('POLYGON((179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5,179.5 -0.5))' as geography32));
select st_astext(st_intersection(o.g, i.g)),
       st_area(st_intersection(o.g, i.g))
from antimeridian_geometry32 o join antimeridian_geometry32 i on o.id = 1 and i.id = 2;

-- The existing R-tree stores a Cartesian envelope. A dateline-crossing
-- SRID-4326 column must therefore use the exact base-table predicate instead
-- of an unsound index candidate scan.
drop table if exists antimeridian_index;
create table antimeridian_index(
    id int primary key,
    g geometry srid 4326 not null
);
create spatial index idx_g on antimeridian_index(g);
insert into antimeridian_index values
    (1, st_geomfromtext('POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))', 4326)),
    (2, st_geomfromtext('POLYGON((160 0,162 0,162 2,160 2,160 0))', 4326));
select id from antimeridian_index
where st_contains(g, st_geomfromtext('POINT(180 0)', 4326))
order by id;
select id from antimeridian_index
where st_intersects(g, st_geomfromtext('POINT(180 0)', 4326))
order by id;

drop table antimeridian_index;
drop table antimeridian_geometry32;
drop table antimeridian_pairs;
drop database geo_antimeridian;
