-- GIS: ST_Point / ST_Point32 numeric point constructors.
-- st_point(x, y) builds POINT(x y) where x is X/longitude and y is Y/latitude.

-- Basic construction, round-tripped through WKT.
select st_astext(st_point(1, 2)) as p;
select st_astext(st_point(116.3975, 39.9087)) as beijing;
select st_geometrytype(st_point(1, 2)) as gtype;

-- x is the first ordinate (X/longitude), y the second (Y/latitude).
select st_x(st_point(116.3975, 39.9087)) as x, st_y(st_point(116.3975, 39.9087)) as y;

-- Integer literals are accepted (cast to double).
select st_astext(st_point(3, 4)) as ints;

-- ST_Point32 returns the float32 GEOMETRY32 variant.
select st_astext(st_point32(1, 2)) as p32;
select st_geometrytype(st_point32(1, 2)) as gtype32;
-- float32 coordinates carry reduced precision vs float64.
select st_astext(st_point32(116.3975, 39.9087)) as bj32;

-- NULL propagation: a NULL ordinate yields NULL.
select st_astext(st_point(NULL, 1)) as a, st_astext(st_point(1, NULL)) as b, st_astext(st_point32(NULL, NULL)) as c;

-- Stored in geometry / geometry32 columns.
drop table if exists sp;
create table sp(id int, g geometry, g32 geometry32);
insert into sp values (1, st_point(1, 2), st_point32(1, 2));
insert into sp values (2, st_point(116.3975, 39.9087), st_point32(116.3975, 39.9087));
select id, st_astext(g) as g, st_astext(g32) as g32 from sp order by id;
drop table if exists sp;

-- Feeds the S2 / H3 index functions directly (no WKT string needed).
select s2_cellid_level(s2_cellid(st_point(116.3975, 39.9087))) as s2_lvl,
       h3_h3index_resolution(h3_h3index(st_point(116.3975, 39.9087), 9)) as h3_res;

-- Regression: INSERT ... VALUES with decimal-literal args (the DefaultBinder
-- used to coerce the literals to the column's GEOMETRY type, breaking overload
-- resolution). Must work for point / point32 subtype columns too.
drop table if exists reg;
create table reg(p point, p32 point32);
insert into reg values (st_point(116.3975, 39.9087), st_point32(116.3975, 39.9087));
insert into reg values (st_point(1.5, 2.5), st_point32(1.5, 2.5));
select st_astext(p) as p, st_astext(p32) as p32 from reg order by 1;
select s2_cellid_level(s2_cellid(p)) as lvl from reg order by 1;
drop table if exists reg;

-- Wrapped constructors must bind the same way: parentheses and a cast around
-- the function call still bind its arguments by their own types.
drop table if exists wrap;
create table wrap(p point, p32 point32);
insert into wrap values ((st_point(116.3975, 39.9087)), (st_point32(116.3975, 39.9087)));
insert into wrap values (cast(st_point(1.5, 2.5) as point), cast(st_point32(1.5, 2.5) as point32));
select st_astext(p) as p, st_astext(p32) as p32 from wrap order by 1;
drop table if exists wrap;
