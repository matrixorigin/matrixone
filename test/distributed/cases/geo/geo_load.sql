-- GIS: loading GEOMETRY values from CSV. Covers the three load paths — external
-- table over a file, LOAD DATA INFILE into a regular table, and LOAD DATA INLINE.
-- Geometry columns parse (E)WKT fields into stored WKB (float32 for GEOMETRY32);
-- an unconstrained GEOMETRY column accepts any subtype, a subtype-constrained
-- column enforces its subtype, and the GEOGRAPHY / *32 aliases work too.

drop database if exists geo_load;
create database geo_load;
use geo_load;

-- ---------- 1. External table over a CSV of geometry WKT ----------
drop table if exists ext_geo;
create external table ext_geo (
    id int,
    pt point,
    ls linestring,
    poly polygon,
    g geometry
) infile{'filepath'='$resources/external_table_file/geo_ext.csv'}
fields terminated by ',' enclosed by '"' ignore 1 lines;

select id, st_astext(pt), st_astext(ls), st_astext(poly), st_astext(g) from ext_geo order by id;
select id, st_x(pt) x, st_y(pt) y, st_geometrytype(g) gtype from ext_geo order by id;

-- ---------- 2. LOAD DATA INFILE into a regular geometry table ----------
drop table if exists load_geo;
create table load_geo (
    id int,
    pt point,
    ls linestring,
    poly polygon,
    g geometry
);
load data infile '$resources/load_data/geo_load.csv' into table load_geo fields terminated by ',' enclosed by '"' ignore 1 lines;
select id, st_astext(pt), st_astext(poly), st_geometrytype(g) gtype from load_geo order by id;
-- stored form is real WKB, not text: accessors and area work
select id, st_x(pt) x, st_y(pt) y, st_area(poly) area from load_geo order by id;

-- ---------- 3. LOAD DATA INLINE ----------
drop table if exists inline_geo;
create table inline_geo (
    id int,
    pt point,
    g geometry
);
load data inline format='csv', data='1,POINT(1 2),"LINESTRING(0 0,1 1)"\n2,POINT(3 4),"POLYGON((0 0,1 0,1 1,0 0))"\n3,POINT(5 6),"MULTIPOINT(7 7,8 8)"\n' into table inline_geo fields terminated by ',' enclosed by '"';
select id, st_astext(pt), st_astext(g), st_geometrytype(g) gtype from inline_geo order by id;

-- ---------- 4. GEOMETRY32 (float32 WKB) and GEOGRAPHY (SRID 4326) ----------
drop table if exists inline_geo32;
create table inline_geo32 (
    id int,
    pt point32,
    gg geography
);
load data inline format='csv', data='1,POINT(0.1 0.2),POINT(1 2)\n2,POINT(10 20),POINT(-87.63 41.88)\n' into table inline_geo32 fields terminated by ',';
select id, st_astext(pt) pt32, st_srid(gg) srid, st_astext(gg) geog from inline_geo32 order by id;

-- ---------- 5. subtype enforcement and invalid input during load ----------
drop table if exists bad_geo;
create table bad_geo (
    id int,
    pt point
);
-- a LINESTRING cannot be stored in a POINT column
load data inline format='csv', data='1,"LINESTRING(0 0,1 1)"\n' into table bad_geo fields terminated by ',' enclosed by '"';
-- malformed WKT is rejected, not silently stored
load data inline format='csv', data='2,not-a-geometry\n' into table bad_geo fields terminated by ',';
select count(*) cnt from bad_geo;

drop database if exists geo_load;
