-- GIS: GEOMETRY type DDL, aliased types, storage, and subtype enforcement.

drop database if exists geo_ddl;
create database geo_ddl;
use geo_ddl;

-- Generic geometry column and each subtype alias.
drop table if exists t_all;
create table t_all(
  g  geometry,
  pt point,
  ls linestring,
  pg polygon,
  mp multipoint,
  ml multilinestring,
  mg multipolygon,
  gc geometrycollection
);
show create table t_all;
drop table t_all;

-- 32-bit float family and geography aliases parse and create.
drop table if exists t_f32;
create table t_f32(a geometry32, b point32, c polygon32, d geography, e geography32);
drop table t_f32;

-- SRID column attribute.
drop table if exists t_srid;
create table t_srid(g point srid 4326);
show create table t_srid;
drop table t_srid;

-- Storage round-trip on a generic geometry column.
drop table if exists g1;
create table g1(id int, g geometry);
insert into g1 values (1, st_geomfromtext('POINT(1 2)'));
insert into g1 values (2, st_geomfromtext('LINESTRING(0 0,3 4)'));
insert into g1 values (3, st_geomfromtext('POLYGON((0 0,2 0,2 2,0 2,0 0))'));
insert into g1 values (4, null);
select id, st_astext(g) as wkt from g1 order by id;
select id, st_geometrytype(g) as gtype from g1 where g is not null order by id;
update g1 set g = st_geomfromtext('POINT(9 9)') where id = 1;
select st_astext(g) as wkt from g1 where id = 1;
delete from g1 where id = 3;
select id, st_astext(g) as wkt from g1 order by id;
drop table g1;

-- A subtype-constrained column accepts its subtype and rejects others.
drop table if exists pts;
create table pts(g point);
insert into pts values (st_geomfromtext('POINT(1 1)'));
insert into pts values (st_geomfromtext('POINT(2 2)'));
select st_astext(g) as wkt from pts order by 1;
-- @regex("cannot store",true)
insert into pts values (st_geomfromtext('LINESTRING(0 0,1 1)'));
drop table pts;

-- A generic geometry column accepts any subtype.
drop table if exists anyg;
create table anyg(g geometry);
insert into anyg values (st_geomfromtext('POINT(1 1)'));
insert into anyg values (st_geomfromtext('POLYGON((0 0,1 0,1 1,0 0))'));
select st_geometrytype(g) as gtype from anyg order by 1;
drop table anyg;

drop database geo_ddl;
