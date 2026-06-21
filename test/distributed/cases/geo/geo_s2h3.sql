-- GIS: S2 cell and H3 index functions. A CellId / H3Index is BIGINT UNSIGNED.
-- See docs/design/s2h3_funcs.md.

-- ---------------------------------------------------------------------------
-- S2
-- ---------------------------------------------------------------------------

-- A point maps to a leaf cell (level 30).
select s2_cellid_level(s2_cellid(st_geomfromtext('POINT(116.3975 39.9087)'))) as leaf_level;

-- Parent level is whatever we ask for.
select s2_cellid_level(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(116.3975 39.9087)')), 10)) as parent_level;

-- Center of a cell is a POINT.
select st_astext(s2_cellid_center(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 5))) as center5;

-- Area is positive and a coarser cell is larger than a finer one.
select s2_cellid_area(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 5))
     > s2_cellid_area(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10)) as coarser_is_bigger;

-- Edge neighbours: 4 of them.
select json_length(s2_cellid_edgeneighbours(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10))) as n_edge;

-- All neighbours: at least the 4 edge neighbours (usually 8).
select json_length(s2_cellid_allneighbours(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10))) >= 4 as has_neighbours;

-- A cell is not its own neighbour.
select s2_cellid_areneighbours(
         s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10),
         s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10)) as self_neighbour;

-- A cell and its first edge neighbour are neighbours.
select s2_cellid_areneighbours(
         c,
         cast(json_unquote(json_extract(s2_cellid_edgeneighbours(c), '$[0]')) as unsigned)) as is_neighbour
from (select s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10) as c) t;

-- Works against a stored BIGINT UNSIGNED column.
drop table if exists s2t;
create table s2t(id int, pt point, cell bigint unsigned);
insert into s2t values (1, st_geomfromtext('POINT(116.3975 39.9087)'), 0);
insert into s2t values (2, st_geomfromtext('POINT(121.4737 31.2304)'), 0);
update s2t set cell = s2_cellid(pt);
select id, s2_cellid_level(cell) as lvl from s2t order by id;

-- Invalid CellId errors.
select s2_cellid_level(0);

-- ---------------------------------------------------------------------------
-- H3
-- ---------------------------------------------------------------------------

-- Default resolution is 15 (finest).
select h3_h3index_resolution(h3_h3index(st_geomfromtext('POINT(116.3975 39.9087)'))) as default_res;

-- Explicit resolution is honoured.
select h3_h3index_resolution(h3_h3index(st_geomfromtext('POINT(116.3975 39.9087)'), 7)) as res7;

-- Center of an H3 cell is a POINT.
select st_astext(h3_h3index_center(h3_h3index(st_geomfromtext('POINT(0 0)'), 5))) as h3_center5;

-- Boundary is a MULTIPOINT; a hexagon has 6 vertices.
select st_geometrytype(h3_h3index_boundary(h3_h3index(st_geomfromtext('POINT(0 0)'), 5))) as boundary_type;
select st_numgeometries(h3_h3index_boundary(h3_h3index(st_geomfromtext('POINT(0 0)'), 5))) as n_vertices;

-- Immediate parent is one resolution coarser.
select h3_h3index_resolution(h3_h3index_parent(h3_h3index(st_geomfromtext('POINT(0 0)'), 7))) as parent_res;

-- Parent at an explicit resolution.
select h3_h3index_resolution(h3_h3index_parent(h3_h3index(st_geomfromtext('POINT(0 0)'), 7), 3)) as parent_res3;

-- Neighbours: a hexagon has 6.
select json_length(h3_h3index_neighbours(h3_h3index(st_geomfromtext('POINT(0 0)'), 7))) as n_neighbours;

-- A cell is not its own neighbour.
select h3_h3index_areneighbours(
         h3_h3index(st_geomfromtext('POINT(0 0)'), 7),
         h3_h3index(st_geomfromtext('POINT(0 0)'), 7)) as self_neighbour;

-- A cell and its first neighbour are neighbours.
select h3_h3index_areneighbours(
         c,
         cast(json_unquote(json_extract(h3_h3index_neighbours(c), '$[0]')) as unsigned)) as is_neighbour
from (select h3_h3index(st_geomfromtext('POINT(0 0)'), 7) as c) t;

-- Works against a stored BIGINT UNSIGNED column.
drop table if exists h3t;
create table h3t(id int, pt point, h3 bigint unsigned);
insert into h3t values (1, st_geomfromtext('POINT(116.3975 39.9087)'), 0);
insert into h3t values (2, st_geomfromtext('POINT(121.4737 31.2304)'), 0);
update h3t set h3 = h3_h3index(pt, 9);
select id, h3_h3index_resolution(h3) as res from h3t order by id;

-- Invalid H3Index errors.
select h3_h3index_resolution(0);

drop table if exists s2t;
drop table if exists h3t;
