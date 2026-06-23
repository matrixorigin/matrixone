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

-- ---------------------------------------------------------------------------
-- NULL propagation (scalar and vectorized)
-- ---------------------------------------------------------------------------
select s2_cellid(NULL) as a, s2_cellid_level(NULL) as b, s2_cellid_center(NULL) as c,
       s2_cellid_area(NULL) as d, s2_cellid_parent(NULL, 5) as e,
       s2_cellid_edgeneighbours(NULL) as f, s2_cellid_areneighbours(NULL, NULL) as g;
select h3_h3index(NULL) as a, h3_h3index(NULL, 9) as b, h3_h3index_resolution(NULL) as c,
       h3_h3index_center(NULL) as d, h3_h3index_boundary(NULL) as e,
       h3_h3index_parent(NULL) as f, h3_h3index_neighbours(NULL) as g,
       h3_h3index_areneighbours(NULL, NULL) as h;

-- A NULL row among valid rows stays NULL (vectorized path).
drop table if exists nullt;
create table nullt(id int, c bigint unsigned);
insert into nullt values (1, s2_cellid(st_geomfromtext('POINT(0 0)'))), (2, NULL), (3, s2_cellid(st_geomfromtext('POINT(1 1)')));
select id, s2_cellid_level(c) as lvl from nullt order by id;
drop table if exists nullt;

-- ---------------------------------------------------------------------------
-- GEOMETRY32 / POINT32 input overloads
-- ---------------------------------------------------------------------------
drop table if exists p32;
create table p32(p point32);
insert into p32 values (st_pointfromtext('POINT(116.3975 39.9087)'));
-- float32 coordinates still yield a valid leaf cell / res-9 index
-- (the raw ids differ from the float64 point due to coordinate precision).
select s2_cellid_level(s2_cellid(p)) as s2_lvl, h3_h3index_resolution(h3_h3index(p, 9)) as h3_res from p32;
drop table if exists p32;

-- ---------------------------------------------------------------------------
-- Boundary resolutions / levels
-- ---------------------------------------------------------------------------
select h3_h3index_resolution(h3_h3index(st_geomfromtext('POINT(0 0)'), 0)) as h3_res0;
select s2_cellid_level(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 0)) as s2_level0;

-- ---------------------------------------------------------------------------
-- Neighbour relationships: symmetry and cross-level
-- ---------------------------------------------------------------------------
-- Symmetry: if B is a neighbour of A then A is a neighbour of B.
select s2_cellid_areneighbours(a, b) and s2_cellid_areneighbours(b, a) as s2_symmetric
from (select c as a, cast(json_unquote(json_extract(s2_cellid_edgeneighbours(c), '$[0]')) as unsigned) as b
      from (select s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10) as c) t0) t1;
select h3_h3index_areneighbours(a, b) and h3_h3index_areneighbours(b, a) as h3_symmetric
from (select c as a, cast(json_unquote(json_extract(h3_h3index_neighbours(c), '$[0]')) as unsigned) as b
      from (select h3_h3index(st_geomfromtext('POINT(0 0)'), 7) as c) t0) t1;
-- Cross-level: a cell and its ancestor are not "neighbours".
select s2_cellid_areneighbours(
         s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10),
         s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 5)) as cross_level;
-- H3 cross-resolution: a cell and its coarser ancestor are not neighbours
-- (different resolutions return false, not an error).
select h3_h3index_areneighbours(
         h3_h3index(st_geomfromtext('POINT(0 0)'), 7),
         h3_h3index_parent(h3_h3index(st_geomfromtext('POINT(0 0)'), 7), 3)) as h3_cross_res;

-- American spelling aliases resolve to the same functions.
select json_length(s2_cellid_allneighbors(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10))) as us_s2_all,
       json_length(s2_cellid_edgeneighbors(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 10))) as us_s2_edge,
       json_length(h3_h3index_neighbors(h3_h3index(st_geomfromtext('POINT(0 0)'), 7))) as us_h3_nb,
       s2_cellid_areneighbors(s2_cellid(st_geomfromtext('POINT(0 0)')), s2_cellid(st_geomfromtext('POINT(0 0)'))) as us_s2_arenb,
       h3_h3index_areneighbors(h3_h3index(st_geomfromtext('POINT(0 0)'), 7), h3_h3index(st_geomfromtext('POINT(0 0)'), 7)) as us_h3_arenb;

-- ---------------------------------------------------------------------------
-- Additional error paths
-- ---------------------------------------------------------------------------
-- longitude/latitude out of range
select s2_cellid(st_geomfromtext('POINT(200 100)'));
select h3_h3index(st_geomfromtext('POINT(0 95)'));
-- empty point
select s2_cellid(st_geomfromtext('POINT EMPTY'));
-- S2 parent level out of [0,30], and finer than the cell
select s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 40);
select s2_cellid_parent(s2_cellid_parent(s2_cellid(st_geomfromtext('POINT(0 0)')), 5), 20);
-- H3 resolution-0 cell has no parent; parent at a finer resolution is an error
select h3_h3index_parent(h3_h3index(st_geomfromtext('POINT(0 0)'), 0));
select h3_h3index_parent(h3_h3index(st_geomfromtext('POINT(0 0)'), 3), 7);

drop table if exists s2t;
drop table if exists h3t;
