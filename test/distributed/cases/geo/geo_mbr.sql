-- GIS: MBR (minimum bounding rectangle) predicates over geometry pairs.

drop table if exists mbr_t;
create table mbr_t(id int, g geometry);
insert into mbr_t values
  (1, st_geomfromtext('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
  (2, st_geomfromtext('POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))')),
  (3, st_geomfromtext('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))')),
  (4, st_geomfromtext('POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))')),
  (5, st_geomfromtext('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'));

-- Containment: outer (1) vs inner (2).
select mbrcontains(a.g, b.g) as contains_, mbrwithin(b.g, a.g) as within_,
       mbrcovers(a.g, b.g) as covers_, mbrcoveredby(b.g, a.g) as coveredby_
from mbr_t a, mbr_t b where a.id = 1 and b.id = 2;

-- Equality.
select mbrequals(a.g, b.g) as eq_self, mbrequals(a.g, c.g) as eq_other
from mbr_t a, mbr_t b, mbr_t c where a.id = 1 and b.id = 1 and c.id = 2;

-- Disjoint / intersects: outer (1) vs far (4) and inner (2).
select mbrdisjoint(a.g, b.g) as disjoint_far, mbrintersects(a.g, c.g) as intersects_inner
from mbr_t a, mbr_t b, mbr_t c where a.id = 1 and b.id = 4 and c.id = 2;

-- Touch along the x=10 edge (1 vs 3) vs partial overlap (1 vs 5).
select mbrtouches(a.g, b.g) as touch_edge, mbrtouches(a.g, c.g) as touch_cross
from mbr_t a, mbr_t b, mbr_t c where a.id = 1 and b.id = 3 and c.id = 5;

-- Overlap: partial (1 vs 5) true; containment (1 vs 2) and edge touch (1 vs 3) false.
select mbroverlaps(a.g, b.g) as ov_cross, mbroverlaps(a.g, c.g) as ov_inner, mbroverlaps(a.g, d.g) as ov_edge
from mbr_t a, mbr_t b, mbr_t c, mbr_t d where a.id = 1 and b.id = 5 and c.id = 2 and d.id = 3;

drop table mbr_t;
