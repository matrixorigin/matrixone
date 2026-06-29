Implement the following SQL functions.  `S2_XXX` functions should be using S2 geometry
and `H3_XXX` are H3 geometries from [H3](https://github.com/uber/h3-go)

`S2_CellId(POINT) returns CellId`: Convert a POINT (longitude, latitude) to a BIGINT UNSIGNED S2 CellId
`S2_CellId_Level(CellId) returns level`: Return the level (int) of the cellid
`S2_CellId_Center(CellId) returns POINT`: Return the center of cellid
`S2_CellId_Area(CellId) returns float64`: Return the aproximate area of cellid
`S2_CellId_Parent(CellId, level) returns CellId`: Return the parent (a CellId) of the cell at level
`S2_CellId_EdgeNeighbours(CellId) returns JSON`: Return edge neighbours at the level of this cell, in a JSON array of uint64.
`S2_CellId_AllNeighbours(CellId) returns JSON`: Return all neighbours at the level of this cell in a JSON array of uint64.
`S2_CellId_AreNeighbours(CellId, CellId) returns true/false`: Return if two cellid are neighbours.

`H3_H3Index(POINT[, resolution]) returns H3Index`: Convert a point to an H3Index (also a BIGINT UNSIGNED). Resolution is optional and defaults to 15.
`H3_H3Index_Resolution(h H3Index) returns resolution`: return the resolution (int) of the H3Index
`H3_H3Index_Center(h H3Index) returns POINT`: return the center of the H3Index
`H3_H3Index_Boundary(h H3Index) returns MULTIPOINT`: Return the vertices of the boundary of the cell of the H3Index.
`H3_H3Index_Parent(h H3Index[, resolution]) returns H3Index`: Return the parent (immediate, or at an optional coarser resolution).
`H3_H3Index_Neighbours(H3Index) returns JSON`: Return all neighbours at the level of this cell in a JSON array of uint64.
`H3_H3Index_AreNeighbours(H3Index, H3Index) returns true/false`: Return if are neighbours.

---

## Implementation

Implemented in `pkg/sql/plan/function/func_s2h3.go`, registered in
`pkg/sql/plan/function/list_builtIn.go`, with function ids in
`pkg/sql/plan/function/function_id.go` (519–533). BVT coverage:
`test/distributed/cases/geo/geo_s2h3.sql`.

Libraries: S2 via `github.com/golang/geo/s2` (already a dependency); H3 via
`github.com/uber/h3-go/v4` (added — cgo-backed, bundles the H3 C library).

### Conventions

- A `POINT` argument carries `(longitude, latitude)`: X is longitude, Y is
  latitude, matching `ST_GeomFromText('POINT(lng lat)')`.
- A `CellId` (S2) and an `H3Index` (H3) are both `BIGINT UNSIGNED` (uint64). S2's
  `CellID` is a uint64 directly; h3-go's `Cell` is an int64 whose bit pattern is
  the index, round-tripped through `uint64`/`int64`.
- Neighbour lists return a **JSON array of uint64** (`jsonb` in the spec maps to
  MatrixOne's `JSON` type — there is no separate `jsonb` type). The values are
  encoded with bytejson's native uint64 type code, so the full 64-bit id is
  preserved (encoding as a JSON float would lose precision above 2^53). Use
  `json_extract(... , '$[0]')` + `cast(... as unsigned)` to read an element back.
- Returned `POINT`/`MULTIPOINT` values are plain `GEOMETRY` (no SRID tag), the
  same as MatrixOne's `ST_PointFromText` etc.; the WKB content carries the
  subtype.
- An invalid `CellId`/`H3Index` (e.g. `0`) or a non-POINT geometry argument
  raises an `invalid input` error; NULL inputs yield NULL.
- A `POINT` is interpreted as geographic degrees: longitude must be in
  `[-180, 180]` and latitude in `[-90, 90]`, and both must be finite.
  Out-of-range or non-finite coordinates raise an error — so projected/planar
  coordinates (e.g. UTM) are rejected; convert to lon/lat first.
- `s2_cellid_areneighbours` and `h3_h3index_areneighbours` evaluate adjacency at
  the level/resolution of the **first** argument. Comparing cells of different
  levels (e.g. a cell against its ancestor) returns `false`, not an error.

### Function reference (as implemented)

S2 (`s2_*`):

| SQL | Args | Returns | Notes |
|-----|------|---------|-------|
| `s2_cellid` | `POINT` | `BIGINT UNSIGNED` | leaf cell (level 30) for the point |
| `s2_cellid_level` | `CellId` | `INT` | 0..30 |
| `s2_cellid_center` | `CellId` | `POINT` | cell center `(lng lat)` |
| `s2_cellid_area` | `CellId` | `DOUBLE` | approximate area in **square metres** (S2 steradians × mean-Earth-radius²) |
| `s2_cellid_parent` | `CellId, level INT` | `BIGINT UNSIGNED` | ancestor cell at `level` (**returns a CellId** — the spec's "returns POINT" was a typo; mirrors `h3_h3index_parent`) |
| `s2_cellid_edgeneighbours` | `CellId` | `JSON` | 4 edge-adjacent CellIds |
| `s2_cellid_allneighbours` | `CellId` | `JSON` | all (edge+vertex) neighbour CellIds at this cell's level |
| `s2_cellid_areneighbours` | `CellId, CellId` | `BOOL` | whether the 2nd is a neighbour of the 1st (evaluated at the 1st cell's level) |

H3 (`h3_*`):

| SQL | Args | Returns | Notes |
|-----|------|---------|-------|
| `h3_h3index` | `POINT` | `BIGINT UNSIGNED` | default resolution **15** (finest — the analogue of S2's leaf cell, since a point alone is otherwise ambiguous in H3) |
| `h3_h3index` | `POINT, res INT` | `BIGINT UNSIGNED` | explicit resolution 0..15 |
| `h3_h3index_resolution` | `H3Index` | `INT` | 0..15 |
| `h3_h3index_center` | `H3Index` | `POINT` | cell center `(lng lat)` |
| `h3_h3index_boundary` | `H3Index` | `MULTIPOINT` | boundary vertices (6 for a hexagon, 5 for a pentagon) |
| `h3_h3index_parent` | `H3Index` | `BIGINT UNSIGNED` | immediate parent (one resolution coarser) |
| `h3_h3index_parent` | `H3Index, res INT` | `BIGINT UNSIGNED` | ancestor at resolution `res` |
| `h3_h3index_neighbours` | `H3Index` | `JSON` | immediate neighbours (grid disk radius 1, excluding the cell) |
| `h3_h3index_areneighbours` | `H3Index, H3Index` | `BOOL` | whether the two cells are neighbours |

British/American spellings are both accepted (`_neighbours`/`_neighbors`,
`_edgeneighbours`/`_edgeneighbors`, etc.).

### Decisions vs. the spec

- `S2_CellId_Parent` returns a `CellId` (BIGINT UNSIGNED), not a POINT. The
  original spec line read "returns POINT" (a copy-paste slip — now corrected
  above); a parent cell is itself a cell, matching the symmetric
  `H3_H3Index_Parent`.
- `H3_H3Index(POINT)` gained an optional resolution argument; the no-arg form
  defaults to resolution 15. H3 cannot pick a cell from a point without a
  resolution, unlike S2 where a point maps to a unique leaf cell.
- `S2_CellId_Area` returns square metres rather than the unit-sphere steradians
  S2 computes natively, so the value is directly usable and comparable to H3.

### Example

```sql
-- index a point, inspect the cell, walk up the hierarchy
select s2_cellid(st_geomfromtext('POINT(116.3975 39.9087)'));      -- 3886697461225194355
select s2_cellid_level(3886697461225194355);                       -- 30
select st_astext(s2_cellid_center(s2_cellid_parent(3886697461225194355, 10)));

select h3_h3index(st_geomfromtext('POINT(116.3975 39.9087)'), 9);  -- an H3Index at res 9
select h3_h3index_neighbours(h3_h3index(st_geomfromtext('POINT(0 0)'), 7));

-- store a cell id in a column and read an element of a neighbour list back
create table places(id int, pt point, cell bigint unsigned);
insert into places values (1, st_geomfromtext('POINT(116.3975 39.9087)'), 0);
update places set cell = s2_cellid(pt);
select s2_cellid_areneighbours(
         cell,
         cast(json_unquote(json_extract(s2_cellid_edgeneighbours(cell), '$[0]')) as unsigned))
from places;
```

### Related: numeric POINT constructor

These functions all take a `POINT` geometry. Besides
`ST_PointFromText('POINT(lng lat)')`, you can build one from numeric
coordinates directly with `ST_Point` / `ST_Point32`:

- `ST_Point(x, y) returns GEOMETRY` — a POINT with float64 coordinates.
- `ST_Point32(x, y) returns GEOMETRY32` — the float32-coordinate variant.

`x` is the X/longitude and `y` the Y/latitude (WKT `POINT(x y)` order); the
result is a plain GEOMETRY (SRID 0). Non-finite coordinates raise an error; a
NULL ordinate yields NULL. Example:

```sql
select s2_cellid(st_point(116.3975, 39.9087));
select h3_h3index(st_point(116.3975, 39.9087), 9);
```
