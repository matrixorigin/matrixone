# GIS / GEOMETRY BVT tests

BVT coverage for the GEOMETRY type and spatial functions (see
`docs/design/gisimpl.md`). Geometry values are stored as bare WKB; SRID lives in
the column/expression type.

## Files

| File | Covers |
|------|--------|
| `geo_io.sql` | WKT↔WKB↔geometry round-trips (`ST_GeomFromText`, `ST_AsText`, `CAST`), all seven kinds + `EMPTY`, number formats, NULL and invalid-input handling. |
| `geo_type_ddl.sql` | DDL for `GEOMETRY` and the subtype aliases, the `GEOMETRY32`/`GEOGRAPHY`/`*32` aliases, `SRID` attribute, `SHOW CREATE TABLE`, insert/select/update/delete storage, and subtype enforcement. |
| `geo_functions_unary.sql` | Accessors (`ST_GeometryType`, `ST_X`, `ST_Y`, `ST_Dimension`, `ST_NumPoints`, `ST_NumGeometries`, `ST_NumInteriorRings`, `ST_IsEmpty`, `ST_IsClosed`, `ST_IsCollection`, …), measures (`ST_Area`, `ST_Length`), and derived geometries (`ST_Centroid`, `ST_Boundary`, `ST_Envelope`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_ExteriorRing`, `ST_InteriorRingN`, `ST_GeometryN`, `ST_PointOnSurface`). |
| `geo_functions_binary.sql` | `ST_Distance` and the relationship predicates `ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, `ST_Equals`, `ST_Covers`, `ST_CoveredBy` (Cartesian / SRID 0). |
| `geo_srid.sql` | SRID carried by the type: `ST_GeomFromText(wkt, srid)` + `ST_SRID`, SRID propagation through derived geometries, `SRID` columns, and binary-function SRID-mismatch rejection. |

## Generating the `.result` files

These are `.sql` test inputs. Generate the paired `.result` files against a
running MatrixOne with mo-tester:

```bash
cd /root/mo-tester
./run.sh -m genrs -n -g -p /root/matrixone/test/distributed/cases/geo
```

Then run them normally:

```bash
./run.sh -n -g -p /root/matrixone/test/distributed/cases/geo
```

## Scope notes

Tests intentionally cover only what is implemented today. Not yet exercised
(pending follow-up work): `GEOMETRY32` value storage / `geometry`↔`geometry32`
casts, and geodetic (SRID 4326) computation — spatial measures here are
Cartesian.
