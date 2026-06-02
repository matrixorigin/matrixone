# GIS / GEOMETRY BVT tests

BVT coverage for the GEOMETRY type and spatial functions (see
`docs/design/gisimpl.md`). Geometry values are stored as bare WKB; SRID lives in
the column/expression type.

## Files

| File | Covers |
|------|--------|
| `geo_io.sql` | WKT↔WKB↔geometry round-trips (`ST_GeomFromText`, `ST_AsText`/`ST_AsWKT`, `ST_AsWKB`/`ST_GeomFromWKB` and the `*Binary` synonyms, `CAST`), all seven kinds + `EMPTY`, number formats, NULL and invalid-input handling. |
| `geo_type_ddl.sql` | DDL for `GEOMETRY` and the subtype aliases, the `GEOMETRY32`/`GEOGRAPHY`/`*32` aliases, `SRID` attribute, `SHOW CREATE TABLE`, insert/select/update/delete storage, and subtype enforcement. |
| `geo_functions_unary.sql` | Accessors (`ST_GeometryType`, `ST_X`, `ST_Y`, `ST_Dimension`, `ST_NumPoints`, `ST_NumGeometries`, `ST_NumInteriorRings`, `ST_IsEmpty`, `ST_IsClosed`, `ST_IsCollection`, …), measures (`ST_Area`, `ST_Length`), and derived geometries (`ST_Centroid`, `ST_Boundary`, `ST_Envelope`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_ExteriorRing`, `ST_InteriorRingN`, `ST_GeometryN`, `ST_PointOnSurface`). |
| `geo_functions_binary.sql` | `ST_Distance` and the relationship predicates `ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, `ST_Equals`, `ST_Covers`, `ST_CoveredBy` (Cartesian / SRID 0). |
| `geo_validity.sql` | `ST_IsSimple`, `ST_IsRing`, `ST_IsValid`. |
| `geo_srid.sql` | SRID carried by the type: `ST_GeomFromText(wkt, srid)` + `ST_SRID`, SRID propagation through derived geometries, `SRID` columns, and binary-function SRID-mismatch rejection. |
| `geo_geometry32.sql` | `GEOMETRY32` (float32-coordinate) DDL, storage round-trips (float32 WKB), spatial functions, and `geometry`↔`geometry32` casts. |
| `geo_geodetic.sql` | SRID 4326 measures (`ST_Length`, `ST_Distance`, `ST_Area`) return meters / m² (geodesic) vs Cartesian for SRID 0, incl. a `GEOGRAPHY` column. |

Together these exercise every implemented `ST_*` function (the I/O constructors
and accessors, the unary measures and derived geometries, the binary distance
and relationship predicates, the validity predicates, `ST_SRID`, and the WKB
binary I/O) plus their synonyms.

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

Tests intentionally cover only what is implemented today. SRID 4326 geodetic
computation is wired into the measures (`ST_Length`/`ST_Distance`/`ST_Area`);
the relationship predicates (`ST_Contains`, `ST_Intersects`, …) are still
evaluated in the Cartesian plane (geodetic predicates are a follow-up).
