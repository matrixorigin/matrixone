# GIS Implementation Plan

Implementation plan for the GEOMETRY / GIS type described in [`gis.md`](./gis.md).

This document breaks the work into self-contained, independently testable sub-tasks. We implement them one at a time, each with its own unit and/or BVT tests, so every step is mergeable on its own.

---

## 0. Context: what already exists vs. what the design requires

A partial GEOMETRY implementation already landed in the tree (PRs #24204, #24342, #24353). The plan **builds on it** rather than starting from scratch. Knowing the gap precisely is what makes the sub-tasks small.

### Already present

| Area | File(s) | State |
|------|---------|-------|
| Type OID `T_geometry = 67` | `pkg/container/types/types.go:79` | Done — varlena, binary charset (`CharsetType` line 439), `TypeLen`/`FixedLength` (lines 872/927), `String()`→`"GEOMETRY"` (722), `OidString()` (769) |
| Parser tokens & grammar | `pkg/sql/parsers/dialect/mysql/mysql_sql.y` (`spatial_type` ~13528, `SRID` attr ~10564), `keywords.go` | `GEOMETRY POINT LINESTRING POLYGON MULTIPOINT MULTILINESTRING MULTIPOLYGON GEOMETRYCOLLECTION` tokens + `SRID` column attribute exist. **No** `GEOMETRY(Type=…, SRID=…)` argument form, **no** `GEOMETRY('point', 4326)` positional form, **no** `GEOMETRY32`/`GEOGRAPHY` aliases |
| AST | `pkg/sql/parsers/tree/types.go` (`GeoMetadata{SRID, SRIDDefined}`) | Carries SRID only, not subtype or float32 flag |
| AST→`types.Type` | `pkg/sql/plan/build_util.go:226` (`getTypeFromAst`) | Encodes subtype+SRID into `plan.Type.Enumvalues` **string** |
| Subtype/SRID metadata helpers | `pkg/sql/plan/mysql_special_types.go` | `geometryMetadataString` / `decodeGeometryMetadata` store `"POINT;SRID=4326"` in `Enumvalues` |
| SHOW CREATE TABLE | `pkg/sql/plan/build_show_util.go:878` | Renders subtype + SRID |
| Casts geometry↔text | `pkg/sql/plan/function/func_cast.go:367,5466` | Implemented over the text payload |
| ~38 `ST_*` function IDs | `pkg/sql/plan/function/function_id.go:596-634` | `ST_ASTEXT`(413) … `ST_COVEREDBY`(451) allocated and registered |
| `ST_*` implementations | `pkg/sql/plan/function/func_unary.go`, `func_binary.go` | Implemented, but operate on **WKT text** via string parsing |
| BVT baseline | `test/distributed/cases/function/func_geometry.{test,result}` | 92 KB — our regression baseline |

### The core gap

1. **Storage payload is WKT text, not WKB.** Today the varlena holds the WKT string with an optional `"SRID=N;"` text prefix (`encodeGeometryPayload`/`decodeGeometryPayload`, `func_unary.go:812-874`). The design mandates **Standard WKB, little-endian** inside the varlena. *(Storage decision, confirmed: the varlena is the storage. We only change the bytes inside it — no storage-engine work.)*
2. **No geometry engine.** Every function re-parses the WKT string. There is no in-memory geometry model, no WKB reader/writer, no real geometric algorithms.
3. **No `GEOMETRY32`** (float32-coordinate variant) and **no aliased types** (`POINT`, …, `GEOGRAPHY`, and their `*32` forms).
4. **No geodetic computation.** SRID 4326 must compute in meters / m² using geodetic algorithms; today everything is string-based.
5. **Metadata lives in `Enumvalues`**, but the design says **Scale = subtype, Precision = SRID**.

### Confirmed design decisions (from review)

- **D1 — Storage:** WKB is just a byte format stored in the existing MatrixOne **varlena**. No storage-engine changes. **The cell holds bare WKB only — no SRID, no format header.** SRID lives solely in the column type (`Width`); coordinate width (float64/float32) is implied by the column OID. (See §1.3.)
- **D2 — Existing code:** **Full WKB rewrite.** Replace the WKT-text varlena payload with a WKB payload and move all geometry logic into a real engine. The existing functions are correct *planar* implementations, so their **algorithms are ported into the engine's `cartesian.go`** — but rewritten to operate on the parsed WKB geometry model instead of re-parsing WKT strings each call. The current text path and `func_geometry.test` are the regression baseline for the Cartesian results.
- **D3 — Geodetic:** **Vendor `github.com/golang/geo` (S2, Apache-2.0)** for spherical (SRID 4326) algorithms. Planar (SRID 0) algorithms we implement ourselves (ported from the existing code per D2).
- **D4 — Metadata:** **Move to `Scale`/`Width`.** `types.Type` has no `Precision` field — `Width` is its analog — so: `Scale` = subtype enum (0–7), `Width` = SRID.
- **D5 — SRID-based dispatch:** Every spatial computation dispatches on SRID **at eval time**: SRID 0 → `cartesian.go` (planar, unitless); SRID 4326 → `geodetic.go` (S2, results in **meters / m²**); any other SRID is an error (matching MySQL). The SRID is read from the **input vector's type** (`Width`, §1.4) — since the cell is bare WKB there is no per-value SRID to read. Today **no** function dispatches on SRID; this is net-new for every measurement/relationship function.
- **D6 — `+SRID` override overload on every function:** Each `ST_*` function gains an **additional overload** taking a trailing integer `SRID` argument that **overrides** the SRID used for computation (and the output geometry's SRID where applicable). This resolves the binary-function case where the two inputs carry different SRIDs, and lets a query force a coordinate system. Example: `ST_Area(g)` uses `g`'s column-type SRID; `ST_Area(g, 4326)` forces the geodetic path regardless of `g`'s type SRID. Implemented as a second overload on the **same function ID**, not a new function name (the `ST_GeomFromText(wkt, srid)` overload already demonstrates the pattern).

---

## 1. Target architecture

### 1.1 New package: `pkg/geo/`

All GIS engine code lives in its own directory (per the design). It has **no dependency on `pkg/sql`** so it is unit-testable in isolation and reusable.

```
pkg/geo/
  geom.go          // Geometry interface + Point/LineString/Polygon/Multi*/GeometryCollection + Coord
  consts.go        // Subtype enum (0..7), SRID consts (0, 4326), payload format flags
  wkt_parse.go     // WKT text  -> Geometry
  wkt_write.go     // Geometry  -> WKT text
  wkb_read.go      // WKB bytes -> Geometry (standard, little-endian; float64 + float32)
  wkb_write.go     // Geometry  -> WKB bytes (float64 + float32)
  wkb_f32.go       // float32 WKB variant for GEOMETRY32 (coords as float32; structure identical)
  cartesian.go     // planar algorithms (SRID 0): area, length, distance, predicates, centroid, envelope...
  geodetic.go      // spherical algorithms (SRID 4326) via golang/geo/s2: distance(m), area(m^2), predicates
  errors.go        // shared error helpers
  *_test.go        // table-driven unit tests per file
```

The SQL layer (`pkg/sql/plan/function/...`, casts, binder) calls into `pkg/geo`. No geometry algorithm logic remains in `func_unary.go`/`func_binary.go` after the migration — those files only marshal vectors and delegate.

### 1.2 Subtype enum (matches the design and WKB type codes)

| Value | Name | WKB geometry-type code |
|-------|------|------------------------|
| 0 | GENERIC | (any) |
| 1 | POINT | 1 |
| 2 | LINESTRING | 2 |
| 3 | POLYGON | 3 |
| 4 | MULTIPOINT | 4 |
| 5 | MULTILINESTRING | 5 |
| 6 | MULTIPOLYGON | 6 |
| 7 | GEOMETRYCOLLECTION | 7 |

`Scale` stores this value. A column declared `GEOMETRY` (generic) has `Scale = 0`; a `POINT` column has `Scale = 1`; etc.

### 1.3 Varlena payload layout (internal storage format)

**The cell stores plain WKB — nothing else.** SRID is *not* stored per value; it lives only in the column type (`Width`, §1.4). The geometry subtype is recoverable from the WKB type code, and the coordinate width (float64 vs float32) is determined by the column **OID** (`T_geometry` vs `T_geometry32`), so neither needs an in-cell header either.

```
+-------------------------------+
| bytes 0..N                    |
| WKB body (standard, LE)       |   <- the entire varlena payload
+-------------------------------+
```

- For `T_geometry`: the payload is **standard WKB** (little-endian, float64 coordinates, type codes from §1.2) — byte-for-byte interoperable with PostGIS/MySQL `ST_AsBinary` output.
- For `T_geometry32`: identical WKB structure but coordinates are float32. This is a MatrixOne extension; the float32 width is implied by the column OID, never by an in-cell flag.
- `ST_AsWKB` / `ST_AsBinary` on a `T_geometry` value return the payload **verbatim**. On a `T_geometry32` value they up-convert float32→float64 so the emitted WKB is always standard.
- Because the cell is pure WKB, **SRID is supplied by the type at every read site** (§1.5), not parsed from the bytes.

> **Why no per-cell SRID (vs. MySQL's 4-byte-SRID + WKB internal format):** per the storage decision, a geometry column has exactly one SRID, recorded once in the column type. Storing it in every row would be redundant and would let a row's SRID drift from its column. Keeping the cell as bare WKB also makes the on-disk bytes directly portable to/from other WKB tools.

### 1.4 Type-system metadata

- `types.T_geometry` (= 67, exists). `Scale` = subtype, `Width` = SRID.
- New `types.T_geometry32` (= **68**, next free value). Same varlena plumbing as `T_geometry`; distinguished only by OID (drives float32 payload).
- Aliased SQL type names map to `(OID, Scale, Width)` at parse/bind time:

| SQL alias | OID | Scale (subtype) | Width (SRID) |
|-----------|-----|-----------------|--------------|
| `GEOMETRY` | T_geometry | 0 GENERIC | 0 (unless SRID given) |
| `POINT` | T_geometry | 1 | 0 |
| `LINESTRING` | T_geometry | 2 | 0 |
| `POLYGON` | T_geometry | 3 | 0 |
| `MULTIPOINT` | T_geometry | 4 | 0 |
| `MULTILINESTRING` | T_geometry | 5 | 0 |
| `MULTIPOLYGON` | T_geometry | 6 | 0 |
| `GEOMETRYCOLLECTION` | T_geometry | 7 | 0 |
| `GEOGRAPHY` | T_geometry | 0 GENERIC | 4326 |
| `GEOMETRY32` + each `*32` alias | T_geometry32 | per row above | per row above |

`GEOMETRY(Type='point', SRID=4326)` and `GEOMETRY('point', 4326)` set `Scale`/`Width` explicitly.

### 1.5 SRID dispatch and the `+SRID` overload (applies to all spatial functions)

This is the central runtime rule for Phase C. Two pieces:

**(a) Effective-SRID resolution.** SRID is read from the **input vector's type** (`ivecs[i].GetType().Width`), since the cell is bare WKB (§1.3). Every spatial function computes an *effective SRID* and dispatches on it:

```
effSRID = explicitSRIDArg                              // if the +SRID overload was used
        else ivecs[0].GetType().Width                  // unary: column/expression SRID from the type
        else resolveBinary(left.Type.Width, right.Type.Width)  // binary: from both input types

dispatch(effSRID):
    0     -> cartesian.go   (planar, unitless results)
    4326  -> geodetic.go    (S2, results in meters / m^2)
    other -> error "unsupported SRID for <FN>"
```

- `resolveBinary` requires the two input **types** to share an SRID (error otherwise — the type-level analog of the existing `ensureMatchingGeometrySRID`); the `+SRID` overload overrides both and removes that constraint.
- A shared helper `func effectiveSRID(ivecs, explicit) (uint32, error)` + `func dispatchSpatial[...](effSRID, cartesianFn, geodeticFn)` lives in one place (`pkg/sql/plan/function/geometry_dispatch.go`) so no per-function SRID logic is duplicated.

**SRID of geometry-producing results.** When a function *returns* a geometry (e.g. `ST_GeomFromText`, `ST_Centroid`, `ST_Envelope`), the result carries its SRID in the **result vector's type `Width`**, set by the overload's `retType`:
- For accessors/derivations (`ST_Centroid(g)`, `ST_GeometryN(g, n)`): `retType` copies `Width` from the input geometry type.
- For constructors with a SRID argument (`ST_GeomFromText(wkt, srid)`): the SRID must be a **constant** so the binder/constant-folder can stamp it into `retType.Width`. A non-constant SRID argument to a geometry-*producing* function is rejected (we cannot record a runtime SRID in the bare-WKB cell). This is the one functional constraint introduced by not storing SRID per cell; for measurement/predicate functions (which return scalars, not geometries) a non-constant `+SRID` argument is fine.

**(b) The `+SRID` overload.** Each `ST_*` function ID carries an extra overload whose `args` append one integer SRID parameter:

| Function | base overload | `+SRID` overload |
|----------|---------------|------------------|
| `ST_Area(g)` | `[geometry] → float64` | `[geometry, int64] → float64` |
| `ST_Length(g)` | `[geometry] → float64` | `[geometry, int64] → float64` |
| `ST_Distance(a,b)` | `[geometry, geometry] → float64` | `[geometry, geometry, int64] → float64` |
| `ST_Contains(a,b)` | `[geometry, geometry] → bool` | `[geometry, geometry, int64] → bool` |
| … (every spatial fn) | base | base + trailing `int64` |

Semantics of the SRID argument:
- **SRID-sensitive functions** (area, length, distance, centroid, all relationship predicates, …): the arg selects the computation system (Cartesian vs geodetic) and overrides input SRIDs.
- **SRID-agnostic functions** (`ST_GeometryType`, `ST_NumPoints`, `ST_AsText`, type/structure accessors): the result value is unchanged; the arg only sets the SRID stamped on any geometry the function *returns*. Kept for a uniform surface ("all functions"), even though it is a no-op on the scalar result.

Validation: the SRID argument must be `0` or `4326` (else error), matching the supported set in `gis.md`.

---

## 2. Phasing

- **Phase A — Engine (`pkg/geo`)**: pure Go, no SQL. Build the geometry model, WKT, WKB (float64 + float32), and the Cartesian + geodetic algorithm kernels. Fully unit-tested in isolation. *Nothing user-visible changes.*
- **Phase B — Type & DDL integration**: wire `GEOMETRY`/`GEOMETRY32` + aliases through parser → `types.Type` (Scale/Width) → varlena WKB payload. Create/insert/load/select work end-to-end. This is the design's **Step 1**.
- **Phase C — Spatial functions**: re-point every `ST_*` at the engine, implement the full MySQL 9.7 spatial reference with **type-SRID dispatch** (Cartesian for SRID 0, geodetic meters/m² for SRID 4326) and a **`+SRID` override overload** on every function. This is the design's **Step 2**.

Phase A has no dependency on B/C and can be fully built and tested first. B depends on A. C depends on A and B.

---

## Phase A — Geometry engine (`pkg/geo`) — ✅ COMPLETE

All of Phase A is implemented and tested in `pkg/geo/` (33 tests passing, `go vet` and `gofmt` clean). Files: `consts.go`, `geom.go`, `wkt_parse.go`, `wkt_write.go`, `wkb.go`, `wkb_read.go`, `wkb_write.go`, `wkb_f32.go`, `cartesian.go`, `geodetic.go` (+ `*_test.go`). The `github.com/golang/geo` dependency was added as a direct require (used by `geodetic.go`). The package has no dependency on `pkg/sql`.

### Task A0 — Package skeleton ✅ DONE
- **Goal:** Create the `pkg/geo` package with the model types and constants. (The `golang/geo` dependency is **deferred to A6**, the first task that imports it, so this step stays `go mod tidy`-clean.)
- **Done:** `pkg/geo/consts.go` (subtype enum §1.2, SRID consts, `Subtype.String/Valid`, `SupportedSRID`) and `pkg/geo/geom.go` (`Geometry` interface; `Coord{X,Y float64}`; structs `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection`; each implements `Type() Subtype` and `Empty() bool`; `LineString.Closed`, compile-time interface assertions).
- **Test:** `pkg/geo/geom_test.go` — constructs each type, asserts `Type()`/`Empty()`/`Closed()`/`String()`/`Valid()`/`SupportedSRID`. `go build`/`go test ./pkg/geo/...` pass; `gofmt`/`go vet` clean.

### Task A1 — WKT parser
- **Goal:** `ParseWKT(s string) (Geometry, error)`.
- **Do:** `pkg/geo/wkt_parse.go`. Handle all 7 subtypes + `... EMPTY`, nested `GEOMETRYCOLLECTION`, whitespace/case tolerance. Reject malformed input with clear errors. Do **not** handle `Z`/`M` (per design).
- **Test:** table-driven cases lifted from MySQL 9.7 and PostGIS docs (valid + invalid). Assert structure for representative inputs.
- **Acceptance:** all sample WKT parse to the expected model; malformed inputs error.

### Task A2 — WKT writer
- **Goal:** `WriteWKT(g Geometry) string` producing MySQL-compatible WKT.
- **Do:** `pkg/geo/wkt_write.go`. Match MySQL's number formatting (no trailing `.0` noise; minimal round-trippable representation).
- **Test:** round-trip `ParseWKT(WriteWKT(g)) ≈ g` for the A1 corpus; golden-string checks against MySQL output for a few canonical geometries.
- **Acceptance:** round-trip stable; golden strings match.

### Task A3 — WKB reader/writer (float64, standard)
- **Goal:** `ReadWKB([]byte) (Geometry, error)` and `WriteWKB(g Geometry) []byte`, standard little-endian, type codes §1.2.
- **Do:** `pkg/geo/wkb_read.go`, `wkb_write.go`. Validate byte-order byte (accept big-endian on read, always emit little-endian), geometry-type code, counts.
- **Test:** cross-check against **known WKB hex** from PostGIS (`ST_AsBinary`) for canonical geometries; `WKT→WKB→WKT` and `WKB→WKT→WKB` round-trips over the A1 corpus.
- **Acceptance:** byte-exact match to PostGIS hex for canonical cases; round-trips stable.

### Task A4 — float32 WKB variant (for GEOMETRY32)
- **Goal:** a float32-coordinate WKB reader/writer; **no SRID/format wrapper** — the cell is bare WKB (§1.3).
- **Do:** `pkg/geo/wkb_f32.go`: `ReadWKBFloat32([]byte) (Geometry, error)`, `WriteWKBFloat32(g) []byte` (identical structure to A3, coords stored as float32). Add the float32→float64 up-convert helper used by `ST_AsWKB` on `GEOMETRY32` values. The caller (SQL layer) picks float32 vs float64 from the column OID; the engine just offers both encoders.
- **Test:** `WKB32→geom→WKB32` round-trip (assert float32 precision loss is the *only* difference vs float64); up-convert produces byte-standard float64 WKB.
- **Acceptance:** float32 WKB round-trips; up-convert yields standard WKB. (No SRID anywhere in the bytes.)

### Task A5 — Cartesian algorithm kernel (SRID 0)
- **Goal:** planar geometric primitives the functions need.
- **Do:** `pkg/geo/cartesian.go`: `Area` (shoelace, with holes), `Length`/`Perimeter`, `Distance` (point/segment/geometry min-distance), `Envelope` (bbox), `Centroid`, `IsClosed`, `IsRing`, `IsSimple`, `PointN`, plus the DE-9IM-style predicate helpers needed in Phase C (point-in-polygon, segment intersection).
- **Test:** unit tests with hand-computed expected values and PostGIS-sourced cases (Cartesian SRID 0).
- **Acceptance:** measures and predicates match expected to a defined epsilon.

### Task A6 — Geodetic algorithm kernel (SRID 4326) via S2
- **Goal:** spherical primitives returning **meters / m²**.
- **Do:** `pkg/geo/geodetic.go`: map our model to `s2` (`s2.LatLng`, `s2.Polyline`, `s2.Polygon`, `s2.Loop`); implement `DistanceMeters`, `LengthMeters`, `AreaSquareMeters`, and the spherical predicates (`Contains`, `Intersects`, …). Use a defined earth radius constant (document it).
- **Test:** known geodesic values (e.g. equator-degree ≈ 111 320 m; a known city-pair distance within tolerance); area of a 1°×1° cell vs published value.
- **Acceptance:** results within stated tolerance of reference values.

---

## Phase B — Type & DDL integration (design Step 1)

> Each Phase B task is shippable on its own with a BVT slice. Order matters: B1→B2→B3→B4 then the rest.

### Task B1 — Add `T_geometry32` to the type system
- **Goal:** `T_geometry32 = 68` fully plumbed, mirroring `T_geometry`.
- **Do:** in `pkg/container/types/types.go` add the const + entries in the `Types` map, `CharsetType` (binary), `ToType` (`Size = VarlenaSize`), `TypeLen`/`FixedLength` (varlen), `String()`→`"GEOMETRY32"`, `OidString()`, `DescString()`. Add `T_geometry32` to every varlen `case` group in `encoding.go` (lines ~383, ~556) and `vector.go` (the ~8 varlen switch groups).
- **Test:** extend `types_test.go` with a `TestT_Geometry32Varlen` mirroring `TestT_GeometryVarlen`; `go test ./pkg/container/...`.
- **Acceptance:** new type behaves as a varlen binary type identically to `T_geometry`.

### Task B2 — Migrate metadata from `Enumvalues` to `Scale`/`Width`
- **Goal:** subtype in `Scale`, SRID in `Width`; remove the `Enumvalues` string encoding for geometry.
- **Do:**
  - `build_util.go:226` (`getTypeFromAst`): set `typ.Scale = subtypeEnum`, `typ.Width = srid` instead of `typ.Enumvalues`.
  - `mysql_special_types.go`: replace `geometryMetadataString`/`decodeGeometryMetadata`/`geometrySubtypeName`/`geometrySRIDValue` to read/write `Scale`/`Width`. Keep the same public helper names so callers in `build_show_util.go`, `build_alter_table.go`, `build_alter_modify_column.go` need minimal edits.
  - `build_show_util.go:878`: render `GEOMETRY(Type='point', SRID=4326)` from `Scale`/`Width`.
- **Test:** unit tests in `mysql_special_types_test.go` + `build_show_util_test.go`; a binder test asserting `getTypeFromAst` yields the right `Scale`/`Width`.
- **Acceptance:** `SHOW CREATE TABLE` round-trips the declared subtype/SRID; no geometry path reads `Enumvalues`.

### Task B3 — Parser: argument forms + `GEOMETRY32`/aliases
- **Goal:** full DDL surface from the design.
- **Do:** in `mysql_sql.y`:
  - Extend `spatial_type` to accept `GEOMETRY '(' geometry_args ')'` for both `Type='point', SRID=4326` (named) and `'point', 4326` (positional).
  - Add tokens/keywords: `GEOMETRY32`, `GEOGRAPHY`, `GEOGRAPHY32`, and `POINT32 LINESTRING32 POLYGON32 MULTIPOINT32 MULTILINESTRING32 MULTIPOLYGON32 GEOMETRYCOLLECTION32` (register in `keywords.go`).
  - Map each alias to `(FamilyString subtype, srid default, float32 flag)`; extend `tree.GeoMetadata` with `Subtype string` and `Float32 bool`.
  - `build_util.go`: honor the float32 flag → `T_geometry32`; honor subtype/SRID into `Scale`/`Width`.
  - Regenerate the parser (`make` goyacc step) and commit `mysql_sql.go`.
- **Test:** parser unit tests for every alias and both argument forms; negative tests (bad SRID range, unknown subtype). `applyColumnAttributesToType` still rejects `SRID` on non-geometry columns.
- **Acceptance:** every alias + argument form parses to the correct `(OID, Scale, Width)`.

### Task B4 — Switch the storage payload to bare WKB
- **Goal:** the varlena holds **bare WKB** (§1.3) — no SRID, no format byte — instead of WKT text.
- **Do:** replace the bodies of `encodeGeometryPayload`/`decodeGeometryPayload` (`func_unary.go:812-874`) with thin wrappers over `geo.WriteWKB`/`geo.ReadWKB` (float32 variant when the column OID is `T_geometry32`). Update the cast path (`func_cast.go:5466`) and bind path (`func_mo.go:1205`): text→geometry = `ParseWKT` → `WriteWKB`; `ST_AsText` = `ReadWKB` → `WriteWKT`. **SRID does not enter the bytes** — it stays in the column type. Drop the `"SRID=N;"` text-prefix logic entirely.
- **Test:** BVT smoke slice — `CREATE TABLE t(g GEOMETRY)`, insert via `ST_GeomFromText`, `SELECT ST_AsText(g)` round-trips; `LENGTH(g)` equals the WKB byte length (e.g. 21 for a 2D point), confirming no SRID/text overhead.
- **Acceptance:** stored bytes are exactly WKB (byte-checkable vs PostGIS `ST_AsBinary`); text round-trips; existing `func_geometry.test` still passes for the I/O cases (others migrate in Phase C).

### Task B5 — I/O constructor & SRID functions on WKB
- **Goal:** the core I/O group works on bare WKB: `ST_GeomFromText`(+SRID), `ST_GeomFromWKB`/`ST_GeomFromBinary`, `ST_AsText`/`ST_AsWKT`, `ST_AsWKB`/`ST_AsBinary`, `ST_SRID` (get + set form), `ST_GeometryType`.
- **Do:** implement in `func_unary.go`/`func_binary.go` delegating to `pkg/geo`. Add missing function IDs (`ST_ASWKB`, `ST_GEOMFROMWKB`, …) to `function_id.go` and `list_builtIn.go` overloads. SRID handling (cell is bare WKB):
  - `ST_SRID(g)` reads `ivecs[0].GetType().Width` — **not** the bytes.
  - `ST_GeomFromText(wkt, srid)` / `ST_SRID(g, srid)` (setter) produce a geometry whose SRID is recorded in the **result vector type `Width`** via `retType`; the constant-SRID rule from §1.5 applies (literal SRID required for geometry-producing forms).
  - `ST_AsWKB` returns the payload verbatim for `T_geometry`, up-converted for `T_geometry32`.
- **Test:** BVT — `func_geometry_io.test` covering WKT↔WKB↔geometry round-trips, `ST_SRID` reflecting the column SRID, set-SRID, type name.
- **Acceptance:** all I/O functions correct for all 7 subtypes incl. `EMPTY`; SRID always sourced from/written to the type.

### Task B6 — Subtype/SRID enforcement at insert/update (port #24353 to Scale/Width)
- **Goal:** inserting a value whose subtype/SRID is incompatible with the column is rejected at bind time.
- **Do:** re-implement the `geometrySubtypeCompatible` / SRID checks against `Scale`/`Width` in the insert/update bind path (`bind_insert.go`, `funcCastForGeometryType`).
- **Test:** BVT — inserting `POINT` into a `POLYGON` column errors; SRID mismatch errors; `GENERIC` column accepts any subtype.
- **Acceptance:** matches MySQL behavior for subtype/SRID column constraints.

### Task B7 — `GEOMETRY32` end-to-end
- **Goal:** float32 columns work for DDL/insert/select and convert to/from `GEOMETRY`.
- **Do:** ensure the float32-WKB encoder/decoder (A4) is selected by OID through B4/B5; add casts `T_geometry ↔ T_geometry32` (float64↔float32 re-encode of the WKB) in `func_cast.go` cast-rule table + implementation. SRID (Width) is preserved by the cast's `retType`.
- **Test:** BVT — `func_geometry32.test`: create `POINT32`, insert, `ST_AsText`, cast to `GEOMETRY` and back; assert float32 precision behavior.
- **Acceptance:** float32 path round-trips; cross-casts work.

### Task B8 — LOAD DATA + frontend output
- **Goal:** bulk load of WKT data and correct client-facing output/metadata.
- **Do:** verify `LOAD DATA` into a geometry column (text→geometry cast path); confirm `mo_columns` shows `GEOMETRY(...)` and result-set metadata/charset is correct in `pkg/.../output.go`. Add `test/distributed/resources/geometry/` sample files.
- **Test:** BVT — `dtype/geometry.test` modeled on `dtype/json.test`: create/insert/update/delete/load/select.
- **Acceptance:** end-to-end DDL+DML+LOAD works; metadata correct. **Design Step 1 complete.**

---

## Phase C — Spatial functions (design Step 2)

Every Phase C task does **three** things for its function group (see §1.5):
1. Re-point the existing `ST_*` functions at `pkg/geo` (operate on parsed WKB, not WKT strings).
2. Add **SRID dispatch**: `effSRID==0` → `cartesian.go`; `effSRID==4326` → `geodetic.go` (meters/m²); other → error.
3. Add the **`+SRID` override overload** (`[..., int64]`) on the same function ID.

Each task ships BVT cases drawn from MySQL 9.7 and PostGIS covering **both** coordinate systems and the `+SRID` form, and must keep `func_geometry.test` green for the Cartesian results.

### Task C0 — Dispatch + overload infrastructure
- **Goal:** the shared machinery every other C task depends on.
- **Do:** `pkg/sql/plan/function/geometry_dispatch.go` — `effectiveSRID(...)` (unary/binary), `validateComputeSRID(srid)` (0 or 4326), and `dispatchSpatial(...)` helpers wiring a `(cartesianFn, geodeticFn)` pair. Establish the registration convention for adding the `+SRID` overload to a function ID's `Overloads` slice in `list_builtIn.go` (one helper/macro so C1–C9 stay mechanical).
- **Test:** unit tests for `effectiveSRID` (type `Width` vs explicit override vs binary type mismatch) and `validateComputeSRID` (rejects e.g. 3857).
- **Acceptance:** a single pilot function (`ST_Area`) wired through C0 returns Cartesian for SRID 0, geodetic m² for SRID 4326, and honors `ST_Area(g, 4326)`. Sets the template for the rest.

### Task C1 — Geometry property/accessor functions
`ST_Dimension`, `ST_Envelope`, `ST_GeometryType`, `ST_IsEmpty`, `ST_IsSimple`, `ST_SRID`, `ST_Boundary`.
- **Test:** BVT `func_geometry_props.test`.

### Task C2 — Point accessors
`ST_X`, `ST_Y` (get + set forms), `ST_Latitude`, `ST_Longitude`.
- **Test:** BVT `func_geometry_point.test`; error on non-point input.

### Task C3 — LineString functions
`ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`, `ST_IsClosed`, `ST_IsRing`, `ST_Length` (Cartesian + geodetic meters).
- **Test:** BVT `func_geometry_line.test`.

### Task C4 — Polygon / surface functions
`ST_Area` (Cartesian + geodetic m²), `ST_Centroid`, `ST_ExteriorRing`, `ST_InteriorRingN`, `ST_NumInteriorRings`, `ST_PointOnSurface`.
- **Test:** BVT `func_geometry_polygon.test`.

### Task C5 — Collection functions
`ST_NumGeometries`, `ST_GeometryN`, `ST_IsCollection`.
- **Test:** BVT `func_geometry_collection.test`.

### Task C6 — Spatial relationship predicates
`ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, `ST_Equals`, `ST_Covers`, `ST_CoveredBy`. Cartesian via the A5 predicate helpers; geodetic via S2 (A6).
- **Test:** BVT `func_geometry_relate.test`; require both operands share SRID (error otherwise, matching MySQL).

### Task C7 — Distance & measurement
`ST_Distance` (meters for 4326), `ST_Distance_Sphere`, plus geodetic `ST_Length`/`ST_Area` validation passes.
- **Test:** BVT `func_geometry_distance.test` with known geodesic distances.

### Task C8 — Format conversion functions
`ST_AsGeoJSON`, `ST_GeomFromGeoJSON`, `ST_AsWKB`/`ST_GeomFromWKB` (finalize), `ST_AsText`/`ST_GeomFromText` (finalize), `ST_GeoHash`/`ST_PointFromGeoHash` *(optional)*.
- **Test:** BVT `func_geometry_format.test`.

### Task C9 — Validity & simplification
`ST_IsValid`, `ST_Validate`, `ST_Simplify` *(if in scope)*.
- **Test:** BVT `func_geometry_valid.test`.

### Task C10 *(stretch / optional)* — Constructive operators
`ST_Buffer`, `ST_ConvexHull`, `ST_Intersection`, `ST_Union`, `ST_Difference`, `ST_SymDifference`. These need a planar overlay/boolean-ops capability beyond S2's spherical ops — scope a dedicated planar geometry-ops approach (own implementation or an additional vetted library) before committing. **Flagged as not required for the design's Step 2 core; revisit after C1–C9.**

### Task C11 — Consolidation & full regression
- **Goal:** confirm no algorithm logic remains in `func_unary.go`/`func_binary.go` (all delegate to `pkg/geo`); the legacy text-based helpers are deleted.
- **Test:** full `func_geometry.test` green; new per-group BVT files green; spot-check a sample of cases against MySQL 9.7 / PostGIS output.
- **Acceptance:** **Design Step 2 complete.**

---

## 3. Dependency graph (build order)

```
A0 ─▶ A1 ─▶ A2
        └─▶ A3 ─▶ A4
A0 ─────────────▶ A5 (needs A1 model)
A0 ─────────────▶ A6 (needs A1 model + golang/geo)

A(all) ─▶ B1 ─▶ B2 ─▶ B3 ─▶ B4 ─▶ B5 ─▶ {B6, B7, B8}
A + B ─▶ C0 ─▶ C1 … C9  (each independent of the others) ─▶ C11
                              C10 optional, after C1–C9
```

- **Phase A** can be developed and merged with zero user-visible change.
- **B4** is the cut-over point where storage becomes WKB; everything before it leaves current behavior intact.
- Phase C tasks are mutually independent — parallelizable once B is done.

---

## 4. Risks & notes

- **Parser regeneration:** B3 regenerates `mysql_sql.go` from the `.y`. Large generated diff; keep it isolated in its own commit. Watch for grammar conflicts when adding the `GEOMETRY '(' … ')'` rule (it may shift/reduce against existing `spatial_type`).
- **WKB byte-compatibility:** validate A3 against real PostGIS `ST_AsBinary` hex, not just self-round-trips, to guarantee interoperability.
- **Geodetic accuracy & datatype:** S2 is spherical, not ellipsoidal; document the earth-radius constant and expected tolerance vs. MySQL (which uses an ellipsoid for some functions). Some BVT expected values may need tolerance-based comparison (`-- @ignore` / regex) rather than exact match.
- **`GEOMETRY32` is a MatrixOne extension** (non-standard float32 WKB). `ST_AsWKB` always emits standard float64 WKB to stay interoperable.
- **`Scale`/`Width` migration (B2)** touches type serialization used across plan caching and `mo_columns`. Add tests that a geometry `types.Type` survives marshal/unmarshal with `Scale`/`Width` intact.
- **Regression safety:** `func_geometry.test` (92 KB) is the baseline. Keep it passing at every Phase B/C step; only its *internal representation expectations* (e.g. `LENGTH(g)` for WKB vs WKT) should change, and only at B4.

---

## 5. Suggested commit/PR breakdown

One PR per task keeps reviews small and each step independently testable:

1. PR-A0…A6 — `pkg/geo` engine (6 PRs, pure unit tests).
2. PR-B1 — `T_geometry32` type plumbing.
3. PR-B2 — metadata → Scale/Width.
4. PR-B3 — parser aliases + argument forms.
5. PR-B4 — WKB payload cut-over.
6. PR-B5…B8 — I/O functions, enforcement, GEOMETRY32 e2e, LOAD/output. *(Design Step 1 ships here.)*
7. PR-C0 — dispatch + `+SRID` overload infrastructure (with `ST_Area` pilot).
8. PR-C1…C9 — spatial function groups (one PR each), each adding Cartesian+geodetic+`+SRID`.
9. PR-C11 — consolidation + full regression. *(Design Step 2 ships here.)*
10. PR-C10 — optional constructive operators (separate track).
