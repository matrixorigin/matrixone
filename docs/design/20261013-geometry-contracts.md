# Geometry contract corrections

Issues: #30032, #30033, #30034, #30046. Base: `3e50e1b32d8da61bbbf70c89989fb08b671809de`.

Design D1 passed before implementation, using `gpt-6.1-sol` with `xhigh`
reasoning, session `01a132fa-3f88-7490-a2c1-4e48dd9b1470`.
The changes correct existing geometry contracts across the kernel, SQL admission
and the MySQL boundary. They introduce no storage or catalog format changes.

## Evidence and ownership

Current-base public SQL reproduces dropped shared-edge intersections, invalid
repeated-vertex lines, admitted unclosed rings, and ASCII WKT under MySQL type
255. Ordinary lines and positive-area intersections pass as controls.

| Contract | Owner | Correction |
| --- | --- | --- |
| Complete polygon intersection | Geo sweep and connector | Preserve exposed lines and points from the existing noded arrangement |
| Line validity | SQL validity helper | Require two exactly distinct positions; adjacent repeats are allowed |
| Ring structure | Geo structural helper, parser and SQL admission | At least four positions including the exactly equal closing position |
| Standard WKB | Typed SQL payload conversion | Select coordinate precision by OID and avoid WKT round trips |
| MySQL geometry | Frontend metadata and row writers | Four-byte little-endian SRID followed by standard float64 WKB |

## Intersection

The existing sweep runs once. Its canonical noded edges include operand identity
and overlap transitions. Opposite-transition overlaps become contact lines.
The connector shares one endpoint index for area boundaries and contact membership;
noncontributing overlap partners still supply operand membership at their vertices.
A contact vertex covered by a selected area boundary or contact line is suppressed.
This coverage proof applies to valid polygon/multipolygon operands, not an expansion
to arbitrary overlapping or heterogeneous geometry collections.

Contact segments are deduplicated and walked once, joining through degree-two
vertices and stopping at branches. Remaining cycles use a bounded walk. Lines,
points and intersection polygon components are ordered deterministically. A
single dimension uses the corresponding primitive/multi; mixed dimensions use
a geometry collection. Disjoint and empty intersections remain `POLYGON EMPTY`.
Union, difference and XOR preserve their selection and serialization.

The extra intersection work is O(E log E) time and O(E) space for E noded edges,
without a second O(N*M) pairwise intersection pass. Existing ring nesting costs
are unchanged. The existing geodetic projection, snapping grids, bounded recovery
and recursive unprojection also handle the lower-dimensional result.

## Admission and compatibility

Ring admission checks exact equality, not spatial predicate tolerance. It does
not auto-close, snap, deduplicate, enforce winding or check topology. Closed
self-intersecting rings still construct and can be tested with `ST_IsValid`.
Whole empty polygons and empty polygon members remain supported; actual empty
rings are rejected. WKT, GeoJSON and SQL WKB/cast admission share the same rule.
Finite-coordinate, point-limit, SRID, subtype and selection-mask owners remain.

WKB readers remain codecs: historical malformed stored WKB stays inspectable
through text and export. Re-admission rejects its malformed structure. Standard
WKB export decodes directly, avoiding accidental application of new WKT admission
rules to stored WKB. Legacy textual payloads and materialized cursor WKT use
the same parser through an explicit serialization-only entry point: grammar and
nesting limits remain enforced, while historical ring coordinates/counts are
preserved without admission checks. Constructors retain strict parsing. No
automatic repair or migration is performed.

## MySQL output

[MySQL](https://dev.mysql.com/doc/refman/8.0/en/gis-data-formats.html) uses a
four-byte SRID prefix and standard double-precision WKB. Internal geometry stays
bare WKB, with SRID in type Width and float32 precision selected by GEOMETRY32.
The typed converter widens GEOMETRY32, including every collection member.

Both fast row writers and both materialized row writers use one frontend encoder.
MysqlColumn retains the immutable source geometry type so materialized WKT,
including prepared cursor results, retains the correct SRID. Missing source type
is an error. Metadata stays type 255, with binary charset 63 and the binary flag.
NULL and length encoding continue to use their existing protocol owners.

Internal row extraction, user variables, background results, CSV/data-branch
exports and cursor memory reservation continue to use WKT. Direct SQL geometry
values intentionally change to compatible binary output; use `ST_AsText` for
readable text. Rollback needs no data rewrite but restores the former wire defect.

Rejected alternatives: only filling contacts when area is empty (loses mixed
results), a second boundary scan (duplicates the kernel and cost), globally
changing WKT extraction (breaks internal consumers), and globally strict WKB
readers (unnecessary historical-data incompatibility).

## Acceptance

Kernel tests cover edge/corner/partial/vertex-edge contacts, holes, mixed dimensions,
covered contacts, operand reversal and nearby disjoint inputs. An independent GEOS
oracle challenges geometry set equality. Parser tests distinguish structural
errors, canonical empties and topology. Function tests retain tiny nonzero lines,
NULL handling, declared precision and existing simplicity behavior.

Protocol tests assert actual bytes through four writer variants, metadata,
ownership, invalid payloads and preserved internal text. Public integration reuses
the canonical SQL fixture for literal/stored/GEOMETRY32 results, rejected batch
rollback, text and binary prepared queries, constant prepared SRIDs and recovery. Dynamic SRID arguments are already rejected
by the binder, including PREPARE; the test asserts that boundary rather than
introducing a second SRID authority.
Existing geometry BVT uses explicit text projection for readable oracles.
Final acceptance also requires owner normal/race validation, incremental static
checks and the independent overall review. Benchmarks compare existing overlay
and intersection controls; no wall-clock assertion enters functional tests.
