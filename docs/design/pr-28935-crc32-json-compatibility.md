# CRC32(JSON) compatibility repair

This focused repair of PR #28935 retains the public SQL function and adds a
stable execution identity instead of migrating stored values. Compatibility
covers main and supported releases, with no migration of unmerged experimental
PR data.

## Contract

| Identity | Binding and execution |
| --- | --- |
| CRC32 overload 0 | Existing catalog/wire expressions hash binary JSON; ordinary non-JSON calls retain their original bytes and scalar casts. |
| CRC32 overload 1 | New JSON or unresolved bindings hash normalized MarshalJSON bytes for JSON; requires MORPC v94. |

The result stays UINT64; the executor also retains the historic UINT32 wrapper.
A stored legacy generated value can intentionally differ from a freshly bound
`CRC32(j)` query. Existing expressions are never silently upgraded, including
INSERT, UPDATE, REPLACE, ODKU, prepared rebinding and index maintenance.

## Boundaries

Placement checks workers and falls back to one CN. Sending rechecks the actual
worker at v94; receiving checks the new identity. Old coordinators send identity
0, which new workers still implement. Unknown capabilities fail closed.

Catalog read and authoring use the existing separate durable admission floors.
Internal ALTER COPY and catalog LIKE/CLONE must carry bound expressions because
regenerated SQL loses their identity. Copies detach expression trees from source
metadata. Old folded defaults without a source expression retain their stored
value; newly folded CRC32 defaults retain their source identity for admission.
CHANGE/MODIFY of a legacy generated column preserves its identity for
an unchanged expression/type; semantic conversion is rejected and requires an
explicit table rebuild. No automatic data or index migration is introduced.

Views are rebound SQL and acquire the new semantics and admission requirement.
Ordinary CTAS result columns remain historical values. Physical restore retaining
catalog identities preserves the old semantics. SQL dump/replay is new DDL and
is not a lossless way to preserve a legacy generated expression's algorithm.

## Upgrade and rollback

Supported sources are main v93 and supported released versions, not earlier
unmerged v86/v94 experiment binaries that changed overload 0 in place. Such data
cannot be distinguished by identity and must not be admitted as a supported
upgrade source. The complete main v93 implementation is present in the base.

Publishing new persisted expressions requires the durable v94 authoring barrier.
Once the durable floor is raised, old CNs must stay excluded, including after
restart. A failed activation does not imply the floor can be lowered. Rollback
must respect the existing admission mechanism; this change adds no floor reset.

## Validation obligations

Executor and binding tests distinguish both identities with fixed independent
checksum oracles. Codec/feature tests cover unresolved types and folded source
expressions. Sender tests isolate a new coordinator and old target. Catalog
fixtures must preserve identity through DML, COPY, LIKE and prepared rebinding.
The function BVT covers generated columns, indexes, DML, prepared reads and ALTER.
Real two-binary rolling upgrade, persistent old-table DML after restart and
post-floor downgrade rejection remain separate environment acceptance tests;
mock version values and same-version CI do not prove them.

## Validation on 2026-09-28

- PASS: complete function, plan codec, planner, compiler and executor utility
  package tests (6,528 top-level tests). Compiler tests use GOMAXPROCS=4;
  existing Parquet fanout tests require more than two worker slots.
- PASS: embedded single-CN SQL and binary PREPARE, parameter reuse, schema-change
  reprepare, generated-column DML, ordinary/unique indexes and ALTER. Persisted
  DDL waits for the actual HAKeeper-driven authoring floor, without overriding
  runtime protocol values.
- PASS: normal mo-tester comparison using runner commit
  `309a0ca650e0c55e90292767fd61e4976818d66d`: `func_crc32.sql` (115 statements)
  and `generated_column.sql` (334 statements), each twice on the same isolated
  single-CN instance. Input staging was byte-identical; ISCP readiness and
  explicit database teardown were checked. No statements were ignored.
- NOT_RUN: real v93/new mixed binaries, old-version persisted-table upgrade and
  restart, physical restore, and post-activation downgrade rejection. Component
  tests and same-version SQL execution do not replace those acceptance tests.
