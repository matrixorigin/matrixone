- Status: draft
- Start Date: 2026-08-26
- Authors: MatrixOne maintainers
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/27100
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/27036

# `PAD_CHAR_TO_FULL_LENGTH` CHAR retrieval and PAD SPACE semantic keys

## Summary

Implement `PAD_CHAR_TO_FULL_LENGTH` as a session `sql_mode` option. When it is
enabled, reading a `CHAR(N)` value returns its storage-width representation,
including trailing padding. The option changes representation only. It must not
change PAD SPACE equality, ordering, joins, `IN`, DISTINCT/GROUP BY, set
membership, or value-producing expression semantics.

## Motivation

ODBC and other fixed-width clients use the returned byte length of `CHAR`
values. Returning `"MO"` for `CHAR(8)` prevents those clients from preserving a
fixed-width record. MatrixOne already accepts the mode, so silently ignoring it
is worse than rejecting it: clients receive a success response and incompatible
data.

The implementation must preserve the pre-mode query result set. In particular,
the visible value `"MO      "` and an otherwise equivalent `"MO"` remain one
logical PAD SPACE value even after an implicit promotion to `VARCHAR` or
`TEXT`.

## Technical Design

### Session and scan contract

`process.ResolvePadCharToFullLength` reads the session SQL mode on each relevant
execution path. Table scans pad loaded `CHAR` vectors to their declared display
width only when the mode is on; unloaded vectors, NULLs, and non-CHAR vectors
are unchanged. The mode is therefore session-scoped, including prepared and
remote execution, rather than a catalog or stored-data migration.

Explicit `CAST(CHAR AS VARCHAR)` preserves the visible padded bytes under the
mode. Comparison-only casts are distinct overloads: they normalize a derived
key without rewriting the value consumed by a projection, `LENGTH`, `HEX`, or
an explicit cast.

### Semantic key provenance

Logical PAD SPACE equality is independent of the physical representation. The
planner marks a string expression with `Type.PadSpace` only when its value can
originate from a `CHAR` input and it crosses a value-preserving boundary. This
marker is carried through projections, aliases, CTEs, value-returning
aggregates, value-returning window functions, conditionals, and the returned
arguments of `LEAST` and `GREATEST`.

At equality consumers, the planner produces an additional physical key that
trims trailing ASCII spaces. The visible projected expression remains unchanged.
Consumers use the additional key for comparisons, hash joins, `IN`/`NOT IN`,
DISTINCT/GROUP BY, distinct aggregates, window partition/peer keys, and
UNION/INTERSECT/MINUS. A value that has no CHAR provenance never receives this
key, avoiding changes to ordinary VARCHAR/TEXT behavior.

### Remote protocol compatibility

The PAD SPACE casts and set-operation key expressions are serialized in a
pipeline. They require `MORPCVersion40`. The actual remote sender path
(`prepareRemoteRunSendingData` through `encodeRemoteScope`) rejects a remote
pipeline that needs these semantics when its service protocol version is below
40, before marshal or dispatch. Ordinary pipelines and sessions with the mode
disabled remain compatible with
older peers. This is fail-closed: a mixed-version deployment never executes the
new plan with an old peer that could interpret its physical bytes differently.

The wire schema additions are optional fields. New readers accept an absent
field as the legacy behavior. Rollback is safe after mode-dependent requests
drain, because neither table data nor catalog metadata is persisted by this
feature.

### Ownership, bounds, and failure behavior

The scan owns the temporary padded vector representation for its existing batch
lifetime. Semantic-key expressions are ordinary planner/operator expressions;
they follow existing batch reset and operator cleanup paths and introduce no
background work, retries, queues, or persistent cache. Each key normalization
is linear in the selected string length and is only evaluated by the existing
equality consumer. A remote compatibility failure is returned before dispatch,
with a deterministic `NotSupported` error.

## Validation

The implementation has paired internal and public evidence:

| Invariant | Internal proof | Public proof |
| --- | --- | --- |
| Scan representation changes only in the enabled session | table-scan/process tests | CHAR retrieval, LENGTH, HEX, and prepared-session SQL cases |
| PAD SPACE results are representation independent | function/planner/hash-map tests | WHERE, joins, IN, DISTINCT/GROUP BY, UNION/INTERSECT/MINUS, windows, aggregates, LEAST/GREATEST cases with mode off/on |
| Explicit casts retain observable bytes | cast tests | CAST/LENGTH/HEX SQL cases |
| Remote peers cannot misinterpret the plan | pipeline encode/decode protocol tests | normal distributed CI coverage |

All public SQL regression statements use a minimal number of rows and restore
the session mode. No timing, retry, or ambient test-order dependency is part of
the contract.

## Drawbacks

Enabled sessions may return longer client values and incur bounded padding/key
normalization work. That is the requested compatibility behavior. The
provenance marker and protocol gate add planner and compatibility complexity
that would not be needed if the mode remained unsupported.

## Rationale and Alternatives

1. **Pad only at final client formatting.** This preserves internal comparisons
   but loses the required observable behavior for SQL expressions such as
   `LENGTH`, `HEX`, and explicit casts.
2. **Globally trim all VARCHAR/TEXT comparisons.** This would alter existing
   non-CHAR semantics and could collapse values that have no fixed-width
   provenance.
3. **Use a semantic key only for CHAR-typed vectors.** Promotions and derived
   projections lose the physical type, leaving reachable wrong-result paths.

The selected design exposes padded values at the scan boundary while carrying
only the necessary CHAR provenance to equality consumers. It preserves legacy
behavior for mode-off sessions and for strings with no CHAR origin.

## Open Questions

No implementation decision is open. This RFC is draft pending independent
design review; that review does not change the already specified compatibility
contract or prevent delivery of the validated implementation.
