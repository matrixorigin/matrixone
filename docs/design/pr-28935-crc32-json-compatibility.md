# CRC32(JSON) compatibility repair

CRC32 of a newly bound JSON expression hashes the normalized JSON text. Existing
persisted expressions must keep the algorithm under which their materialized
values and indexes were created.

## Execution and persistence

| Bound identity | Behavior |
| --- | --- |
| CRC32 overload 0 | Legacy JSON expressions hash binary JSON; non-JSON calls retain their existing byte and scalar-cast behavior. |
| CRC32 overload 1 | New JSON or unresolved bindings hash normalized `MarshalJSON` bytes for JSON. |

The result remains UINT64, including the historic UINT32 wrapper where needed.
The same JSON value can consequently have a legacy generated value different
from a freshly bound `CRC32(j)`. This is intentional: INSERT, UPDATE, REPLACE,
ODKU, prepared rebinding and index maintenance must retain the persisted
expression's original identity. No automatic row or index migration is performed.

Internal ALTER COPY and catalog LIKE/CLONE carry detached bound expressions;
display SQL alone cannot reconstruct the legacy identity. Old folded defaults
without a source expression retain their stored value. Newly folded CRC32
JSON defaults retain their source expression so later catalog operations can
preserve the bound identity. CHANGE/MODIFY keeps an unchanged legacy generated,
DEFAULT, ON UPDATE or CHECK expression only if its referenced inputs remain
compatible; unsafe semantic conversion is rejected and requires a rebuild.
The legacy DML fallback refuses legacy generated expressions it cannot maintain.

Views rebound from SQL use the new binding. CTAS result columns retain their
materialized values. A physical backup and restore retains catalog identities;
SQL dump/replay creates new DDL and is not a lossless preservation mechanism
for an old generated expression's algorithm.

## Upgrade contract

The repository supports shutdown upgrades: stop all services, replace the
binaries, and start the cluster against the existing data. The new binary must
read supported-release catalog expressions and preserve their legacy identity,
including after restart and physical restore. Rolling mixed-version operation
and downgrade are not supported. Experimental data written by unmerged binaries
that changed overload 0 in place cannot be distinguished by that identity and
is not a supported upgrade source.

Neither the CRC32 execution identity nor bounded prepared migration changes RPC
framing, handshake or envelope. Neither adds a MORPC version or a per-feature
peer, placement, send, receive, or persisted-expression capability gate. Existing
unrelated gates, including the UINT64 string-numeric result contract, remain.

## Prepared connection migration

Connection migration replays PREPARE SQL rather than transferring its bound
expression. The source and destination therefore reject statements when replay
cannot prove the CRC32 identity safe. Closed catalog-free scalar forms are
admitted only when the classifier can establish replay equivalence; unknown
forms, views, UDFs and uncertain CRC32 arguments remain pinned. Rejection must
leave the source handle usable, and destination preflight must run before user
state replay.

The separate [bounded direct-table migration design](pr-28935-direct-table-migration.md)
admits a small independently witnessed direct INT32 table SELECT domain. Its
catalog, authorization, generation, typed-state and disposable-target checks
remain necessary independently of mixed-version support.

## Validation obligations

Executor and binder regressions must distinguish both overloads with independent
checksums. Catalog tests cover generated expressions, defaults, ON UPDATE, CHECK,
DML, COPY/LIKE/CLONE and prepared rebinding. The CRC32 SQL BVT covers public
scalar, generated-column, DML and index behavior. Existing unrelated protocol
controls must remain effective when combined with either CRC32 identity.

A supported-release-created physical table must also be exercised through the
candidate shutdown upgrade, subsequent DML, restart and physical restore. Local
same-binary tests and historical receipts do not establish that acceptance for
an edited candidate. Real-version results and reviewer-specific requirements
are recorded as task evidence; mixed-version rejoin and downgrade are outside
the supported contract and are not claimed as passes.
