# CRC32(JSON) compatibility repair

Newly bound `CRC32(JSON)` hashes the JSON value's visible SQL text. Existing
persisted expressions must keep the algorithm under which their materialized
values and indexes were created.

## Execution and persistence

Both paths use the existing CRC32 overload 0. A previously bound expression
with a JSON argument retains its binary-JSON input. A newly bound JSON argument
is explicitly converted to text before the existing CRC32 function runs. Text,
binary, numeric and NULL inputs retain their respective conversion behavior. No
new CRC32 overload or query protocol field is required.

The same JSON value can consequently have a legacy generated value different
from a freshly bound `CRC32(j)`. INSERT, UPDATE, REPLACE, ODKU, prepared
rebinding and index maintenance must preserve the persisted expression's
original input. No automatic row or index migration is performed.

Internal ALTER COPY and catalog LIKE/CLONE carry detached bound expressions;
display SQL alone cannot reconstruct the legacy identity. Old folded defaults
without a source expression retain their stored value. New JSON-to-text defaults
retain their source expression under the existing folding rules. CHANGE/MODIFY
keeps an unchanged legacy generated, DEFAULT, ON UPDATE or CHECK expression
only if its referenced inputs remain compatible; unsafe conversion is rejected
and requires a rebuild. The legacy DML fallback refuses legacy generated
expressions it cannot maintain.

Views rebound from SQL use the new binding. CTAS result columns retain their
materialized values. A physical backup and restore retains catalog identities;
SQL dump/replay creates new DDL and is not a lossless preservation mechanism
for an old generated expression's algorithm.

## Upgrade and existing protocol contracts

The repository supports shutdown upgrades: stop all services, replace the
binaries, and start the cluster against the existing data. The new binary must
read supported-release catalog expressions and preserve their legacy identity,
including after restart and physical restore. Rolling mixed-version operation
and downgrade are not claimed. Experimental data written by unmerged binaries
that changed overload 0 in place cannot be distinguished by that identity and
is not a supported upgrade source.

A newly bound CRC32(JSON) inherits the existing v129 JSON-to-text expression
admission, just like `CRC32(CAST(j AS CHAR))`. A legacy binary-JSON CRC32 keeps
its existing v80 UINT64 result contract. These are existing expression checks,
not a new CRC32-specific version, signature, or query protocol field.

## Prepared connection migration

Ordinary eligible prepared statements can migrate between CNs on the same
binary without CRC-specific admission or direct-table proof fields. Existing
authorization and generic session-snapshot checks continue to apply. A
preflight refusal leaves the target untouched; after replay starts, failure or
panic disposes the target after operation admission is released, leaving the
source available for an independently authorized retry. For typed system
snapshots, CN-wide runtime-filter settings are applied only after the fallible
replay steps succeed. The legacy raw-SET replay path is unchanged.

## Validation obligations

Binder and execution regressions must exercise both the legacy JSON input and
the current CAST-to-text expression with independent checksums. Catalog tests
cover generated expressions, defaults, ON UPDATE, CHECK, DML, COPY/LIKE/CLONE
and prepared rebinding. The CRC32 SQL BVT covers scalar, generated-column, DML
and index behavior. Existing protocol checks must remain effective for the
actual serialized expressions, including when other features require a higher
version.

A supported-release-created physical table must also be exercised through the
candidate shutdown upgrade, subsequent DML, restart and physical restore. Local
same-binary tests and historical receipts do not establish that acceptance for
an edited candidate. Real-version results and reviewer-specific requirements
are recorded as task evidence; mixed-version rejoin and downgrade are not
claimed as passes.
