# CRC32(JSON) compatibility

New SQL binds `CRC32(j)` for JSON `j` as the existing CRC32 byte function over
an explicit JSON-to-TEXT expression. Explicit conversion preserves the quotes
and escapes of a JSON string; the TEXT expression does not impose a VARCHAR
width on the JSON document. SQL NULL remains NULL, while JSON `null` is text.
Other input domains continue to use their existing CRC32 behavior.

A supported-release catalog expression that already calls CRC32 on binary JSON
retains its original function and argument. Its materialized values and index
entries are not rewritten. Thus an old generated column can differ from a new
query's `CRC32(j)` for the same row; later supported writes must continue to
use the stored expression. Parameter rebinding must preserve the old argument,
while a newly bound untyped parameter specializes to its actual input domain.

Internal ALTER COPY and LIKE carry the old bound expressions when replaying
unchanged definitions. Referenced column types and positions must remain valid;
a changed expression or incompatible input is not an implicit lossless change.
Existing folded defaults without source expressions keep their stored values.
The existing JSON text feature guard keeps new CRC32 JSON default expressions
visible through ordinary constant folding and catalog serialization.

The legacy DML planner does not calculate generated columns. The current
candidate retains its rejection for a legacy CRC32 generated expression on
that fallback path; this is an unresolved availability gap, not evidence that
all old-table writes are supported.

## Approaches considered

| Approach | Benefit | Evidence or cost still required |
| --- | --- | --- |
| Existing CRC32 with a newly bound JSON-to-TEXT expression | No new CRC32 signature; matches the SQL conversion contract | Rebinding, folding and DDL must retain old expression behavior; the candidate remains under validation. |
| Separate new and old CRC32 execution identities | Directly distinguishes the two algorithms | Adds a durable function identity and its compatibility maintenance; the reviewer requested avoiding this extra signature. |
| Apply the new algorithm to every expression and rebuild old data | One final semantic value | Must recompute rows, indexes and constraints with recoverable failure handling; this changes the selected old-data contract. |

## Connection migration and protocol

Ordinary prepared-statement migration uses the existing SQL replay,
authorization, session-state and target-failure lifecycle. It adds no CRC32
proof messages, connection-migration preflight or CRC32-specific RPC version.
Existing protocol checks remain: in particular, the existing JSON-to-text CAST
contract can require version 129, and the CRC32 UINT64 result contract can
require version 80. The stopped-upgrade contract does not promise mixed-version
CN operation or downgrade.

## Validation required

Compare fixed JSON text bytes against an independent CRC32 oracle, including
quotes, escapes, long and multibyte strings, SQL NULL and JSON null. Exercise
prepared binding and rebind, selection and executor reuse, non-JSON domains,
legacy and new generated values, indexes, ALTER/LIKE, defaults and checks, and
ordinary and CRC32 prepared connection migration. Validate actual supported
release-created rows and indexes through shutdown upgrade, subsequent writes,
restart and restoration of a backup made by the old release.

The earlier v4.2.5-to-`533cf11915cf` upgrade and same-head CI evidence applies
only to that published candidate. This edited candidate requires its own
validation, and its legacy fallback gap remains open.
