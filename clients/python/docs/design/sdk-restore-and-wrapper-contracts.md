# SDK restore and wrapper contract corrections

The snapshot restore grammar uses `{snapshot='name'}` on the target scope. A
restore is owned by the server and is not enlisted in a client session's
transaction; session-bound managers reject before dispatch. PITR uses separate
`FROM PITR` syntax and is unchanged. Atomic identifier components and snapshot
string tokens use the existing SDK SQL rendering authorities.

File wrappers reserve the native option names they map themselves. Callers use
the documented wrapper spelling or call `from_file` directly; unambiguous native
options continue through. LOAD SET accepts `NULLIF(target_column, literal)`
with an unqualified identifier and a quoted string or numeric literal. Row
variables and other expressions are rejected before execution because this
wrapper cannot introduce the corresponding LOAD variables.

Column-only mapped queries infer their first selectable as root, but every
other projected outer source must be supplied through an explicit join before
compilation. This retains selectable identity for schema names and aliases and
prevents accidental cross products. Explicit-root query behavior is unchanged.

Client-level ALTER TABLE and TRUNCATE, like DATA BRANCH MERGE/PICK, use a
checkout temporarily set to autocommit and restore the original state before
pool reuse. Explicit sessions retain transaction ownership. Failed restoration
continues to discard the physical connection; execution error and cancellation
precedence remain as before. Ordinary tables take the same DDL route because
lineage is server-owned and is not safe to infer from stale client metadata.

IVF/HNSW status decoding recognizes server boolean forms and rejects unknown
values. Analytics distinguishes a missing PyArrow package from an import error
inside installed dependencies. Full-text index population quotes each atomic
identifier before producing internal SQL; this changes no index dispatch or
persistence format.

Public SQL, error and transaction contracts require native server validation in
addition to offline grammar and failure-path tests. Account/cluster restores
require an exclusively owned disposable server; they must not be run against a
shared target. Builder parameter substitution, vector upsert and ordinary
full-text search are retained from earlier fixes and verified rather than
reimplemented here.
