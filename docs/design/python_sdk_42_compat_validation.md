# Python SDK companion fixes on 4.2

This document describes PR #32723, related to #29928/#26068 and SDK PR #32671.
The native companion preserves 4.2-dev scalar type display. It does not bring
main's canonical scalar formatter or its client-reflection guarantees to 4.2.

## Scalar metadata contract

`SHOW COLUMNS`, `DESCRIBE`, `information_schema.COLUMNS.COLUMN_TYPE` and
`SHOW CREATE TABLE` retain the existing 4.2 spelling for ordinary scalar types.
Examples include `INT(32)`, `BOOL(0)`, `BIGINT UNSIGNED(64)`, `DATE(0)`, `TEXT(0)`
and `FLOAT(5)`. SHOW CREATE retains `bool`. Internal OIDs, dimensions, DATA_TYPE,
value packets and storage encoding are unchanged by the display decision.
There is no new scalar formatter or legacy-formatter layer.

These are compatibility observations, not a claim that every old string is
canonical MySQL DDL: the old FLOAT display can omit scale, and unsigned suffix
order is retained. Standard SQLAlchemy BOOL/table and view reflection are not
fixed by this native PR. The native BOOL values, NULL/default semantics and
ordinary SQL replay remain covered; the withdrawn canonical-output BOOL client
feature is not replaced with a skip or negative-only acceptance.

Independent ENUM/SET correctness work remains: preserve direct view provenance
before projection remapping, retain exact domain members/case and quote members
through existing literal owners. The dedicated empty ENUM domain remains
`enum('')`. Public domain replay and the existing 4.2 DUMP/hash spelling have
separate existing owners; scalar normalization does not justify changing either.

## Other retained closures

| Closure | Owner and resulting behavior |
| --- | --- |
| TLS | Drain frontend-owned read-ahead bytes before underlying TCP input; both handshake branches keep the existing socket ownership. Linux terminal probing unwraps the known buffered/TLS wrappers and keeps FIN grace. |
| SQL mode | Derive response status from the executing session; no second cached SQL-mode state. |
| FULLTEXT | Quote identifier components through the existing SQL quoting owner in planner, post-DML and ISCP; keep asynchronous parser parameters. |
| Numeric parser | Preserve numeric token boundaries and incomplete-exponent errors, while retaining 4.2 wire metadata. |
| Stage | Preserve host-form URIs; support logical names with path-form URIs, one-time name decoding, existing URL/file authority, atomic combined ALTER and explicit CREATE ENABLE false. |
| Parquet | Accept representable FLOAT32 infinities; keep finite overflow, NaN and signed-zero behavior. |
| Branch | Store the database-owned data-branch category in existing dat_type; retain all ordinary-table receipt checks, DDL catalog-key serialization and existing RC retry ownership. |
| Restore | Preserve branch identity through named/timestamp/PITR restore; preflight source definitions before irreversible target effects and retain statement/account context boundaries. |

Branch design and accepted review are in `data_branch_identity_42.md` and
`data_branch_identity_42_review_r1.md`. No catalog column, RPC field, storage
format, new worker, queue, retry state machine or mixed-version gate is added.
The branch category is a new persisted value in an existing catalog column.

## Stage URI boundary

Existing `stage://name/path` references remain supported. Path form such as
`stage:///two%20words/file.csv` addresses logical Stage names which are not URL
hosts. Split the first encoded segment before decoding, so `%2F` belongs to the
name and `%252F` denotes a literal `%2F`. Empty names remain invalid; dot-only
names use host form. Datalink `size=c` remains invalid after Stage resolution.
A missing process/base/cache returns an internal error instead of panicking.

Published 4.2.8 is unchanged. Path-form native support requires a 4.2 release
containing this PR; existing SDK Stage helpers still use host form. An API that
accepts a complete encoded URI is required for names needing path form.

## Validation and evidence boundaries

The display follow-up restores the affected existing golden type rows from the
verified 4.2 baseline, including binary-classified result files. Other query,
value, error, Stage-state, branch and restore changes are retained. Added scalar
metadata tests use literal 4.2 outputs and real-service comparisons; they do not
accept the candidate's generated output without review.

On 57, Linux/amd64, Go 1.26.4 with matching CPU native libraries:

- All three owning packages (types, plan and function) passed normal and race
  runs with -short -tags=matrixone_test.
  The focused metadata/round-trip/decode selection passed all 10 root tests.
  The pre-existing TestDecimal128Float skip remains unchanged.
- gofmt, standard vet and configured incremental golangci-lint passed on the
  three owning packages. Pinned molint exited zero and retained three pre-existing
  checkrecover diagnostics in unchanged expr_opt.go and returning.go.
- An isolated service built from actual 4.2-dev
  ad39691b6b45f26688d42c47d9451bc89c957679 supplied the baseline. All eight
  query snapshots match exactly: SHOW CREATE, ordinary/full columns, view
  DESCRIBE, table/view information_schema, native type decoding and BOOL/NULL
  values. Both baseline and candidate shut down normally.
- The bool, create_table_like and show_create_table BVT scripts passed 274/274
  comparisons twice on the same candidate instance, with zero failed, ignored
  or abnormal statements and explicit database/fixture cleanup checks. The
  tester used its normal comparison mode; JDBC header comparison was disabled,
  while COLUMN_TYPE cells were compared. Existing golden rows were restored
  from the baseline, not generated from the candidate.
- Unmodified standard SQLAlchemy 2.0.43/PyMySQL passed the three retained positive
  ENUM/SET cases, covering both SQL modes and the empty-member domain. These
  cases do not load the SDK and do not certify BOOL or view reflection.
- Early private harness setup failures (Unix-socket path length and tester
  configuration/result lookup) were recorded separately, corrected without
  product or assertion changes, and are not counted as successful runs.

The scope design was independently reviewed with gpt-6.1-sol/max; the final
implementation review and exact source revision are recorded in the PR body.
Earlier canonical metadata/standard BOOL reflection results do not certify this
display policy. Unchanged TLS, Stage, FULLTEXT, parser, Parquet and branch/restore
closures reuse their matching evidence from the series through 428a9ea84ab1
with revision and scope attribution; those suites were not rerun by this
display-only follow-up.

## Release limits

Original published 4.2.8 remains unchanged. Supported SDK paths and release
limitations are maintained independently in PR #32671. Standard unmodified
SQLAlchemy reflection limitations remain outside this display-preserving PR;
see #32746 and the native BOOL metadata issues rather than claiming full client
compatibility from raw SHOW results.

Upgrades are full-cluster shutdown only; older binaries must not open newly
written data. Old unmarked empty databases cannot safely be inferred as branches.
Persisted ENUM/SET views that already lost provenance need recreation. Native
Arrow LOAD and VECF4/VECF8 are not introduced by this backport. No CI runner is
added, and local validation does not certify remote CI.
