# Native Unicode UNIQUE activation amendment for #29206

## Review state

**Proposed for independent design approval.** This document is the versioned
amendment for the implementation in PR #30113. It must be reviewed and
approved as a design before the implementation is accepted. It does not claim
approval merely because it is present in the implementation branch.

The parent contract remains
[`issue-29206-uca400-backend.md`](issue-29206-uca400-backend.md). That document
deliberately rejected native Unicode `PRIMARY KEY`/`UNIQUE` parts for its first
release; this amendment narrows and supersedes that one item only. All other
parent restrictions remain in force.

## Change classification and trigger

This is a feature and persistent compatibility change, not an ordinary bug
fix. It crosses planner, protobuf metadata, index-table, DML, foreign-key and
remote-plan owners, changes durable index-key bytes, and introduces a new
mixed-version admission contract. The design gate is therefore mandatory even
though the implementation is split across one PR.

## Problem and evidence

The UCA400 backend can now produce a deterministic comparison key, but the
parent design leaves native Unicode columns fenced out of UNIQUE indexes. A
table using `utf8mb4_unicode_ci` or `utf8_unicode_ci` consequently cannot
enforce the same equivalence relation for uniqueness as it uses for scalar
comparison and grouping. Reusing the legacy byte key would allow collation-
equivalent values such as `Alpha` and `alpha` to occupy different UNIQUE rows.

The amendment enables only the existing V1 key owner and its ordinary hidden
UNIQUE index tables. It does not introduce a sidecar, a second collation
implementation, or an implicit migration of existing tables.

## Supported scope

The first release admits exactly the following authoring shape:

* an ordinary, persistent row table created by `CREATE TABLE`;
* a native Unicode `utf8mb4_unicode_ci` or `utf8_unicode_ci` column already
  resolved to `RevisionV1`;
* one or more ordinary UNIQUE definitions whose native-Unicode parts are
  encoded with `KeyFormatV1`;
* the normal table/index transaction and existing hidden UNIQUE index-table
  lifecycle.

The following remain rejected and are not implied by this amendment:

* native Unicode PRIMARY KEY parts;
* `CREATE UNIQUE INDEX`, `ALTER TABLE ADD/MODIFY/CHANGE`, or conversion of an
  existing indexed column;
* temporary, cluster, CTAS, external, Iceberg, MongoDB, Kafka, data-stream or
  other typed external tables;
* secondary, FULLTEXT, vector or spatial indexes in the same newly authored
  native-Unicode-UNIQUE table;
* automatic rebuild, collision resolution, row selection or migration of an
  existing legacy table.

If a future change expands any item above, it requires another design
amendment rather than silently widening this contract.

## Invariants and ownership

1. For every non-NULL UNIQUE tuple, equality of the SQL UCA400 domain is
   exactly equality of the V1 physical key. No original spelling or raw-value
   suffix is appended as a tie breaker.
2. The same canonicalization is used by duplicate admission, hidden-index
   probe, lock key, insert, update, delete, replace, ODKU and foreign-key
   action paths. A path that cannot construct a V1 key fails before publishing
   a partial index mutation.
3. NULL handling and prefix extraction retain the existing SQL UNIQUE rules;
   prefix length is applied to the original value before weight-key creation.
4. Original user text stays in the base row. A decoded V1 key is opaque and is
   never returned as SQL text; covering plans fetch the base value when needed.
5. Existing binary/legacy tables retain their metadata and bytes. A table-level
   `KeyFormatV1` marker is valid only when its index metadata proves the native
   Unicode UNIQUE contract; an optimizer-pruned shell cannot remove that proof.
6. No new goroutine, global locale, registry lookup, sidecar table or durable
   background state is introduced. Existing transaction ownership remains the
   rollback owner for base and hidden-index mutations.

## Durable and wire contract

The table and every participating UNIQUE index carry `KeyFormatV1` and
`RevisionV1` metadata. The authoring and execution boundary requires
`MORPCVersion114`, which is distinct from the existing v109–v113 contracts.
Older workers reject the plan as unsupported; they must not reinterpret V1 as
legacy or `general_ci`.

Creation is forward-only in this release. Restore, checkpoint replay, `SHOW
CREATE`, `LIKE` and catalog reads preserve the charset, collation revision and
key format as one metadata unit. A downgrade or mixed-version deployment may
retain the catalog row, but an older worker cannot execute a plan that carries
the V1 native-UNIQUE marker. There is no in-place downgrade conversion. A
rollback before commit uses the existing transaction rollback path and leaves
the source catalog and hidden index tables unchanged.

## Failure and recovery behavior

* unsupported operation, protocol version, malformed UTF-8, invalid key
  framing, overflow or unrepresentable prefix fails closed before a durable
  mutation;
* duplicate keys are rejected using the canonical hidden-index key, with the
  existing transaction error and rollback semantics;
* crash/restart recovery replays the persisted table/index metadata and V1 key
  bytes; it does not regenerate keys using the local legacy collation;
* an existing legacy table is never auto-upgraded merely because a newer CN
  understands V1.

## Alternatives considered

* **Keep the admission fence.** Safest compatibility choice, but leaves
  native-Unicode comparison and uniqueness inconsistent and does not deliver
  the requested capability.
* **Store legacy/raw text bytes in UNIQUE indexes.** Smaller implementation,
  but violates the equality invariant (`Alpha` and `alpha` can diverge) and
  would require a later incompatible rebuild.
* **Add a sidecar normalized-value table.** Could preserve old index bytes, but
  introduces a second transaction/restore owner and new failure and cleanup
  states. The existing hidden-index owner already provides the required atomic
  lifecycle, so V1 is preferred.

## Resource and performance boundary

V1 key construction uses the parent UCA400 bounds (`54*L+1` payload bytes and
`54*L+2` copied key bytes per value) and the existing statement/index memory
accounting. Keys retained by hidden index vectors are charged to that owner;
temporary comparison scratch is released at the end of the operation. The
feature does not claim bytewise-key performance. Acceptance requires the
existing binary/legacy fast path to remain unchanged and no unbounded retained
scratch or per-row metadata lookup.

## Validation and acceptance criteria

The implementation must keep the following deterministic evidence green:

* CREATE admission and protocol-113/114 boundary tests, including rejection
  on older authoring protocol;
* full protobuf collation-admission validation and remote-plan fail-closed
  checks;
* duplicate and NULL semantics for single and composite native-Unicode UNIQUE
  keys;
* insert, update, delete, replace, ODKU and FK action paths, including rollback
  after a failed duplicate;
* restart/restore metadata preservation and `SHOW CREATE`/catalog visibility;
* the embedded-CN `TestUnicodeCollationConsumerContract`, full owning planner,
  compile, utility and protobuf suites, plus the required distributed CI/BVT
  checks.

The current PR contains focused planner and DML regressions for these owners.
Any implementation deviation from this document requires this amendment to be
updated and re-reviewed before merging.

## Decision record

* Change scope: native Unicode UNIQUE activation, PR #30113, as a follow-up to
  #29206/#28164.
* Trigger: persistent key format, wire protocol, mixed-version and
  cross-subsystem ownership changes.
* Design revision: this file, `20261015-issue-29206-native-unicode-unique-amendment.md`.
* Blocking decision: **pending independent maintainer/design approval**.
* Implementation deviation: none known; the implementation remains subject to
  the supported-scope and fail-closed boundaries above.
