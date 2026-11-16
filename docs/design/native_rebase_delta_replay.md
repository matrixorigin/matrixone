# Native Rebase Delta Replay

## Contract

This optimization implements the approved cost-first Rebase performance plan.
It changes neither SQL/catalog/adapter interfaces nor R2 activation. Production
R2 remains disabled. The reference implementation is `64efbb2840`.

The fixed endpoints are `Bf, F, Bu, U`. Existing authorization, schema,
inventory/source protection, base equality and retained-base obligations are
unchanged. The result and all six summary counts must equal exact SQL replay.
An unsupported optimization layout uses SQL before candidate creation; a runtime
error never changes algorithms, retries publication or partially commits.

## Choice

The original implementation uses full-state joins for accounting and DML.
Measurements identified accounting and base proof, rather than DML, as its
dominant cost. Retaining that implementation is the correctness fallback.

A fully typed row merger could avoid SQL probes, but would duplicate the
existing stored-value/collation comparison semantics. Instead, reuse internal
DIFF collection to produce a bounded superset of candidate keys and normalize
each key batch with the existing exact fixed-state SQL comparisons. This is
not a second full-table join cache: every endpoint is filtered inside its
relation, and each key is probed once. Physical event order is not an input to
conflict decisions. Globally ordered cursors and parallel extraction are not
assumed.

## Flow And Ownership

1. The existing coordinator authorizes and protects all endpoints and checks
   schema and base equality in its caller-owned transaction.
2. Optimization eligibility is decided from schema: integer PK components
   (at most 16), at most 128 columns, and an 8 KiB conservative per-row SQL
   representation bound. Other layouts keep the SQL path. No-PK tables still
   require exact four-endpoint multiset equality.
3. For each base/head pair, use internal relation, lineage and change-handle
   resolution with the same `BackgroundExec`. Do not invoke the public
   DIFF/MERGE agency, which acquires its own transaction executor.
4. Collect all data/tombstone keys before DIFF's logical value comparison.
   This preserves candidates for signed zero and NaN payload changes. Event
   false positives, repeated writes, write-back and physical compaction are
   normalized by the exact SQL probe, not counted as logical changes.
5. A spillable key set deduplicates candidate keys with 64-row probes. Its
   local duplicate map is bounded by one window and uses the typed tuple
   codec. Composite PK declaration order is normalized to visible-column
   order, including decoding tombstones' physical composite key.
6. One exact probe returns presence/equality predicates, feature row values
   and replay keys. One decision produces accounting and actions together.
   The upstream count covers every upstream candidate, not only its overlap
   with feature changes.
7. Typed action batches are marshalled into the existing OUTPUT AS spool.
   All eligible-table reads and FAIL-policy checks finish before clone.
   After clone/base retention, consume each table's deletes and then inserts,
   once, through normal DML on the same transaction.
8. Existing publication/generation/UNKNOWN recovery remains authoritative.
   No action spool or provisional summary commits independently.

The statement owns key storage, its work pool, change handles, return batches,
action files and replay batches. Each acquisition registers cleanup before
later work. Reader/producer buffers are borrowed only for the callback; any
retained data is copied into owned storage. No new background coordinator or
cross-statement cache is introduced.

## Resource And Failure Boundaries

All change maps and visible-state recovery use one statement allocator. The
Rebase-only strict-capacity flag disables legacy single-entry heap fallback;
ordinary DIFF/MERGE/PICK defaults are unchanged. The key set has a charged
256 KiB bounded scratch reservation; retained action workspaces reserve 4 MiB
from the same budget, covering bounded probes, formatting and spool buffers.
These reservations do not replace the existing SQL engine memory/spill limits.

SQL results are closed after each probe; key windows and copied vectors are
released after each batch. Action spools use the existing anonymous spill-file
owner. Successful consumption closes files/batches and releases reservations
immediately. Cancellation, budget/I/O failure and transaction rollback use the
same cleanup, with errors propagated. Cleanup errors abort before commit.

The key-set iteration lock may call its read-only SQL consumer; that consumer
does not access the same key set. Extraction and consumption are serial, with
no executor calls racing on the transaction. No process lock substitutes for
the source transaction dependencies.

## Verification And Delivery

Required coverage includes exact summary/result controls, all conflict
policies, same-result changes, reversed/composite integer PKs, NULL, signed
zero/NaN payload identity, no-PK rejection, continuous Rebase and Merge,
multi-table failure/rollback, cancellation, capacity failure and real spill
success/failure cleanup. Shared DIFF consumers receive focused regression.

Diagnostic plans are captured only outside the verb timer, after untimed
warmup validation. They are not formal performance samples. Formal acceptance
uses one frozen candidate build and adapter with plans/profiles disabled:
Small and W1, two independent pairs in opposite orders, one warmup plus ten
measured calls per arm. Both W1 native/script median ratios must be at most
1.05; Small fails if slower by both 10 percent and 20 ms. Existing validity,
resource and cleanup gates remain mandatory. A diagnostic improvement or a
passing component test is not final acceptance.

The optimization is not delivery-qualified until that evidence and the final
self-review are complete. Per-run logs, manifests and timing records remain
outside the source tree.
