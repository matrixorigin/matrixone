# M0 missing-column feasibility result

**Result: route 2, a restricted append invariant, is feasible for the local
`mo_m0` prototype.** It is not yet a production feature and does not select M0
in ordinary builds.

This document records the original feasibility reasoning. For the later CF9
version-carrier repair, normal Linux qualification, candidate-only SF100 screen,
shared default-build changes and remaining Draft/CI gates, see [M0_STATUS.md](M0_STATUS.md).

## Route decision

| Route | Result | Limit |
|---|---|---|
| Historical-schema/lineage reconstruction | Not selected; not disproven | Object writer TableID is zero in the B-style writer and simple global identity is unavailable. |
| Restricted append invariant | Selected and implemented under `mo_m0` | Only the explicitly proven hidden-composite-PK domain is admitted. |

No new persistent format field is required for this domain. The object header
version, current schema version/column identities, and object block metadata are
sufficient after ownership is established by the relation and the transition
domain is enforced.

## Contract

Let `k` be the stable hidden composite PK seqnum and `b=k+1` the initial
ordinary column count. A valid root has dense ordinary seqnums `0..b-1`, rowid
last, and version zero. Every successful independent ADD appends exactly one
nullable/no-backfill column and increments both `NextColSeqnum` and version.

For a current schema version `v`, `NextColSeqnum=b+v`. For an object header
version `u`, require `u<=v`, width `b+u`, and every block's complete typed prefix
`[0,b+u)`. A nullable position at or after `b+u` is a legal absent column only
after the complete object proof succeeds. A missing expected prefix, width/type
mismatch or unsupported transition is an error and is never filled as NULL.

Clone must preserve the full internal layout and version. Other DDL is outside
the domain and must not partially enter it. All participating CN/TN writers must
derive the output object version from the complete current write schema rather
than a stale batch or block marker.

## Implemented boundaries

- CN admission checks the root and one-column transition, transaction mode,
  current persisted objects, catalog state and uncommitted objects.
- Planner and executor choose the in-place ADD path only when the `mo_m0` hook
  is initialized. Default builds use inert hooks and the ordinary path.
- CN clone planning carries complete column IDs, seqnums, hidden-key position,
  appended columns and source version.
- Read selection validates an object before pruning. Metadata scan, statistics,
  `IS NULL`/`IS NOT NULL` pruning, `COUNT(column)` and block reads only interpret
  an absent suffix as NULL after the proof marker is present. The append domain
  uses the proving slow path instead of the unproven fast-filter path.
- CN sinkers and TN flush/merge writers derive the object schema version from a
  complete dense write prefix. Incomplete or non-domain writes retain the
  original fallback and fail later in the strict reader.
- Catalog cache reconstruction uses `attnotnull` as the authoritative
  nullability source, and PRE_INSERT NOT NULL validation resolves columns by
  name rather than assuming visible-vector and physical-column offsets match.

## Evidence

| Obligation | Evidence | Result |
|---|---|---|
| Persisted object proof, dense prefix, foreign same-version definition and clone identity | `m0-unit-group-r2` | PASS |
| Current schema width recovers a stale catalog/block writer version | M0 catalog/jobs unit tests; TN flush/merge SQL | PASS |
| Formal customer ALTERs, reads, statistics, clone, snapshot, branch, DML, L0 merge and restart | `m0-success-concurrent-r2` | PASS |
| Concurrent ADD/ADD safe subset with focused race evidence | `m0-success-concurrent-r2`, `m0-concurrent-race-r1` | PASS |
| Private-def, TN-registered and catalog-delete rollback; cancellation; second-ALTER atomicity | `m0-rollback-cancel-merge-r3` | PASS |
| DML-before-ALTER and ALTER-holds-lock barrier interleaving | `m0-insert-interleave-r6` | PASS |
| Missing suffix `IS NULL`/`IS NOT NULL` pruning and same-relation cache refresh | `m0-missing-column-zm-r1`, `m0-cache-legacy-unit-r2` | PASS |
| TN merge reads old input without a CN caller proof | `m0-rollback-cancel-merge-r3` | PASS |
| Legacy `mo_tables.rel_logical_id` compatibility | `m0-cache-legacy-unit-r2` | PASS |
| Default compile, COPY plan and COPY execution controls | `m0-default-compile-closure-r2`, `m0-default-copy-plan-r1`, `m0-default-copy-exec-r1` | PASS |
| Affected static closure | `m0-vet-default-r2`, `m0-vet-mo-r1`, `m0-lint-incremental-r2` | PASS |

## Evidence limits

- SQL restart now covers a CN restart and a TN+CN restart after the test issues
  `globalcheckpoint`; the recovery fixture separately covers WAL and
  checkpoint-plus-later-WAL.
- No crash/torn-write, multi-CN or mixed-version reader/writer proof exists.
- The original feasibility stage had no Linux/SF100 result. Later CF9 small-data
  qualification and a candidate-only first/warm screen are recorded separately
  in M0_STATUS.md; they are not production qualification or a new matched pair.
- A mixed binary deployment has not been exercised. The default reader
  fail-closed behavior is only a package-level proof.
- No isolated proof/filter performance benefit is claimed. The prototype uses
  the proving slow path in
  the append domain and disables broad count rewrites while M0 is compiled.

## Remaining Production Qualification

1. Review the complete Draft diff, run its new BVT on the exact publication head,
   and arrange opt-in experimental-tag CI. The existing CF9 Linux qualification
   is retained, not a reason to repeat unchanged tests.
2. Close UPDATE/background-debt attribution and any later performance acceptance
   with an explicitly comparable workload; the historical screen alone is not
   an end-to-end or statistically established claim.
3. Qualify multi-CN and mixed-version routing so old writers cannot publish into
   the M0 domain and old readers fail closed before consuming new objects.
4. Add crash/torn-write/power-loss coverage.
5. Keep default selection and deployment disabled until those gates pass.
