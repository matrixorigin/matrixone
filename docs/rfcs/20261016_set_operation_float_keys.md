- Status: in progress
- Start Date: 2026-10-16
- Issue: https://github.com/matrixorigin/matrixone/issues/30155
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/30184
- Accepted architecture: revision 13; this edit condenses the revision 36 record.
- Historical implementation snapshot: `5d4a6a0983218167aa0746623f410c19df8d2fe9`
- Current official baseline: `5fe08a3307e246e1ab9951ab690b50557af0f509`

# FLOAT/DOUBLE set membership

## Scope and acceptance

Issue #30155 reports that MySQL 8.0.46 returns UNION/INTERSECT/EXCEPT counts
2/0/1 for opposite signed zeros, whereas MatrixOne returns 1/1/0. Match that
FLOAT/DOUBLE membership behavior, including INTERSECT ALL and EXCEPT ALL
multiplicities, while returning original values and column types.

Same-sign duplicates still deduplicate. Scalar `-0 = +0`, ordinary GROUP BY,
SELECT DISTINCT, distinct aggregates/HLL, joins, DECIMAL zeros and UNION ALL
retain their existing semantics. Existing NaN, FLOAT scale and PAD SPACE
normalization remain with their current owners. This does not reopen the
aggregate/HLL issues #27425 and #28856 or add a general correlation feature.

Only changes necessary for this membership contract and regressions caused by
its implementation belong in this PR. Independently discovered defects,
general optimizer improvements and broader deployment qualification are
separate work. Review feedback does not by itself expand these acceptance
criteria. A smaller safe implementation may replace a mechanism; dropping a
demonstrated regression test is not a scope reduction.

## Set-only keys and planning

### Why retain the zero sign here?

The reason is the operation-specific compatibility contract in #30155, not a
general rule that IEEE signed zeros compare unequal. Its MySQL 8.0.46 controls
show scalar `-0e0 = 0e0` is true while opposite-zero UNION/INTERSECT/EXCEPT counts
are 2/0/1. Normalizing both signs to the same *membership* key would reproduce
the reported 1/1/0 bug. A comparison key can still be normalized: this one must
retain the zero-sign distinction required by set membership. It does not alter
the original value or prescribe equality for scalar expressions, joins,
GROUP BY or aggregates. In particular, #27425's aggregate normalization remains
in place. Changing the requested MySQL compatibility target would require a
separate explicit scope decision.

`set_operation_float_key` produces a UINT64 equality key after the common
set-operation type is resolved. It preserves raw negative-zero bits; other
values use the existing canonical NaN/scale rules. The visible result remains
the original value. Existing physical-key propagation, global duplicate
elimination and spill recovery carry these keys. UNION ALL needs no new key.

Prepared specialization rebuilds keys for the resolved type on every execution;
integer -> float -> integer must acquire and then remove the float requirement.
Hidden correlation identities retain scalar equality rather than float set
membership. Filters on unchanged integer/bool outputs can still reach both
branches; predicates without an equivalence-preservation proof stay above the
set operation.

The old textual identical-arm collapse happened before binding and replaced
UNION membership with ordinary DISTINCT. That shortcut can lose a signed zero.
Keeping typed arms exposes correlation and evaluation boundaries formerly
hidden by the shortcut. Existing baseline/head reviews demonstrated regressions
in previously executable correlated queries during early versions of the fix.
Their repair belongs to the changed path, not to an unrestricted mandate to
improve all correlated queries.

The current implementation retains three routes:

1. Equality-correlated plans keep their existing partition identity.
2. Proven deterministic, non-reusable identical arms can reuse an aggregate or
   pagination producer. Singleton boundaries preserve the original result;
   multirow boundaries use set-key DISTINCT above a limit-free projection.
   LIMIT/OFFSET and aggregate empty-input behavior remain with their owners.
3. Admitted nonidentity-correlated scalar UNION uses occurrence-based
   reaggregation. Parameters, volatile expressions and producer boundaries
   cannot be discarded merely because their SQL text matches.

## Correlated scalar execution

The selector in [correlated_scalar_set.go](../../pkg/sql/plan/correlated_scalar_set.go)
requires one visible scalar output and an admissible UNION shape. Existing
checks reject unsupported aggregation, pagination, volatility, deep correlation
and outer-output shapes; this is not arbitrary correlated-query support.

```text
outer stream -> streaming occurrence ID -> LEFT JOIN with presence marker
             -> AGG by (occurrence ID, marker, visible membership key)
             -> AGG per occurrence: count(marker), any_value(original value)
             -> existing SINGLE cardinality check
```

Duplicate outer values receive distinct occurrence IDs. Hidden inner identity
columns are not visible set members. A missing row has a NULL marker; a real
NULL member has a true marker. Zero members returns scalar NULL, one returns
its original representative, and multiple distinct members use the existing
1242 cardinality error. ANY_VALUE does not suppress that error.

The marked composition runs only on the coordinator at DOP 1. Existing WINDOW,
LEFT/LoopJoin, aggregation and SINGLE operators own their buffers and cleanup;
the marked WINDOW streams IDs instead of materializing the whole outer input.
Prepared attempts and resets obtain fresh IDs and leases.

`ScalarSetReaggregation` identifies this specific scalar-UNION composition; it
does not enable a general scalar comparison mode. In the dependent operator,
the flag requires `ScalarSetLease`, the shared execution-attempt work budget.
That borrowed lease charges nested candidate matching to the same bounded
attempt and prevents each invocation from receiving a fresh budget or CN
permit. Lease release belongs to the statement attempt's cleanup owner.

## Resource boundaries and failure paths

All marked stages, including nested dependent execution, share one statement
attempt account and work lease:

- Participating memory: **256 MiB**, or the smaller inherited limit. Retained
  borrowed backing is reserved before publication, including offsets/validity.
- Work: **16,777,216 candidate pairs**, including predicates that are false.
  Yielding or allocation failure does not refund the counter.
- Concurrency: at most **two marked attempts per CN**, with one permit per
  attempt rather than one permit per nested invocation.
- Work/concurrency rejection is terminal error **20833**.

Reset, cancellation, partial Prepare and spill failure use the existing owner
cleanup paths. A permit is returned only after a matching sealed, valid
zero-live account proof. Uncertain or panicking cleanup cannot recycle damaged
owners or return an unproven permit.

For O outer occurrences and I candidate rows, general matching can examine O*I
pairs even when few rows survive. The first aggregation groups matched members
per occurrence; the second restores one scalar result per occurrence. Wide
outer rows, retained join state and spill/reload buffers contribute to memory.
The participating-account cap is not a bound on total process RSS or arbitrary
upstream producers. No universal latency SLA or distributed scalar capacity is
claimed.

## Protocol and persistence

On top of the current baseline, this PR assigns function **620** (FLOAT/DOUBLE
overloads), registry sentinel **621**, and MORPC **132**. The scalar marker uses
Node field **96** and Instruction field **65**. Existing function/field identities
are unchanged; historical unpublished IDs are not aliases for current IDs.

Placement, actual destination and stream checks, receive-before-decode,
executor Prepare, retained CTAS requirements and persisted-expression admission
use the same floor. Immediate predecessor 131 cannot execute this capability.
Other features retain their independent floors. Prepared type changes recheck
the resulting requirements. Marked scalar scopes are never sent remotely,
regardless of advertised version.

Before downgrade or restoration onto older binaries, affected persisted
expressions must be inventoried and removed or rewritten, or capable binaries
must be retained. Restart alone is not a migration. Live mixed-version rollout
and backup restoration have not been qualified by the recorded tests.

## Alternatives and retained decision

| Alternative | Reason not selected for this cleanup |
| --- | --- |
| Change shared float equality globally | Changes scalar, grouping and aggregate contracts outside #30155. |
| Keep textual collapse with ordinary DISTINCT | Loses the set-specific signed-zero distinction. |
| Reject every affected correlated query | Can regress previously supported queries. |
| Extend single-arm typed DISTINCT reuse to all identical arms | Candidate simplification, but correlation identities added during pull-up can change visible membership/cardinality; requires focused baseline/candidate proof before replacing the current route. |
| General per-outer executor or distributed occurrence IDs | Adds execution or distributed lifecycle work beyond this issue. |

The accepted bounded composition is retained in this documentation cleanup.
No production, generated code or executable test is changed.

## Validation and evidence location

The PR body contains the current validation summary: source revision, command
or CI run, result, reused versus fresh evidence, and limitations. Reproducible
tests stay in source. Raw logs, per-round benchmark samples, source fingerprints
and historical integration receipts are execution artifacts, not new source
deliverables; future runs keep them in the task directory or CI artifacts.

Existing historical evidence remains accessible at immutable revisions:

- [Original issue and controls](https://github.com/matrixorigin/matrixone/issues/30155).
- [Early baseline/head regression review](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5524739837).
- [Revision 13 design assessment](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5567698353)
  and its [protocol-collision correction](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5567799063).
- [Historical full implementation review](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5640948599).
- [Revision 36 design and history](https://github.com/iamlinjunhong/matrixone/blob/5d4a6a0983218167aa0746623f410c19df8d2fe9/docs/rfcs/20261016_set_operation_float_keys.md).
- [Historical raw receipts](https://github.com/iamlinjunhong/matrixone/tree/5d4a6a0983218167aa0746623f410c19df8d2fe9/docs/rfcs/validation).

Historical results remain tied to their actual source, environment and mode.
This condensation is not a new implementation approval or performance run.
