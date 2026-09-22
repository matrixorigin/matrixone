# EXCEPT ALL / MINUS ALL: design and acceptance, revision 1

- Status: drafted; maintainer design approval pending
- Start date: 2026-09-22
- Implementation PR: [#29228](https://github.com/matrixorigin/matrixone/pull/29228)
- Issue: [#28234](https://github.com/matrixorigin/matrixone/issues/28234)
- Implementation baseline: `3727c5c5436ca136d7b0312f75bc223a11692a85`
- Initial implementation: `9fbfade5116e46829286b6b357cea529ac1b7399`
- Binary-ancestor correction: `318e7a9dc244445fe4bf029d98315444b4c80839`

This document records the implemented design for review, not retroactive human
approval. GPT-6 medium design and independent implementation reviews informed
the implementation; those model reviews do not replace maintainer approval.
The PR review history is the public approval record. Changes to this document
must identify a new design revision when they change the contract below.

## Semantics and ownership

For each equality key, emit `max(left_count - right_count, 0)` occurrences.
NULL participates in set equality. Surviving rows retain their original left
values: physical PAD SPACE normalization is for equality, not output rewriting.
CHAR/VARCHAR promotion uses the existing planner's physical equality keys;
binary values must not be trimmed. Prepared specialization refreshes the keys
after input/output type changes.

The new `MinusAll` operator consumes the complete right input into a
NULL-inclusive hash table and uint64 multiplicity array, then probes the left
input. A match consumes at most one remaining occurrence. A fully suppressed
batch is not end-of-stream. Counter overflow returns an error.

Each logical subtraction has one owner with DOP 1. Both complete streams are
gathered once; upstream producers remain parallel. The existing parallel set
operator path broadcasts input, so creating independent subtraction owners
there would duplicate output. Hash partitioning and spilling are deferred,
not silently emulated with broadcast. There is no throughput-speedup claim.

The compiler constructs two leaf merge children with receiver ranges `[0,1)`
and `[1,2)`. Serialization is post-order. Remote decoding validates and restores
the two children for Minus, MinusAll, Intersect and IntersectAll, including
nested PreScopes and unary parents. This shared restoration is necessary for
an existing binary operator surrounding a new MinusAll; the previous
MinusAll-only restoration left a remote IntersectAll with one child.

## Capacity and failure behavior

Right-side state is proportional to distinct normalized right keys, not just
output size: hash storage plus geometrically grown uint64 counters (8 bytes
per allocated counter slot, capacity rounded upward), key-evaluation scratch,
and batch storage. Wide/high-cardinality right inputs can therefore exhaust
the owner's memory even when the result is empty. There is no spill path, no
feature-specific memory cap, no guaranteed maximum input size and no automatic
fallback to a partitioned algorithm. Mpool accounting is not a hard admission
or physical-memory guarantee. Allocation errors propagate; operational query
budgets remain subject to the existing engine's enforcement.

Reset releases hash/count state and resets key evaluators; it may retain output
batch capacity for reuse. Free releases retained operator resources. Tests
assert zero mpool balance after complete child/operator/process teardown, not
immediately after Reset. Build/probe errors must allow reset/reuse, and LIMIT
must allow upstream cleanup without hanging.

## Mixed-version and rollout contract

All CNs that can plan or execute this feature must run a build containing this
PR before EXCEPT ALL / MINUS ALL is enabled for workloads. Mixed old/new CN
execution is unsupported. Appending the VM opcode preserves existing numbers;
it does **not** negotiate the new capability. A pre-feature decoder receiving
that opcode has no MinusAll case and returns an unexpected-operator error.
An old coordinator still rejects ALL during planning. No transparent local
fallback, mixed-version success, or new feature-specific capability gate is
claimed. Existing PAD SPACE protocol version 40 checks concern equality
semantics only and do not establish MinusAll support.

Binary-tree reconstruction changes no protobuf layout or stored table format.
Do not infer rolling-upgrade compatibility from that fact. Roll out execution
CNs uniformly and drain/recreate sessions/prepared statements before relying
on the new capability. A negotiated fallback requires a separate design.

## Acceptance and reproducible evidence

The following named tests are committed, so their assertions and SQL can be
reviewed independently of local logs:

| Requirement | Evidence owner |
| --- | --- |
| Multiplicity, NULL, batch boundaries, error/reset/reuse and complete mpool release | `pkg/sql/colexec/minusall` |
| All four binary ancestors, ordered merge ranges, connector identity, nested malformed-wire rejection | `TestMinusAllTransportRoundTrip`, `TestMinusAllTransportRejectsMalformedNestedShape` |
| Prepared type specialization refreshes physical keys rather than only changing a WHERE filter | `pkg/sql/plan/minusall_prepared_key_test.go` |
| Single-CN and actual peer-owned execution, nested operations, exact counts, empty inputs, LIMIT and prepared reuse | `TestIssue28234ExceptAllCluster` |
| Peer address, DOP 1 and MinusAll in the same scope's own pipeline, not a child scan | `TestMinusAllOwnerPlanAssertion` and live EXPLAIN ANALYZE assertions |
| CHAR/VARCHAR trailing-space equality, preserved left bytes, binary no-trim and repeated bound-key changes | Two equality passes on the same live fixture/session/prepared statement |
| Cluster teardown | Successful Close plus bounded independent checks that captured CN SQL listeners no longer accept connections |

Run with the repository CPU CGo wrapper, for example:

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=300s \
  -run 'TestIssue28234ExceptAllCluster|TestMinusAllOwnerPlanAssertion' \
  ./pkg/tests/issues/isolated
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=240s \
  ./pkg/sql/colexec/minusall ./pkg/sql/compile
```

The integration fixture uses private embedded log/TN/CN services with the real
MySQL listener and remote pipeline transport. In two-CN mode ingress is drained
from placement; the executed plan must put MinusAll on the peer. Verbose output
contains complete representative executed plans. A bounded same-instance
repeat challenges stale key/count state without restarting services.

This is **not** standalone/proxy BVT evidence. Release acceptance also requires
terminal results from the PR's applicable standalone/proxy and two-CN CI jobs
at the candidate revision. Pending, skipped, superseded, or unrelated-revision
runs are not passes. Record links and conclusions in the PR when available;
do not freeze an in-progress CI status as an approval in this document.
