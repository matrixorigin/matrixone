# Python UDF current-stage design contract

| Field | Value |
| --- | --- |
| Design revision | `python-udf-current-stage-r1-2026-09-24` |
| Applies to | MatrixOne PR #29152, test/development-stage Python UDF |
| Implementation baseline reviewed | `9847da80d6c7096cb0460e46a5cf710e9714af3a` |
| Approval | Feature owner `iamlinjunhong` approved this exact revision for the current test/development stage on 2026-09-24 in [PR comment](https://github.com/matrixorigin/matrixone/pull/29152#issuecomment-5809313638). |
| Not covered | Production tenant isolation, sandbox, Operator rollout, or cross-version rollback/restore |

This is the approved governing contract for the current PR stage. It consolidates
the shared Catalog, SQL/planner, Python ABI, Flight, and resource-lifecycle
decisions needed to review this implementation. It does not approve future
sandbox or production deployment. The test/development scope is explicit:
Python is opt-in and unisolated; old demo definitions are rejected and must be
recreated through the current DDL. There is no legacy Python adapter or
fallback execution path.

## Identity, Catalog, and publication

Python functions use the shared `mo_user_defined_function` identity namespace.
The current catalog upgrade adds `active_revision`, `namespace_version`, the
canonical input/return descriptors, and signature schema/fingerprint columns.
`mo_function_revisions` stores the immutable typed revision keyed by
`(function_id, revision)`, including language, full argument and return
descriptors, ABI/adapter/SDK/schema contracts, artifact and environment digests,
NULL policy, volatility, security mode, and definition fingerprint. Revision
publication and head activation occur in the caller's catalog transaction.
Python executes as `INVOKER`, is `VOLATILE`, may error, and is not leakproof.

The artifact is an immutable, account-scoped FileService object addressed by
digest; each artifact is at most 1 MiB. CN resolves the exact account/digest
before Open, and the worker receives verified source bytes without object-store
credentials or query-time dependency installation. CREATE/REPLACE validates the
current contract before committing a catalog definition. Artifact publication
precedes catalog commit, so a failed validation/transaction can leave an
unreferenced content-addressed object. No artifact garbage collector or
per-account total artifact quota exists today; per-object size is bounded, total
retained artifact storage is not. This is an explicit test/development-stage
limitation, not a production storage-retention claim.

The type descriptor is canonical over SQL type ID and applicable width, scale,
charset, offset-width, JSON, and temporal encodings. The trusted adapter builds
Arrow field/schema metadata from that descriptor; it validates both physical
Arrow type and the SQL value domain. Handler-provided metadata cannot redefine
the function type. CREATE/REPLACE compiles the submitted module and checks the
declared synchronous module-level handler; it does not execute top-level user
code or install packages. The environment digest includes the current runtime
environment, including tzdata. An environment mismatch fails closed; definitions
must be explicitly recreated for a changed contract.

The current mappings encode JSON as `canonical_text` and DATE/DATETIME/TIMESTAMP
with `sql_zero_struct`; SQL zero temporal values remain explicit values and are
never rewritten as NULL. SCALAR handlers receive `(ctx, scalar...)`; VECTOR
handlers receive `(ctx, arrays...)`. The default NULL policy calls the handler
without filtering NULL rows; `RETURNS NULL ON NULL INPUT` removes those rows
before the call and scatters NULLs into their result positions.

`REPLACE` preserves language and the exact Python argument/return descriptors;
changing language or signature requires DROP/CREATE. DROP/CREATE receives a new
identity. Old Python demo rows without a current revision head, unsupported
definition/SDK/plan contracts, and corrupt revision heads fail closed before
user code runs. They are not auto-migrated. No query resolves a mutable `latest`
revision or takes source from an executable plan.

## SQL compatibility, plans, and migration

The shared revision catalog is additive to the existing SQL UDF contract.
During a rolling tenant upgrade, SQL UDF writers continue using the legacy
`mo_user_defined_function` row while `mo_function_revisions` is absent. Once the
revision catalog is available, SQL writers publish the compatibility row and
revision in the same transaction. Readers use the legacy SQL row only when the
revision schema/head is absent or both head fields are zero. A non-zero but
missing or malformed revision is a catalog error; it never silently falls back.
The SQL UDF language, body, security, and evaluation semantics remain owned by
the existing SQL path. The Python demo's old execution behavior is not a
compatibility promise.

Upgrade `4.0.7` (minimum source `4.0.6`) changes the shared function identity
columns/index and creates `mo_function_revisions`. The upgrade handler is gated
on common MORPC protocol 61 so a tenant migration does not widen catalog tables
while an old writer can still serve them. Python DDL independently probes the
required tenant schema and refuses creation/replacement until it is present;
worker capability negotiation is a separate exact-contract gate. This is
feature-level admission, not a requirement that every component have the same
software version string.

The migration has no down handler. Do not manually remove the revision table,
columns, or index, and do not claim that restoring an old binary restores Python
execution. Binary rollback after applying this catalog migration is not a
supported recovery path in this contract; ordinary SQL behavior in that mixed
state has not been verified here. Python definitions must remain intact and
require a current-contract binary to execute. The current evidence does not
include a tested downgrade after Python definitions have been written.
The upgrade-compatibility CI checks were skipped on the reviewed exact head.

Current-contract snapshot restore preserves the selected revision and
republishes its artifact into the destination account's namespace. The SQL
control function and source-account isolation are covered in
[`snapshot_restore.sql`](../../test/distributed/cases/udf_python/snapshot_restore.sql).
Cross-version restore and downgrade are outside the evidence and remain
unsupported until tested. Restore must not synthesize a revision from a legacy
Python row.

The plan contains a typed `RoutineCall`, an exact `FunctionRef` (account,
database, identity, revision, namespace), a typed implementation oneof, and a
dependency envelope. Python source is not embedded in the executable plan.
Plan reuse validates both the exact selected revision and a fingerprint of the
complete same-name overload namespace across SQL and Python. The dependency
closure is capped at 1,024 routines and 1 MiB. This invalidates prepared/cache
plans when a newly added overload changes binding, even if the previously
selected identity still exists. Plans without the namespace fingerprint rebind;
an overflowed namespace read makes the plan non-reusable rather than accepting a
truncated fingerprint. Execution uses the bound exact reference and never
resolves by name. Namespace validation reads at most 65,536 candidates (one
extra detects overflow) and 64 MiB of decoded metadata; on overflow it forces
rebind without rejecting otherwise valid SQL.

## SQL evaluation and result semantics

`ExternalRoutineEval` is a physical expression evaluator in the existing MO
expression framework. The parent supplies the batch and CASE/selection mask;
the evaluator runs each bound argument expression, including planned casts,
once under that mask, applies
`RETURNS NULL ON NULL INPUT`, compacts selected rows, validates the returned
Arrow schema and SQL value domain, then scatters results to original row
positions. Unselected CASE branches are not invoked. Python's default
volatility prevents planning-time constant folding. Empty input, a batch with
no rows selected, and a batch whose rows are all removed by strict NULL
filtering return locally without acquiring Gateway K, a worker group, or a
ledger entry. Zero-argument VECTOR calls receive `ctx.num_rows` for output
shape.

The ordinary Python BVT includes SELECT, WHERE, JOIN ON/outer-join predicates,
aggregate arguments, window ordering, nested calls, INSERT/UPDATE/DELETE, and
failure atomicity. It also covers scalar/vector modes, NULL policies, create,
replace, exact-overload invalidation, drop/recreate, Arrow compute, and
current-contract account restore. These are current tested placements; this
contract does not claim every possible SQL syntactic position without a case.

## Flight protocol and terminal ownership

Gateway is a CN library; the worker is the Flight server. There is no separately
deployed Gateway daemon. The current capability vector must match exactly
(wire protocol 1, ABI/adapter/SDK/definition/plan/type contracts, tzdata,
modes, NULL policies, frame/handler limits, and worker lease epoch). The worker
advertises `window_batches=1` and `cumulative_ack=false`. A mismatch fails
before handler execution; there is no old-worker fallback. Control envelopes
are limited to 1 MiB, JSON nesting to 64, and each fencing ID component to
256 bytes.

After admission reserves the group/member, K, and ledger, the Gateway opens the
exchange. With W=1, each non-final batch follows
`InputBatch(i) → InputConsumed(i) → ResultBatch(i) → AcknowledgeResults(i)`
before the Gateway sends batch `i+1`. After sending the final
`InputBatch(last_seq)`, the Gateway sends `EndInput(last_seq)` and half-closes
its send direction; it then receives the final `InputConsumed`, result, and ACK,
followed by `Finish → AcknowledgeFinish(finish_id) → terminal success`.

`EndInput` closes only the input direction; Flight send-half-close is normal
and the result direction remains readable. `InputConsumed` returns ownership of
the corresponding input backing. The result slot remains charged until it has
been validated and materialized in MO. W=1 permits one batch of progress at a
time. Finish is valid only after EndInput and acknowledgement of all result
sequences. The worker freezes success only after accepting the idempotent
Finish acknowledgement. If the ACK was accepted but its response was lost, the
Gateway retries only that same idempotent ACK; if confirmation remains unknown,
it reports `FINISH_UNCONFIRMED`. It never retries Open or replays work that may
have reached STARTED.

The CN Gateway owns K, group/member admission, and its terminal ledger. A
member releases only its own resources; the group owner releases the group and
reservation. The current integration uses one invocation member per group.
Empty and all-NULL work is completed locally by the evaluator, without a remote
group. For admitted work, partial Open, cancellation, and close have one owner
and a terminal outcome. The worker owns H, child process/session cleanup, and
its own ledger. Generation/lease fences reject stale callbacks and messages. A
terminal TTL alone never grants a new execution epoch. On cancel, timeout, or
transport failure, Gateway cancels/closes the exchange before returning its
local K or ledger ownership; the worker retains H and cleanup ownership until
the child is terminated and reaped. STARTED work is not transparently replayed.

Worker handler children are process-group scoped and must be terminated and
reaped by the worker's cleanup owner before H is returned. A failed or partial
cleanup retains bounded cleanup ownership instead of releasing H early. Linux
process-tree failure tests provide implementation evidence; this process model
does not confine a handler's access to the host or its Python heap.

## Resource budgets and trust boundary

Zero in an enabled CN client config means the runtime default, not unlimited.
The current defaults are:

| Resource | Owner and default | Contract |
| --- | --- | --- |
| K, active invocations | CN Gateway: 8 per CN | Non-blocking admission; a full budget rejects rather than queueing while holding input vectors. |
| W, stream window | Gateway/worker: 1 batch | No multiple in-flight batches or cumulative ACK. |
| H, handler subprocesses | Worker: 8 total, 8/account, 4/owner | Worker constants and capability contract; admission is all-or-nothing. |
| Batch | CN/worker: 16 MiB and 65,536 rows | Each Arrow batch is bounded before transfer. |
| Invocation | CN/worker: 1,048,576 rows; CN result: 256 MiB | Result bound is per invocation. At K=8, retained result vectors alone can reach 2 GiB per CN. |
| Request | CN: 30 s default; worker ACK: 60 s | Configured handler timeout and protocol ACK timeout have distinct owners. |
| CN terminal state | 8,192 entries, 8 MiB, 10 min | Active entries plus tombstones and separately reserved closed-group fences are bounded. |
| Worker terminal state | 10,000 entries, 16 MiB, 5 min | Independent bound; worker values intentionally differ from CN values. |
| Worker pending cleanup | 10,000 invocations | Derived from worker ledger capacity; cleanup failure retains ownership rather than releasing H. |
| Same-invocation burst reuse | Worker: 64 batches, 64 MiB, or 30 s | The first reached limit ends reuse; statement deadline and started inputs do not reset. |

The per-invocation result maximum multiplied by K is a deliberate worst-case
capacity implication, not a process-wide RSS limit. Arrow/IPC, pickle/pipe,
temporary snapshots, materialization, Python heap, queues, file descriptors,
and child processes have different owners; wire-byte limits do not cap arbitrary
Python heap growth. Artifact count and total FileService bytes are not capped.
The CN's completion retention is longer than the worker's (10 versus 5 minutes);
the limits are compatible, not equal. The CN retains its own terminal/group
fences and never transparently resends STARTED work after a worker tombstone
expires.
The adapter is unisolated, Flight has no TLS/authentication in this stage, and
the fencing tuple is not an authentication token. These limits support
test/development correctness and backpressure, not hostile-code containment.

## Evidence and remaining limits

| Contract | Evidence on the reviewed branch/head | Status |
| --- | --- | --- |
| Shared revision schema and SQL legacy fallback | `pkg/bootstrap/versions/v4_0_7/upgrade_test.go`; `pkg/frontend/python_udf_catalog_test.go` | Unit/schema contract covered; upgrade-compatibility CI was skipped. |
| Exact FunctionRef and overload invalidation | `pkg/frontend/routine_plan_validation_test.go`; `pkg/frontend/routine_namespace_test.go`; `test/distributed/cases/udf_python/overload_namespace.sql` | Unit and SQL prepared-plan cases present. |
| SQL expression placement and result semantics | `pkg/sql/colexec/external_routine_eval.go`; `test/distributed/cases/udf_python/relational_positions.sql`, `dml_positions.sql`, `vector_mode.sql` | Physical evaluator and ordinary BVT cases present. |
| Current-contract restore | `pkg/frontend/clone_database_source.go`; `pkg/frontend/snapshot_catalog_restore.go`; `snapshot_restore.sql` | Same-contract account restore case present; cross-version restore not verified. |
| Flight state, ACK, cancellation, resource limits | `pkg/udf/python/gateway_test.go`, `gateway_admission_test.go`, `capability_lifecycle_test.go`, `gateway_integration_test.go`, `pkg/udf/python/worker/test_worker.py`, `test_watchdog.py` | Unit, process, and real Flight cases present; this design does not substitute for implementation/lifecycle review. |
| Exact-head CI | PR #29152 validation reported shared build, Linux UT, SCA, coverage, and active multi-CN BVT passed | Upgrade-compatibility checks skipped; no downgrade evidence. |

The reviewed change is not declared production-ready. Artifact GC/total-storage
quota, cross-version migration rollback/restore, authenticated transport, and
sandboxing remain explicit limits. The owner-approved acceptance is limited to
the test/development stage. This approval closes the current-stage design gate;
the reviewer must still complete the implementation/lifecycle review before the
PR review is complete.
