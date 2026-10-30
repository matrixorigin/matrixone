# Python UDF current-stage design contract

| Field | Value |
| --- | --- |
| Design revision | `python-udf-current-stage-r38-2026-10-30` |
| Applies to | MatrixOne PR #29152, test/development-stage Python UDF |
| Upstream repair baseline | `bbfe55669a1440424914d45a283a1d0e36f722fa` (unrelated existing MORPC gates through 134 and the frozen consolidated 4.0.7 catalog preserved) |
| Approval | Feature owner approved the r1 current-stage contract on 2026-09-24 in [PR comment](https://github.com/matrixorigin/matrixone/pull/29152#issuecomment-5809313638). The r38 contract preserves the official consolidated and frozen 4.0.7 catalog, follows it with the separate shared-routine repair in 4.0.19 after the official 4.0.18 COLUMNS repair, and follows full-cluster shutdown upgrade, with no Python/shared-routine MORPC capability gate. Independent Architecture and SQL/Planner approval of this revision is pending; no production or security scope is added. The immutable approval artifact and independent-review status remain recorded in the [design approval record](python_udf_current_stage_approval.md). |
| Not covered | Production tenant isolation, sandbox, Operator rollout, or cross-version rollback/restore |

This is the versioned governing proposal for the current PR stage. The owner
approved the historical r1 scope; that approval is not independent technical
approval of this revised contract. It consolidates
the shared Catalog, SQL/planner, Python ABI, Flight, and resource-lifecycle
decisions needed to review this implementation. It does not approve future
sandbox or production deployment. The test/development scope is explicit:
Python is opt-in and unisolated; old demo definitions are rejected and must be
recreated through the current DDL. There is no legacy Python adapter or
fallback execution path.

## Identity, Catalog, and publication

Python functions use the shared `mo_user_defined_function` identity namespace.
The complete JSON routine body must fit the catalog TEXT capacity (65,535
encoded UTF-8 bytes) before source artifact publication or catalog mutation.
This includes source escaping and all typed metadata; raw artifact size alone
is insufficient. Rejected CREATE/REPLACE does not publish a new catalog revision.
Definition validation also checks definition-owned Open content against the
external control budget; the complete invocation-dependent envelope remains
checked at execution. TEXT/BLOB descriptor widths retain SQL byte limits
(including TINYTEXT/TINYBLOB); CHAR/VARCHAR widths count Unicode characters.
Existing width-zero descriptors keep unspecified-width semantics. DECIMAL
UNSIGNED/ZEROFILL declarations are rejected before narrowing plan metadata: the
current external descriptor has no numeric-attribute channel. Plain DECIMAL
and existing SQL UDF type-name semantics remain unchanged. String-domain
inference consumes frozen RoutineCall return types rather than the legacy
Python synthetic-argument checker. These corrections add no persisted format
version, in-cluster gate or transparent retry.
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
follows worker definition validation and precedes catalog commit. A failed
validation writes no object. A later failed transaction may retain an
unreferenced object, charged to a finite shared FileService admission budget.
The r37 repair admits at most 1,024 new account/digest identities across the
whole backing store, each with at most 1 MiB of source and an immutable identity
of at most 85 bytes. The budget is global, not reset by a CN restart, account,
database, transaction, DROP, or REPLACE. Exhaustion rejects new object
publication before writing source; resolving or republishing existing objects
continues. This is a conservative development-stage capacity limit, not a
production quota/GC service or a throughput claim.

Admission uses the existing FileService exclusive-create primitive with no
check-then-write fallback. Each fixed slot holds one account/digest identity;
publishers probe a bounded set from a deterministic starting slot. The same
identity reuses its slot after an interrupted write; concurrent different
identities cannot own the same slot. Source publication is also exclusive and
an existing object must pass its exact handler/content digest check. Unsupported
backends fail closed before either reservation or source write. Slots are never
reclaimed automatically, including after failed catalog commit: a failed
publisher cannot delete a digest another revision or snapshot may use.
Replacement, DROP and historical snapshots retain their existing catalog/MVCC
ownership and immutable objects. No reference scan, background worker or
delete operation is added. Pre-r37 objects remain readable and unchanged; the
new budget bounds additional retained growth, not that finite pre-existing
baseline. Backups must preserve the admission namespace alongside artifacts;
an absent namespace in an older backup starts the bounded additional budget.

The type descriptor is canonical over SQL type ID and applicable width, scale,
charset, offset-width, JSON, and temporal encodings. The trusted adapter builds
Arrow field/schema metadata from that descriptor; it validates both physical
Arrow type and the SQL value domain. Handler-provided metadata cannot redefine
the function type. CREATE/REPLACE compiles the submitted module and checks the
declared synchronous module-level handler; it does not execute top-level user
code or install packages. The environment digest includes the current runtime
environment, including tzdata. An environment mismatch fails closed; definitions
must be explicitly recreated for a changed contract.

MEDIUMINT, signed or unsigned, is unsupported by the current Python ABI. Its
24-bit SQL value domain must not be normalized into an ordinary 32-bit integer
descriptor. Python argument and return declarations reject it before artifact,
worker, or catalog publication; physical input validation also rejects it.
Ordinary INT and INT UNSIGNED retain their existing 32-bit contract.

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
Shutdown upgrade replaces every binary before services resume. When historical
catalogs or snapshots lack `mo_function_revisions`, SQL UDFs retain the legacy
`mo_user_defined_function` row path. When the revision catalog exists, writers
publish the compatibility row, immutable revision and active head in the same
caller-owned transaction, and readers use the selected revision. No runtime
protocol floor selects between these paths; only persisted schema availability
and validated catalog contents do.
A non-zero head with a missing or malformed revision is a catalog error; it is
never silently repaired or synthesized. The SQL UDF language, body, security,
and evaluation semantics remain owned by the existing SQL path. The Python
demo's old execution behavior is not a compatibility promise.

The official consolidated `4.0.7` handler, entry list and offset are frozen.
The nine idempotent shared-function schema entries belong exclusively to the
registered `4.0.19` repair, whose minimum direct source is `4.0.18`. Released
sources below that version finish the official upgrade route, including the
bounded `4.0.7` cluster and tenant stages and the official `4.0.18` COLUMNS
repair, before the routine repair. A tenant already at official `4.0.18` receives
`4.0.19` directly; neither published predecessor is redefined.
Unpublished intermediate `4.0.8` through `4.0.17` handlers are not restored.
The consolidated route rejects those sources unless an explicit handler is
registered. Cluster admission, pending-route recovery, login/manual reads and
their locked rereads, and scheduled tenant batches reject them before catalog
mutation or completion caching. They cannot skip the consolidated migration
merely because they satisfy the repair's minimum version.

Appending the repair does not bypass the predecessor's safeguards: reject a
ready `4.0.7` with a different offset and any `4.0.7` self-upgrade route before
catalog mutation. The existing legacy CDC preflight follows an unfinished
`4.0.7` step, including recovery/recheck of a route targeting `4.0.19`; an
already-completed `4.0.7` repair-only route does not reclassify current CDC tasks
as legacy. Snapshot decisions and paged cluster work retain their original
step owner. Login and manual compensation continue from committed `4.0.7`
paging through the routine repair before caching completion; a failed repair
remains retryable and never marks the tenant checked.

Missing, partial and complete routine schemas converge through the existing
idempotent checks. Repair metadata does not require a feature protocol version.
`Prepare` only selects `mo_catalog` and performs no DDL. Python DDL probes the
required tenant schema and refuses execution until it is present. Cached/prepared
plans still validate exact catalog identity and namespace; physical evaluators
still validate account, descriptors and the external worker contract.

The shared `Query` protobuf preserves official `correlation = 19` and
`integer_precision = 20`; this PR uses the next free field,
`routine_dependencies = 21`. All three survive serialization, generated
descriptor lookup and deep-copy without aliasing. The previously proposed
routine field 20 was never a merged allocation; it is replaced rather than
read as an alternative tag, since official field 20 has different semantics.
Routine dependencies belong to executable plans and in-memory cache/prepared
validation. Catalog revisions persist typed definitions and fingerprints,
not this Query envelope. Rebuild plans and sessions on the supported full-cluster
shutdown upgrade; old unmerged PR plan/cache blobs are not a persistence or
mixed-binary compatibility contract. Routine metadata introduces
no placement, destination capability probe, stream floor or receiver version
gate. Unrelated existing gates, including dependent programs, remain unchanged.
The read-only `GetPythonUdfStatus` uses query method 47, request field 49 and
response field 50 without a feature-version check. Session KILL/disconnect
methods 45/46 retain their existing capability 120. External worker capability
negotiation remains a separate exact-contract check.

The migration has no down handler. Do not manually remove the revision table,
columns, or index, and do not claim that restoring an old binary restores Python
execution. Binary rollback after applying this catalog migration is not a
supported recovery path in this contract. In-cluster mixed binaries and binary
downgrade are unsupported; no per-feature admission state is maintained for them. Python definitions must remain intact and
require a current-contract binary to execute. The current evidence does not
include a tested downgrade after Python definitions have been written.
Historical skipped upgrade lanes are not claimed as passing evidence; persisted
legacy catalog and same-contract restore behavior retain their explicit tests.

Historical routine restore first inspects `sql_mode` in the source account at
the selected snapshot, before dropping or replacing target rows. Only a proved
missing column uses the released `PIPES_AS_CONCAT` default; metadata errors fail
before mutation. Stored procedures retain the official current-schema path.
For UDFs, complete descriptor/heads are preserved when present, revision-only
snapshots retain their heads, and older snapshots get explicit zero/empty
identity fields. Immutable revision rows retain the existing language-specific
validation. This projection does not claim arbitrary historical SQL parser
semantics are preserved across released versions.

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
terminal TTL alone never grants a new execution epoch.

The external capability contract requires `group_identity_contract=expiring-v1`.
A transport group is `py1/<not-after-unix-ms>/<canonical UUID>`, issued for ten
minutes and renewed by the evaluator with five minutes remaining. Statement,
account and invocation identities remain separate. The deadline is inseparable
from the group ID: changing it creates a new identity. Every new Open rejects
legacy, malformed or expired group identities before resource admission. A
higher epoch or different invocation ID cannot reopen an expired group.
Gateway and worker collect only inactive closed fences after this deadline;
active invocations and pending cleanup retain ownership even past expiry.
Terminal records additionally retain their existing TTL before collection.
Each owner advances its prior logical admission time by nonnegative monotonic
elapsed time, then takes the maximum of that result and current wall time.
This keeps retirement progressing even after a forward clock jump and rollback;
rollback cannot resurrect a retired identity. Admission rejects deadlines more
than fifteen minutes ahead (ten-minute lease plus five-minute skew allowance),
so even producer clock errors cannot pin accepted fences indefinitely.
Clock skew may reject a group early; it never authorizes replay after
local retirement. Worker restarts retain the existing instance-lease fence.
Finite retention windows may still cause temporary resource exhaustion under
load; completion no longer consumes permanent lifetime group capacity. This
contract does not add authentication, a new scheduler, MORPC gates or retries.

On cancel, timeout, or transport failure, Gateway cancels/closes the exchange before returning its
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
| Shared revision schema and shutdown-upgrade behavior | `pkg/bootstrap/versions/v4_0_19/upgrade_test.go`; `pkg/frontend/python_udf_catalog_test.go`; prepared, evaluator and remote payload regression tests | Schema-based historical SQL compatibility, malformed-head rejection, normal revision publication, evaluator execution without protocol state, and no routine-only destination probes. Real first-login repair models released 4.0.6, frozen 4.0.7 and official 4.0.18 sources, preserves legacy SQL results, and verifies idempotent replay. External ABI/Flight negotiation remains independently tested; old per-feature rollback assertions are superseded. |
| Exact FunctionRef and overload invalidation | `pkg/frontend/routine_plan_validation_test.go`; `pkg/frontend/routine_namespace_test.go`; `test/distributed/cases/udf_python/overload_namespace.sql` | Unit and SQL prepared-plan cases present. |
| SQL expression placement and result semantics | `pkg/sql/colexec/external_routine_eval.go`; `test/distributed/cases/udf_python/relational_positions.sql`, `dml_positions.sql`, `vector_mode.sql` | Physical evaluator and ordinary BVT cases present. |
| Current-contract restore | `pkg/frontend/clone_database_source.go`; `pkg/frontend/snapshot_catalog_restore.go`; `snapshot_restore.sql` | Same-contract account restore case present; cross-version restore not verified. |
| Flight state, ACK, cancellation, resource limits | `pkg/udf/python/gateway_test.go`, `gateway_admission_test.go`, `capability_lifecycle_test.go`, `gateway_integration_test.go`, `pkg/udf/python/worker/test_worker.py`, `test_watchdog.py` | Unit, process, and real Flight cases present; this design does not substitute for implementation/lifecycle review. |
| Historical CI at the reviewed implementation baseline | PR #29152 baseline `9847da80d6c7096cb0460e46a5cf710e9714af3a`; [MatrixOne ALL CI run 35953275189](https://github.com/matrixorigin/matrixone/actions/runs/35953275189) passed | Exact-head run 35988821412 is tracked separately; its UT coverage producer failed, so it is not passing coverage evidence. Upgrade-compatibility checks skipped; no downgrade evidence. |

The reviewed change is not declared production-ready. Artifact GC/total-storage
quota, cross-version migration rollback/restore, authenticated transport, and
sandboxing remain explicit limits. The owner-approved acceptance is limited to
the test/development stage. It closes the feature-owner scope record for that
limited stage; it does not close the independent Architecture or SQL/Planner
approval gate, which remains pending in the approval record. Implementation and
lifecycle review, as well as any broader enablement, remain separate decisions.
