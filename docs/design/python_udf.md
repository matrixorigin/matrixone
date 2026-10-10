# Python UDF implementation and rollout scope

This describes the implemented, owner-authorized external Python adapter and its
PR acceptance boundary. The relevant current-stage Catalog, SQL/planner,
migration/restore, Python ABI, Flight, and resource contracts are consolidated
in the [versioned current-stage design](python_udf_current_stage.md). Broader
long-term designs continue to inform future work; this page does not approve a
production tenant-isolation model.

## Owner-approved stage scope

On 2026-09-24, feature owner `iamlinjunhong` approved this PR for testing and
development-stage acceptance. The user guide and SQL examples are recorded in
[issue #28132](https://github.com/matrixorigin/matrixone/issues/28132#issuecomment-5748253137).
This PR does not claim production rollout, sandbox isolation, protection for
untrusted Python, or authenticated/TLS Flight transport. Those remain separate
production acceptance gates. Python stays disabled in generic launch and requires
explicit opt-in in the test/development configurations.

This stage decision is the feature owner's authorization for the current PR
scope; it is not a claim that Architecture, Security, SQL/Planner, Runtime,
Catalog, or Operator owners have approved the future production design.

## Execution and identity

CREATE/REPLACE validates Python syntax and the handler contract before publishing
a definition. The shared Catalog stores immutable typed revisions and complete
argument/return descriptors. Artifacts are digest-addressed, account-scoped and
published through FileService; worker execution receives the verified artifact
from CN. The worker needs no object-storage credentials or query-time package
installation. Old demo definitions are inert: explicitly recreate them using the
current DDL and SDK. Unknown definition, plan, Arrow, SDK or wire contracts fail
before user execution; no legacy fallback or automatic adapter is provided.

Planning resolves an exact FunctionRef and RoutineCall. Prepared/cache reuse
validates the selected revision and the complete overload namespace state; see
[routine namespace validation](routine_namespace_validation.md). Execution never
resolves a mutable latest revision. Clone and restore preserve supported catalog
contracts and republish Python artifacts for the destination account.

CN keeps native MO vectors. The expression framework dispatches typed Python calls
to ExternalRoutineEval; the parent evaluator owns argument evaluation, casts,
CASE/selection and NULL guards. The physical stage compacts selected rows,
admits bounded asynchronous work and scatters verified results, preserving empty
input, NULLs, zero arguments, nested calls and default VOLATILE semantics.

The Gateway is a CN library, not a separately deployed service. It connects to
Arrow Flight served by the Python worker. No second Flight daemon is required.
A worker creates bounded handler subprocesses; each handler/module can be reused
only within the invocation's bounded burst. It does not pool user heaps across
invocations. SCALAR handlers receive `(ctx, scalar...)`; VECTOR handlers receive
`(ctx, arrays...)` and use `ctx.num_rows` for output shape. Arrow field/schema
metadata comes from the trusted adapter, not handler-supplied identity.

## Completion and resources

K bounds active Gateway invocations, H bounds handler execution slots, and W=1
bounds stream batch progress. Admission, group/member state, terminal records and
per-account/per-owner quotas have independent owners and budgets. Protocol steps
include EndInput, InputConsumed, result ACK and Finish/FinishAck. Started work is
never transparently replayed. Cancellation and capability refresh waiters progress
independently; generation fences reject stale completions.

Process cleanup must cover both termination and reaping. Handler sessions own
pipes, child/watchdog processes and H/quota until cleanup succeeds; partial
initialization transfers a failed session to the same bounded pending-cleanup
owner. Linux containers run `/usr/bin/tini -- python -u worker.py`; init adopts
orphaned descendants after their handler leader exits. An Operator command override
must retain this init process. Native Supervisor launches rely on the host's init
(or an explicit subreaper in a container test harness).

## Enablement and deployment boundary

Generic launch keeps Python disabled. The explicit worker-enabled launch and
ordinary BVT entry point enable the unisolated adapter. See
[Python BVT and local launch](../../test/distributed/cases/udf_python/README.md).
Compose configures a separate worker endpoint for each CN. The `PYTHON_UDF`
mo-service role wraps a Go Supervisor; the standalone worker image runs Python
under init. These are different launch forms for the same Flight service.

Gateway and worker must match the current capability contract, including tzdata.
The worker image replaces its entire zoneinfo tree from the MO runtime base;
upgrading tzdata also changes the environment digest of definitions. Check existing
definitions before enabling an incompatible environment and explicitly rebuild
where required. Matching software version strings alone are not this check.

`enabled` and `allow-unisolated` are explicit gates. This implementation does not
provide sandbox isolation or authenticated/TLS Flight transport. Same-Pod Operator
configuration binds the worker to loopback and controls Pod composition; it does
not add broad CN ingress grants. External worker exposure needs a separately
approved security/deployment design. Worker availability is a Python execution
condition, not evidence of production tenant isolation.

## Verification and remaining scope

The regression suite covers scalar/vector Arrow computation, types and malformed
values, guarded SQL positions and DML atomicity, revision/overload invalidation,
exact-descriptor DROP and cross-account snapshot restore. Runtime tests cover
ACK/Finish, cancellation, quotas, initialization/cleanup failure, worker death,
stale events and real Flight consumers. Operator status wire fixtures pin the
producer/consumer error contract.

Linux PR validation uses the repository CGo wrapper, two CNs with distinct
Supervisor/worker endpoints, ordinary SQL/Python BVT, current worker image and
process-tree fault injection. Record the exact revision, selected tests, image
and terminal result with each run; unit or CNI echo tests are not complete
Operator rollout evidence.

Sandbox, production deployment approval, imported dependency environments and
W>1/cumulative ACK enablement remain separate work. W=1 measurements do not prove
that a wider window has no benefit. This PR is related to issue #28132 and does
not close that issue's broader isolated-runtime acceptance criteria.
