# Incomplete temporal strings — revision 6

Issue: #30234. Implementation: https://github.com/matrixorigin/matrixone/pull/30311.
Status: proposed; independent approval remains external to this repair.

## Problem and invariant

String inputs with incomplete calendar components must reach their consuming
function without a global DATE/DATETIME conversion. The PR appends execution
identities for DATE_FORMAT, DATEDIFF, TO_DAYS, TO_SECONDS, DAYOFWEEK and WEEK,
and adds rejection warnings to existing string LAST_DAY and DATE_ADD/SUB paths.
An older worker either lacks the new identity or produces different diagnostics.
The existing signed WEEK result detector also misses new overloads 4 and 5,
causing the folded-view admission regression in run 38751428918, attempt 1.

Every expression using these contracts must require MORPC 119. Official main
8ed73ff815 has latest version 118; the candidate
is latest + 1 and must be recalculated if main changes before delivery.
Version 118 remains reserved for transactional database defaults and table charset alteration.
Official v115 CalendarIntervalNormalization remains independently detected and gated;
Official v116 SELECT division warnings retain their execution-only floor;
official v117 collation-aware LEAST/GREATEST ordering retains its own floor;
expressions combining these capabilities require the maximum floor, 119.
Division warning policy does not add a persisted expression requirement.
Independent signed-result and session-snapshot requirements remain intact.
Typed temporal storage, global casts and released overload identities do not
change. SQL value/warning semantics follow the existing issue acceptance matrix
and function tests; this document closes their distributed compatibility.

## Owners and flow

The function registry owns identities. The protobuf expression walker detects
`IncompleteTemporalStrings` by numeric function/overload identity, never names
or constant values: DATE_FORMAT 2; DATEDIFF 1–3; TO_DAYS/TO_SECONDS 1;
DAYOFWEEK 3; WEEK 4/5; LAST_DAY 0/1; DATE_ADD 2/3/6; DATE_SUB 2/3/5.
WEEK 0–5 with INT32 results additionally retains its v112 result feature;
one-argument WEEK 0/1/4 retains its session-snapshot feature.

The existing planner observer records the maximum requirement before folding
and persists it with VIEW SQL. Runtime and authoring floors reject unsupported
persisted owners. Placement falls back to one CN when a peer cannot support
119. Actual destination validation probes the Query RPC endpoint again for every
send. That connection can reach a different worker generation from the execution
connection. After variable folding and final pipeline encoding, affected fragments
carry an execution requirement of max(existing vector requirement, 119). The
sender performs the existing protocol handshake on the very stream that will
carry the pipeline, before sending any execution payload. Only a correctly
identified terminal handshake response advertising at least 119 admits it; an
old, unknown, malformed, closed or canceled stream fails closed. Query RPC 119
with an execution peer at 118 therefore sends no pipeline payload. Receiver
validation rejects an insufficient or unknown floor before constructing
operators. Compile and Run admission recheck the expression floor before pipeline
initialization and operator Prepare, including cached executions after a protocol
rollback. No capability cache or
new resource owner is introduced. The existing five-second handshake timeout
and parent cancellation bound the connection fence. On rejection remoteRun
returns its sender to Scope.RemoteRun, which closes it through the existing
cleanup owner; no stop-response wait is needed before a payload has started.

## Alternatives and rollout

Broadening only the v112/v98 WEEK detectors fixes the assertion but allows
pre-PR v118 workers to receive unknown overloads. Reusing typed overloads and
encoding zero components in DATE/DATETIME would violate the storage contract.
Disabling remote execution for all temporal functions would unnecessarily
restrict unaffected typed expressions. A single additional expression feature
uses the established version-floor mechanism with the smallest boundary.

Upgrade all CN binaries before authoring these expressions at floor 119.
During mixed-version rollout, one-CN fallback is safe on the upgraded
coordinator. A rollback of the effective floor to 118 rejects newly authored
persisted expressions and cached affected pipelines before operator Prepare; existing
typed WEEK expressions retain floor 112. Catalogs containing new floor-119
owners require upgraded readers or removal/rebinding before binary downgrade;
no automatic catalog rewrite or claim that an old binary enforces a future
floor is made. Backup/restore preserves the existing owner metadata. Failures
use the existing unsupported-protocol diagnostics and acquire no new resources.

## Cost and verification

Detection adds bounded comparisons per visited function and one boolean in the
existing feature set. The walker remains linear in expression size. Compile/Run
admission takes a constant-time fast path at a supported floor; an insufficient
or unknown floor adds one owner traversal. Query RPC probe count is unchanged.
Each affected remote fragment adds one bounded execution-stream handshake round
trip before its payload (shared with any existing vector requirement, not a
second handshake). Unaffected typed temporal and ordinary literal projections
add none. Memory retention, background work and statement warning ownership
are unchanged. No new SQL privilege or external trust boundary is introduced.

Acceptance: serialized detection covers every affected identity plus typed and
unknown-overload controls; one-argument string WEEK requires a captured default;
folded VIEW authoring and persisted owners reject 118 and accept 119; typed
WEEK still rejects 111 and accepts 112. Placement, actual send, receiver decode
and Prepare accept 119 and reject its immediate predecessor. A production-bound
projection with Query RPC 119 and execution peer 118 must perform one handshake
and send no payload; peer 119 admits the payload on that same stream. Tests
cover sender cleanup on rejection, malformed replies, send errors and cancellation,
plus nested/folded pipelines and composition with vector floors, including rollback
after an earlier success. A real-bound composition of string WEEK, calendar
DATE_ADD, SELECT division and collation-aware LEAST retains all predicates;
LEAST alone remains at 117 while the combined expression requires 119.
Run focused tests, owning packages and incremental
static checks. Reuse the PR's passing BVT evidence for unchanged value semantics.
No throughput claim, benchmark or new cluster fixture is needed for the bounded
feature predicates. Independent design approval is not asserted by this file.
