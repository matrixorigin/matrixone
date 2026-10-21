# Incomplete temporal strings — revision 9

Issue: #30234. Implementation: https://github.com/matrixorigin/matrixone/pull/30311.
Status: revision 7 semantics were independently approved on 2026-10-18.
Revision 9 updates the official successor epoch and preserves invalid-string conversion behavior; its
implementation design was reviewed by gpt-6-astra / medium. Independent review
of this revision remains external to this repair.

## Problem and invariant

This is a consumer-specific compatibility fix: formatting preserves incomplete
components while strict calendar calculations return NULL with diagnostics.
A global cast or sql_mode-dependent rejection cannot substitute for each
consumer rule. RPC versioning does not implement those date rules: it prevents
old workers from executing unknown appended identities or old warning behavior.

String inputs with incomplete calendar components must reach their consuming
function without a global DATE/DATETIME conversion. The PR appends execution
identities for DATE_FORMAT, DATEDIFF, TO_DAYS, TO_SECONDS, DAYOFWEEK and WEEK,
and adds rejection warnings to existing string LAST_DAY and DATE_ADD/SUB paths.
An older worker either lacks the new identity or produces different diagnostics.
The existing signed WEEK result detector also misses new overloads 4 and 5,
causing the folded-view admission regression in run 38751428918, attempt 1.

Every expression using these contracts must require MORPC 127. Official main
7038c8520e5fc1ba456914b3c389ccedb69e22f7 has latest version 126; the candidate
is latest + 1 and must be recalculated if main changes before delivery.
Version 118 remains reserved for transactional database defaults and table charset alteration;
version 119 remains reserved for coordinator-owned parameterized subquery programs.
Official 120 KILL, 121 unsigned DECIMAL DIV, 122 TIMESTAMP/TIME contracts,
123 nonblocking row locks, 124 fractional TIMESTAMPADD SECOND, 125 LOAD
IGNORE FK diagnostics and 126 parser-derived view metadata retain their own gates. This PR appends only 127.
Official v115 CalendarIntervalNormalization remains independently detected and gated;
Official v116 SELECT division warnings retain their execution-only floor;
official v117 collation-aware LEAST/GREATEST ordering retains its own floor;
expressions combining these capabilities require the maximum floor, 127.
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
127. Actual destination validation probes the Query RPC endpoint again for every
send. That connection can reach a different worker generation from the execution
connection. After variable folding and final pipeline encoding, affected fragments
carry an execution requirement of max(existing vector requirement, 127). The
sender performs the existing protocol handshake on the very stream that will
carry the pipeline, before sending any execution payload. Only a correctly
identified terminal handshake response advertising at least 127 admits it; an
old, unknown, malformed, closed or canceled stream fails closed. Query RPC 127
with an execution peer at 126 therefore sends no pipeline payload. Receiver
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
pre-PR v126 workers to receive unknown overloads. Reusing typed overloads and
encoding zero components in DATE/DATETIME would violate the storage contract.
Disabling remote execution for all temporal functions would unnecessarily
restrict unaffected typed expressions. A single additional expression feature
uses the established version-floor mechanism with the smallest boundary.

Upgrade all CN binaries before authoring these expressions at floor 127.
During mixed-version rollout, one-CN fallback is safe on the upgraded
coordinator. A rollback of the effective floor to 126 rejects newly authored
persisted expressions and cached affected pipelines before operator Prepare; existing
typed WEEK expressions retain floor 112. Catalogs containing new floor-127
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
folded VIEW authoring and persisted owners reject 126 and accept 127; typed
WEEK still rejects 111 and accepts 112. Placement, actual send, receiver decode
and Prepare accept 127 and reject its immediate predecessor. A production-bound
projection with Query RPC 127 and execution peer 126 must perform one handshake
and send no payload; peer 127 admits the payload on that same stream. Tests
cover sender cleanup on rejection, malformed replies, send errors and cancellation,
plus nested/folded pipelines and composition with vector floors, including rollback
after an earlier success. A real-bound composition of string WEEK, calendar
DATE_ADD, SELECT division and collation-aware LEAST retains all predicates;
LEAST alone remains at 117 while the combined expression requires 127.
Run focused tests, owning packages and incremental
static checks. Reuse the PR's passing BVT evidence for unchanged value semantics.
The bounded feature predicates require no new cluster fixture. Measure the
TO_SECONDS hot path with the existing real-bound expression benchmark; scalar
CPU measurements do not establish whole-query throughput.

## Result metadata and source-qualified predecessor

DATE_FORMAT literal-pattern widths cover actual raw-calendar output, including
nine-digit week values for %U/%u/%V/%v and ten-digit %X/%x week-years. Padding
is a minimum, not a maximum. TIME_FORMAT retains its existing bounds. The
2020-00-00 / %v case must preserve 613566753 through CTAS and later INSERT,
with catalog width 9; typed/literal/dynamic pattern controls remain covered.

At official main 7038c8520e5fc1ba456914b3c389ccedb69e22f7 (MORPC126),
list_builtIn.go blob 16b2c8a64c9f2f31be9de1814ae0e95f7c706a60 registers
DATE_FORMAT only 0/1, DATEDIFF and TO_DAYS/TO_SECONDS only 0, WEEK 0–3,
and DAYOFWEEK 0–2. An AST inspection of that exact source verifies that all
new string identities are absent and released overload 0 remains present.
This source-qualified predecessor evidence is paired with 126-reject/127-accept
placement, persisted, send/stream and receive tests. It is not execution of an
old binary or a claim that lowering the new binary's floor changes its registry.
Independent 121/122/124/126 features retain their own floors and compose with 127.

## Integration with prepared metadata and catalog origins

For direct string/unknown DATE_ADD/SUB markers, retain raw components through
arithmetic so incomplete-input warnings are not lost inside a pre-arithmetic
TIMESTAMP conversion. Apply the original SQL unit's DATE or DATETIME(6) result
cast afterward. Existing explicit casts and known non-string inputs retain the
released path. When an unknown template specializes to a numeric or typed
source, retain main's TIMESTAMP interpretation before raw arithmetic; numeric
epochs must not turn into calendar digits. NULL intervals suppress diagnostics.

DUMP/LOAD verifies complete historical binding eras. A private verification
context reconstructs the released registry for the six appended string
functions and DATE_FORMAT's old %X/%x/%v bounds, together with applicable
nullability and interval eras. Every candidate still must equal the entire
supplied tree; mixed or forged metadata is rejected. Ordinary query/DDL binding
never uses this context. Frozen generated-expression fixtures were captured
on the historical 838cc78aa1efe46693876a48c05c202979047b7b / MORPC125
source overlay, covering %Y-%m-%d, %X, %x and %v. Historical main numeric
prepared SELECT/CTAS behavior was also checked through that overlay. These
fixtures are distinct from the current 7038c852 / MORPC126 registry proof above.

Revision 9 conversion correction: malformed complete strings in DATE_FORMAT retain
normal expression-cast behavior (NULL without diagnostics), while raw incomplete
calendars, zero-date SQL modes and fractional precision keep their existing rules.
The six failing BVT inputs are regression controls; invalid rows must not abort
valid neighbors or poison a reused prepared statement. View metadata alone keeps
floor 126 and composition with temporal strings requires 127.
