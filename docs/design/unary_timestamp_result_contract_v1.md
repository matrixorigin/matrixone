# Unary TIMESTAMP result contract, revision 1

Status: implemented compatibility design; independent review pending.
PR: https://github.com/matrixorigin/matrixone/pull/30224
Review addressed: https://github.com/matrixorigin/matrixone/pull/30224#pullrequestreview-5525155127

## Invariant

A serialized function identity has one physical result-vector type. Unary
`TIMESTAMP` now binds a DATETIME result, preserving civil time and source FSP;
reusing its released TIMESTAMP identities lets an older worker allocate the
wrong result wrapper and panic during a rolling upgrade.

## Identities and admission

| Encoded identity (function 250) | Physical result | Binding | Required protocol |
| --- | --- | --- | --- |
| 0–4 (DATE, DATETIME, TIMESTAMP, VARCHAR, CHAR) | TIMESTAMP | Execution of released expressions only | Existing temporal floor 98 |
| 5 (two arguments) | DATETIME | Unchanged | Existing temporal floor 98 |
| 6–10 (corresponding unary signatures) | DATETIME | All new unary binding, including PREPARE/rebind | 122 |

Official main `873282742714436bff407dfcdbbe879147162bd3` advertises protocol 121;
this single capability uses the next value, 122. A later rebase must allocate
again from official main and update all gates and predecessor tests together.
No protobuf fields or RPCs are added. The existing registry binding filter
selects new identities while ID-based execution retains the released handlers.
An older binary has no unary identities 6–10; it cannot dispatch them to a
released TIMESTAMP handler.

The common expression walk identifies new unary IDs and rejects an ID/result
shape mismatch. Its independent feature is included in worker placement,
sender rollout validation, the actual destination probe before encoding, and
receiver validation after decoding and before operator construction. An old or
unknown destination causes local placement or a send error, never a downgrade
of the expression's result type. Capabilities are observed again at each send.
Other temporal contracts remain at protocol 98.

The same walk computes catalog read and authoring requirements for defaults,
on-update expressions, generated columns (including functional-index backing
columns), checks and view queries. The existing pre-fold view observer stores
122 even when binding folds the expression to a literal. Existing catalog
admission and its durable required floor continue to own publication and CN
readiness. Local executor construction validates the ID/result shape before
allocating a result wrapper; valid local execution needs no per-row protocol
check. PREPARE specialization must bind the appended identity and retain its
DATETIME metadata and value contract.

## Upgrade and rollback

Before capability 122 is enabled, old catalog unary expressions continue to
execute with their original TIMESTAMP handlers and timezone semantics on new
binaries. New DATETIME unary expressions can execute locally, but cannot be
sent to protocol 112/121 workers or published below read/authoring floor 122.

After catalog expressions requiring 122 are published, downgrading the binary
alone is unsupported: the existing durable admission floor keeps an older CN
out. Removing or rebinding those dependencies must follow the existing catalog
admission procedure before any downgrade; this patch supplies no automatic
floor reduction or lossy expression rewrite. Expressions from the unmerged PR
that paired old IDs with DATETIME require SQL rebinding rather than ABI guessing.

## Acceptance evidence

Focused tests cover actual binding, all five old/new serialized unary domains
in UTC and +08, NULL/masks/reuse, and invalid vector shapes. Protocol tests cover
112/121 rejection, 122 acceptance, destination change after placement, rollback
after success, unknown capability, sender and decoded receiver. Catalog tests
cover read/authoring floors independently and serialized folded-view metadata.
Existing prepared SELECT/CTAS/reuse tests assert the new identity where retained;
BVT executes nonconstant DATETIME input via PREPARE and stored-view replay.

No new mutable state, resource owner, wait, queue or per-row work is introduced.
The versioned checks extend existing bounded admission paths. A live mixed
binary cluster is not claimed by the deterministic component tests.

## Maintenance revision 5: admission before CTAS effects

The execution epoch is MORPC 122, after main's released epochs 114–121.
Official epoch 120 remains reserved for current-session KILL authorization and
proxy disconnect cleanup. Official epoch 121 preserves exact unsigned DECIMAL
DIV domains. This PR appends epoch 122 and rejects immediate-
predecessor 121 peers at each temporal boundary. The same epoch protects new
unary TIMESTAMP DATETIME overloads and prepared
ADDTIME/SUBTIME TIME results over non-TIME inputs. Native TIME durations and
released TIMESTAMP overloads retain their existing contracts. Placement,
receiver validation and the existing execution-stream handshake use this common
classification; no second handshake or execution path is introduced.

Ordinary CTAS releases its bound source query and retains population SQL.
Consequently expression inspection at Compile/Run alone loses the requirement,
including functions folded into constants. An execution-only scalar on
CreateTable retains the minimum required epoch. The existing pre-fold binder
observer records only the changed temporal contracts through child contexts;
a final source-query inspection catches specialization. Prepared CTAS keeps
its authoritative query and leaves the scalar zero until a fresh ordinary
plan is built. The field survives protobuf serialization and plan cloning but
is not a table/catalog compatibility floor.

The shared Compile/Run admission owner checks the scalar before its supported-
epoch fast path, including unknown, invalid, negative and future requirements.
Borrowed CREATE plans are checked before name/catalog refresh and again after
refresh; cached executions are checked before effects after capability rollback.
Temporary CTAS clears the scalar and source query together with population SQL
on the schema-only intermediate plan. Population INSERT keeps its existing
expression and destination admission.

Planning adds bounded node-local observation and at most one source-plan walk.
Execution adds an O(1) scalar comparison on CTAS; no per-row work, persistent
cache, retained full source query, SQL reparse or additional RPC is required.
The additive wire field does not make old executors capable of the new result
ABI; existing placement and execution admission remain required.

Design approved before implementation by gpt-6.1-sol/xhigh, session
01a15150-54d1-7452-9200-30a52fc4cddf. Validation covers ordinary, folded, nested,
prepared and temporary CTAS, wire/clone preservation, pre-catalog rejection,
capability rollback, public prepared temporal results and native TIME controls.
