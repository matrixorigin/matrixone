# INSERT IGNORE constraint-warning diagnostics

Design revision: 2 (2026-09-10). Owner: XuPeng-SH.  Issue: [#28254](https://github.com/matrixorigin/matrixone/issues/28254).  Implementation: [#28459](https://github.com/matrixorigin/matrixone/pull/28459).

This document defines the warning contract for rows that are already skipped
by `INSERT IGNORE` or `UPDATE IGNORE`.  It is intentionally limited to
constraint diagnostics; conversion warnings, transaction diagnostics, and a
statement-wide replacement for the frontend diagnostic area are separate
designs.

## Contract and invariants

For every input row skipped by a primary-key, unique-key, or CHECK constraint,
the successful statement reports one warning with the original condition code.
The existing arbitration order chooses the first rejecting constraint, so a row
that violates several constraints still produces one warning.  The exact
warning count is preserved independently of the at-most-64 records retained
for `SHOW WARNINGS`.

The data decision is unchanged: only the existing IGNORE rejection path may
record a warning.  Ordinary `INSERT`, `UPDATE`, `REPLACE`, and ODKU behavior
remain unchanged.  Rendering is diagnostic-only; malformed or legacy key
bytes may lose the rendered record, but must not make an already-ignored row
fail the statement.

The following execution invariants are required:

1. A failed, canceled, panicking, or superseded physical attempt publishes no
   warnings.
2. A retry publishes only the successful retry's records; a late callback from
   an older attempt is inert.
3. Nested/internal execution contributes to its parent's attempt sink rather
   than publishing directly to the frontend session.
4. Remote terminal diagnostics are associated with the sender's captured sink,
   not with a mutable process or session looked up after a retry.
5. Warning collection is bounded by retained-record count and does not grow
   with the number of skipped rows.

## Ownership and lifecycle

`Compile.Run` owns the attempt boundary already used by the common execution
warning implementation.  At the start of a run it captures the destination,
installs a fresh bounded collector on the root and all existing scopes, and
restores those process bindings on every exit path.  Local operators call
`process.AppendWarningBatch`, which follows the current process sink.  Child
processes inherit the sink pointer without replacing `Session`.

The collector's mutex linearizes append against close.  Closing a failed
collector discards its count and records; closing a successful collector
detaches the bounded records and publishes once, outside the lock, after
result and resource finalization.  A closed collector rejects repeated and
late appends.  Retry compilation creates a new collector before compiling the
new physical scopes.  The old collector is never reopened.

Internal SQL receives the explicit sink through its context and installs it on
the newly created process.  This preserves the parent attempt even when the
internal executor creates a new top-level process.  A nil sink means there is
no diagnostic destination and does not allocate a collector.

Remote fragments use the same optional terminal JSON fields as existing
diagnostics.  The remote server accumulates its fragment warnings and sends
the exact count plus at most the retained records at terminal completion.  The
client captures the initiating process's sink before sending the RPC.  If the
attempt is retried or canceled, the captured collector is sealed and the late
terminal is dropped.

## Producers and user-facing keys

The three duplicate producers are covered without changing their arbitration:

- hash-build deduplication records the rejected input row;
- dedup-join probing records the selected conflicting key; and
- the ordered `PreInsertUnique` arbiter records the first conflicting primary
  or unique key, including same-batch multi-key checks.

`PreInsertUkCtx` carries parallel logical key names and type counts, with the
concatenated logical types for composite/index keys.  This lets warnings render
user values instead of serialized index bytes.  Plans from before these fields
are accepted with a safe best-effort fallback.  Metadata bounds are checked
before slicing so malformed cached/remote plans cannot panic the diagnostic
path.  The decoded metadata is built at most once per prepared
`PreInsertUnique` operator, and only when a retained warning actually needs
text rendering; no-conflict batches and the already-bounded tail do not pay
that cost, and it is not copied for every input batch.

CHECK assertions in IGNORE mode are barrier filters: false rows are removed,
one `ER_CHECK_CONSTRAINT_VIOLATED` warning is accumulated per row, and the
message bytes are owned before they outlive the expression invocation.  The
assertion is non-foldable in this mode so a constant-false expression still
evaluates once per input row and preserves the count.

## Mixed-version protocol boundary

Older CNs know the existing assertion function ID but interpret a false result
as an error.  Therefore IGNORE-aware CHECK execution is assigned reserved
MORPC capability version 63.  The current mainline advertises v63; during a
rolling upgrade this PR keeps CHECK filters on the coordinator until all
participating CNs advertise v63.  Remote source scans remain remote, and
ordinary CHECK enforcement is unchanged.

Placement checks and a second serialization-time check prevent a scope compiled
before a capability change from being sent with the new meaning.  The v62
VARCHAR OCT capability remains intact; this change does not reinterpret or
lower any existing protocol version.

## Cost and failure containment

Each accumulator and remote collector retains at most 64 records and at most
256 KiB of message bytes; an individual message is capped at 4 KiB.  Counts
use saturating `uint64` arithmetic and are independent of either bound.  Each
producer stops formatting after its local retained capacity, while the attempt
collector applies the same global bound; a large ignored statement therefore
does not retain or transmit every duplicate key.  The attempt binding map is
proportional to live processes/scopes, not rows.
No goroutine, timer, retry loop, or unbounded queue is introduced.  Key
formatting has a recoverable diagnostic boundary: an invalid internal tuple
returns an internal rendering error to the IGNORE caller, which increments the
count and keeps the row skipped; the ordinary non-IGNORE duplicate path still
returns its error.

## Validation

Acceptance requires the owning package tests and focused race tests for the
collector, retry/late-terminal lifecycle, duplicate producers, CHECK barrier,
protocol gate, malformed key rendering, and metadata bounds.  The public BVT
must cover PK, unique, composite/index, same-batch, UPDATE IGNORE, and CHECK
cases, including a constant CHECK predicate and the 64-record boundary.  A
mixed-version two-binary run is useful rollout evidence but is not claimed by
this PR; deterministic serialization and coordinator-fallback tests are the
compatibility gate.
