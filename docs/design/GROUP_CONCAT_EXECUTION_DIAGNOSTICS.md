# GROUP_CONCAT limits and execution-attempt diagnostics

Design revision: 1 (2026-09-10). Owner: XuPeng-SH.
Issues: [#28252](https://github.com/matrixorigin/matrixone/issues/28252),
[#28253](https://github.com/matrixorigin/matrixone/issues/28253).
Implementation: [#28504](https://github.com/matrixorigin/matrixone/pull/28504).

This document makes the design reviewable before renewed implementation
approval. It was written after implementation commit `fcb92061cf`; it does not
retroactively establish that the earlier implementation was design-first.
The exact document commit and a separate review decision must be linked in the
PR before the implementation is accepted.

## Problem and acceptance contract

Truncating GROUP_CONCAT must produce warning 1260, without duplicate warnings
when materialized results are revisited or when execution retries. A prepared
statement must retain its original group_concat_max_len floor even if its
physical compile is discarded or rebuilt. An execution with a larger limit
must not raise that floor for subsequent executions.

The compatibility target is GROUP_CONCAT truncation diagnostics and the prepared
limit transitions covered by the SQL regression, not complete equivalence of
every MySQL diagnostics-area behavior. In particular, this change defines
successful Compile.Run as the publication boundary for its execution warnings.
It does not make the whole frontend statement or transaction atomic with those
warnings, and does not redesign parse/bind diagnostics, commit failure handling,
SHOW WARNINGS lifetime, or the frontend's error diagnostics.

Required invariants:

1. A physical aggregate finalization reports its truncations at most once.
2. Failed, canceled, panicking, or superseded Run attempts cannot publish their
   collected warnings. A successful retry publishes only its own generation.
3. Late callbacks retain their original sink. They cannot discover a newer
   attempt by reading a mutable process or session destination.
4. Session identity and its optional interfaces remain intact. Diagnostic
   routing is an independent process field; children inherit its destination.
5. Warning count is independent of retained records. The session's
   `max_error_count` (default 1024, range 0 through 65535) limits retained
   records without reducing the batch warning count; zero disables detail
   retention.
6. A prepared or cached logical plan reads `max_error_count` at each execution
   boundary. Physical aggregate generations receive that statement snapshot;
   the capacity is not fixed in the logical plan.

## Alternatives and decision

| Alternative | Benefit | Reason not selected |
|---|---|---|
| Write immediately to Session and deduplicate equal messages | Small local change | Two independent aggregates may legitimately emit equal warnings; failed attempts and late RPCs contaminate the statement |
| Snapshot and restore the Session diagnostic area on retry | Preserves existing writers | Concurrent old callbacks can append after restoration; rollback can erase nested or unrelated diagnostics; it still lacks a generation boundary |
| Per-attempt sink, with explicit final publication | Isolates producers and retries without replacing Session | Selected; introduces a bounded collector and process binding lifecycle that must be tested |

The prepared statement owns the floor, rather than the aggregate executor or
cached physical plan: AP placement and specialization can deliberately omit
the cache, and definition changes can replace the physical generation.

## Ownership and transitions

The aggregate owns truncation counters and sampled row numbers from Flush until
consume/free. The operator consumes warnings after successful materialization.
The Run owner owns the attempt collector and a map of process bindings to
restore. RPC senders capture the collector pointer before asynchronous work.
The frontend Session owns diagnostics only after publication.

| Event | Required transition |
|---|---|
| Start Run | Capture destination; install a fresh open collector on the root and existing scopes |
| Create child process | Inherit the current destination; do not replace Session |
| Warning append | Under collector mutex, append count and at most the remaining record capacity; ignore a sealed collector |
| Retryable failure | Seal/discard before cancellation; finalize failed resources; restore bindings before releasing pooled retry compiles |
| Recompile retry | Install a new collector before compilation, then bind the new scopes; retry compilation warnings belong to this attempt |
| Retry compilation fails | Discard the new attempt and restore bindings on unwind |
| Successful Run | Finish resource finalization, result sealing and final plan processing; seal once, restore bindings, then publish the batch |
| Terminal error, cancellation or panic | Leave success false; discard and restore on unwind |
| Repeated finish / late callback | Sealed collector is inert; no duplicate publication |

The mutex linearizes append versus close; publication to the destination happens
outside the mutex. Closing does not wait for RPC completion and therefore cannot
depend on the work it is canceling. Existing execution finalization still owns
worker/RPC teardown; the collector is not a replacement for that protocol.

Process binding writes occur at execution boundaries, before workers start and
after the corresponding workers are finalized. Asynchronous RPC completion uses
the captured sink, not a process field that may already have been rebound.
Each retry allocates a new collector; a sealed collector is never reopened.

For nested Run, the captured destination is the parent's collector. Child
success appends into that parent, not directly into the frontend. Parent failure
therefore discards child warnings too. Child failure discards its own records.
Nested execution that intentionally uses a separate Session follows that
Session's existing ownership instead; no cross-session merge is introduced.

Outside Run, GetWarningSink falls back to Session, preserving the existing
parse/compile/direct-expression path. Consequently initial compilation and
retry compilation do not have identical diagnostics publication boundaries;
full statement-wide diagnostic transactions are outside this design. Background
processes without a compatible destination do not allocate an attempt collector.

## Aggregate identity, counts and row numbers

Two independently written GROUP_CONCAT calls can have the same value but distinct
diagnostic effects. Their physical slots remain separate. Aliases and ordinals
refer to the already selected expression and reuse its slot. AST-node identity
handles HAVING-before-SELECT binding; alias wrappers preserve the exact projected
slot. Ordinary aggregate common-expression behavior is not changed.

ORDER BY reuse follows the existing tested compatibility policy: an exact
selected GROUP_CONCAT expression reuses its slot; a grouped wrapper expression
can be an independent call. A scalar aggregate wrapper reuses the selected
aggregate. These cases need separate planner and SQL oracles.

Each truncating finalized group contributes one warning. The reported Row N is
the aggregate executor's 1-based contributing-row counter in finalization order;
NULL-filtered and deduplicated inputs do not contribute, and a group stops
counting after truncation. The counter restarts for a new finalization. It is
not a source-table row ID or a globally ordered distributed row number.

Distributed terminal envelopes sum fragment warning counts and retain bounded
diagnostics. Records keep each fragment's production order, and the
coordinator merges batches in the order it receives them; no global ordering or
reproducible row numbering across different physical plans is promised. Before
the terminal JSON is attached to MORPC, a conservative byte budget is applied
to the diagnostic array. The exact count is always sent, while only the
longest prefix that fits the frame is sent; a remote terminal may therefore
carry fewer records than its configured `max_error_count` when messages are
large.
Ordinary partial-state transport must not itself emit a second copy of a warning
for the same finalization; the finalizing operator owns reporting. Window
finalizations can represent separate frame evaluations.

## Prepared statements and compatibility

PREPARE captures a validated session limit once, including when no physical
compile is retained. Execute passes it to fresh/specialized compiles; cached
Reset refreshes effective limits; ordinary and definition-change retry compiles
inherit the same immutable floor. Group, MergeGroup, Window and TimeWin share
the refresh rule. Non-prepared statements use the current session limit.

DEALLOCATE or a new PREPARE ends the old floor lifetime. Internal recompile of
the same PrepareStmt preserves it. Current CN session migration reconstructs
prepared statements by reparsing, so migration starts a new floor lifetime from
the receiving session. Preservation across migration needs a separate serialized
prepared-state contract and is not claimed here. Restart has no new durable
state to recover; rollback of this code needs no catalog/data conversion.

Warnings use the existing optional terminal JSON fields. The initiating
statement's `max_error_count` is carried in the appended protobuf SessionInfo
fields. Older receivers ignore those fields and use the legacy 64-record
capacity; during mixed-version operation, successful SQL execution remains
compatible but complete warning coverage is not guaranteed. Once all CNs are
upgraded, the configured capacity is preserved through collection, subject to
the terminal frame byte budget.
The single-record optional sink fallback reports retained records only; exact
totals above retention require the batch interface used by the frontend and
attempt collectors. The frontend's wire warning count saturates at uint16 max.

## Cost, retention and failure containment

Each aggregate retains at most `max_error_count` uint64 row numbers plus two
counters. Each collector retains at most that many diagnostic records and a
uint64 count. The process binding map and temporary visited-scope map are
O(processes/scopes in the plan), not O(rows or warnings). Retries release
previous binding maps and record slices; only already outstanding callbacks can
retain a sealed old collector until their existing RPC lifecycle ends. No new
goroutine, queue, timer, retry, or log is added.

Retained record count is bounded independently of total count. Retained *bytes*
are O(`max_error_count` times maximum produced message length), while terminal
serialization applies a separate conservative frame byte budget:
GROUP_CONCAT messages contain only a row number, while existing scalar conversion
messages can contain input text. This design does not newly cap scalar message
length, and does not claim to bound the existing aggregate payload/spill memory.
Plan size and simultaneously live fragments also multiply the per-owner bound.

The per-contributing-row addition is counter arithmetic. Message formatting and
batch publication occur at warning/finalization boundaries. Collectors use a
short mutex section and copy/transfer at most `max_error_count` records.
No-warning foreground Run still pays for a collector and binding traversal;
no-sink background Run skips it. Independent duplicate GROUP_CONCAT calls now pay for independent
aggregate evaluation; this is the accepted cost of preserving their diagnostic
effects. Avoid claiming measured speedups without comparative evidence.

## Validation and rollout criteria

| Contract | Existing regression owner |
|---|---|
| Count/retention, consume once, UTF-8 and separator truncation, failed/later-group finalization | aggexec/concat2_test.go |
| Operator publication after materialization; window/time-window integration | group, window and timewin packages |
| Real aggregate output followed by retry/error/panic; old remote terminal during next attempt | TestGroupConcatWarningAttemptOutcomes |
| Nested success/failure, bounded records and exact total, repeated seal | TestWarningAttemptNestedAndBounded |
| Concurrent close/append and no-sink allocation control | TestWarningAttemptConcurrentSeal |
| PREPARE without cached compile, fresh and both retry paths, cached refresh | frontend prepared-floor test, compile warning_attempt_test.go and scope_test.go |
| Independent calls versus alias/ordinal reuse | planner GROUP_CONCAT binding tests |
| Public results, SHOW WARNINGS, high/low prepared transitions | function_group_concat.sql/result |

Acceptance requires relevant package tests, focused race evidence for collector
and retry ownership, and public SQL regression evidence. Reuse evidence only
when code, base dependencies, flags and test selection remain applicable. Existing
reported Linux results are provenance, not a new run performed for this document.
After rebase, inspect dependency changes before deciding what must run again.

No new rollout flag is needed. Diagnose a report by recording prepared/current
limits, statement/attempt outcome, physical topology, full warning count versus
retained messages, and whether the cluster is mixed-version. Do not infer a
production fault from unrelated CI timeout or missing test artifacts.

Open non-blocking limits owned by the PR owner: migration floor transfer,
globally ordered distributed warning positions, full statement-wide diagnostics,
and byte-capping of existing scalar messages require separate scoped decisions
if their contracts are expanded. They are not silently promised by this patch.
