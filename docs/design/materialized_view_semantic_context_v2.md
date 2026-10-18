# Native materialized-view semantic context, revision 2

Maintenance source: 4a5fb3fd89df9037db5bc76d0b06b27f93a63ada.
Integration target: 3fc00709eccc277c175c7a09eed2605ca1aaee29.
The required gpt-6.1-sol/xhigh design gate approved this bounded correction.
Historical evidence documents retain their original revisions and topologies.

Immutable catalog MVDefinition remains the source-query semantics owner. New
format 2 definitions persist an explicitly present division precision (0..30),
full SQL mode including empty mode, and portable named timezone rules or an
explicit fixed offset. Binding and refresh use the existing resolver/context
owners; internal executor forwarding includes sql_mode, preserving frontend
precedence, scope and detached defaults. Named-zone load failure rejects refresh;
rule-bearing Local without a portable identity rejects creation. No general
session snapshot or parallel resolver is introduced.

Format 1 remains readable for stored rows, SHOW, disabling policies and cleanup.
Its original division precision cannot be reconstructed: refresh requires
recreation, without guessing from output scale or adopting default 4. Migration
4.0.17 pauses legacy periodic policies and advances the existing epoch so queued
work cannot refresh them. Preserve main migrations through 4.0.16 and charset
protocol118 and correlated-program protocol119. MV authoring, scans, task
registration and upgrade require protocol120 after the main integration; see
[the compatibility and execution-consumer follow-up](materialized_view_protocol120_integration.md).

Preserve main's full table-definition resolution before LIMIT 0 pruning. For MV
subtrees only, the compiler's empty-result shortcut yields to its existing scan
validation; unpopulated MVs still reject, including LIMIT 0 and WHERE FALSE.
Ordinary-table shortcuts and source-privilege ownership remain intact.

Retention keeps existing invocation bounds (16 views, two pages of128 per view),
terminal-only conditional deletion and deduplication. Count raw rows per page
across batches. Either full page schedules next_cleanup at current_timestamp;
both short pages schedule one hour later. Existing30-second reconciliation owns
retry/backoff and rotates serviced views by due timestamp. Propagate errors and
cancellation without postponing unfinished work; no new queue/cursor/worker.

Validation must cover semantic0/4/10/30 and malformed/legacy definitions, actual
stored results and metadata under changed refresh context, populated/unpopulated
LIMIT0/WHERE FALSE, live source privileges including prepared reuse, atomic
failure preservation, dependency/teardown/reuse, bounded retention/draining and
error cancellation, migration/protocol boundaries, one race pass and configured
incremental SCA. The focused native compatibility/crash/replay workflow remains
a separate required evidence item. Local component proof does not relabel
historical old-binary/topology evidence or grant external release acceptance.
