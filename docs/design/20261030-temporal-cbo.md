# Temporal range costing through ordinary relations

The semantic range transformation is defined in [temporal index ranges](20261028-temporal-index-ranges.md). This document defines the narrower AUTO covering implementation. A mixed datetime/timestamp comparison retains its original SQL meaning, including session timezone, fractional precision and NULL handling. A typed inverse range is only a necessary storage pruning condition. Every row returned by the reader evaluates the complete original predicate.

## Ownership and metadata

A synchronous, non-background root planning attempt borrows the actual native relations already returned by ordinary frontend resolution. The statement scope holds at most 64 bindings and releases its references at lexical close; it never closes a borrowed relation. Nested or overlapping attempts, failed nonblocking gates, duplicate identities, capacity exhaustion and unsupported sources conservatively decline this optional path.

Checkpoint replay and object-history GC retain their ordinary engine lifecycle. This optimization does not fix the baseline case in which GC removes deletion history before delayed checkpoint replay.

The existing transaction resolver supplies schema visibility. SI may retain its canonical relation at its snapshot; RC's ordinary table-cache invalidation checks newer catalog versions. A logtail connection generation is not an independent schema authority. Operator transaction identity, snapshot revision and workspace visibility are checked around capture and again before publication. These stamps detect transitions, including ABA; they do not make concurrent use of a transaction workspace safe.

The optional native relation API validates the current read domain, copies cost statistics, and verifies the selected index pair. Read validation repeats the native transaction/workspace checks without IO or retained state. Statistics capture copies bounded scalar cost statistics under the existing GlobalStats lock, acquired with TryLock. The observation must be the same pointer as the ordinary stats cache entry for the exact account/database/table key. An explicitly incompatible bound schema version declines; an unbound ordinary observation may supply estimates only. No sampled object metadata, checkpoint decoding, subscription, worker, queue, service or secondary cache is created.

Pair verification checks the actual selected base/hidden pair immediately before changing the column map or publishing a new scan. It checks the same transaction domain, account/database, hidden relation kind and parent, index membership/completeness, supported physical key/payload layout, complete schema and the legitimate pruned base definition. The hidden definition used by the scan must be the exact instance captured from the winning ordinary resolve. Bounded work or cancellation failure leaves the original plan intact.

## Cost and admission

The late ordinary index-selection pass considers only supported parameter-free SELECT owners whose original expressions are diagnostic-safe. Unsupported operators, early-stop shapes, unsafe projections or predicates, named timezones and non-local scheduling retain the existing path. FORCE and backfill do not gain optional authority from this scorer.

Costing uses the existing regular-index work model, ordinary column min/max/NDV/null statistics and the current statement's temporal bound. The bound observed for estimation never replaces the executable NOW expression. Base scan credit for pruning requires the actual eligible mirrored block filter and ordinary overlap estimate. Index input bytes use a block floor, while output and residual costs use estimated matching rows. Complete candidate construction remains bounded and charged. The removed duplicate row candidate contributes no extra row-executor charge; the ordinary initial filter term, full SQL residual, extraction/output and fixed construction terms remain.

The common candidate constructor normalizes its interval union when the bound has a proved positive interior domain. Registered CURRENT_TIMESTAMP with a valid literal FSP maps every int64 Unix nanosecond input well inside the calendar domain, independently of the current clock. Immutable typed TIMESTAMP literals require the same offset, rounding and strict-bound margin and the existing source/parameter ownership proof. Unsupported or unproved expressions keep the original union.

For a DATETIME key, `>`/`>=` then merge the main interval with the adjacent upper physical tail; `<`/`<=` merge the lower tail, zero point and main interval. Equality removes only a point already contained in its main interval. The original empty guard and the dynamic endpoint remain executable. Behind that guard, disabled narrowing or a Full range still covers the physical domain; a false guard continues to select no rows. No observed timestamp is embedded in the plan, and out-of-calendar values remain covered. This reduces expression copying, protocol walks and fold construction through the existing operators and readers.

These are ordinary heuristics, not full-block bounds or confidence guarantees. Skew, stale stats, unsorted data, memtables and fail-open reader paths can inspect more rows than estimated. No threshold depends on a benchmark table, row count or SQL string.

## Execution and fallback

After native pair verification, the covering scan keeps the complete remapped original predicate in FilterList and the sole marked necessary candidate in BlockFilterList. The residual depends on the hidden key even if SELECT projects only the PK. Final remapping must retain that key. Unverified helper and legacy paths retain their row candidate.

Compile forwards the marked block expression to the independent storage predicate and preserves SQL residual execution in TableScan/Restrict. Unsupported PK pruning may fail open or return wider boundary blocks; it never supplies SQL truth. Only the specifically marked internal empty guard can trigger the empty-reader shortcut. An unrelated folded false expression cannot.

Current statement clock, fixed timezone, local placement and ForceOneCN are revalidated before narrowing. Drift follows the existing safe rebuild and reauthorization path; retries do not inherit stale narrowing permission. Cancellation propagates the selected cause before plan publication. A later stop cannot retrospectively undo a published result.

## Optional reuse in the ordinary session cache

The ordinary no-cache policy remains the default. A narrowly qualified, external singleton COM_QUERY may reuse an audited covering-index template in the existing session LRU. Prepared, internal, background, multi-statement, hinted and unsupported SELECT owners retain normal planning. BASE choices are always costed afresh; retaining a BASE topology across statement clocks can miss a newly selective index range.

Qualification first proves that the original binder's two no-cache flags came solely from the supported statement-time expression. Final auditing checks the complete remapped row predicate, outputs, selected native index and exact constructor-generated block candidate. A complete bounded protobuf preflight precedes cloning. Certification never evaluates NOW, changes the SQL residual, or removes the temporal replan marker. Pure static SERIAL literal folding is compared using the ordinary constant folder on an isolated copy.

The original scope produces detached raw and execution plans. A one-shot wrapper seal records the actual Compile, its successful terminal result and unchanged generation; the Compile reference is cleared before publication and never enters the LRU. Session publication repeats the audit. The cached entry holds only the private raw plan, existing AST and metadata plus a special-entry bit; no relation, workspace, process, scope, Compile or independent cache owner survives there.

A pending hit exposes neither its raw plan through Plan() nor a usable old snapshot. Dispatch and direct Compile share admission: verify request/session/process/transaction ownership, then ordinary schema and statistics dependencies, current environment and exact cache generation. Only the private, previously bounded generation can take a full protobuf clone without repeating the raw preflight. The public clone helper still preflights arbitrary callers. Execution receives the detached clone and matching snapshot together; current clock, zone, permissions and placement remain execution-owned.

Cancellation returns its cause without eviction or parser work. A stale special hit invalidates only the expected generation, obtains a freshly owned parsed AST and clears its pending snapshot/reuse state. Retry cannot overwrite the certified raw template. Request serialization remains the existing session contract; this optimization does not establish safety for arbitrary concurrent session Close.

This is template reuse, not adaptive recosting. A covering-index choice can become less suitable as the clock changes even when table statistics remain unchanged. Validate both density directions against a fresh-plan control; a materially regressing optional cache must be removed rather than hidden behind benchmark-specific clock epochs or thresholds.

## Validation contract

Focused tests must exercise an eligible late-path positive control alongside isolated admission negatives; exact hidden-definition capture, schema/parent mismatch, transaction/snapshot ABA, cancellation and budget exhaustion; lexical cleanup without closing borrowed relations; storage forwarding with only a block marker; final key retention; real reader fail-open and wider boundary blocks with authoritative residuals.

Public SQL compares rows, warnings and errors with IGNORE INDEX across supported comparison directions, precisions, NULLs and fixed zones, and conservative prepared/LIMIT/aggregate controls. Transaction and DDL/reconnect checks use the normal resolver. Performance compares quiet identical SQL on ordered, shuffled and independent held-out distributions, including sparse and dense cases, against the frozen main and equivalent manual controls. Source review or a selected fast plan alone does not establish a performance pass.
