# Forward foreign-key execution validation (v5)

Design gate: gpt-6.1-sol / xhigh, session `01a128b6-fab3-72f2-bc51-80981947b8d2`, reviewed production base `4022729da24d71b082c22b52aaed3faba2b77ea5`. Approved for implementation; runtime acceptance remains required.

## Problem and invariant

CREATE records an incoming foreign key during planning, waits, then resolves its child by name and writes the saved definition. A concurrent replacement can therefore receive a foreign key it never declared. Two reproduced neighbors use the same stale positive/negative facts: a previously absent parent appears, or a previously empty incoming set gains a pending child while CREATE waits.

Every installed incoming FK must come from the current catalog declaration and the current physical child definition, protected through transaction completion. Historical readers retain their snapshot. Normal pessimistic RC DDL refreshes only before effects, through the existing applied-logtail/workspace owner. Internal replacement borrows its outer admission and visibility.

## Existing owners and selected correction

The planner retains its incoming preview for planning errors and early G admission. Execution treats it as a participant hint, never an authoritative child binding. The existing catalog reader/decoder and semantic FK binder are shared with execution. Incoming selected-key backfill moves to execution, where the chosen current key is known.

The existing metadata name boundary T protects absent as well as present names. CREATE publishes pending declarations while holding their parent-name T, as ALTER ADD FK already does. The plan records unresolved parent names and observes the cached actor-group mo_tables lock binding before FK-dependent catalog reads. Observation must not allocate or wait.

A live lock table retains commit timestamps, but a recreated table does not inherit its predecessor's watermark. Execution therefore checks exact binding identity and the actual admission result. A missing/changed binding, missing timestamp, or timestamp at/after the exclusive plan snapshot requires the existing TN-ordered applied frontier and one definition-change replan before allocation/publication. A statement-local admitted-name bit travels through the existing retry owner and resets on reuse. The fixed target/declared-parent domain is verified on carryover. There is no new retry state machine or persistent cache.

Fresh FK CREATE enters G before local locks and can wait there. Transactions entering with retained locks acquire G nonwaiting. Metadata pins under G are also nonwaiting. Newly discovered participants permit one definition-change retry; repeated changes abort the transaction instead of accumulating lock domains indefinitely.

For positive incoming repair, pin the candidate child names T S, obtain an applied frontier after pins, then read current declarations and current child definitions. Bind using the existing column/type/generated-column/key-selection rules. Preserve unrelated constraints, update each child once and add each reverse child ID once. Retain the resolved relation; remove the old post-effects lookup by name. No child physical X lock is acquired under G.

Standalone DROP INDEX lacks target-name and physical-definition ownership. Correct that existing DDL responsibility: fresh transactions wait for the provisional physical definition first, acquire D/T nonwaiting, refresh via the shared index-DDL snapshot helper, verify current physical identity, then read/replace the complete constraint. Retained transactions acquire the physical definition nonwaiting. This avoids a metadata-owner/DML-owner wait cycle and removes the need for a CN/TN constraint CAS protocol.

Internal TRUNCATE/COPY pins FK participants before its outer frontier and first effects, and certifies inner CREATE with the existing lifecycle receipt. It does not advance visibility after outer DROP. Borrowed execution shares the existing deferred whole-transaction rollback owner; fatal errors seal the operator even if callbacks catch them. One shared classification handles wrapped/joined errors.

## CI boundary corrections

The correction design is gpt-6.1-sol / xhigh session `01a12985-fd30-7230-b012-abb8dea28082`, with the checkpoint amendment recorded separately. Runtime evidence is reported in the PR validation section.

Borrowed internal CREATE pins its names before physical compilation. If binding/frontier validation requires a refresh, the existing planner rebuilds once within the same admitted name domain, preserving caller writes and statement ownership. Arbitrary later execution failures still return to the caller; child execution never invokes the outer retry transition. Planner and execution both exclude foreign-key-banned catalog databases.

CTAS binding and its source-privilege precheck use private copies of the caller's current SELECT AST. Qualification cannot change inferred column names across admission retries or prepared execution. The CREATE planner uses its same private source for column inference and generated INSERT SQL; explicit qualifiers, aliases and parameter offsets remain intact. The existing AST copy utility also isolates mutable maps and SAMPLE columns. Ordinary SELECT/DML do not enter these copy boundaries. This correction and its privilege amendment were designed by gpt-6.1-sol/xhigh sessions `01a12a1a-4d02-7ff0-a3b9-433d16928f29` and `01a12a25-d1ca-7d13-a9ee-a3a4fccb3dbb`.

Partition replacement includes actual descendant names from the existing partition metadata and naming owner. Descendants borrow the lifecycle frontier but receive no root FK catalog payload. Actor, transaction and lifetime validation remains mandatory; payload preservation applies only to its exact root target.

The existing transaction creation journal certifies unpublished scalar replacement and cleanup only when both physical and original logical generations were created in that transaction. Statement rollback revokes this proof. Scalar shape and FK/lineage/history exclusions still apply, and D/T ownership remains required. Certified private DELETE/TRUNCATE/DROP avoids late global lifecycle acquisition; replacements of published logical generations retain normal admission. No name-prefix exemption or additional journal is introduced.

DATA BRANCH DELETE locks its target D before refreshing and validating membership. Private lineage writers acquire C then G/X directly, rather than taking G/S and upgrading after other writers have joined. The original lineage publication write remains in the same transaction. NEXTVAL and other fatal nested failures retain their original typed error when terminal cleanup also fails; secondary rollback errors are logged separately.

Checkpoint consumers already exclusively own `HandleObjectEntry`; live events use the separate object-list handlers. Tests now exercise those real entry points rather than treating a checkpoint callback as a live event. No additional replay API is needed.

## Performance and failure ownership

Stable ordinary CREATE adds one local binding observation and comparison, with no extra SQL or TN barrier. Unknown/newer bindings add one frontier/replan. Positive incoming repair adds participant pins, one frontier and one fresh incoming query; that query can scan tenant FK rows. Metadata churn can cause a bounded extra plan query. Ordinary SELECT/DML does not enter these checks.

Large DROP DATABASE adds no per-table FK scan, pin or barrier. Its measured index cleanup optimization suppresses redundant per-table mo_indexes DELETE only inside database-owned recursion. The existing bulk DELETE by the resolved database ID owns cleanup in the same transaction. Ordinary/internal DROP TABLE retains table cleanup. Physical/index-plugin/CDC/PITR cleanup stays with its existing owners.

All attempt resources have explicit cleanup; transaction locks remain with the existing terminal owner. Capacity exhaustion fails instead of escalating exact name pins. Cancellation, conflict and infrastructure failure must terminate the correct owning transaction without nested Rollback waiting on its own active SQL token.

## Confirmed historical defects included in this change

TRUNCATE's internal DROP previously removed catalog-only FK declarations that SHOW CREATE could not reconstruct. The synchronous replacement receipt now preserves those declarations through its exact target's DROP/CREATE. Materialized FK hints retain their selected referenced key and original actions; that key must still match the pinned current parent. Incoming bindings during internal parent reconstruction also retain the catalog-selected key, validated against the reconstructed parent; legacy empty bindings remain conservative. This includes whole-table DELETE optimized to internal TRUNCATE. Explicit DROP/CREATE uses normal new-parent key selection. Self references stay with the existing outgoing self-FK owner. Ordinary DROP deletes declarations, and COPY keeps its existing transfer policy. Actor, transaction, exact target and lifetime checks prevent preservation from escaping its replacement owner.

Cold historical checkpoint replay previously discarded persisted appendable data/tombstone objects with no deletion timestamp. Only the private checkpoint admission guards change: nonzero durable stats are admitted, with existing object identity, deletion precedence and commit/abort visibility filtering. Live object-list guards remain with their current owner. Checkpoint creation registers and optionally prefetches the object without copying or scanning the in-memory tombstone index. Only appendable deletion reclaims matching-object versions; both tombstone loops stop at the object boundary before filtering timestamps. Checkpoint uses DeleteTime while live logtail retains its startTS cutoff. Two-CN tests cover current opens, warm and cold historical reads, checkpoint plus incremental writes, and first historical access after DROP.

Lock retry pressure previously treated reclaimable host cache as unavailable memory. The bounded retry path now classifies existing available-headroom readings against the smallest finite host/cgroup capacity. Unknown measurements remain within the existing retry deadline. Queue capacity, retry budgets, backoff and cancellation are unchanged. The original restart probe also exposed allocator generation retirement; readiness diagnosis separates that bounded protection from the reproduced memory-classification defect.

## Validation evidence

Matched public SQL runs with 5,000 actual tables measured baseline DROP at 101.825/101.924 seconds and database-owned cleanup at 68.405/65.836 seconds (32.8–35.4% less elapsed). Each excludes CREATE setup and requires the committed catalog count to fall from 5,000 to zero. Whole-run peak RSS rose 1.22%; this is not DROP-isolated memory evidence. The combined implementation measured DROP at 66.806 seconds (34.4% less elapsed), with setup at 43.233 seconds versus baseline 40.020/40.204 seconds. Combined whole-run peak RSS was 0.61% above the matched baseline; it is not an isolated DROP measurement. Final QA gates are recorded separately before delivery.

## Acceptance

- Deterministic original and negative-dependency schedules, with actual physical IDs, catalog/SHOW CREATE agreement and accepted/rejected SQL writes under checks=1.
- Current declaration changes, compatible/incompatible replacements, compound keys/actions and read-own-writes.
- Binding recreation, unknown binding, remote eviction without local-owner watermark loss, finite retry under unrelated metadata churn.
- Incoming repair versus DROP INDEX/ALTER/DML in both directions; retained transactions, cancellation/deadline and successful subsequent reuse.
- Owned/borrowed executor fatal errors, swallowed callback errors, prior-write rollback and lock release.
- Scalar/protected/FK internal replacement and failure after DROP, with no inner frontier.
- Checkpoint/logtail/cold cache and historical snapshot boundaries. Every reproduced historical defect discovered in this investigation is now included by explicit user request; additional root causes require their own incremental design gates.
- 5000-table matched latency comparisons and actual committed catalog cleanup, with failure rates retained in performance results.
- Incremental formatting/static analysis, owning-package race checks, focused integration regressions and final gpt-6.1-sol/xhigh overall review.

No mixed-version upgrade protocol is required. No existing-data validation is added to declarations authored with foreign_key_checks=0.

Online MERGE/PICK owns stage initialization, batch production/application, and cleanup synchronously. LCA SQL and retry-capable apply SQL cannot overlap in their shared transaction; online shard delivery uses one execution lane, while hash construction and read-only DIFF retain parallelism. This removes retry-triggered sibling cancellation without adding SQL locks or result spooling. Design amendment: `gpt-6.1-sol/xhigh`, session `01a12a33-9b70-7733-a833-0c569d079da9`.
