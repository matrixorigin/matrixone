# Independent root design decision — D2-DTS-1

Reviewed revision: the exact bytes of `heartbeat-20261019-2129.yoyQB2/worker-28935-v2/migration-contract-v2.md` (SHA recorded in `28935-design-decision.json`). Decision precedes any authorized implementation. This is a technical design decision, not a GitHub approval, CR acceptance, test PASS or rollout claim.

Decision: **PASS for implementing only the bounded D2-DTS-1 repair domain**. Do not mark the full migration/CRC issue or any reviewer acceptance complete.

The v1 generic graph-hash proposal is withdrawn. D2 closes the four root design blockers for a smaller domain: raw SQL and AST independently admit a finite fully-qualified direct-table INT32 SELECT; pre-pruning `Prepare.Schemas` plus a complete ordinary-table catalog schema establish the object witness; target preflight is typed local decoding/admitted metadata reads, not a live scratch SQL session; actual target binding guards Resolve and publication against the witnessed object/generation; later drift requires explicit reprepare. This separates a bounded immutable statement generation from an unsound claim to lock out all concurrent DDL globally.

Checked current-source anchors include the actual issue29148 SQL (`select id from issue29148_owner_<scenario>.t order by id`), plain `t(id int, tenant int)`, explicit-schema one-table shape, `prepareSchemaRefWithSnapshot` DB/table/version assignments, and captured `schedulingSQLMode`/`sqlModeFlagsSet`. Thus the finite grammar genuinely contains the reported positive fixtures rather than changing them to an unrelated helper case. Existing omitted migration capability is a setup mismatch and needs an explicit upgrade-capable positive client plus a separate old-capability rejection control; retain every original result/policy/stale-handle oracle.

Accepted decisions:

- Only empty-policy, no-remap, no-private-catalog, parameter-free, bounded one-base-table SELECT is admitted. Views/generated/unknown syntax, nonempty current policies and other domains remain fail-closed; this is not a bypass of current target authorization.
- Target failure disposition is explicit opt-in: preflight rejection changes no user state and consumes no once; after replay begins the disposable target is closed, rather than claiming rollback of arbitrary old target state. Proxy source remains owned and usable until successful publication. Closing a target while its migration operation is active must not wait on that same operation.
- Certified replay never writes CN-global runtime setters. Presence/value equality and local restoration are distinct; connection cleanup cannot compensate global writes or foreign actors.
- Published migrated handles retain a generation contract and return need-reprepare on invalidating catalog/mode/protocol changes. This is an explicit conservative compatibility boundary, not transparent rebinding to a newly created same-name view.
- Bounds and exact wire semantic fields are specified; no generic graph cache/hash, source-plan transport, unbounded dependency capture or source authorization transfer is introduced.

Implementation obligations / non-waivable checks:

1. Put the reviewed versioned design and this distinct pre-implementation decision in reviewer-accessible PR docs without changing normative bytes; record any metadata-only wrapper separately.
2. Keep all raw/AST/object/schema/parameter/policy/capability/disposition gates conjunctive; unknown fields or incomplete metadata refuse transfer. No source-branch sync absent a real repair dependency/conflict.
3. The actual implementation must prove preflight no mutation, actual-bind and SetPrepareStmt generation ownership, failure-close ordering, CN-global non-write, source reuse and fresh-target retry. If an existing mandatory oracle requires a positive unsupported domain, return that concrete contradiction for a design amendment; do not weaken the oracle or quietly widen the proof.
4. Execute the original two real migration subtests and original follow-on policy/handle assertions, plus the finite negative and injected DDL/failure/cleanup controls. A test-only capability flag without actual production witness/binding implementation is not a repair.
5. Recheck the authoritative main protocol prefix before publication. Unmerged parallel branch claims do not reserve a version, but current handler/wire/floor must form a real cumulative implementation prefix.

Broader policy migration, materialized CRC upgrade values, arbitrary SQL support, retaining a preexisting target after post-replay failure and deployed mixed-binary acceptance remain separate unresolved requirements where applicable. No resolve, “Addressed/Fixed” reply or re-review is authorized by this design PASS alone.
