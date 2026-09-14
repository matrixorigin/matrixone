# Remove the legacy CN merge subsystem

- Status: approved by decision owner `aptend`
- Revision: v2, 2026-09-14
- Owner: MatrixOne storage maintainers
- Approval record: PR #28850 design-decision response for revision v2
- Issues: [#28729](https://github.com/matrixorigin/matrixone/issues/28729), [#27885](https://github.com/matrixorigin/matrixone/issues/27885)
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/28850
- Base: `origin/main` at `7ffce04a0921c684ccfe92863eac5f56279068fc`

## Problem and invariant

The TN merge scheduler has been the maintained object-compaction owner since the
CN merge scheduler was removed. The remaining CN path is manually reachable via
`mo_ctl('CN', 'MERGEOBJECTS', ...)` and a legacy async-task executor. It still
spans SQL argument and policy parsing, disttae object reads and writes, sharding
forwarders, temporary transfer-map files, and a private TN commit RPC. It has no
active task producer or resource admission owner, but remains a public and
distributed mutation path. The two owning issues demonstrate that this detached
path can panic and can consume unbounded CN memory.

After this change, the invariant is: every supported object merge is selected,
admitted, executed, committed, and cleaned up by the TN merge subsystem. A CN
must not produce merge objects, transfer maps, merge tasks, or merge-commit
writes. `MERGEOBJECTS` is no longer a supported `mo_ctl` command.

Goals:

- remove every runtime producer, consumer, handler, interface method, and
  CN-only helper in the legacy path;
- retain TN automatic merge and the TN `inspect merge` controls;
- retain protobuf numeric and persisted-metadata tombstones, without retaining
  runtime CN merge compatibility;
- keep a deterministic public rejection oracle for the removed SQL command.

Non-goals:

- redesign TN merge policy, scheduling, admission, or transfer-page behavior;
- remove the shared `MergeCommitEntry`, transfer-map types used by flush, or
  `mergesort` implementation used by TN;
- make a new user-facing synchronous force-merge API.
- support an old CN issuing a manual CN merge request to a new TN during a
  mixed-version deployment.

## Selected design and alternatives

The selected design removes the runtime CN merge closure in one change:

1. unregister `MERGEOBJECTS` and delete its parser and object-selection policy;
2. remove the legacy CN async-task executor;
3. remove `engine.Relation` CN merge methods, disttae implementation, combined
   and sharding delegates, and shard handlers;
4. remove the CN merge-commit TN write handler and its transfer-file decoder;
5. remove dead CN-host branches, status commands, configuration plumbing, and
   cleanup helpers left in the TN scheduler and shared merge code;
6. replace CN merge use in the branch/GC BVT with the maintained TN one-shot L0
   trigger plus an observable metadata barrier.

Alternatives considered:

| Alternative | Decision |
|---|---|
| Only unregister `MERGEOBJECTS` | Rejected: leaves an unowned distributed mutation protocol, task executor, interfaces, and resource lifecycle to rot. |
| Keep the command but route it to TN scheduler | Rejected: invents a new synchronous public contract and wait protocol; TN already exposes an internal operator trigger. |
| Retain the TN commit or cleanup handler for an upgrade window | Rejected: the command has no automatic producer and the decision owner reports no deployed consumer; retaining either handler preserves the dead distributed protocol and its maintenance burden. |
| Delete protobuf symbols and reuse their numbers immediately | Rejected: creates needless source/wire and persisted-metadata incompatibility during rolling upgrade and rollback. |

## Compatibility and lifecycle

The old protobuf names and numeric slots remain decode and reservation
tombstones in this revision: `OpCommitMerge`, `MergeTaskEntry`,
`TaskCode.MergeObject`, shard `MergeObjectsParam`, and the persisted
`min_cn_merge_size` fields. No new code may produce or handle them. Removing or
reserving those generated definitions is a later cleanup.

Runtime compatibility for CN merge is deliberately not provided:

- new CN to old or new TN: new CN never emits a CN merge request;
- old CN to new TN: the retired write opcode has no handler and returns the
  existing unsupported-write error;
- a persisted legacy async task fetched by a new CN finds no registered
  executor and is completed as failed by the task runner without retry;
- old shard requests to a new CN have no registered read method and fail without
  running a merge.

`MERGEOBJECTS` has no background or automatic producer; it can be emitted only
by an explicit manual command on an old CN. The deployment precondition is that
operators do not issue that removed command once a TN rollout begins. If this
precondition is violated, an old CN can write merge output objects before the
new TN rejects `OpCommitMerge`, and those outputs are not guaranteed to be
cleaned by the new TN. The decision owner accepts this unsupported-path risk
because there is no deployed consumer and retaining a commit or cleanup handler
would keep the subsystem being removed.

Schema-extra cloning continues to preserve `min_cn_merge_size` bytes so a
rolling downgrade does not erase metadata understood by an older binary. The
removed TN TOML offload keys were already write-only; the decoder tolerates
unknown keys, so existing files remain loadable. Rollback is a normal binary
rollback for stored metadata: the tombstone wire and catalog definitions are
still present. It does not make manual CN merge requests safe during the
unsupported mixed-version interval.

## Ownership and unhappy paths

Before removal, CN owned readers, an arena, merge output objects, in-memory or S3
transfer maps, and the request lifetime, while TN owned the transactional commit
and final cleanup. After removal, a new CN cannot create those resources. An old
CN manually invoked during the explicitly unsupported mixed-version interval is
outside this ownership contract, as recorded above.

| Audit | Result |
|---|---|
| Q1: one destruction owner | The supported CN creation path and its cross-node cleanup handoff are deleted. TN merge retains its existing task-owned cleanup and transaction entry. The accepted unsupported old-CN/manual-command path has no new-TN cleanup owner. |
| Q2: waits terminate | The ten-minute legacy task wait and CN-to-TN write/retry edge are deleted. No replacement wait is added. The BVT waits only on visible object metadata with a bounded harness deadline. |
| Q3: growth is bounded | A new CN cannot accumulate CN merge state, and TN admission and scheduling bounds are unchanged. Repeated manual requests from an old CN during the unsupported interval can accumulate orphan objects; this is the accepted compatibility exclusion above. |

No new state, goroutine, queue, cache, retry, file, or metric is introduced.
Security and tenant behavior reduce in scope because the cross-account
`accountID` override in the manual command is removed.

## Verification and acceptance

Acceptance requires:

- source search finds no executable CN merge command, executor, relation method,
  shard method, commit-write handler, or CN-host scheduler branch;
- focused and owning-package Go tests pass for ctl, cnservice, disttae,
  task/storage/RPC, TN merge, and directly affected consumers;
- incremental vet/lint pass for the changed package closure;
- the `mo_ctl_merge` BVT returns `command MERGEOBJECTS not supported`;
- the branch/GC case uses a TN one-shot merge and observes compaction before GC;
- the final diff retains TN scheduler/jobs/transaction-entry/mergesort behavior
  and contains no generated protobuf churn.

No mixed-version CN merge success or cleanup test is required because runtime
compatibility for this unused manual command is explicitly removed. The
retained negative unit test proves that a new TN rejects the retired opcode.

The public rejection BVT has zero setup rows and no external fixture beyond its
own SQL session. The existing branch/GC fixture and cardinality are retained;
only its compaction mechanism changes. No benchmark, race, restart, or scale run
is required because this change deletes the CN concurrency/resource path and
does not alter the retained TN scheduler state machine or hot path.

## Decision record

Change scope: complete legacy CN object-merge subsystem.

Trigger: major refactor crossing SQL, CN, disttae, shard, TN RPC, task, and merge
ownership boundaries; it changes a public command and mixed-version behavior.

Decision: PASS for revision v2. Decision owner `aptend` explicitly selected
complete CN merge removal and rejected a compatibility handler because the
manual command has no deployed consumer. Protobuf/catalog tombstones remain for
decode and numeric reservation only; old-CN runtime merge compatibility is not
part of the supported contract. There are no open design blockers.

Implementation deviations: none at implementation start.
