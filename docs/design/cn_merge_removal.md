# Remove the legacy CN merge subsystem

- Status: approved in conversation before implementation
- Revision: v1, 2026-09-14
- Owner: MatrixOne storage maintainers
- Issues: [#28729](https://github.com/matrixorigin/matrixone/issues/28729), [#27885](https://github.com/matrixorigin/matrixone/issues/27885)
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/28850
- Base: `origin/main` at `61efcf43a145c53435933e14d98ff2ba61afabd5`

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
- preserve rolling-version and persisted-metadata decode compatibility;
- keep a deterministic public rejection oracle for the removed SQL command.

Non-goals:

- redesign TN merge policy, scheduling, admission, or transfer-page behavior;
- remove the shared `MergeCommitEntry`, transfer-map types used by flush, or
  `mergesort` implementation used by TN;
- make a new user-facing synchronous force-merge API.

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
| Delete protobuf symbols and reuse their numbers immediately | Rejected: creates needless source/wire and persisted-metadata incompatibility during rolling upgrade and rollback. |

## Compatibility and lifecycle

The old protobuf names and numeric slots remain compatibility tombstones in this
revision: `OpCommitMerge`, `MergeTaskEntry`, `TaskCode.MergeObject`, shard
`MergeObjectsParam`, and the persisted `min_cn_merge_size` fields. No new code
may produce or handle them. Removing or reserving those generated definitions is
a later compatibility-window change.

Mixed-version behavior fails closed:

- new CN to old or new TN: new CN never emits a CN merge request;
- old CN to new TN: the retired write opcode has no handler and returns the
  existing unsupported-write error before mutating storage;
- a persisted legacy async task fetched by a new CN finds no registered
  executor and is completed as failed by the task runner without retry;
- old shard requests to a new CN have no registered read method and fail without
  running a merge.

Schema-extra cloning continues to preserve `min_cn_merge_size` bytes so a
rolling downgrade does not erase metadata understood by an older binary. The
removed TN TOML offload keys were already write-only; the decoder tolerates
unknown keys, so existing files remain loadable. Rollback is a normal binary
rollback: the tombstone wire and catalog definitions are still present.

## Ownership and unhappy paths

Before removal, CN owned readers, an arena, merge output objects, in-memory or S3
transfer maps, and the request lifetime, while TN owned the transactional commit
and final cleanup. After removal, none of those resources can be created on CN.

| Audit | Result |
|---|---|
| Q1: one destruction owner | The entire CN creation path and its cross-node cleanup handoff are deleted. TN merge retains its existing task-owned cleanup and transaction entry. |
| Q2: waits terminate | The ten-minute legacy task wait and CN-to-TN write/retry edge are deleted. No replacement wait is added. The BVT waits only on visible object metadata with a bounded harness deadline. |
| Q3: growth is bounded | CN object batches and transfer maps can no longer accumulate. TN admission and scheduling bounds are unchanged. |

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

The public rejection BVT has zero setup rows and no external fixture beyond its
own SQL session. The existing branch/GC fixture and cardinality are retained;
only its compaction mechanism changes. No benchmark, race, restart, or scale run
is required because this change deletes the CN concurrency/resource path and
does not alter the retained TN scheduler state machine or hot path.

## Decision record

Change scope: complete legacy CN object-merge subsystem.

Trigger: major refactor crossing SQL, CN, disttae, shard, TN RPC, task, and merge
ownership boundaries; it changes a public command and mixed-version behavior.

Decision: PASS for revision v1. The user explicitly selected complete CN merge
removal; runtime deletion with protobuf/catalog tombstones is the compatibility
constraint applied to that decision. There are no open design blockers.

Implementation deviations: none at implementation start.
