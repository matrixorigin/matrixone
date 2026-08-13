- Status: draft
- Start Date: 2026-08-13
- Authors: XuPeng-SH
- Parent RFC: [Canonical Compute-Group Execution](00000000_compute_group_execution.md)
- Issue: [#25451](https://github.com/matrixorigin/matrixone/issues/25451)

# Compute-Group Execution Acceptance Matrix

# Purpose

This document converts the compute-group product contract into independently
observable acceptance cases. It is normative: an implementation cannot replace
a row with a nearby unit test, weaken the oracle to “did not panic”, or mark a
distributed contract complete using a synthetic worker list only.

Milestone A accepts this matrix as the contract. Milestone B must automate all
rows marked `B` before the feature can be enabled in production. Rows marked
`D` define the later admission boundary and must remain unsupported—not
partially emulated—until an admission owner exists.

#26109 delivers the Milestone A reset plus independent legacy-scheduler fixes
for writable-workspace ingress participation and ingress-only
`LOAD DATA LOCAL`. The rejected policy implementation remains removed, and
none of the unchecked Milestone B gates below is claimed as complete: these
local invariants do not provide compute-group identity, authorization, or
transaction pinning. #25451 remains the implementation gate until named tests
and current results, including the multi-CN witnesses and disabled-path
benchmark, exist.

# Test topology

All distributed rows use a deterministic fixture with explicit readiness
signals and no sleeps:

| Object | Identity | Membership | ACL |
|---|---|---|---|
| Group `a-primary` | immutable ID `ga1` | ingress `a1`, worker `a2` | account A |
| Group `a-analytics` | immutable ID `ga2` | workers `a3`, `a4` | account A; explicit-use role only |
| Group `b-primary` | immutable ID `gb1` | ingress `b1`, worker `b2` | account B |
| Group `system-primary` | immutable ID `gs1` | ingress `s1`, worker `s2` | system account only |
| Replacement object used only by CG-ID-03 | immutable ID `ga3`, absent initially; created later with name `a-primary` | worker `a5` | account A |

The harness can add members for a case, but it must never identify a group from
labels. Every CN exposes a controllable pipeline address, work state,
capability version, and membership generation. The fixture exposes barriers
for placement reached, RemoteRun started, workspace dirtied, drain published,
and cleanup completed.

# Oracle rules

1. Public behavior is asserted through SQL/protocol results, error class, side
   effects, and service health.
2. White-box assertions inspect typed group ID/generation, placement workers,
   and ownership counts. They do not parse incidental log text.
3. A rejected statement asserts zero execution-side effects and zero remote
   starts.
4. Every concurrency case uses explicit barriers and bounded contexts, never
   `time.Sleep` as a phase oracle.
5. Each regression must fail on the unfixed revision for the claimed semantic
   reason and pass unchanged on the fixed revision.
6. Cleanup assertions require exact counts, not merely non-zero calls.

# Error taxonomy

Names are provisional, but distinct classes are required:

| Class | Meaning | Retryable by choosing the same group? |
|---|---|---|
| `compute_group_binding_required` | Enabled account has neither an authorized connection selection nor a default | no, configuration action required |
| `compute_group_not_accessible` | Requested name/ID is absent from the authenticated account's filtered binding view; globally absent and foreign-owned are indistinguishable | no |
| `compute_group_unknown` | An ID already present in the account binding is now deleted/dangling | no, configuration action required |
| `compute_group_unauthorized` | Authenticated role lacks explicit-use permission for a group in its account binding | no |
| `compute_group_unavailable` | Known group has no routable healthy member or is draining | yes after external state change |
| `compute_group_incompatible` | Protocol/capability generation cannot execute safely | yes after upgrade |
| `compute_group_constraint_conflict` | Authorized group excludes required ingress/local owner | no for this binding/statement |
| `compute_group_transaction_pinned` | Override conflicts with active transaction group | no until transaction ends |
| `compute_group_overloaded` | Healthy group is at admitted capacity | reserved for Milestone D |
| `compute_group_queue_timeout` | Bounded admission wait expired/cancelled | reserved for Milestone D |

Not-accessible, dangling/unknown, unauthorized, unavailable, incompatible, and
overloaded must never become a generic “no worker” fallback. Internal audit
may distinguish absent from foreign-owned, but the client-facing error and
timing bucket for those two cases are the same.

# A. Disabled path, identity, and authorization

| ID | Gate | Scenario | Independent control | Required oracle |
|---|---|---|---|---|
| CG-DIS-01 | B | Feature disabled for account A | Current main at same commit | Exact query result and worker topology match legacy; resolver/catalog/RPC/cache counters stay zero. |
| CG-DIS-02 | B | Upgrade installs group-capable binaries but no account is enabled | Pre-upgrade binary | Login, prepare, execute, DML, and DDL behavior are unchanged; no background group goroutine or retained entry. |
| CG-DIS-03 | B | Disabled-path microbenchmark | Same benchmark on current main/pre-feature code; enabled mode is measured separately | Candidate-disabled adds zero allocations and paired p50/p95 regression is at most 2% versus legacy main; enabled cost is reported but is not the control. |
| CG-ID-01 | B | Select `a-primary` by name | Select same workers using raw matching labels | Resolved identity is `ga1`; labels are absent from the request and cannot change the result. |
| CG-ID-02 | B | Name absent from account A's filtered binding view | Foreign-owned `b-primary` requested by A | Both fail `compute_group_not_accessible` with the same public shape/timing bucket before discovery or side effects. |
| CG-ID-03 | B | Delete `ga1`, recreate name `a-primary` as `ga3` | Explicitly rebind to `ga3` | Old binding stays dangling and fails; it never attaches to `ga3` by name or labels. |
| CG-ID-04 | B | CN advertises labels matching `a-primary` but group ID `gb1` | CN advertises `ga1` | Mismatched CN is never eligible. |
| CG-ID-05 | B | Proxy and SQL resolve `ga2` concurrently | Resolve from the same sealed registry fixture | Both observe the same `(group ID, generation, member UUID set)`; neither recomputes membership from labels. |
| CG-AUTH-01 | B | Account A uses its default `ga1` binding | Same query with feature disabled | Query runs only on `ga1` members. |
| CG-AUTH-02 | B | Authorized A role explicitly selects `ga2` | A role without explicit-use grant | Authorized query uses only `ga2`; control fails `compute_group_unauthorized`. |
| CG-AUTH-03 | B | Account B requests `ga1` by name and raw/stale ID | B requests a globally absent name/ID | All fail `compute_group_not_accessible` with indistinguishable public result/timing and no discovery; account A's authorized control succeeds. |
| CG-AUTH-04 | B | System account requests ordinary-account `ga1` | System account requests its exclusively owned and explicitly bound `gs1` | `ga1` is not accessible and cannot be granted across owners; `gs1` succeeds. There is no implicit system-account bypass or ownership exception. |
| CG-AUTH-05 | B | User-supplied labels/account name try to widen candidate set | Authenticated IDs with no extra input | Effective ACL and worker set are identical; untrusted labels are ignored/rejected. |
| CG-AUTH-06 | B | Delete account A, then recreate the same account name with a new immutable account ID | Original account remains active | Old binding is revoked/tombstoned and cannot authorize the recreated account; all A-owned groups enter bounded drain/delete and release CN/ticket/registry resources exactly once; no name lookup reattaches them. |
| CG-AUTH-07 | B | Replay, expire, or alter a post-auth handoff ticket | Fresh single-use ticket for the authenticated immutable IDs | Target CN rejects before session creation or SQL side effects; Proxy cannot substitute labels or claimed account name, and provisional resources are released once. |
| CG-AUTH-08 | B | Issue ticket at membership generation N, then publish N+1 or delete before claim/consume | Fresh ticket issued from N+1 | Old ticket is aborted/rejected before session publication and cannot be replayed; fresh control consumes once on an N+1 target. |
| CG-AUTH-09 | B | Proxy claims a ticket, then cancellation, dial failure, or lost target-validation response occurs | Normal claim/consume | No target session becomes externally visible; claim aborts or its bounded lease expires exactly once, a late response cannot consume, and retry reauthorizes to a new nonce. |
| CG-AUTH-10 | B | Target consumes and publishes an attach-blocked session, but consume response or attach acknowledgement is lost | Normal consume/attach | Idempotent status returns the exact target/session tuple; Proxy either attaches once or revokes and observes cleanup before a new nonce. Unacknowledged lease expiry closes the old session; it runs zero SQL and cannot coexist with a retry session. |
| CG-AUTH-11 | B | Commit/revoke A role's `ga2` grant while account binding continues to allow `ga2` | Account binding removes `ga2` while a stale role grant remains | Grant transaction monotonically bumps role-grant generation and explicit selection succeeds only from the sealed pair; revoke fails new attempts/retries and aborts an active transaction at its boundary; stale grant never widens the account binding. |

# B. Binding, session, transaction, and local ownership

| ID | Gate | Scenario | Independent control | Required oracle |
|---|---|---|---|---|
| CG-BIND-00 | B | Feature enabled with no authorized connection selection and no administrator default | Feature disabled for the same account | Login fails `compute_group_binding_required` after authentication and before target session creation; it never falls back to legacy/labels. |
| CG-BIND-01 | B | Statement override and session selection exist on a connection established from the default | Remove one source at a time | Statement > session execution selection > connection ingress binding; each decision records source/generation, and the default is used only to establish ingress. |
| CG-BIND-02 | B | On a `ga1` ingress connection, change session execution selection to `ga2` outside a transaction and run a stateless read | Fresh connection whose ingress is `ga2` | Relational/user operators execute only on `ga2`; original ingress remains `ga1` and performs only authenticated coordination, fan-in, serialization, and protocol forwarding. Its separately accounted control cost contains no fallback fragment; no implicit migration occurs. |
| CG-BIND-03 | B | Publish binding generation N+1 while an N attempt is blocked after remote start | Publish N+1 before resolving the control attempt | Running N attempt retains its immutable decision; the next attempt reauthorizes and records N+1, with no hybrid generation. |
| CG-BIND-04 | B | Change account A's default from `ga1` to `ga2` while a `ga1` connection is idle | Open a new connection after the change | Existing ingress remains `ga1` while authorized; new connection establishes ingress in `ga2`; neither is silently migrated. |
| CG-TXN-01 | B | `BEGIN` under `ga1`, then change session to `ga2` | Change after COMMIT | Active change fails `compute_group_transaction_pinned`; post-COMMIT change succeeds. |
| CG-TXN-02 | B | Implicit transaction first DML under `ga1` | Autocommit read-only SELECT | Group ID is pinned before the first write side effect and released exactly once at terminal state. |
| CG-TXN-03 | B | Dirty ingress workspace followed by large SELECT with `ga1` | Same SELECT in clean transaction | Dirty case includes ingress and reads its own write; clean control may use another `ga1` member. |
| CG-TXN-04 | B | Dirty workspace with statement override to ingress-excluding `ga2` | Clean read-only override to `ga2` | Dirty case fails constraint conflict before RemoteRun; clean control runs on `ga2`. |
| CG-TXN-05 | B | Retry after a worker failure in active `ga1` transaction | Healthy first attempt | Retry remains authorized to `ga1`, reacquires a current `ga1` snapshot, and never uses `ga2`/`gb1`. |
| CG-TXN-06 | B | Revoke `ga1` ACL while a transaction statement is running | Non-revoking binding-generation update | Running statement may finish; before the next statement or COMMIT, the transaction is rejected, rolled back, and its pin released exactly once within the configured transaction/session bound. |
| CG-TXN-07 | B | First implicit/autocommit DML on `ga1` ingress with execution selection `ga2` | First DML with execution selection `ga1` | Ingress-excluding case fails constraint conflict before lock acquisition, workspace creation, RemoteRun, or data side effects; control pins `ga1` before its first side effect. |
| CG-LOCAL-01 | B | `LOAD DATA LOCAL INFILE` while ingress is in selected group | Non-LOCAL object-store LOAD | LOCAL keeps its reader on ingress and completes, or fails before upload if ingress is excluded; upload goroutine always terminates. |
| CG-LOCAL-02 | B | LOCAL INFILE statement override to ingress-excluding `ga2` | Same data via remotely readable object storage | LOCAL fails constraint conflict before pipe creation/write; object-store control may use `ga2`. |
| CG-LOCAL-03 | B | Query reads a temporary/local object | Equivalent durable table | Local object retains ingress participation or fails before execution; durable control may route remotely. |
| CG-LOCAL-04 | B | Statement depends on session-owned non-serializable state | Stateless equivalent | Stateful case retains ingress or fails explicitly; no partial remote execution. |
| CG-DDL-01 | B | DDL on `ga1` ingress with session execution selection also `ga1` | Same DDL with no session selection | Both preserve the existing ingress/lock/commit owner and are behaviorally equivalent. |
| CG-DDL-02 | B | DDL on `ga1` ingress with session execution selection `ga2` | DDL with an explicit statement override | Session case fails constraint conflict before lock/catalog/remote side effects; explicit override is rejected before the same side effects. |
| CG-PREP-01 | B | PREPARE under default `ga1`, switch session to `ga2`, EXECUTE | Direct execution under `ga2` | EXECUTE resolves `ga2`; PREPARE owns no group snapshot or capacity. |
| CG-PREP-02 | B | Reuse prepared statement after `ga2` membership/address generation changes | Fresh prepare after change | Reused execution rebuilds stale topology and is equivalent to fresh execution. |
| CG-CANCEL-01 | B | Cancel while group resolution/refresh is blocked | Successful resolution | Cancellation returns within bound; waiter, snapshot ref, and transaction pin counts return to baseline exactly once. |
| CG-CANCEL-02 | B | Cancel running remote attempt | Normal completion | All remote pipelines terminate, result writer closes once, and no group reference remains. |
| CG-PROXY-01 | B | Reuse a cached connection after its group membership generation changes | Fresh connection at the new generation | Reused connection revalidates the same group ID and generation before statements; it cannot retain an evicted member. |
| CG-PROXY-02 | B | Proxy rebalances a safe idle session during scale-down | One case each with an in-flight statement, transaction, dirty workspace, temp/local object, local reader, user/advisory lock, cursor, non-serializable or unknown state | Safe session moves only to the same group ID and rebinds generation; every unsafe/unknown control remains on ingress or closes without partial transfer. `!inTxn` alone never authorizes migration. |
| CG-PROXY-03 | B | No member remains in the selected group during rebalance | A member remains healthy | Session is closed/unavailable according to contract; it never falls back to unlabeled/shared/another-account CNs. |

# C. Membership, drain, deletion, retry, and rolling upgrade

| ID | Gate | Scenario | Independent control | Required oracle |
|---|---|---|---|---|
| CG-GEN-01 | B | Add/remove a member from `ga2` | No membership change | Generation increments once; new attempt sees new set; already running attempt retains sealed old snapshot. |
| CG-GEN-02 | B | Change a member's pipeline address without changing ID | Fresh member ID | Prepared and cached topology cannot use the old address after the generation boundary. |
| CG-GEN-03 | B | Delayed callback from generation N after N+1 is published | Callback before N+1 | Old callback cannot mutate, retire, or reopen N+1 state. |
| CG-GEN-04 | B | Concurrent membership, address, and drain updates | Apply the same events serially in authority order | Registry publishes sealed monotonic generations; no observer sees a hybrid member/address/work-state set. |
| CG-GEN-05 | B | Group moves Provisioning→Ready after its first sealed compatible snapshot | Incomplete or mixed-generation snapshot | Ready and its generation publish atomically to Proxy/auth/SQL; control remains unavailable and cannot activate a binding. |
| CG-GEN-06 | B | Restart/re-elect the control-plane owner after publishing generation N, then reconcile a change | Restart with no change | Unchanged case may republish exactly sealed N; changed case durably publishes a value greater than N, never zero/reused, and ambiguous state remains unavailable rather than label-reconstructed. |
| CG-DRAIN-01 | B | Publish Draining while one attempt runs | Working group | New attempt fails unavailable; running attempt finishes before configured deadline. |
| CG-DRAIN-02 | B | Running attempt exceeds drain deadline | Attempt finishes just before deadline | Overdue attempt is cancelled and cleanup completes once; control completes normally. |
| CG-DRAIN-03 | B | Prepared topology contains newly Draining worker | Fresh execution after drain | EXECUTE invalidates topology before RemoteRun and never places new work on the draining CN. |
| CG-DEL-01 | B | Delete group with active and future attempts | Drain without delete | Future attempts fail; active attempt follows drain policy; bindings become dangling, not label-resolved. |
| CG-DEL-02 | B | Delete after drain while one participant delays its generation-fenced no-reference acknowledgement | All participants acknowledge | Finalization waits only to the configured lease/deadline, cleanup is effective once, stale acknowledgements cannot finalize a recreated ID, and late tickets/references hit the tombstone. |
| CG-RETRY-01 | B | First attempt loses selected worker, another member in same generation is healthy | No alternate member | Retry may choose the same group member set only; no-member control returns unavailable. |
| CG-RETRY-02 | B | Account ACL or explicit role grant is revoked between first attempt and retry | Both authorization generations unchanged | Retry reauthorization fails before remote start; control retries inside same group and records the sealed binding/grant generations. |
| CG-UPG-01 | B | One Working member of target `ga1` lacks group protocol capability | All `ga1` members capable while unrelated `gb1` remains old | Target-member incompatibility blocks enable/bind; unrelated old group does not, proving group-local rollout scope. |
| CG-UPG-02 | B | One Draining member still serves an existing session and lacks capability | Drained member | Draining executable member blocks activation; fully Drained member does not receive new work. |
| CG-UPG-03 | B | Roll back after feature enable with pinned attempts | Disabled account with no attempts | New bindings stop first, pinned attempts drain, then capability use ends; no mixed wire contract. |
| CG-UPG-04 | B | Add an incompatible candidate member, then downgrade a member already executable in the current Ready generation | Upgrade the candidate before publication | Additive candidate stays staged while Ready N remains usable; downgrade publishes Incompatible for new attempts and receivers fail closed; recovery uses a generation greater than N. |

# D. Placement, topology, observability, and performance

| ID | Gate | Scenario | Independent control | Required oracle |
|---|---|---|---|---|
| CG-PLACE-01 | B | Read-only analytics on four `ga2` members | Same selection key repeated | Selected subset is deterministic, contains only `ga2`, and respects execution topology constraints. |
| CG-PLACE-02 | B | Same SQL changes planner TP/AP mode after stats/index change | Fixed explicit binding | Resolved group ID does not change; only topology inside that group may change. |
| CG-PLACE-03 | B | Selected remote worker has no routable address | Healthy address | Fails before scope execution; never falls back to ingress or another group. |
| CG-PLACE-04 | B | Multi-CN topology collapses to one remote member | Two remote members | Remote scope tree remains standalone executable and all local dispatch receivers have an owner. |
| CG-PLACE-05 | B | Heterogeneous advertised CPU counts | Homogeneous counts | DOP is bounded by each member capability, has no overflow, and does not change authorization. |
| CG-WIRE-01 | B | Deliver a `ga1` RemoteExecutionGrant to `gb1` or to a service ID other than its target | Valid target in `ga1` | Receiver rejects before operator construction/route claim side effects; account labels or sender eligibility cannot widen the target. |
| CG-WIRE-02 | B | Materialize a `ga1` generation-N route, then remove its target or publish N+1 before remote start | Route starts before the barrier | Stale route is rejected before operator start and coordinator rebuilds/retries only with fresh authorization; already-started control follows pinned drain rules. |
| CG-WIRE-03 | B | Feature-enabled receiver gets an old/mixed-version envelope with group/generation/grant fields absent | Complete compatible grant | Missing-field route fails incompatible with zero operator starts; it never takes the legacy path or falls back to ingress. |
| CG-WIRE-04 | B | Replay the same `(attempt_id, route_id, target)` after its first claim/start and after completion | Fresh route ID in a fresh attempt | Replay is rejected before another operator/side effect; claim/tombstone state is released within its declared bound. |
| CG-WIRE-05 | B | Receiver starts a route but its start response is lost, then retry is requested | Start acknowledgement delivered normally | Coordinator idempotently rejoins or cancels and observes the old route terminal before any replacement starts; at most one DML side effect occurs and route/snapshot owners release once. |
| CG-OBS-01 | B | Successful default, session, and statement bindings | Rejected authorization/constraint cases | Trace exposes requested/resolved group, binding source/gen, membership gen, outcome, worker count, and retry with bounded enums. |
| CG-OBS-02 | B | EXPLAIN/preview under `ga2` | Actual execution with same inputs | Preview is representative, does not mutate cache ownership or reserve resources, and never claims admission. |
| CG-OBS-03 | B | Unknown names generated at high cardinality | Repeated known name | Metrics cardinality remains bounded; misses do not create unbounded cache/log state. |
| CG-PERF-01 | B | Enabled group with 2 members in a 100-CN cluster | Same group in a 4-CN cluster | Per-query work is proportional to group size; no all-cluster RPC/poll. |
| CG-PERF-02 | B | Sequential short-lived sessions on one enabled account | Concurrent distinct accounts beyond cache capacity | Stable state is reused without per-statement catalog I/O; idle retention remains at its declared bound. |

# E. Admission boundary (future, must not be approximated in Milestone B)

| ID | Gate | Scenario | Required oracle |
|---|---|---|---|
| CG-ADM-01 | D | Healthy group at concurrent-query limit | New request enters a bounded FIFO/fair queue or returns documented retryable overload; it is not marked unavailable. |
| CG-ADM-02 | D | Queue full | Immediate `compute_group_overloaded`; waiter count never exceeds capacity. |
| CG-ADM-03 | D | Queued request cancelled/deadline expires | Prompt removal and exactly-once notification/release; no permit is consumed. |
| CG-ADM-04 | D | Permit holder succeeds, errors, panics, is cancelled, loses CN, or retries | Exactly one permit release for every terminal path; retry reacquires rather than transfers stale ownership. |
| CG-ADM-05 | D | Two accounts share a group under skewed load | Declared per-account fairness/limits hold; one account cannot starve or exhaust all bounded waiters. |
| CG-ADM-06 | D | Admission owner restarts | Recovered accounting cannot double-admit or leak reservations; stale generation callbacks are fenced. |

# Ownership and wait-for audit matrix

| Resource/wait | Creation/claim | Effective terminal owner | Required release/termination | Bound |
|---|---|---|---|---|
| Group snapshot ref | Attempt resolution | Attempt | success/error/cancel/retry handoff | current + active generations |
| Transaction group pin | BEGIN/implicit first statement | Transaction | commit/rollback/abort/connection close | active transactions |
| Role-grant snapshot/cache | Transactional privilege grant/revoke | Security authorization generation | revoke/delete/generation replacement/cache eviction | durable grants or fixed-capacity generation-aware cache |
| Handoff ticket/claim | Post-auth authorization resolution / Proxy atomic claim | Security ticket service until target consume linearizes | consume/abort/cancel/route failure/claim lease/TTL | issue rate × TTL plus bounded tombstones |
| Registry refresh waiter | Cache miss/stale snapshot | Caller generation | result/context/internal deadline | active callers + configured cap |
| Prepared topology | EXECUTE materialization | Prepared entry/attempt | invalidation/deallocate/session close | prepared-statement limit |
| Remote route claim | Receiver validates `(attempt_id, route_id)` | Remote attempt | completion/error/cancel/expiry; replay tombstone then expires | active routes + bounded tombstones |
| Remote pipeline | Topology start | Attempt | completion/error/cancel/drain deadline | admitted worker topology |
| Trace record | Attempt decision | Trace ring/exporter | bounded export/overwrite | fixed ring/cardinality |
| Admission permit (D) | Successful admission | Attempt | every terminal path exactly once | configured permits |
| Admission waiter (D) | At-capacity request | Admission queue | admit/cancel/timeout/reject | configured queue length |

# Evidence protocol for automated rows

Every row records the candidate commit, relevant control-plane generations,
expected public result/error, exact side-effect counts, owning package/suite,
and the evidence applicable to its class:

| Evidence class | Applicable rows | Required independent evidence |
|---|---|---|
| Compatibility control | CG-DIS-01/02 | Candidate-disabled and legacy/pre-upgrade both pass the same public witness; no unfixed failure is expected. |
| Performance | CG-DIS-03 and CG-PERF-* | Raw paired benchmark samples, allocation counts, environment, statistical comparison, and legacy-main control; race or an unfixed semantic failure is not required. |
| New contract capability | Identity/auth/binding/local/placement rows not fixing a pre-existing bug | Pre-feature code is demonstrably unsupported or rejects the operation; candidate passes the public witness and typed white-box oracle. |
| Correctness regression | Any row claimed to fix behavior already intended on main | The unchanged test fails on the exact unfixed revision for the claimed reason and passes on the fixed revision. |
| Concurrency/lifecycle | CG-BIND-03, CG-TXN-06, CG-CANCEL-*, CG-PROXY-*, CG-GEN-*, CG-DRAIN-*, CG-DEL-*, CG-RETRY-*, CG-UPG-* | Deterministic phase barriers, bounded contexts, exact ownership counts, focused race stress, and owning-package race run. |
| Distributed protocol/topology | Rows involving Proxy, handoff, more than one CN, RemoteRun, membership, or rolling upgrade | Multi-CN public witness plus typed group/generation/worker oracle; a synthetic worker-list unit test alone is insufficient. |

A row can belong to more than one class and must satisfy their union. A race
command is not invented for a pure benchmark, and a multi-CN witness is not
required for a local disabled-path unit control. Conversely, classification
cannot be used to omit evidence relevant to the behavior under test.

Across the complete Milestone B implementation, the validation ladder is:

```text
focused semantic regression
-> focused deterministic race stress
-> full owning package under race once
-> multi-CN public-path acceptance
-> build and vet for all changed packages
-> disabled/enabled paired benchmark
```

Each stage applies to the owning changes for which it is meaningful. No PASS
result produced before the last relevant semantic edit or rebase is reusable.

# Milestone B exit gate

- [ ] Every `B` row has a named test and owner.
- [ ] Every externally visible contract has a multi-CN public witness.
- [ ] All rejection rows assert no side effects and bounded latency.
- [ ] All lifecycle rows assert exact ownership/release counts.
- [ ] Disabled mode has no catalog/RPC/cache/goroutine and passes the benchmark.
- [ ] No test identifies a group through raw labels or TP/AP classification.
- [ ] No test relies on sleep, order, retry-until-pass, or ambient cluster state.
- [ ] Operator, Proxy, transaction, security, pipeline, and SQL owners approve
      the rows they own.
