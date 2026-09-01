# Cross-CN DDL Visibility Fence

- Status: **Approved**
- Revision: 3
- Approval: user approval recorded in the PR implementation session on 2026-08-30
- Owning issue: #27743
- Implementation PR: #27756
- Protocol version: MORPC v43

## 1. Classification and motivation

This change is a distributed-protocol feature rather than a narrow local fix. It crosses frontend transaction commit, CN lifecycle and query RPC, cluster membership, HAKeeper replicated state, protobuf compatibility, and proxy admission. It changes persistent state, wire contracts, mixed-version rollout, restart, and rollback behavior; therefore it requires design approval.

Issue #27743 demonstrates a missing catalog-visibility invariant: a DDL can commit on CN A while CN B admits a fresh snapshot before applying the corresponding catalog logtail. The client then observes `no such table` or equivalent stale-catalog behavior.

## 2. Invariant and success criteria

### Safety invariant

After the v43 deployment epoch is committed, every CN capable of public DDL production must satisfy both conditions:

1. its runtime protocol is at least v43, so DDL commit performs `SyncCommitV2` fan-out; and
2. it belongs to the exact generation/address membership set atomically committed by HAKeeper, or it remains fail-closed until it joins a later exact cut.

Before a CN opens public ingress, its local catalog frontier must be at least the maximum frontier reported by all participants in the applicable cut.

### Negation

It is unsafe for any public or already-connected CN to commit DDL below v43 while another public CN can admit a snapshot that has not applied that commit.

### Measurable criteria

- A create on CN0 followed immediately by a first read/load on CN1 succeeds without explicit `SYNCCOMMIT`.
- Markerless startup preserves the current-main v42 baseline before the v43 cut.
- No local downgrade below v43 is accepted after the monotonic v43 epoch is committed.
- Membership change between final scan and epoch commit rejects the old target set atomically.
- Restart, timeout, persistence failure, response loss, and leader failover remain fail-closed.

## 3. State ownership

| State | Owner | Persistence | Meaning |
|---|---|---|---|
| Compiled latest protocol | binary/runtime | binary | Receiver/sender code capability only |
| Local deployed protocol | CN metadata file | durable local FS | This CN completed or provisionally entered a cut |
| Cluster deployed epoch | HAKeeper CNState | replicated snapshot/log | Monotonic cluster-wide committed cut |
| Admission generation/address | HAKeeper CNState | replicated snapshot/log | Identity of one CN incarnation |
| Prepared/Fenced/Complete | CN service and HAKeeper CNState | heartbeat-replicated phase proof plus local marker | Progress bound to one authoritative CN incarnation |
| Last committed DDL frontier | DDLCommitGate and HAKeeper CNState | monotonic cluster-lifetime maximum replicated in HAKeeper state | Durable activation high-water mark that survives CN restart, replacement, and store removal |
| Public DDL gate | frontend `DDLCommitGate` | process-local | Whether new public/background DDL may enter |
| Proxy ingress readiness | CN heartbeat/HAKeeper | replicated latest state | Whether new routed sessions may enter |

HAKeeper is the first owner of cluster epoch and membership linearization. CN local metadata is not authoritative for cluster membership.

## 4. Protocol states

A CN is in one of these logical states:

1. **Baseline v42**: v43 not deployed; existing v38-v42 contracts remain active. Public ingress may be open.
2. **Withdrawing**: activation blocks new DDL, withdraws ingress, and drains active DDL.
3. **Prepared v43**: local old-protocol producers are drained; runtime can receive v43 RPCs.
4. **Provisionally fenced**: local frontier synchronization completed and `-43` is durable; ingress remains closed.
5. **Cluster committed**: HAKeeper atomically validated exact `(serviceID, generation, queryAddress)` membership and advanced epoch to 43.
6. **Locally committed**: CN persisted `43`; only then may it republish ingress and unblock DDL.
7. **Markerless post-cut**: no local marker but HAKeeper epoch is 43; runtime remains v43 and ingress/DDL remain closed until a complete retry.

The cluster epoch commit is the linearization point. Prepared, Fenced, and the last committed DDL frontier are published through each incarnation's heartbeat. The commit heartbeat contains the exact target tuples. In one replicated transition HAKeeper updates the sender heartbeat, compares all eligible raw CNState members, exact generation/address, receiver capability, and that each current incarnation itself published Prepared and Fenced, then advances the epoch only on exact equality. A replacement cannot reuse an older incarnation's Fenced proof. A join before this transition invalidates the target set; a join after it observes epoch 43 and is rejected as ingress-ready.

## 5. End-to-end flow

### First rollout

1. All LogStore/HAKeeper replicas advertise support for the epoch schema.
2. Every CN runs v43-capable code but markerless CNs keep protocol baseline v42.
3. `mo_ctl SetProtocolVersion` refreshes raw authoritative CN membership.
4. The requested set must exactly match all eligible CN tuples and each target must advertise the v43 receiver/barrier capability.
5. Targets concurrently withdraw ingress, block and drain DDL, capture their reconstructed latest committed timestamp, then heartbeat Prepared together with that frontier. HAKeeper advances a cluster-lifetime maximum and never lowers it when an incarnation restarts, is replaced, or is removed.
6. Each target reads and applies the replicated cluster-lifetime frontier, durably enters provisional Fenced, and heartbeats Fenced. This avoids cyclic QueryService control RPCs while every target is already serving a long-running activation RPC.
7. Each target confirms all exact current incarnations are Fenced in HAKeeper; HAKeeper atomically revalidates those tuple-bound proofs and commits epoch 43.
8. Each CN persists local committed 43, republishes ingress if listeners are live, and unblocks public DDL.

### Steady-state DDL

A public real-user DDL, and background DDL after public listeners are enabled, enters `DDLCommitGate`. After commit, the producer synchronously advances the monotonic HAKeeper cluster frontier before acknowledging success; publication failure fails the statement closed. The returned `CommandBatch.ViewMetadataAdmission.Generation` must exactly equal the sender generation. A stale sender rejected by HAKeeper therefore revokes itself and cannot mistake RPC delivery for durable frontier acceptance. This removes both the commit-success-to-periodic-heartbeat crash window and the old-incarnation-after-takeover window. Protocol v43 then triggers `SyncCommitV2` to all barrier-ready CNs. The operation succeeds only after generation-bound durable frontier publication and required receivers have applied/synchronized the commit frontier. Bootstrap background work before ingress remains exempt to avoid depending on an unavailable HAKeeper/QueryService.

### Scale-out and replacement

A markerless CN performs an atomic ingress heartbeat handshake. If it linearizes before the cluster commit, it becomes authoritative membership and invalidates any old target proof. If it linearizes after commit, HAKeeper forces ingress false and returns epoch 43. It never becomes a public v42 producer after the cut.

## 6. Failure, retry, and lifecycle behavior

- **Withdrawal failure or drain timeout**: ingress and DDL gate remain closed; retry uses raw QueryService identity.
- **Frontier RPC/application failure**: CN remains Prepared or provisional and closed.
- **Provisional persistence failure**: Fenced is never published.
- **Atomic epoch membership rejection**: epoch does not advance; all targets remain closed for retry with a refreshed set.
- **Epoch response loss**: epoch may be committed, but local marker remains provisional and ingress closed; retry learns the committed epoch.
- **Committed local persistence failure**: cluster epoch remains committed; this CN remains closed and restarts from provisional state.
- **Ingress publication uncertainty**: perform a bounded cleanup withdrawal; never assume publication failed.
- **Restart**: committed marker runs startup frontier synchronization before opening; provisional or markerless post-cut starts v43 fail-closed.
- **Shutdown**: stop periodic heartbeat publication, then withdraw ingress/barrier state before stopping QueryService.
- **Leader failover**: activation is gated until every voting and non-voting LogStore advertises epoch-schema capability, so any eligible HAKeeper leader preserves the field.

Retries are idempotent for the same generation and target set. Replacement generations or addresses are rejected before local state mutation.

## 7. Compatibility, rollout, downgrade, and rollback

### Mixed-version baseline

Current main already deploys v42 semantics. A fresh v43-capable process without a DDL marker therefore remains at v42, not v37. The v43 DDL state is separate from unrelated v38-v42 capabilities.

### Rollout order

1. Upgrade all voting and non-voting LogStores/HAKeeper replicas.
2. Upgrade every CN; verify barrier receiver capability in raw inventory.
3. Invoke one exact complete-target v43 activation.
4. Verify epoch 43 and all eligible CN ingress/committed markers.

### Downgrade policy

Before epoch 43, ordinary protocol changes at or below v42 remain possible. After epoch 43, local downgrade below v43 is rejected. A safe rollback would require a separately designed atomic cluster rollback that withdraws and drains every DDL producer before lowering the epoch; this revision deliberately does not implement epoch rollback.

Binary rollback after epoch 43 is unsupported until such a rollback protocol exists. Operators must restore forward to a v43-capable binary.

## 8. Proxy and direct ingress

Proxy routing consumes HAKeeper admission/readiness and therefore excludes fail-closed CNs. Direct SQL listeners are additionally protected by the local `DDLCommitGate`; suppressing proxy routing alone is insufficient because existing and direct sessions survive routing changes. Listener-ready and ingress-ready are separate lifecycle facts.

## 9. Performance and capacity

Let `N` be eligible CN count.

- Activation performs O(N) membership validation and scans heartbeat-replicated phase/frontier state. It is an operator-triggered bounded transition, not a per-statement hot path. Target count is capped at 1024.
- DDL commit fan-out is O(N) RPCs and O(N) response ownership. DDL is low frequency relative to DML; no unbounded queue or background worker is introduced.
- Requests are bounded by existing discovery/RPC contexts. Target maps and response slices are released after each operation.
- Expected added DDL latency is the slowest required CN frontier application plus network fan-out. Rollout acceptance should record N-CN p50/p95 for representative 3-CN and larger staging clusters; no latency claim is made without that evidence.

## 10. Observability and operations

Required diagnostic state is available through protocol-version RPCs, CN heartbeat inventory, local deployed marker, HAKeeper cluster epoch, admission generation/address, barrier readiness, and ingress readiness. Activation errors identify missing targets, stale generation/address, unsupported HAKeeper replicas, membership drift, persistence failure, or frontier timeout.

Follow-up metrics should count activation attempts/failures by phase and DDL fan-out latency/failures. Until those metrics land, logs and `mo_ctl` inventory are the operational source.

## 11. Security and abuse bounds

The protocol adds no tenant data exposure and uses internal QueryService/HAKeeper channels. Exact generation/address checks prevent stale incarnation control. The 1024-target cap and bounded contexts limit operator-triggered amplification. Existing authorization for `mo_ctl` remains the trust boundary.

## 12. Alternatives

### A. Periodic or statement-local sleeps

Rejected: timing does not establish catalog visibility, is flaky, and adds unconditional latency.

### B. Read HAKeeper epoch once at startup

Rejected: a join can race the final commit after the read. Re-reading at ingress remains TOCTOU unless join and commit share one replicated ordering.

### C. Always use latest compiled scalar protocol

Rejected: compiled capability is not proof that every producer completed the distributed cut.

### D. Separate DDL feature epoch from the shared MORPC scalar

Architecturally clean and avoids scalar coupling, but still requires the same membership/ingress linearization and broader API migration. This revision retains MORPC v43 as the sender/receiver capability gate while storing deployment completion separately.

### E. TN-only global catalog barrier on every new transaction

Could provide a stronger generic read contract, but materially changes every transaction admission path and latency. The selected approach scopes cost to DDL and activation while preserving the required immediate cross-CN visibility.

## 13. Validation matrix

| Contract | Deterministic evidence |
|---|---|
| v42 baseline preserved | markerless default-v43 startup UT |
| post-cut downgrade rejected | completed-v43 downgrade UT |
| exact authoritative targets | ctl raw-membership omission/capability UT |
| join/commit atomicity | CNState RSM ordering UT: final scan, join, stale commit |
| HAKeeper failover compatibility | old capability view rejection UT |
| provisional/committed persistence | injected Replace failures plus distinct-service restart UT |
| startup and live frontier fencing | mock frontier/application ordering UT and race runs |
| automatic public behavior | two-CN embedded create on CN0 followed immediately by first CN1 read without `SYNCCOMMIT` |
| cancellation and publication uncertainty | bounded timeout, stale heartbeat, cleanup withdrawal UTs |

## 14. Decision log and open items

- Chosen linearization point: HAKeeper replicated heartbeat transition with exact tuple proof.
- Chosen baseline: preserve current-main v42 before v43 activation.
- Chosen downgrade behavior: reject after monotonic epoch commit.
- Chosen direct-ingress protection: local DDL gate in addition to proxy admission.
- Non-blocking follow-up: add dedicated activation/fan-out metrics and publish N-CN staging latency evidence.
- Design gate: revision 1 was explicitly approved on 2026-08-30; no blocking design item remains.
