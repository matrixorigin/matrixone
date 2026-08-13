- Status: draft
- Start Date: 2026-08-13
- Authors: XuPeng-SH
- Implementation PR: [#26109](https://github.com/matrixorigin/matrixone/pull/26109)
- Issue for this RFC: [#25451](https://github.com/matrixorigin/matrixone/issues/25451)

# Canonical Compute-Group Execution

# Summary

MatrixOne should execute each statement inside one authorized, named compute
group while keeping concrete CN selection internal. The compute group is the
same operational object that MatrixOne Operator currently calls a `cnGroup`;
SQL must not introduce a second pool identity backed by independently supplied
labels.

This RFC defines the product and ownership contract required before a public
SQL surface is implemented. It deliberately replaces the account-level
`query_workload_policy` JSON proposed by #26109. The first implementation is a
static compute-group execution MVP with strict authorization and failure. It
does not include workload classification or resource admission.

This revision of #26109 delivers the Milestone A contract and removes the
rejected policy implementation. It also extracts two independently valid
legacy-scheduler safety fixes: writable workspaces retain ingress
participation, and `LOAD DATA LOCAL` remains ingress-only. Those fixes neither
resolve nor authorize a compute group and do not claim that Milestone B exists
or that its implementation acceptance gate is closed. #25451 remains open
until named owners implement every applicable acceptance row and attach the
required multi-CN and disabled-path benchmark evidence.

The companion [acceptance matrix](00000000_compute_group_acceptance_matrix.md)
is normative. A production implementation is not reviewable until every
Milestone B row has an automated owner and oracle.

# Motivation

MatrixOne can discover CNs, filter candidates, select deterministic subsets,
and materialize remote pipelines. What it cannot currently promise is that a
customer-selected name denotes one stable resource boundary.

The current operational model has three gaps:

1. Operator's `cnGroups` list is keyed by `name`, but a list entry has no
   immutable group ID, lifecycle generation, or ACL.
2. CN service metadata publishes raw labels, work state, addresses, and static
   capacity. It does not publish the owning cnGroup identity or its membership
   generation.
3. Proxy routes connections by matching untrusted connection attributes to CN
   labels. A matching label is not proof that the account or role may execute
   on the corresponding resources.

HAKeeper's task `cnPool` is reconstructed from a service snapshot for each
scheduling decision; it is not a durable resource pool. Existing protobuf
fields named `GroupID` refer to lock-table groups and must not be reused for
compute identity. Proxy's label hash and labeled-to-unlabeled/shared fallback
also cannot be promoted into a compute-group contract: both can choose a new
label-equivalent CN without preserving an immutable group generation.

Consequently, a SQL object containing both `pool: ap` and an unrelated label
map cannot define where work is allowed to run. The two values can disagree,
name reuse has ABA ambiguity, and no component owns rename, deletion, drain,
or authorization semantics.

Planner modes such as TP, AP-one-CN, and AP-multi-CN are also unsuitable public
assignment keys. Statistics, indexes, parameters, optimizer thresholds, and
software versions can change those modes for the same SQL. They remain
execution capabilities and topology constraints only.

# Goals

- Establish one canonical identity across Operator, Proxy, CN metadata, and
  SQL execution.
- Define ownership and authorization before public syntax or catalog storage.
- Preserve current behavior exactly when the feature is disabled.
- Make every explicit selection strict: unknown, unavailable, incompatible,
  or unauthorized groups fail before execution side effects.
- Pin immutable identity and topology snapshots at documented boundaries.
- Derive ingress, transaction, session, and local-input constraints from
  runtime ownership rather than customer configuration.
- Define bounded lifecycle, observability, and rolling-upgrade behavior.

# Non-goals

- TP/AP-driven group assignment.
- A raw-label selector API.
- Live CPU scoring or polling the cluster for every query.
- CPU, memory, fairness, or isolation claims based on worker-count/DOP caps.
- Shared-group admission, queueing, or autoscaling in the static MVP.
- Transaction affinity by warehouse or partition owner.
- DDL or maintenance routing outside the ingress owner.

# Terminology

| Term | Definition |
|---|---|
| Compute group | Operator-owned CN resource set. This is the product name used in this RFC for the existing `cnGroup` concept. |
| Group ID | Opaque, immutable identifier assigned once by the control plane. Names and labels are not IDs. |
| Group name | Human-readable, cluster-scoped lookup name. It is an alias, not an authorization token. |
| Membership generation | Monotonic version of membership and execution-relevant group metadata. |
| Execution incarnation | Opaque CN process-boot identity that changes before RPC admission on every restart and is never reused for a service ID. |
| Authority epoch | Durable control-plane leadership/process epoch advanced before a replacement authority publishes liveness proofs. |
| Liveness revision | Monotonic version within an authority epoch for lease refreshes that preserve a sealed topology; routine renewal does not advance membership generation. |
| Group snapshot | Immutable tuple of group ID, name, membership generation, authority epoch/liveness revision, eligible CN service IDs and execution incarnations, addresses, work states, authority liveness leases, capabilities, and a bounded validity deadline. |
| Binding | Authorized mapping from trusted request identity to a group ID. |
| Ingress binding | Group chosen after authentication for the connection's ingress CN; fixed for that connection and revalidated before statements. |
| Execution selection | Optional session or statement choice used only for statements whose hard constraints permit execution away from ingress. |
| Attempt | One statement execution or retry after authorization and hard-constraint resolution. |
| Admission | Capacity reservation and bounded queue ownership. It is a later milestone and is not implied by placement. |
| Ingress CN | CN that owns the client connection, session, result writer, and transaction workspace. |
| Execution worker | Authorized group member that may run relational, DML, table-function, or user-expression operators for an attempt. |
| Ingress control path | Authentication, SQL parse/bind/optimize and topology coordination, transaction coordination, remote-pipeline fan-in, result serialization, and protocol forwarding retained by ingress; it is not an execution worker grant. |

# Contract Invariants

| Dimension | Invariant |
|---|---|
| Identity | A group name resolves to exactly one immutable group ID at a time; labels never create or recover identity. |
| Authorization | Candidate resolution may narrow an authorized group but can never widen it. |
| Disabled path | With the feature disabled, no group catalog lookup, publication RPC, cache entry, allocation, or placement change occurs. |
| Safety | Hard execution constraints are intersected with the authorized group. An empty intersection fails before side effects. |
| Attempt immutability | One attempt observes one binding generation and one group snapshot. |
| Transaction pinning | An active transaction has at most one group ID. A later statement cannot move it. |
| Retry | A retry re-authorizes and reacquires resources inside the pinned group ID; failure cannot widen the group. |
| Ownership | Each snapshot, ticket/claim, transaction pin, remote route, and future admission permit has one effective terminal owner. |
| Liveness | Every wait is cancellable or bounded. The static MVP introduces no scheduling queue. |
| Boundedness | Runtime state is bounded by configured groups, active bindings/attempts, and a fixed-size or generation-aware cache. |

# Technical Design

## 1. Authority and canonical identity

MatrixOne Operator/control plane is the only owner of compute-group creation,
scale, drain, and deletion. SQL does not create groups and cannot manufacture
membership from labels.

Operator must extend each cnGroup's observed state with:

```text
ComputeGroupRef {
    cluster_id             opaque immutable ID
    group_id               opaque immutable ID
    owner_account_id       immutable MatrixOne account ID
    display_name           cluster-scoped unique alias
    membership_generation  monotonic uint64
    authority_epoch        durable monotonic uint64
    liveness_revision      monotonic uint64 within authority epoch
    lifecycle              Provisioning | Ready | Incompatible | Draining | Deleting
    capability_version     monotonic or content-addressed version
}
```

`group_id` is generated once and persisted in control-plane status. It must not
be derived from the display name or labels. Deleting and recreating the same
name creates a new group ID. This prevents a stale binding from silently
authorizing a different resource set.

The MVP does not support in-place rename. Changing a group name is
delete-and-create with a new ID, followed by an explicit binding update. A
future atomic rename may preserve ID, but it must update the name registry and
audit log as one control-plane operation.

Membership generation increments when any placement-relevant fact changes:

- CN membership or CN service identity;
- CN execution incarnation, including a restart that retains the same service
  ID and address;
- routable pipeline/query address;
- a member crossing an authority-liveness eligibility boundary or changing
  Working/Draining/Drained state;
- capability compatibility;
- static capacity used for DOP calculation.

Live load and routine renewal of an otherwise unchanged authority lease are not
part of this generation in the static MVP.

The control plane durably persists group ID, the last issued membership
generation, and authority epoch before publication. A replacement authority
advances the epoch with a durable compare-and-swap before accepting or
publishing lease renewals; revision comparison is lexicographic by
`(authority_epoch, liveness_revision)`. After leader/process restart it may
reuse the last sealed topology, but it must never reset or reuse a membership
generation or authority epoch. A reconciled topology change is published at a
strictly greater membership generation; missing or ambiguous durable state
keeps the group Provisioning/unavailable rather than reconstructing identity
from labels.

## 2. Propagation and registry

The control plane publishes `ComputeGroupRef` to both Proxy and MatrixOne's CN
topology inventory. Every CN heartbeat carries `group_id`, its current
`execution_incarnation`, and the observed membership/capability generation in
addition to labels. The authority turns accepted heartbeats into an
epoch-fenced liveness proof in the registry; a CN cannot self-extend that
lease. A CN creates its incarnation before opening execution RPCs; it is not
restored from service configuration or a previous process.

Proxy and SQL consume the same versioned registry snapshot. They may maintain
local immutable copies, but they must agree on `(group_id, generation, member
service IDs, execution incarnations)`; neither side independently recomputes
membership from request labels. Connection-cache reuse, tunnel rebalance, and
scale-down migration carry that tuple and may target only a member of the same
group generation (or a later generation of the same group ID after
revalidation).

An idle connection is migratable only when all state needed by the destination
has a defined transfer protocol. At minimum it has no in-flight statement,
active transaction, dirty workspace, temporary/local object, local-input
reader, user/advisory lock, open cursor, or non-serializable session state.
Unknown state makes the predicate false. Merely observing `!inTxn` is not a
safety proof. Migration revalidates the same account, group ID, binding and
membership generations at the destination; it never changes execution-group
selection as a side effect.

The group registry publishes lifecycle records for every state. A snapshot is
usable for a new execution only after all of these gates are true:

1. the group ID and lifecycle are known;
2. each listed member proves the same group ID and current execution
   incarnation;
3. membership and addresses form one sealed generation;
4. required execution protocol capabilities are present;
5. every member's authority liveness lease and the snapshot validity deadline
   cover new-attempt resolution;
6. the group is not Draining or Deleting for new attempts.

Cached `WorkState=Working` is not a liveness proof after its authority lease
expires. A consumer refreshes within a bounded deadline or rejects the group as
unavailable; it never executes from an indefinitely stale snapshot. Member
timeout either makes the group unavailable or is sealed as an explicit removal
in a greater generation—never as a silent smaller view of the old generation.

A routine heartbeat that proves the same service/incarnation, route, work
state, and capability may advance only `(authority_epoch,
liveness_revision)` and the bounded lease deadline while retaining the same
membership generation. The refreshed registry view is immutable at
`(membership_generation, authority_epoch, liveness_revision)`;
it cannot alter the worker tuple pinned by an attempt. Crossing from live to
expired, or back to live after expiry, is an eligibility transition and cannot
be hidden as a same-generation lease refresh. This separation prevents every
heartbeat from invalidating prepared topology, tickets, and routes while still
making stale `Working` metadata unusable for a new attempt or route claim.

Two live execution incarnations claiming one service ID are ambiguous, not a
last-heartbeat-wins update. The group is unavailable for new attempts until the
authority proves one incarnation terminal and seals the survivor in a greater
generation. A delayed heartbeat or callback from the old incarnation cannot
overwrite that sealed state.

The sealed member-to-route mapping is one-to-one. Two live member tuples that
advertise the same execution address, or one tuple with two unresolved current
addresses, make the generation unavailable; a consumer must not de-duplicate
the collision into a smaller apparently healthy group. Address fallback is a
legacy compatibility behavior and is forbidden for canonical identity proof.

Labels remain useful Operator implementation attributes, but SQL cannot accept
them from a user or use them to reconstruct a missing group. A CN with matching
labels but a different or absent group ID is ineligible.

The existing topology transport may be extended to carry these fields. A new
per-account catalog table and per-policy publication RPC are explicitly not
part of this design.

An incompatible member being added is staged outside the current sealed Ready
generation until it upgrades, so the existing generation remains usable. If a
member already executable in the current generation loses required capability,
the authority immediately publishes `Incompatible` and rejects new attempts;
it does not silently omit that member or keep advertising Ready. Recovery
publishes a new compatible Ready generation. A remote receiver also validates
the pinned protocol generation so an already-running attempt fails closed if
the member can no longer honor it.

## 3. ACL and binding ownership

Authorization has two independent gates:

1. The control plane authorizes an account ID to use the group ID.
2. MatrixOne authorizes the authenticated role/session to select a non-default
   group.

Names supplied by a connection or SQL statement are untrusted lookup keys.
After authentication, name resolution is scoped to group IDs already present
in the sealed `AccountComputeBinding`; MatrixOne does not first probe a global
name registry and then authorize the result. A name absent from that
account-scoped view and a foreign account's group are externally
indistinguishable. Only after this filtered lookup may role authorization and
lifecycle validation make the identity effective.

The external control plane is the durable owner of account-to-group bindings.
Its API and published registry contain a versioned object equivalent to:

```text
AccountComputeBinding {
    cluster_id          opaque immutable ID
    account_id          immutable MatrixOne account ID
    binding_generation  monotonic uint64
    default_group_id    optional immutable group ID
    allowed_group_ids   bounded set of immutable group IDs
    lifecycle           Active | Revoked | Deleted
}
```

MatrixOne security is the durable owner of explicit role-level delegation in
its privilege catalog. Milestone B includes a versioned object equivalent to:

```text
RoleComputeGrant {
    account_id          immutable MatrixOne account ID
    role_id             immutable MatrixOne role ID
    grant_generation    monotonic uint64 per account
    allowed_group_ids   bounded set of immutable group IDs
    lifecycle           Active | Revoked | Deleted
}
```

Grant/revoke is transactional with the privilege change and monotonically
advances `grant_generation`. A grant can only narrow the account binding: it is
effective when its group ID is also allowed by the current
`AccountComputeBinding`, and it can never restore a group removed there.
Security is the authorization linearization owner. It seals and publishes an
`AuthorizationSnapshot` containing both account-binding and role-grant
generations; authentication, Proxy handoff, SQL attempts, and retries consume
that tuple rather than independently joining stale versions. Account/group
removal invalidates dependent grants, while role revoke follows the same
fail-new-and-transaction-boundary policy defined below. Exact table and SQL
spelling remain unresolved; durable ownership and generation semantics do not.

The sealed snapshot is trusted server state, not fields copied from a user
context. Its CN-to-CN use carries a verifiable Security assertion or opaque
authorization handle bound to the immutable account, role, group, and
generation tuple. The concrete signature/MAC/handle mechanism is a Milestone B
security-and-pipeline decision, but a receiver must be able to distinguish a
grant derived from that snapshot from one fabricated by an authenticated CN
that merely knows the public metadata.

Binding creation and update use compare-and-swap on `binding_generation` and
validate that every group is Ready and owned by `account_id`. MatrixOne's
account-lifecycle owner must publish create/delete events keyed by immutable
account ID to this API. Account deletion revokes and tombstones the binding;
recreating the same account name produces a new account ID and cannot inherit
it. Group deletion leaves references dangling until an explicit update. A
sealed generation is published atomically to authentication, Proxy, and SQL
consumers; no consumer joins independently observed ACL and group snapshots.
Tombstone retention and reconciliation lag are bounded deployment settings.

The current Proxy protocol routes before CN authentication, so an account name
or connection attribute cannot safely choose the target group. Public
connection selection requires this authenticated handoff:

1. Proxy sends the untrusted login to an authentication-capable endpoint; this
   provisional endpoint is not an execution-group decision.
2. MatrixOne authenticates the user and resolves immutable account and role
   IDs.
3. Security resolves one sealed `AuthorizationSnapshot` and issues
   a short-lived, single-use handoff ticket containing cluster, account, role,
   group, binding generation, role-grant generation, membership generation,
   capability version, ticket-issuer epoch, expiry, and nonce.
4. Proxy atomically claims the ticket against the same group-registry
   generation, binding the claim to a target service ID and execution
   incarnation, and routes it; the target CN validates and consumes it before
   publishing the session.

The security ticket service owns this state machine:

```text
Issued --claim--> Claimed(proxy service/incarnation, target service/incarnation, bounded lease)
Issued --expiry--> Expired
Claimed --consume--> Consumed(target service/incarnation, session ID)
Claimed --abort/lease expiry--> Aborted/Expired
Consumed --attach/revoke/attach lease expiry--> Terminal
```

Claim is atomic and generation-fenced. Target validation and durable consume
linearize before target session publication; only the consuming target
incarnation may publish that session. Consume records `(target service ID,
execution incarnation, session ID)` and starts a bounded, renewable attach
lease. Proxy acknowledges attach by comparing that tuple through an idempotent
ticket-status query; repeated attach/status messages return the same tuple and
never create another session.

Claim, status, attach, abort, and revoke are authenticated operations bound to
the claiming Proxy service/incarnation. Another Proxy or CN cannot observe the
tuple or take over the session merely by knowing the nonce. If the claimant
process dies, its bounded leases drive revoke/cleanup; any future Proxy failover
requires an explicit issuer-authorized ownership-transfer CAS and is not
implicitly granted by transport authentication.

If cancellation, route/dial failure, generation change, or a lost validation
response prevents consume, Proxy aborts the claim or its bounded claim lease
expires. If consume succeeded but its response/attach acknowledgement is lost,
Proxy queries status: it either attaches to that exact target session and
renews its lease, or explicitly revokes it and waits for target cleanup before
performing fresh authentication. The target session cannot accept SQL until
attach is acknowledged; an unacknowledged session closes and releases all
state when its lease expires. A login retry receives a new nonce only after the
old ticket is confirmed Aborted/Expired or its consumed session is terminal.
A late consume after abort/expiry and any replay fail before session creation.
Ticket/session records are bounded by issue rate times TTL and attach-lease
tombstone retention.

Ticket-service restart is a protocol transition, not permission to forget
anti-replay state. Before issuing after restart, the service durably advances
its issuer epoch; every pre-restart Issued/Claimed ticket is then rejected and
cannot consume late. Consumed tuples and their terminal anti-replay records are
durable through restart until attach, revoke, or lease-expiry cleanup reaches a
recorded terminal state. A terminal tombstone remains for at least the maximum
ticket validity plus clock-skew allowance before bounded reclamation. The epoch
advance is a durable cluster-wide compare-and-swap, so process replacement or
leader re-election cannot reuse it. A target restart changes its execution
incarnation, so an attach/status operation can never bind a consumed tuple to a
replacement process that reused the same service ID or address.

No SQL statement, session restoration, or user side effect may start before
the target CN completes validation and ticket consume. Proxy never substitutes
labels or a group inferred from the claimed account name. Ticket format,
issuer, anti-replay store, and bounded failure cleanup are joint exit gates
owned by Proxy and security.

For the static MVP:

- a compute group is exclusively assigned to exactly one immutable account ID,
  ordinary or system;
- an administrator may configure one default group for that account;
- the default binding applies to authenticated sessions of the account;
- an explicit override requires a group-use privilege delegated by the
  account administrator;
- the system account has no implicit cross-account bypass;
- cross-account shared groups are disabled until bounded admission and
  per-account fairness exist.

The Milestone B default binding and account ACL live in that external
`AccountComputeBinding`, beside the group object. Explicit role delegation
lives in MatrixOne's security-owned privilege catalog. Neither creates a
workload-policy table or copies group membership into tenant catalogs.

The eventual privilege name and SQL spelling are unresolved, but the semantic
check is not: authorization is against immutable account ID, role ID, and group
ID, never account/group display labels.

## 4. Binding precedence and boundaries

When the feature is enabled, connection establishment first chooses the
ingress binding:

```text
authorized connection selection
    > administrator default binding
    > compute_group_binding_required
```

This decision occurs after authentication through the handoff above. It fixes
the ingress group ID for the connection. A default-binding update affects new
connections; it does not silently migrate an existing connection. Proxy may
move a safe session only between members of that same group ID under the
generation rules in this RFC.

Each statement then chooses an execution group:

```text
authorized statement override
    > authorized session execution selection
    > authenticated connection ingress binding
```

When the feature is disabled, the resolver is not called and legacy scheduling
is preserved. There is no implicit fallback from an enabled but unresolved
binding to legacy behavior.

Session execution selection does not migrate the ingress CN. It is useful only
when the statement's hard constraints can be satisfied wholly inside the
selected group. Candidate syntax, included only to make the behavior concrete:

```sql
SET SESSION compute_group = 'analytics';
SELECT /*+ COMPUTE_GROUP(analytics) */ ...;
```

The syntax is non-normative until privilege, protocol, and parser owners accept
it. Connection attributes use the same post-authentication name lookup and
authorization path as SQL; Proxy must not route from the attribute before
authorization.

Transaction rules are normative:

- autocommit resolves an execution selection per statement and revalidates the
  ingress binding;
- `BEGIN` pins the current effective group ID before transaction state is
  created;
- an implicit transaction pins during statement admission before side effects;
- changing the session group inside an active transaction is rejected;
- a statement override inside a transaction must resolve to the pinned group
  ID or is rejected;
- PREPARE stores no capacity or group snapshot; EXECUTE resolves the effective
  binding and then pins the attempt;
- COMMIT/ROLLBACK releases the transaction pin.

## 5. Resolution pipeline

The scheduler executes these stages in order:

```text
authenticated account/role + requested name
    -> acquire sealed account-binding + role-grant generations
    -> resolve name only among that binding's authorized group IDs
    -> authorize role and binding generation
    -> acquire immutable group snapshot
    -> derive hard execution constraints
    -> intersect constraints with group membership
    -> (future) acquire admission permit
    -> deterministic placement inside the remaining set
    -> materialize and validate topology
    -> execute
    -> release attempt-owned resources exactly once
```

No later stage may expand the set produced by an earlier stage. Filtered lookup
or discovery failure, an empty intersection, or topology validation failure
returns an error before execution starts.

TP/AP-one-CN/AP-multi-CN influence topology only after group authorization.
They never select a group.

## 6. Hard execution constraints

The following constraints are derived internally and are not user settings:

- `LOAD DATA LOCAL INFILE` input is owned by the ingress process;
- uncommitted transaction workspace writes exist only on ingress;
- temporary/local objects and non-serializable session state stay on ingress;
- local locks and transaction commit ownership cannot be migrated by query
  scheduling;
- DDL and maintenance retain their existing ingress/lock/commit owners;
- a RemoteRun transaction snapshot is not an ingress workspace transfer.

The selected compute group bounds execution workers, not the connection's
ingress control path. When ingress is outside the selected group, it may only
authenticate, parse/bind/optimize and materialize topology, coordinate the
transaction, fan in already-produced remote results, serialize them, and
forward the client protocol. It may not execute a scan, DML expression, join,
aggregate, sort, window, table function, runtime user expression, or fallback
fragment for that attempt. Planning may retain existing semantics-preserving
folding of deterministic built-ins, but it must not invoke a volatile,
side-effecting, or user-defined function as a substitute for worker execution.
Traces account for ingress planning/control bytes and CPU separately from group
worker activity, and the MVP makes no resource-isolation claim for this
unavoidable control overhead.

If a statement requires ingress participation and ingress is a member of the
authorized group, placement includes the canonical ingress worker. If ingress
is outside the group, the statement fails with a constraint-conflict error.
The scheduler must not add ingress from another group and must not start local
upload or DML side effects before this check.

DDL and maintenance use the authenticated connection's ingress group. An
explicit statement override is rejected in the MVP. A session execution
selection equal to the ingress group is harmless; one selecting another group
fails `compute_group_constraint_conflict` before lock acquisition, catalog
mutation, or remote start. It never changes DDL topology or migrates ingress.

## 7. Lifecycle and generation semantics

### Group state

| From | Event | To | New attempts | Existing attempts |
|---|---|---|---|---|
| absent | Operator creates ID | Provisioning | reject unavailable | none |
| Provisioning | sealed compatible snapshot | Ready | admit | none |
| Ready | sealed membership/address/capability/eligibility change | Ready, generation + 1 | use new snapshot | retain pinned snapshot |
| Ready | member process restarts with a new execution incarnation | Ready, generation + 1 after sealing | use new incarnation | old-incarnation routes fail closed; started work follows failure/retry fencing |
| Ready | current executable member loses capability | Incompatible, generation + 1 | reject incompatible | receiver validates pinned protocol and fails closed if necessary |
| Incompatible | sealed compatible snapshot | Ready, generation + 1 | admit on new snapshot | none from incompatible generation |
| Ready | drain requested | Draining, generation + 1 | reject | routes already Claimed/Started may finish until the bounded deadline; unclaimed routes fail closed |
| Draining | deletion begins | Deleting, generation + 1 | reject | bounded cancellation/cleanup |
| Deleting | generation-scoped deletion barrier completes | absent | reject unknown | none |

A group name may be reused only with a new ID. Bindings to the old ID remain
dangling and fail until explicitly updated.

The static MVP has no distributed attempt registry that could safely backdate
new route starts after a drain barrier. Therefore a route materialized but not
yet claimed when Draining publishes is rejected before operator construction,
even if its coordinator resolved the attempt earlier. The attempt unwinds its
other routes, and a replacement attempt remains unavailable while the group is
Draining. Only a receiver claim that linearized before the barrier may advance
to Started and run until the drain deadline. A future design may relax this
only with a generation-fenced attempt-admission record owned by the drain
authority.

The control plane owns the deletion barrier. Operator, Proxy, authentication,
and SQL registry participants acknowledge that a specific group ID/generation
has no local session ingress references, transaction pins, attempts, snapshots,
or unconsumed handoff tickets. Acknowledgements are idempotent and fenced by
generation; a stale acknowledgement cannot finalize a later object. Finalize
occurs after all acknowledgements or their bounded leases expire following the
drain deadline. Late tickets/references are rejected by the tombstone. This is
a bounded distributed barrier, not an unbounded global reference-count wait.

### Binding state

| From | Event | To | Result |
|---|---|---|---|
| absent | authorized bind | active(generation N) | new attempts may resolve |
| active | binding change | active(generation N+1) | new attempts use N+1; running attempts retain N |
| active | ACL revoke | revoked | no new attempts; running statement finishes, then its transaction is aborted at the next statement/commit boundary |
| active | group deleted | dangling | fail; never resolve by name/labels |
| active/revoked | account deleted | tombstoned | reject; same-name account recreation gets a new ID and no inherited binding |

The MVP revoke policy is fail-new-and-abort-at-transaction-boundary. It does
not asynchronously kill a statement already running under a valid pinned
attempt. Before the next statement or COMMIT starts, reauthorization observes
the revoke, marks the transaction aborted, rolls it back exactly once, and
releases its group pin. An idle revoked transaction is bounded by the existing
transaction/session lifetime; an emergency kill remains an explicit
administrative action. A non-revoking binding update does not redirect a
running attempt or active transaction; the latter remains on its pinned group
while that group is still authorized.

Account deletion atomically tombstones the account binding and requests drain
for every group whose immutable `owner_account_id` is that account. Those
groups follow the normal bounded Draining -> Deleting barrier, and their names
cannot be rebound until deletion/tombstone rules allow a new object. The
account tombstone remains until all owned groups finalize or an explicit
administrative orphan-reclamation workflow records terminal ownership; Ready
orphan groups are never left indefinitely. Account-name recreation has a new
ID and neither cancels nor inherits this cleanup.

### Retry

A retry retains account ID, role ID, transaction group ID, requested binding
source, and prior account-binding/role-grant generations. Because a retry is a
new attempt, it rechecks the current sealed authorization generations and
obtains a fresh membership snapshot for the same group ID. Newer generations
may continue to authorize that ID, but they cannot redirect the retry to
another group. If authorization or usability changed, the retry fails before
remote start. It never selects another group as a worker-failure fallback.

## 8. Unavailable and overloaded are different

The static MVP can determine configuration and availability only:

- group ID referenced by the authenticated account's binding but now
  deleted/dangling: unknown-group error;
- requested name absent from the authenticated account's binding view, whether
  globally absent or foreign-owned: not-accessible error with no existence
  distinction;
- enabled account with neither a connection selection nor default binding:
  binding-required error;
- known account-scoped group for which the authenticated role lacks explicit
  selection privilege: authorization error;
- incompatible generation or mixed-version member: incompatible-group error;
- no routable healthy member or Draining group: unavailable-group error;
- hard-constraint intersection empty: constraint-conflict error.

It cannot honestly determine group capacity. Therefore it does not queue and
does not emit a compute-group overload error. Normal execution pressure retains
existing engine behavior without a resource-governance claim.

A later admission owner may add concurrent permits and a bounded cancellable
queue. At that point healthy-at-capacity, queue-full, and queue-timeout become
separate observable outcomes. Worker-count/DOP remains topology, not admission.

## 9. Snapshot ownership, waits, and bounds

The implementation must satisfy these Q1-Q3 obligations:

| Q | Object | Required proof |
|---|---|---|
| Q1 | Group snapshot | Attempt owns one reference; success, failure, cancellation, retry handoff, and panic-safe cleanup release it effectively once. |
| Q1 | Transaction pin | Transaction owns the group-ID pin; COMMIT, ROLLBACK, abort, connection close, and recovery have one effective release. |
| Q1 | Handoff ticket/claim/session | Ticket service owns state through consume; target owns an attach-blocked session only after consume, and attach/revoke/lease expiry yields one terminal cleanup. |
| Q1 | Remote route claim | Receiver transfers a validated route claim to its attempt; completion/error/cancel/start uncertainty/retry reaches one terminal route and attempt owner. |
| Q2 | Registry refresh | Wait observes caller cancellation and a bounded internal deadline; rejection does not wait behind the data path it controls. |
| Q2 | Drain | New admission closes independently of running work; cleanup has a bounded deadline. |
| Q2 | Handoff recovery | Claim and attach leases bound lost responses; status/attach/revoke are idempotent and a new nonce waits for the old session's terminal state. |
| Q2 | Remote start/retry | Lost start acknowledgement is resolved by bounded status/cancel; a replacement attempt cannot overlap an uncertain predecessor. |
| Q3 | Registry/cache | Retains current generation plus actively referenced old generations; unused generations are reclaimed. |
| Q3 | Binding cache | Bounded by durable configured bindings or a fixed-capacity eviction policy; name misses and unauthorized requests are not cached without a bound. |
| Q3 | Ticket/route tombstones | Retention is bounded by admitted issue/route rates, configured TTL/leases, and fixed tombstone limits with fail-closed backpressure at the limit. |

Admission adds its own permit, waiter, queue, and exactly-once release proofs and
is intentionally deferred.

## 10. Prepared execution and topology materialization

Prepared plans may cache logical/physical planning work, but execution validates:

- the effective group ID and binding generation;
- membership generation and worker addresses;
- hard-constraint state, including dirty workspace and local input;
- RemoteRun protocol compatibility;
- topology closure and route availability.

A mismatch rebuilds only the placement/topology portion that is stale. It does
not silently reuse workers from another group. Remote execution carries the
pinned authorization in an authenticated envelope equivalent to:

```text
RemoteExecutionGrant {
    account_id, role_id, group_id
    binding_generation, role_grant_generation
    membership_generation, capability_version
    root_coordinator_service_id, root_coordinator_execution_incarnation
    attempt_id, route_id, parent_route_id
    expected_sender_service_id, expected_sender_execution_incarnation
    target_service_id
    target_execution_incarnation, expiry
    authorization_proof_or_handle
}
```

Before any operator starts, the root coordinator closes the materialized
remote-route DAG and derives one target-specific grant for every edge from the
same sealed authorization snapshot; this does not add a per-query control-plane
RPC. In a nested A -> B -> C topology, B may forward C's opaque pre-derived
grant but cannot mint a child grant, change its target, or widen the group. The
grant binds the root coordinator, expected authenticated immediate sender,
parent route, target, and shared attempt. A topology that cannot be closed this
way is unsupported in the static MVP and fails before start.

The envelope is bound to the authenticated root coordinator
service/incarnation and its Security assertion—it is not accepted merely
because its protobuf fields are internally consistent. `(attempt_id, route_id)`
is unique per target and claimed once. Before operator construction, the
receiver verifies the root coordinator identity and authorization proof, the
transport peer against the expected sender/parent edge, target
service/incarnation, account/group identity, current executable membership
generation/capability/liveness and lifecycle, expiry, and one-time route claim.
A fabricated or altered grant, a route from another group, a route whose
target was removed or restarted before start, a stale generation/incarnation,
missing fields from an old sender, or a replay fails before any operator/side
effect. Route state is
`Issued -> Claimed -> Started -> Terminal`, and status/cancel are idempotent.
Claim is the start barrier for lifecycle purposes: once it has linearized, a
later Draining publication may let that claim reach Started under the pinned
snapshot until the drain deadline; an unclaimed grant receives no such right.
Once a route has started, its attempt owns the pinned snapshot and follows
normal drain/cancel rules. If a start response is lost, the coordinator queries
status and either rejoins that exact attempt or cancels and observes its
terminal state; it must not launch replacement work while the predecessor is
uncertain. Retry uses a new attempt/route identity and fresh authorization only
after every predecessor route is terminal. DML retry also retains the
transaction/idempotency fence; it cannot duplicate a committed side effect.
Route-claim state is released at attempt completion and bounded by active
routes plus expiry tombstones; reaching the bound fails closed.

Route claims and tombstones are scoped to the receiver execution incarnation.
After restart, the new process rejects every old-incarnation grant even though
it does not retain the old process's in-memory claim table. A route that had
started in the failed incarnation is non-rejoinable: the coordinator observes
that incarnation's terminal loss, then resolves the durable transaction or
idempotency outcome before it may create replacement work. Process death alone
is not proof that a DML side effect did not commit.

## 11. Observability

Each attempt records bounded fields:

- requested group name and resolved group ID/name;
- binding source and generation;
- group membership generation;
- authorization, hard-constraint, availability, and future admission outcome;
- selected worker count, topology class, and DOP;
- retry attempt and drain/membership-change reason;
- explicit fallback, which is always `none` in the MVP.

Metrics use bounded outcome enums. Group IDs/names may appear in per-query
traces and logs but not as unbounded metric labels. EXPLAIN/preview performs
read-only resolution, marks the result as representative, and never acquires
capacity or mutates cache ownership.

## 12. Compatibility and rollout

- The feature defaults off for every account after upgrade.
- Off mode executes the current main path before any group resolver call.
- Enabling a binding requires Operator, Proxy, its authentication/handoff
  participants, the topology registry, and every executable/draining CN that
  can serve the target group to advertise the same minimum capability.
  Unrelated groups and their CNs do not block a group-local rollout.
- Capability failure blocks enablement before a binding becomes active.
- Mixed-version clusters retain legacy behavior for disabled accounts.
- There is no transitional label fallback in the MVP.
- Rollback disables new bindings first, drains pinned attempts, and then
  removes capability use. Durable group identity remains control-plane data.

No tenant bootstrap table or policy publication RPC is introduced by this RFC.

## 13. Fast-path performance contract

The disabled path must add:

- zero allocations per statement;
- zero catalog or network operations;
- no cache entry or background refresh goroutine;
- no placement or topology difference;
- no statistically material compile/execute latency regression in the
  no-policy benchmark (target: no more than 2% at p50 and p95 over repeated
  paired runs on the same host).

Enabled-path work is proportional to the selected group's members, not all CNs
in the cluster. Registry publication is event-driven; per-query cluster polling
is forbidden.

# Delivery Boundaries

The implementation proposed by #26109 must not be retained merely because it
already exists. In particular, this RFC removes or rejects:

- `ALTER ACCOUNT CONFIG ... query_workload_policy`;
- the JSON containing `pool`, raw labels, TP/AP classes, `current_cn`, fallback,
  empty-worker policy, and `max_workers`;
- its tenant catalog table, cache lifecycle, bootstrap gate, and publication
  RPC;
- TP/AP as public workload identities.

Independently useful scheduler, topology, and observability changes may be
extracted only when current main has another stable consumer and their tests do
not depend on the rejected policy model. This PR extracts the legacy execution
invariants for dirty workspaces and `LOAD DATA LOCAL`: ingress must participate
when it owns uncommitted state, and a client-local input stream is ingress-only.
They remain inputs to a future compute-group resolver, not evidence that group
identity, authorization, transaction pinning, or RemoteRun fencing exists.

Two candidate fixes found while auditing #26109 are explicitly outside this
RFC and should be delivered as isolated correctness PRs if their regressions
confirm them:

- [#27069](https://github.com/matrixorigin/matrixone/pull/27069) advances the
  per-CN offset partition while distributing `generate_series`, so multiple
  CNs do not consume the same first offset segment;
- propagate RemoteRun `LAST_INSERT_ID` terminal state back to the coordinator
  without allowing a zero/unsupported remote value to erase an existing value.

Neither fix justifies retaining the rejected workload-policy control plane.

# Drawbacks

- This delays the public feature and discards substantial implementation work.
- Operator, Proxy, topology, transaction, and SQL owners must agree on a shared
  identity and lifecycle instead of evolving their components independently.
- The static MVP cannot claim overload isolation or safely share a group across
  accounts.
- An immutable control-plane ID requires schema/protocol changes outside this
  repository before SQL work can proceed.

These costs are smaller than releasing a second, label-backed pool identity
whose semantics cannot be made stable after customers depend on it.

# Rationale / Alternatives

## Keep the JSON and define `pool` as display-only

Rejected. The effective labels would remain the real identity, with no ACL,
rename/delete, or generation owner.

## Make labels the public identity

Rejected. Labels are mutable implementation attributes, can overlap, and do
not protect against deletion/recreation or cross-account widening.

## Use TP/AP as automatic selectors

Rejected for the stable MVP. They may later inform an opt-in recommendation,
but optimizer output cannot be the durable assignment contract.

## Route only at Proxy connection time

Insufficient. It cannot express an authorized read-only statement override,
prepared/retry semantics, or deterministic placement within a group. Proxy
selection remains one binding source, not the entire execution contract.

## Implement admission now

Deferred. Static identity, authorization, and correctness must work before a
permit owner and fairness policy are added. Until then the product makes no
capacity guarantee.

# Unresolved Questions and Required Owners

| Question | Required owner(s) | Must resolve before |
|---|---|---|
| Exact external product name: compute group, CN group, or warehouse | Product, docs | Public API naming |
| Operator persistence and API shape for immutable group ID/generation | Operator, control plane | Milestone B implementation |
| Exact schema and SQL spelling for Security-owned `RoleComputeGrant` | Security, frontend | Explicit SQL/session selection |
| Connection attribute spelling and post-auth Proxy handoff | Proxy, security | Connection selection |
| Statement override syntax | Parser, frontend, product | Statement selection |
| Drain deadline and emergency revoke defaults | Operator, transaction, SRE | Production enablement |
| Remote receiver validation protocol and rolling-upgrade version | Pipeline, query service | Remote execution |
| Whether a future atomic rename is needed | Product, Operator | Rename support |
| Admission fairness and shared-account policy | Resource governance | Shared groups |

Until these owners accept the contract and all Milestone B acceptance rows have
test owners, this RFC remains draft and no public DDL/catalog surface should be
merged.
