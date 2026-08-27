# Authentication Catalog Freshness Across CNs

- Status: Accepted for implementation
- Base revision: `6b8987741df912f38cece5bd026349d79672faf9`
- Incident evidence: [PR #27758 CI job](https://github.com/matrixorigin/matrixone/actions/runs/33067372509/job/98502820591?pr=27758)
- Related changes: [PR #27717](https://github.com/matrixorigin/matrixone/pull/27717), [PR #27737](https://github.com/matrixorigin/matrixone/pull/27737)
- Last updated: 2026-08-27

## 1. Summary

MatrixOne deliberately permits ordinary transactions to start from the latest
logtail already applied by their local CN. This avoids a physical freshness wait
on the transaction hot path, but a newly routed connection can consequently
authenticate from an older catalog snapshot than a security change that has
already committed on another CN.

The observed failure was a `REVOKE` followed by a `GRANT` of a user's stored
default role. The grant returned successfully, but a new implicit login routed
to another CN still saw the revoked generation, selected `public`, and rejected
a table read that the regranted role permits. The same shape applies to account
status, user/password state, explicit role grants, and every other catalog row
read by `AuthenticateUser`.

Authentication must therefore opt out of sacrificed freshness. Immediately
before its background transaction is created, it establishes a session minimum
snapshot at a timestamp strictly beyond the local HLC's uncertainty upper
bound. Existing transaction creation then waits for the local logtail to reach
that bound. All authentication reads remain in one transaction and therefore
observe one complete catalog generation.

## 2. Scope

This change covers normal user authentication on a CN, including:

- account existence and suspended/restricted status;
- user existence, password, lock, and password-expiration metadata;
- implicit and explicit default-role existence and grants;
- system variables and an optional login database resolved by the same
  authentication transaction.

This change does not:

- alter the bootstrap-only special-user path, which intentionally does not read
  catalog authentication state;
- invalidate privilege state in an already authenticated session; that is the
  separate active-session lifecycle addressed by PR #27737;
- change the default ordinary-SQL freshness policy; the freshness-preserving
  transaction branch only stops discarding an existing caller minimum;
- add a QueryService command, a cluster membership dependency, or persisted
  state.

## 3. Root Cause

With `enable-sacrificing-freshness` enabled, transaction creation uses the
session's last commit timestamp as its minimum snapshot. A new session has no
last commit, so its authentication transaction may start at the latest logtail
already applied on that CN. That timestamp can precede a catalog commit that a
different CN has already acknowledged to its client.

PR #27717 correctly stopped trusting `mo_user.default_role` by itself, but its
new `mo_user_grant` validation runs inside the same potentially stale
transaction. It can therefore correctly interpret the wrong catalog generation.

The violated boundary is real-time authorization ordering:

```text
authorization mutation commits and returns
    -> a later login begins on any healthy CN
    -> that login must not authenticate from a pre-mutation catalog generation
```

## 4. Invariants and Linearization

1. **Freshness:** before creating a normal user's authentication transaction,
   the session minimum snapshot is advanced to a fence strictly greater than
   the HLC uncertainty upper bound captured for that login.
2. **Local application:** transaction creation may proceed only after the CN's
   timestamp waiter reports that logtail through the fence has been applied.
3. **Single generation:** every catalog read used to accept or reject one login
   uses the same transaction snapshot. A concurrent security mutation belongs
   either before or after that snapshot; authentication never combines both
   generations.
4. **Fail closed:** a missing runtime/clock, invalid clock offset, timestamp
   overflow, cancellation, timeout, closed transaction client, or stalled
   logtail rejects the login. No path falls back to the locally applied stale
   timestamp.
5. **Monotonic session state:** installing the authentication fence can advance
   but never lower the session minimum timestamp. SQL executed after login also
   cannot start behind the catalog generation that authenticated the session.
6. **Isolation:** bootstrap special users retain their existing catalog-free
   behavior, and unrelated sessions and CNs share no new mutable state.

The login freshness linearization point is the HLC upper-bound capture. A
security commit concurrent with that capture may legally appear on either side;
a security commit whose success was observed before a later login request is
covered by the clock uncertainty bound and must be visible.

## 5. Chosen Design

Add a small frontend helper that:

1. obtains the service runtime and transaction clock;
2. captures the clock's uncertainty upper bound;
3. constructs the smallest physical timestamp strictly greater than every HLC
   timestamp at that upper-bound physical time;
4. advances `Session.lastCommitTS` through the existing monotonic update path.

`AuthenticateUser` calls the helper after the special-user bypass and before
constructing its background executor. The background session already inherits
the upstream session's minimum timestamp. `TxnClient.New` already passes that
minimum to `TimestampWaiter.GetTimestamp`, which waits for local logtail and
returns a snapshot after the applied timestamp.

The transaction client's freshness-preserving branch must choose
`max(clock.Now(), caller minimum)`. It previously replaced the minimum with
`clock.Now()`, which would silently discard the authentication fence whenever
`enable-sacrificing-freshness` was disabled. The default sacrificing-freshness
branch remains unchanged.

The authentication transaction keeps the configured SI or RC isolation. In
particular, the repair does not force RC: one fresh SI snapshot is preferable
to several individually fresh statements because authentication is a compound
security decision.

## 6. Alternatives Rejected

### Force read committed isolation

RC advances each statement to the latest timestamp already applied locally. It
does not teach a lagging CN about a remote commit, so it does not close the
observed race. It also permits account, user, role, and lock checks from
different catalog generations.

### Query every CN during every login

Collecting `GetCommit` from all CNs and synchronizing the maximum can provide a
fence, but adds O(cluster size) RPCs, depends on a membership snapshot, and
requires a security policy for partial responses. The HLC/logtail mechanism is
already the transaction layer's freshness primitive and has no fan-out cost.

### Broadcast every authorization commit

Synchronous `SyncCommit` fan-out moves cost to the rarer DCL path, but a remote
failure occurs after the catalog transaction has irreversibly committed. The
client would receive an ambiguous error for a successful security mutation,
and other authentication mutations would still need separate integration.

### Disable sacrificed freshness globally

This would impose a logtail freshness wait on every transaction, including TP
hot paths unrelated to security. The required boundary is login, not all SQL.

## 7. Failure, Restart, and Compatibility Semantics

- The caller's authentication context remains the deadline and cancellation
  owner of the transaction's timestamp wait.
- A lagging but healthy CN waits and then authenticates from the complete
  snapshot. A CN whose logtail cannot reach the fence rejects the login rather
  than authorizing stale state.
- Restart creates no recovery work: a new CN establishes a new fence for each
  login and waits for its own logtail.
- No wire or persisted format changes are introduced. Rolling upgrades are
  protocol-compatible; the stronger guarantee applies to logins routed to an
  upgraded CN and becomes cluster-wide after all serving CNs are upgraded.
- Retrying a failed login creates a new, monotonic fence. Duplicate attempts do
  not accumulate state or weaken the boundary.
- The fence inherits MatrixOne's existing HLC clock contract. When offset
  checking is enabled, the configured maximum offset is included explicitly;
  when it is disabled, the deployment retains the same external clock
  synchronization assumption as the transaction client's freshness-preserving
  snapshot path.

## 8. Performance Budget

- Default ordinary SQL transaction creation, compilation, privilege-cache
  hits, and execution receive no new branch, lock, allocation, RPC, or cache
  operation.
- The non-default freshness-preserving transaction branch replaces one
  assignment with one timestamp comparison so its documented caller-minimum
  contract is monotonic; the default transaction hot path is unchanged.
- Each normal login adds one local HLC read and reuses the existing transaction
  timestamp waiter. With clock uncertainty disabled, it is normally bounded by
  the next local logtail progress update and adds no cluster RPC.
- When clock-offset checking is enabled, correctness may require waiting through
  the configured uncertainty interval. This cost is isolated to connection
  establishment and is preferable to either stale authorization or a cluster
  RPC fan-out. Connection pooling amortizes it for TP workloads.
- No new process-lifetime or per-session collection is introduced.

## 9. Validation Map

| Behavior | Evidence |
| --- | --- |
| Fence uses the HLC uncertainty upper bound and dominates its logical range | focused frontend unit test with a deterministic clock |
| Existing larger session minimum is never lowered | focused frontend unit test |
| Missing runtime/clock, invalid offset, and timestamp overflow fail closed | focused frontend unit tests |
| Authentication installs the fence before background transaction creation | focused frontend unit test at the executor seam |
| Both transaction freshness modes preserve a later caller minimum | focused txn-client unit test |
| Transaction creation waits for the supplied minimum and honors cancellation | existing txn-client timestamp-waiter tests plus owning-package tests |
| Revoke fallback and immediate regrant are visible to new implicit/explicit sessions | existing `revoked_default_role_login` BVT on multi-CN topology |
| Public authentication behavior remains unchanged on one CN | repeated existing BVT and frontend owning-package tests |

The existing BVT is the exact public reproduction and already contains the
positive grant, revoke fallback, denied privilege, regrant recovery, implicit
login, explicit login, and secondary-role control. Adding another SQL case
would duplicate its fixture and oracles, so this change reuses it unchanged.

## 10. Acceptance Criteria

1. The previously failing multi-CN BVT passes without sleeps, polling, retries,
   or result weakening.
2. Authentication cannot continue when the freshness fence cannot be built or
   reached.
3. The final diff contains no ordinary-query hot-path change and no protocol or
   persisted-state change.
4. Focused and owning-package frontend tests pass in normal and applicable race
   modes; formatting, vet, build, and diff checks pass for every changed Go
   package.
