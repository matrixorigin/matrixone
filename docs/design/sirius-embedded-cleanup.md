# Sirius integration cleanup

Owner: #28966. Follow-up to #30251. Implementation: the PR containing this
document, on `refactor/embedded-cleanup`.

The user approved this design in the cleanup plan and requested implementation
on 2026-10-16. The scope decision preserves existing native TAE and
manifest functionality; adding MO TAE execution or automatic source selection
is outside this PR.

## Problem and invariants

#30251 routes selected SQL through embedded Sirius and normal MO readers. The
frontend still contains an unreachable HTTP SQL rewriter, response converter,
configuration, and transport. Its only live helper recognizes the two existing
Sirius hints. The removed remote-reader owner also left unused GC protection
adapters and comparison utilities.

Both `/*+ SIDECAR */` and `/*+ SIDECAR GPU */` continue to select embedded GPU
execution for one eligible statement. Normal MO execution, prepared execution,
and statement context isolation retain their existing contracts. There is no
new selector, backend, compatibility handler, or fallback.

Native TAE scanning, manifest generation, `/debug/tae/manifest`, the inspect
command, and TN registration/unregistration remain available. The unused
`GenerateManifestForTxn` wrapper is removed because it ignores its transaction
argument; this cleanup does not introduce snapshot-bound manifests.

## Changes and ownership

| Closure | Change and consumer | Risk and proof |
| --- | --- | --- |
| Frontend selection | Replace the old offloader with a Boolean Sirius hint helper consumed by statement selection; retain both hint spellings | R2: recognition, statement isolation, prepared/context regressions, public GPU fixture |
| Configuration | Remove `frontend.sidecarUrl`, `sidecar_url`, and frontend debug-address wiring | R2: standard unknown-variable errors and omission from variable listings; normal TOML unknown-key policy remains |
| GC protection | Remove the sidecar adapter, guard, replay/rollback and terminal-only protection state; retain normal CCPR protection | R3: registration/GC exclusion, TTL, soft-delete and capacity tests plus focused and package race validation |
| Unused utilities | Delete the unreferenced comparison package and transaction-ignoring manifest wrapper | R1: reference closure and manifest owning-package tests |
| Documentation | Correct current-state claims and preserve historical evidence | R0: inspect links, commands and recorded identities |

Delete the HTTP client/transport, SQL rewriting and traversal, synthetic table
node, JSON response types/conversion, response-size limit, offload sentinel,
debug-address state and setter, and their dedicated tests. Retain active hint
and statement-isolation tests under Sirius names. Remove obsolete configuration
registrations without aliases or special unknown-variable handling.
The existing catalog loader ignores unregistered variable names and SHOW
enumerates registered definitions, so persisted old values need no migration
or special compatibility list. The released v4.1.5 and v4.2.5 sources do not
export typed session-system-variable snapshots; their proxy replays explicit
SET statements. Sidecar has no customer users, so this cleanup does not support
replaying its removed setting or preserving snapshots from intermediate main
builds. Keep the strict unknown-variable decoder unchanged, without a retired
name exception or compatibility registry.
Remove the old request smoke test that discards all results after recovered
failures; the public GPU fixture and retained statement-context tests provide
concrete selection oracles.

Delete `SidecarReadProtector` and its tests. Remove `SyncProtectionGuard`,
`EnsureSyncProtection`, terminal registration/release, guard rollback, and
`TerminalReleaseOnly`, including their private helpers and special cleanup
branches. Ordinary registration returns only an error and needs no guard object.
Keep general protection queries, renewal, soft deletion, TTL cleanup, capacity
reclamation, object validation, and real CCPR/RPC consumers.

## GC state and lifetime

Registration first acquires `protectionBarrier.TryRLock`; failure returns the
existing GC-running error without waiting. After acquisition it retains the
read lock through validation and map insertion and releases it on every return.
The manager map mutex remains nested inside this barrier. Preserve existing
validation order and error codes.

GC acquires the barrier exclusively before publishing `gcRunning`, and retains
it through cleanup. Therefore a registration cannot cross GC's admission
boundary, including the interval before `gcRunning` becomes true. GC can wait
for an admitted registration; a new registration never waits for GC. No new
goroutines, callbacks, persistent state, or lifecycle generations are added.

Ordinary entries remain bounded by `maxCount`, are renewed by their existing
owner, and are reclaimed by existing TTL or soft-delete/checkpoint rules.
Removing unused terminal-only entries introduces no live-state conversion:
their removed remote-reader producer is absent from the current release.

## Alternatives and boundaries

Keeping the unreachable implementation would retain misleading controls and
unowned lifecycle code. Adding a native TAE route would require a separate
snapshot, storage and GC ownership design. The selected change deletes the dead
closures and preserves the working TAE tooling without expanding execution.

The shared Arrow components, native ABI/capability bits, numeric-profile
negotiation, active embedded input/result leases, unrelated uses of the word
sidecar, and the abandoned sidecar repository are outside the deletion closure.
The pinned native source and SDK artifacts do not change. Old Flight deployments
still use the previous release to drain and recover before upgrading.

Local GPU validation reuses SDK manifest
`878c0ec8b2d27f8efbac9325159903fb5ba87d5448692ab95938f10cb18077bc`
from its original native worktree. Its clean native source revision
`06400cf48073e51955067bfd75c5c4defe955a72` matches this PR's submodule pin.
The SDK consumer prepare/package/verification checks remain enabled. This
qualifies local consumer execution without rebuilding or relabeling the SDK;
the normal release build's source-path binding is unchanged.

## Validation and delivery

1. Preserve Boolean hint acceptance and negatives, per-statement selection,
   prepared execution, and compile-context tests. Exercise both hints through
   the existing small public GPU fixture.
2. Prove standard session/global read/set errors for the removed variable and
   omission from variable listings; extend the existing system-variable BVT.
3. Run normal sync-protection tests, deterministic GC-barrier rejection tests,
   focused adaptive race validation, and one owning-package race run.
4. Run existing manifest generation, HTTP lifecycle and inspect tests; retain
   the manifest BVT. Reuse native TAE evidence where its inputs are unchanged.
5. Run formatting, incremental static checks, affected owning-package tests,
   service compilation, and the mandatory pre-push gate. Verify all deleted
   symbols have no remaining executable consumers.

No new cluster or benchmark campaign is required. Reuse the existing public SQL
and BVT fixtures. Historical measurements and immutable evidence remain intact;
documentation identifies historical commands rather than presenting them as
current execution paths. Use the MatrixOne PR template and keep evidence links
outside the section used as the commit message.
