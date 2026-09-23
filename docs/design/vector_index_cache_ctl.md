# Sys-admin control plane for the vector / fulltext2 index cache

Feature: #28985 · PR: #29024 · Scope: a set of sys-admin `mo_ctl` operations over the per-CN
`VectorIndexCache`.

This records the design and tradeoffs for the operational control plane added to the
vector/fulltext2 index cache, and the human approval required before implementation approval.
Acceptance is tracked **per revision** below.

## Revision history

- **Rev 1** — original design: interface (§2), security boundary via the statement-path privilege
  gate (§3), partial-failure (§5), rolling-upgrade (§6), rollback (§7). **Approved by fengttt.**
- **Rev 2** — adds the **execution-entry authorization backstop** (§3) and the **synchronous
  eviction and teardown contract** (§4: completion, multi-generation, and failed-load ownership,
  cancellation, retention). **Approved by fengttt.**

---

## 1. Problem

A warm per-CN index-cache entry is refreshed across CNs only by a periodic freshness sweep, so its
default cross-CN convergence window is coarse (minutes). Two needs follow: tests and operators must
be able to (a) make convergence fast enough to assert deterministically, and (b) observe and
directly correct cache residency when a placement issue is suspected. Neither should change the
default behaviour or move freshness onto the query path.

## 2. Interface (contract, not implementation)

Four sys-admin, cluster-wide `mo_ctl` operations, each broadcast to every CN:

- **Set freshness interval** — override the periodic sweep cadence live; `0` restores the default.
  A positive value below a safety floor is rejected.
- **Get cache info** — report, for one exact cache key, how many entries are held summed across CNs
  (and the CN count). Read-only.
- **Evict** — drop one exact cache key on every CN synchronously, so the next query reloads.
- **List keys** — enumerate the exact cache keys held across CNs (union, per-key CN count) so an
  operator can discover the key to feed Get/Evict.

The default cadence and the eventual cross-CN freshness contract are unchanged when the knob is
unused; the override is process-local and never persisted.

## 3. Decisions and invariants

- **Security boundary.** These operations read or mutate cluster-wide, cross-tenant cache state, so
  they are restricted to the **sys account's admin role**, enforced at **two points**: the `mo_ctl`
  statement-path privilege gate (which already admits only that principal), and a re-check at the
  single point every `mo_ctl` invocation is actually executed. The statement-path gate inspects the
  *bound plan*; an `mo_ctl` evaluated **during binding** (folded inside a default / window-frame /
  range expression) fires its cluster-wide effect and is then replaced by a constant before that
  scan can see it. The execution-entry re-check closes that bypass class independently of where the
  call is evaluated, enforcing the same sys-account + admin-role rule by identity — it only ever
  rejects calls the statement-path gate would also reject.
- **Exact-key contract.** Get and Evict act on the **exact** cache key (algorithm-specific:
  bag-of-tables index by table name, IVF-family index by table + generation). Get reports exactly
  what Evict would remove; a bare table name does not match a generation-qualified key. List keys is
  the discovery tool that makes the exact key obtainable.
- **Safety bound.** A positive freshness interval has a floor; below it the sweep would peg every CN
  with housekeeping/scan work. The floor is small enough for tests to converge in seconds.
- **Freshness stays a pull.** The knob only changes the cadence of the existing periodic sweep; it
  never validates freshness on the query path and performs no manual eviction of its own.

## 4. Synchronous eviction and teardown contract

Evict is **synchronous**: on each CN it returns only after the key's teardown has fully completed --
the in-flight search drained and the native resource freed -- so a caller polling "is it gone" (via
Get) can never observe the key while an old resource is still alive.

- **Completion ownership.** Exactly one caller wins the removal of a given entry and destroys it; the
  returned count reflects what that call actually removed, never a pre-read occupancy that two racing
  callers could both claim.
- **Multi-generation ownership.** Several generations of one key can be tearing down at once -- a
  generation removed from the map but still destroying, while a reload under the same key is itself
  evicted. Each teardown carries its own completion signal, and a caller that finds the key already
  gone waits on **every** in-flight teardown of that key, not just one, before reporting completion.
- **Failed-load teardown.** A load that fails after admission tears its entry down on the same
  completion path and registers it the same way, so a concurrent Evict waits for it too rather than
  reporting a premature "gone". That teardown deliberately skips the key-wide invalidation a normal
  evict performs, so it cannot clear a newer generation's reusable state.
- **Cancellation and bounded waits.** Whoever removes an entry always follows with its destroy, which
  closes the completion signal, so the destroy runs to completion and every wait terminates. A caller
  *waiting* on another's teardown does so under a bounded context and stops if cancelled, without
  affecting the in-progress destroy.
- **Retention.** An evicted or failed-load entry's resource is released before the operation returns,
  not deferred, and a generation that holds no searchable vectors is not retained. Cross-CN freshness
  of a *warm* entry remains the eventual periodic-sweep contract in section 1 -- eviction bounds
  resources, it does not add a freshness guarantee.

## 5. Partial-failure semantics

- **Set** is **best-effort**: apply on every reachable CN, report the ones that failed, and only
  error when no CN applied. An idempotent, non-persisted cadence override must not hard-fail the
  whole call for one transiently-unreachable or not-yet-upgraded CN.
- **Get / List** aggregate across CNs and **fail on the first CN error** — a partial reading would
  misreport residency, which callers use to decide convergence.
- **Evict** **fails on the first CN error** — an eviction that reached only some CNs is not "done",
  so the caller must see the failure rather than a misleading success.

## 6. Rolling upgrade

The operations are **protocol-gated** behind a dedicated MORPCVersion: in a mixed-version cluster the
negotiated cluster protocol stays below that version until every CN is upgraded, so a call is
rejected cleanly and cluster-wide during the upgrade window and begins working once the upgrade
completes. This avoids dispatching to a CN that has no handler. (Chosen over an always-available
registration, whose only alternative behaviour is a messy per-CN dispatch error mid-upgrade.)

## 7. Rollback

- The cadence override is **not persisted**: a CN restart reverts to the default, and Set with `0`
  restores it explicitly.
- Get, List, and Evict hold no durable state; Evict only drops resident entries that the next query
  reloads.
- Disabling the feature is therefore a no-op on durable state; there is no migration to undo.

## 8. Tradeoffs / non-goals

- **Point-in-time, not linearizable.** Get/List gather sequential per-CN RPCs; a concurrent
  load/evict on another CN can shift a count between calls. Acceptable for ops/diagnostics; not a
  cluster-consistent total.
- **Interface is `mo_ctl`, not DDL/SQL surface.** This is deliberately an operator/test control
  plane, not a client-facing consistency guarantee. Read-your-writes across CNs remains a separate,
  opt-in concern that this feature does not promise.
- **No per-tenant scoping.** The operations are sys-admin-only and act cluster-wide by design; they
  are not exposed to individual tenants.

## 9. Approval

Acceptance is per revision (see the Revision history at the top):

- **Rev 1** — approved by **fengttt**.
- **Rev 2** (execution-entry auth §3 + synchronous eviction/teardown §4) — approved by **fengttt**.

Implementation approval (code review) is tracked separately on PR #29024.
