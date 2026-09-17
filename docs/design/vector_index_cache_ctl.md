# Sys-admin control plane for the vector / fulltext2 index cache

Status: **approved** · Approver: **fengttt** · Feature: #28985 · PR: #29024 ·
Scope: a set of sys-admin `mo_ctl` operations over the per-CN `VectorIndexCache`.

This records the design and tradeoffs for the operational control plane added to the
vector/fulltext2 index cache, and the human approval required before implementation approval.

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
  they are restricted to the **sys account's admin role**. This is enforced by the existing `mo_ctl`
  statement-path privilege gate, which already admits only that principal; the operations add no
  authorization of their own.
- **Exact-key contract.** Get and Evict act on the **exact** cache key (algorithm-specific:
  bag-of-tables index by table name, IVF-family index by table + generation). Get reports exactly
  what Evict would remove; a bare table name does not match a generation-qualified key. List keys is
  the discovery tool that makes the exact key obtainable.
- **Safety bound.** A positive freshness interval has a floor; below it the sweep would peg every CN
  with housekeeping/scan work. The floor is small enough for tests to converge in seconds.
- **Freshness stays a pull.** The knob only changes the cadence of the existing periodic sweep; it
  never validates freshness on the query path and performs no manual eviction of its own.

## 4. Partial-failure semantics

- **Set** is **best-effort**: apply on every reachable CN, report the ones that failed, and only
  error when no CN applied. An idempotent, non-persisted cadence override must not hard-fail the
  whole call for one transiently-unreachable or not-yet-upgraded CN.
- **Get / List** aggregate across CNs and **fail on the first CN error** — a partial reading would
  misreport residency, which callers use to decide convergence.
- **Evict** **fails on the first CN error** — an eviction that reached only some CNs is not "done",
  so the caller must see the failure rather than a misleading success.

## 5. Rolling upgrade

The operations are **protocol-gated** behind a dedicated MORPCVersion: in a mixed-version cluster the
negotiated cluster protocol stays below that version until every CN is upgraded, so a call is
rejected cleanly and cluster-wide during the upgrade window and begins working once the upgrade
completes. This avoids dispatching to a CN that has no handler. (Chosen over an always-available
registration, whose only alternative behaviour is a messy per-CN dispatch error mid-upgrade.)

## 6. Rollback

- The cadence override is **not persisted**: a CN restart reverts to the default, and Set with `0`
  restores it explicitly.
- Get, List, and Evict hold no durable state; Evict only drops resident entries that the next query
  reloads.
- Disabling the feature is therefore a no-op on durable state; there is no migration to undo.

## 7. Tradeoffs / non-goals

- **Point-in-time, not linearizable.** Get/List gather sequential per-CN RPCs; a concurrent
  load/evict on another CN can shift a count between calls. Acceptable for ops/diagnostics; not a
  cluster-consistent total.
- **Interface is `mo_ctl`, not DDL/SQL surface.** This is deliberately an operator/test control
  plane, not a client-facing consistency guarantee. Read-your-writes across CNs remains a separate,
  opt-in concern that this feature does not promise.
- **No per-tenant scoping.** The operations are sys-admin-only and act cluster-wide by design; they
  are not exposed to individual tenants.

## 8. Approval

The interface, security boundary, partial-failure, rolling-upgrade, and rollback decisions above are
approved by **fengttt** for this revision. Implementation approval (code review) is tracked
separately on PR #29024.
