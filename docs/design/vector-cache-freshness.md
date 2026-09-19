# Asynchronous cross-CN index-cache freshness

Revision 1, issue #27632. User-approved policy: keep asynchronous pull, target
roughly 30-second healthy convergence, and do not read metadata on each query.
Implementation base: `bde7e0cad87c3b125e92795afdd4dd8af0d4b115`.

## Evidence and contract

The current five-minute idle TTL is renewed by searches. Housekeeping runs every
2.5 minutes and checks generation freshness every fourth tick; detected stale
entries wait for another housekeeping pass. Thus the normal worst-phase window
is approximately 10–12.5 minutes, excluding slow checks and in-flight searches.
This is existing eventual-consistency policy, not a broken HNSW fingerprint.
Issue reports use independent CN processes and F32/F64, eleven vectors at model
capacity three. Same-process CNs share the cache and cannot prove this scenario.

The new invariant is that generation checking is independent of idle expiration
and promptly retires the exact stale resident generation. A query still does no
freshness SQL. Snapshot generations remain immutable and exempt. No next-query
or strict wall-clock freshness guarantee is introduced.

## Mechanism and ownership (R3 lifecycle closure)

Only the generic cache production implementation changes:

1. Keep housekeeping, idle TTL, governor and lifecycle-hook periods unchanged.
   Add a separate 30-second ticker to the existing service loop and stop it when
   the loop exits. A private channel-driven loop permits deterministic UTs.
2. Reuse the single-flight CAS across the entire asynchronous sweep, including
   synchronous eviction/destruction. Busy ticks are dropped; there is no queue,
   per-entry worker, or retired-generation backlog.
3. Collect key/expected-entry pairs, excluding snapshots. Before checking an
   entry, TryRLock it and revalidate map identity, loaded status, not evicting and
   not exited. Skip contention until another sweep. Hold the read lock across
   IsStale to prevent concurrent algorithm teardown/reload.
4. Release the lock before eviction, observe exit again, and call existing
   evictEntry(key, expected, generation_changed). Its seal and compare/delete
   own exactly one destruction and protect replacement entries from ABA removal.
   Active readers finish before destruction; new readers retry the replacement.
   Delete the now-redundant stale flag/mark-then-housekeeping state. TTL claims
   still recheck expiration under ttlMu; generation eviction bypasses idle TTL.
5. Preserve StaleChecker signatures, checksum/count fingerprints, one-minute
   metadata SQL timeouts, and (stale,error) semantics. Shutdown racing the final
   exit observation is safe through shared eviction ownership, not a new atomic
   prohibition on every operation after exit.

Q1 (ownership): the service loop owns both tickers; one sweep owns its temporary
entry slice/read locks, and the existing eviction winner owns Destroy. Q2
(wait-for): metadata holds an entry read lock; release it before requesting the
eviction write lock. Searches can delay destruction; no read-to-write upgrade.
Q3 (growth): at most one freshness worker and O(N) entry references per cache;
no tick backlog or asynchronous retirement list.

## Costs, limitations, alternatives and rollback

Healthy checks increase approximately 20x to N/30 checker calls per second
per CN for N eligible indexes. HNSW uses one metadata SQL per call; FULLTEXT2,
CAGRA and IVF-PQ normally use two. A sweep is sequential. Slow metadata, many
entries, queued writers, active searches or existing synchronous housekeeping
destruction can delay convergence beyond 30 seconds. A metadata read lock can
delay Remove/Destroy and indirectly later readers behind a queued writer. The
SQL timeout does not bound native work, lock waits or the entire sweep/shutdown.
Existing true+error eviction can amplify failure-time reloads/logs at the faster
cadence. These are explicit operational risks, not hidden SLA claims.

Rejected: per-query metadata checks (hot-path I/O), shortening all housekeeping
periods (unrelated governor/hooks churn), push invalidation (distributed protocol
expansion), checker API/GPU changes and retirement queues (unnecessary lifecycle
surface). Rollback to the previous binary restores the old convergence window;
there is no disk/catalog/SQL/wire format change or mixed-version incompatibility.

## Validation and delivery map

- Generic cache: injected tick/channel barriers prove independent cadence,
  single-flight through checks and destruction, read lifetime, replacement ABA,
  shutdown/eviction ownership, state/snapshot skip, contention retry and errors.
  No timing sleeps in UTs. Existing cache lifecycle tests remain applicable.
- Consumers: HNSW and fulltext2 CPU checker contracts; no GPU interface changes.
  Focused tests precede owning-cache normal/race runs and incremental SCA over
  merge-base changed Go packages plus relevant consumers.
- Native QA: four independent LOG/TN/CN/CN processes and fixed reader/writer
  endpoints. Warm reader; publish mixed UPDATE/DELETE/INSERT on writer using
  F32/F64 four-model indexes; confirm changed metadata at reader; compare actual
  indexed results to an unindexed exact oracle and record convergence duration.
  Cover LIMIT/OFFSET, deletion exclusion, rollback and post-convergence stability.
  A 120-second polling cutoff is a fixture budget, not the promised cadence.
- BVT: existing async and snapshot cases, normal comparison twice with catalog
  teardown, preserving bulk-fragment coverage and documented result formatting.
  Only replace cold-cache avoidance after native independent-process proof.
- Final GPT-6 medium whole-change review must resolve blockers before push/PR.
  Record actual commands, terminal outcomes, remaining risks and review revision.
