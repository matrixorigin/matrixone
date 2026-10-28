# General hash aggregation throughput

Status: implemented as an internal single-CN cost policy.
Work item: user-requested systematic aggregation optimization; one implementation
branch, with no query- or dataset-specific behavior.
Base: `e9dc8c420818732d6a5691bef1b7ebf6e4d8a87f` (verified upstream main).

## Problem and evidence

The official ClickBench Q5 projection contains 99,997,497 signed, non-null 64-bit
keys and 17,630,976 distinct values. Q5 is
`SELECT COUNT(DISTINCT UserID) FROM hits`. A four-core comparison initially found
a substantial gap to DuckDB. Profiles identify integer insertion planning and
hash-table growth as major costs. The candidate profile attributes 43.18% flat
CPU to planning and 12.89% cumulative CPU to in-place rehashing. Hardware cache
counters are unavailable, so memory locality is a hypothesis rather than a
measured cache-miss claim.

Input order matters: exact aggregation must also perform well on a permutation
of the same multiset. A fixed local-memory cap reduced original-order time but
increased shuffled intermediate rows from 35.6M to 98.6M and regressed latency.
It is rejected. Adaptive reduction must avoid paying for an ineffective local
hash pass and must reconsider its decision when the distribution changes.

## Invariants

Preserve exact equality, NULL semantics, first-occurrence IDs within each hash
owner, output values, insertion flags and cardinality. Preview is immutable;
fallible admission precedes publication, and capacity rejection retries the same
input offset. Existing table-version, stale-plan and rollback checks remain.

A local pre-aggregation stream is allowed to contain duplicate keys only because
a downstream Group completes the same key after exchange. It is never treated
as globally unique. Aggregate state, grouping sets, ordered aggregation and
physical key subsets retain their existing paths.

There is no SQL, persisted-data, hash-code or wire-format change. The new local
cost hint defaults to disabled and is intentionally dropped during remote
serialization, which preserves full local reduction and the original resource
policy. Local worker clones preserve the hint.

## Implementation

1. Keep integer insertion planning sequential. Hoist immutable segment address
   masks and advance directly when an existing or tentative mapping resolves.
   Keep the tentative-slot overlay and all commit validation. String probing
   remains unchanged because its proposed rewrite regressed controls.
2. Copy a single non-null, non-constant eight-byte key into iterator-owned
   scratch. Its representation already matches the existing H8 grammar.
   Floating-point canonicalization, NULLs, constants and composite keys keep the
   generic codec. No plan borrows input storage across preview and commit.
3. Carry the insertion count already validated by preflight through group-key
   publication and output-batch splitting. Preserve provenance preflight,
   application and finalization even for duplicate-only chunks, and preserve
   every existing aggregate update.
4. Enable adaptive local reduction only when the compiler proves a pure-key
   local/final pair, a single-CN exchange and eligible local inputs. Preserve
   coordinator, remote, partial-metadata, dynamic-grouping and physical-key-subset
   exclusions. Keep the final Group and session spill thresholds unchanged.

The local hint selects a hash-table working-set target. It measures Hash.Size,
not total operator memory: a few wide keys can have a large payload and still
benefit from a small probe table. Whole-operator memory remains governed by the
existing automatic spill policy and hard query/CN allocation ledger. The target
is checked after a whole input batch, so it can be exceeded by that admitted
batch and transient preflight allocations.

When the table reaches the target, emit its existing intermediate key batches.
If unique groups exceed half the input rows in this resident epoch, project keys
without local hashing for eight epochs' worth of rows. Then sample through the
ordinary Group again. This asks the extra hash pass to remove at least half of
the downstream rows and bounds the duration of a stale decision. It does not
inspect values, adjacency, names or SQL text. Counter arithmetic saturates.

## Ownership and transitions

| Transition | Ownership and termination |
| --- | --- |
| Build -> Eval | Finish admission/publication for the input batch before emitting keys; Group owns the output batches. |
| Eval -> EvalReset | Drain every output batch before freeing the consumed hash/key state and restarting input. |
| Bypass -> resample | One header borrows child/expression vectors; the next child call retires it. Prepared expressions own computed vectors. Never Clean the borrowed batch. |
| Yield / EOF | Preserve ExecYield even with nil Batch; only genuine nil-batch EOF ends input. Non-empty ExecStop is normalized to ExecNext, matching Group's existing contract. |
| Error / cancellation | Check cancellation after child/expression execution and before publication. Free clears the borrowed header before expression cleanup. |
| Capacity recovery | Disable the local policy for the rest of the execution before generic disk recovery; drain every queued key using the original recovery path. |
| Reset / reuse | Clear all local counters and borrowed references. Prepare restores the immutable operator hint. |

The exchange copies vectors or acknowledges a direct handoff before advancing
its producer. Bounded ready/recycle queues and cancellation/ending signals retain
their existing owners. No goroutine, lock, channel, timer, unbounded container or
new asynchronous cleanup owner is added. The new state is private to the existing
Group executor, with bounded scalar counters and one borrowed header.

## Alternatives

Unbounded local aggregation duplicates a large final table. Omitting it based
only on planner estimates can discard useful reduction; the existing conservative
omission rule remains. A fixed small table repeats useless hashing on high-cardinality
input. Adaptive reduction reuses existing partial output and projection contracts
without introducing a partitioned aggregation subsystem and its additional owners.

Batch cell snapshots and a separate confirmed-hit pass were rejected because hot
working-set controls regressed. Adjacent-repeat shortcuts are not part of this
implementation. The algorithm earns its default through whole-working-set and
same-multiset ordering comparisons, not a ClickBench-specific fast path.

## Validation and acceptance

Owning closures are hashtable, hashmap, group and compile. Use independent map
and final-set oracles, existing admission/spill/provenance coverage, and focused
compiler/clone/remote-fallback tests. Exercise cross-flush duplicates, NULLs,
strings, tuples, signed extrema, wide low-cardinality keys, distribution changes,
computed-key ownership, yield/resume, input/expression errors, cancellation, EOF
and reuse. Do not put timing assertions in UT.

Measure baseline/candidate SQL sequentially with ABBA ordering, fixed affinity,
identical data and exact result checks. Include both orders of the full Q5
multiset, unique/low/high/skewed cardinalities, NULL/string/composite keys,
grouped DISTINCT and one/four-worker controls. Verify which plans actually
activate local reduction. Record CPU, RSS, process swap and spill alongside wall
time. Do not overlap measurement with our builds/tests/data generation.

The initial original-order Q5 target is 25% lower latency; an unmet target must be
reported. Repeated control regressions above 5% block default enablement. Require
exact results, no spill/swap in the in-memory benchmark and no material resident
memory increase. Shared-host interference limits precision and must be reported.
Compare working-set sizes and recheck any apparent control regression separately.
Raw manifests, profiles, logs and experimental binaries remain outside source.
