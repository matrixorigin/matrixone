# Bounded distributed ORDER BY / LIMIT

Status: review 5127191041 corrections validated locally; new-head CI pending (2026-09-07). Historical
measurements below are not acceptance evidence for the revised worktree.
Owner: issue #28285; regression constraint: #27968 must remain executable.
Current base: `e60b3eb3bdfd8ad5f1b5ae3eb8745037371f44d8`.
Reviewed head: `7764c28ecdf038b4c7ea0edf26870c52de52103c`.
Implementation branch: `xp/fix-28285-bounded-merge-top`.

## Problem and review decision

Reject the earlier proposal to unconditionally restore resident MergeTop.
It addresses the #28285 plan regression but leaves large valid outputs limited
by one allocation and mistakes a bound on row count for a bound on physical
capacity. Keep the vector ownership/error-classification fixes from #27999.

The required result is efficient completion within available memory and spill
resources, including both the large-LIMIT narrow-row case and wide payloads.
A controlled error is appropriate when actual resources or the supported
single-record representation are exhausted. It is not an acceptable substitute
for executing an otherwise feasible query solely because all output rows were
put in one vector.

Observed facts:

- #28285 identifies `17c161a3`, immediately after `0db2d5e`, as the regression.
  It replaces global MergeTop with blocking MergeOrder followed by Limit for
  large/runtime multi-scope LIMITs.
- MergeOrder receives every candidate batch before emitting its first result.
  MergeTop also consumes all input, but discards losing candidates while doing so.
- MergeTop retains all selected payload in one batch, then shuffles and returns
  that batch. Allocation peaks include input clones and replacement capacity.
- In `Vector.Copy`, replacing a non-inline variable-length value appends area
  bytes. Dead payload remains until reclamation; N live rows do not imply only
  N rows' payload capacity. This also matters for variable-length local Top keys.
- #27968 requests up to 2,499,999 VECF32(1024) payloads: approximately 9.54 GiB
  for that column if the limit is filled. The issue reports the actual vector
  grow crossing 2 GiB. That ceiling can reject a feasible query even on a
  machine with sufficient total RAM; actual post-delete row count must be
  recorded in reproduction, not inferred from LIMIT.
- Local Top spill logic is unchanged between the two historical commits.
  It writes incoming full-row batches and reconstructs selected rows in chunks.
- A WaitRegister can have multiple senders. `newParallelScope` duplicates the
  operator tree; ordinary Merge and per-CN scope grouping can interleave batches.
  An individually sorted batch is not evidence of a sorted receiver stream.

Existing 55 evidence is diagnostic, not acceptance of this new proposal:
10M/500K took 10.92s versus 13.64s; old 50M/2.5M INSERT took 290.7s.
The new 50M timeout occurred while executing EXPLAIN PHYPLAN, which runs its
inner query. It is not a matched INSERT timing. Finding no directory entries
does not prove zero spill: open temporary files can already be unlinked.

## Algebra and contracts

### Review 5127191041: correction contract

The two reproduced regressions are ordinary corrections to admission policy,
not a new distributed protocol. Retain the ordered gather for #28285 (no
OFFSET, K=5M), but restore the existing 16,384 candidate-prefix rewrite bound
for OFFSET. An external payload file does not externalize Top's O(K) keys,
references, or their growth overlap, including across parallel workers.

For small local Top, a varlen type alone is not grounds for disk I/O. Admit
winning rows into a byte-limited resident batch; compare losers before copying.
Before a winner can exceed the resident logical window, compact dead replacement
history if live rows plus that candidate fit in half the window. Otherwise
write only the current survivors, construct their key/reference state, and
publish the resident-to-spill transition before consuming the pending row.
Failed copy/write/allocation aborts the attempt: no partial-row retry and no
output publication. All temporary/old/new capacity remains allocation-accounted.
The window is at most 16 MiB and one eighth of the single-allocation ceiling;
it is an admission policy, not a second physical-memory ledger or RSS claim.
The existing account remains authoritative and may reject a minimal work set.

Spill-mode Top delays serializing an input batch until its first winner. A
fully rejected batch has zero payload writes; it still must be scanned for
ordering keys. Ordered distributed producers retain their bounded-output
contract. A genuinely single-worker scope needs no ordered-edge hint.

Validation: exact OFFSET result and cleanup under the review's 160 MiB budget;
small-varlen zero-spill control; replacement compaction and mid-batch migration;
wide rows, disk/allocation rejection, cancel/reset; matched small-Top benchmark
and final-head issue-shaped 100M/K=5M execution. Historical timings are not reused
as final performance acceptance.

Let S be input rows, P actual producers (not merely CNs), and K the demanded
ordered prefix. For LIMIT L OFFSET O, K = O + L only when checked addition and
the SQL semantics permit it. Otherwise retain the existing correct full-order
path. LIMIT 0 and runtime parameters retain their established public semantics.

For the same ordering relation and bag semantics:

`TopK(union inputs) = TopK(union TopK(each input))`.

Rows excluded from a prefix have K rows ahead of them in that subset. They
cannot be necessary to produce a valid global prefix. Equal keys preserve
multiplicity; arbitrary tie selection is allowed only where SQL leaves it
unspecified. Tests use unique tie-breakers when exact row identity matters.

After each producer has an ordered stream, its head is a bound on its unseen
tail. The minimum of all active heads is the next output. No output is safe
while an active stream has neither a head nor EOF. A slow stream must be waited
for or failed/cancelled, never treated as empty.

This supports these invariants:

1. Ordinary Top-K returns exactly min(K, S) rows in the existing SQL order.
2. Global merging does not retain K payload rows or consume all P*K candidates
   before it can return output.
3. Reconstructed payload and ordered-gather output have a 64 MiB logical payload
   window and an 8,192-row window, reduced further for a smaller allocator cap.
   Vector capacity rounding, sidecars, input windows, and growth overlap are
   charged by the existing allocation accounts, not by `Batch.Size()`. The
   64 MiB window is not an exact physical peak/RSS guarantee. Large result
   cardinality alone never requires one large payload allocation.
4. Local Top spills source payload, but v1 still retains O(K) row references
   and ordering keys per producer. Those allocations remain query-accounted;
   a fully external key-selection state is a separate follow-up boundary.
5. Success, rejection, error, cancellation, and reuse have one cleanup owner.
6. Early stop applies only to the exclusively owned pure read subtree. Shared
   producers, found-rows consumers, and required effects retain their semantics.

## Selected architecture

```text
scan / filter / join / aggregate output
    -> local Top-K builder, resident or external
    -> ordered stream per actual worker
    -> bounded ordered fan-in within CN, when necessary
    -> ordered CN stream
    -> bounded ordered fan-in at coordinator
    -> OFFSET / LIMIT semantics -> SELECT or transactional INSERT consumer
```

The new capability is an ordered gather at a scope boundary. Keep it distinct
from MergeOrder, whose input contract is a collection of sorted runs rather
than one sorted stream per receiver. It does not add a global spill store.
PostgreSQL's Gather Merge provides an architectural precedent for preserving
worker order; MatrixOne must additionally close its own parallel expansion,
transport, accounting, and cleanup contracts. See the
[PostgreSQL documentation](https://www.postgresql.org/docs/17/how-parallel-query-works.html).

Implementation v1 uses the following deliberately narrow activation contract:

- protocol version 53 plus serialized Top output, MergeTop input, and receiver
  edge bits prove that every participating CN understands the boundary;
- the planner activates it only when every ORDER BY key is already a materialized
  input column. Non-column expressions retain the compatible MergeTop or
  MergeOrder plan until hidden-key materialization is implemented, so a
  volatile expression is never silently reevaluated as a new ordering key;
- every runtime DOP worker gets its own one-sender edge, a CN-local gather
  reduces those streams to one ordered CN stream, and the coordinator repeats
  the same bounded merge;
- for large static limits, a plan uses the hierarchy when the estimated candidate set
  `min(input rows, K * runtime fanout) * row size` exceeds 512 MiB. This is four
  MergeOrder spill windows: below it, the existing vectorized path avoids the
  extra hierarchy. Runtime parameters use the full estimated input as their
  candidate upper bound; missing/invalid estimates choose the bounded path.
  Small static limits additionally require a type-based resident payload proof:
  all projected/order values are fixed width, with K times their widths fitting
  a 16 MiB allowance (including conservative bitmap space). Runtime limits and
  unbounded/unknown payload types choose the hierarchy independently of estimates.
  When the protocol/order-expression gate forbids hierarchy, these unsafe resident
  plans use Top plus MergeOrder. Every updated local Top also checks its actual
  first input schema before retaining payload. Non-ordered small Top uses actual
  winner-byte admission with compaction/migration as specified above; a varlen
  type alone does not require disk. Single-worker scopes do not request an
  ordered-edge contract they do not consume;
- once the hierarchy is selected, every local Top uses external payload storage
  and byte-bounded ordered output even when K is at or below 16,384. This keeps
  wide rows from being reconstructed into one resident result vector;
- output is bounded by both 8,192 rows and 64 MiB (except that one individually
  valid row must be allowed to make progress). The logical window is additionally
  capped at one quarter of the allocator's single-allocation ceiling, reserving
  slack for vector growth. Fixed and variable-width winner
  runs are copied in bounded chunks using actual varlen payload sizes;
- cleanup shares one 30-second deadline across every receiver. The first
  implementation drains already-produced local Top output rather than
  introducing a new receiver-stop terminal state; producer early-stop remains
  disabled until error arbitration can prove that it cannot hide a substantive
  producer failure. This preserves correctness while still removing the
  blocking global MergeOrder materialization that caused #28285.

### A. Preserve order across the physical topology

- Introduce an explicit ordered-input scope/operator contract with ordering
  expressions, optional prefix demand, and one logical sorted stream per edge.
  Ordering keys are materialized before the boundary and carried with the row;
  derived/volatile keys are not independently reevaluated by successive merges.
- After runtime DOP expansion, each Top producer has a distinct local edge to
  an ordered gather. Do not let cloned connectors share an ordered receiver.
  Keep parallel scans; do not force the entire scan to DOP=1.
- Per-CN regrouping must use ordered fan-in on this path. Preserve the existing
  sibling dependency ownership that makes remote trees independently runnable.
  CN count and receiver count are not substitutes for this proof.
- Plain Merge, shuffle, and arbitrary projection invalidate an ordering proof
  unless explicitly shown to preserve the same keys. Validate the final graph
  after cloning/grouping and after remote decode, before any row is published.
- Empty and failed-to-start producers complete their own edge. Do not invent
  EOF to get around missing registration or a scheduling failure.

### B. Stream the ordered prefix with bounded buffers

- Maintain one borrowed current batch and one cursor per input; the heap holds
  stream indexes, not copies of result rows. Refill only an exhausted input.
- Append winners into a bounded output batch. Release consumed input batches
  after their referenced rows have been copied. No final K-row Shuffle exists.
- Use row and byte limits for chunks. Reduce a chunk before publication on
  pressure; a single large row must satisfy the existing record/allocation
  limit and be admitted explicitly. A row cap alone is insufficient.
- Credits bound prefetch by both count and bytes. Include decode buffers,
  sender spool buffers, borrowed batches, output, and heap capacity in the
  query/CN account. Reuse existing batch-flow and allocation primitives.
- Bound per-node fan-in using a tree where needed. Also bound the sum of all
  active tree nodes on a CN: a tree alone does not bound total memory.
- Provide capacity for a minimal head window and output progress before
  admitting the stage. If necessary choose lower DOP before producer startup;
  never wait for memory that can only be released by a blocked downstream.
- Check for errors/cancellation between bounded work units. After K rows,
  publish prefix completion and stop further requests. Already admitted
  prefetch is finite and included in the accounting and cost model.

### C. Local Top resource boundary

The global change removes the reported P*K payload materialization. This PR
also makes spill reconstruction row-and-byte bounded: each selected row records
its actual output width while the source batch is available, and evaluation
uses that metadata to reconstruct at most 8,192 rows / 64 MiB per batch. A
single individually valid row may exceed the normal batch target to guarantee
progress. This closes the single-vector failure for a narrow ordering key with
wide projected payload, including the #27968 shape.

The remaining local selection state is not independent of K. Spill mode keeps
O(K) row references and materialized ordering keys per producer. All of that
state is allocation-accounted and fails with a controlled resource error, but
v1 does not claim that an arbitrarily wide ordering key succeeds merely because
disk is available. A future fully external key-selection implementation must:

- Keep a resident heap fast path for fitting selection state. SQL row counts
  and estimated widths can guide policy, but exact allocation admission owns
  safety. Compact dead variable-length bytes at a bounded threshold, with
  admitted replacement capacity, or transition to external selection.
- Reserve a bounded spill/output working window before growing resident state
  to its limit. Reclamation must not depend on a large new allocation after
  the resident state has consumed the entire account.
- At a committed input boundary, externalize the current surviving candidates
  and process the remaining input as byte-bounded sorted runs. The Top-K
  identity permits discarding earlier losers. A failed multi-column mutation
  cannot be used as that boundary: preflight/checkpoint unpublished changes,
  and preserve the input cursor until the mutation or transition commits.
- Publish a run only after its write succeeds. Release its source state only
  after publication. On partial write or failed recovery, abort the query and
  clean all owned resources; never replay a partly applied row as new input.
- Use bounded fan-in external merging, truncating each completed merged run
  to K. Keep merge-pass depth and resident run metadata bounded; use the
  existing run management/codec where possible, with explicit common ownership
  for primitives shared by Top and MergeOrder. Do not copy an entire spill engine.
- Disk admission covers live input runs plus the new output run until commit.
  FD admission covers active readers and writer, not total run count. Exhausted
  disk, FDs, depth, or the minimal working set returns the appropriate error.
- Output an ordered stream in chunks without a single K-row payload vector.

The current source-payload spill writes a batch only after its first candidate
is admitted; fully rejected batches are not written. It can still write substantial input data and
requires a full scan of an unordered source. A future external key selector can
require multiple merge passes; neither O(K) total spill nor payload read-locality
improvements are claimed here.

## Ownership, errors, and generations

| State/owner | Transition or wait | Required terminal behavior |
|---|---|---|
| Ordered stage / scope | prepare -> register/admit -> prime all heads | Failed admission/start closes created edges and propagates the real error |
| Gather input cursor | borrow -> compare/copy -> release -> refill | One receiver owns the borrow; no next receive invalidates another stream's batch |
| Output batch / gather | build -> publish to consumer | Never publish partially built rows after an allocation or comparison error |
| Local builder | resident -> externalizing -> external -> ordered output | Run publication is the state switch; old ownership remains valid until then |
| Prefix completion / scope | emit K -> stop owned producers -> quiesce | Internal stop is success; query cancel and substantive errors remain errors |
| Shared input / consumer | early stop of one reader | Release that reader only; do not cancel other consumers |
| Transport / scope | full credit queue or missing head | Independent cancellation/stop wakes both sender and receiver |
| Prepared attempt | terminate -> quiesce -> Reset -> new admission | No previous cursor, EOF, limit, credit, error, or callback enters the next generation |

Reuse End/Error/Abort, StopSending, and current scope error arbitration.
Call returns execution status; Reset performs terminal signaling. Do not send
terminals from Call. V1 cleanup drains the finite remaining local Top output
because the current protocol cannot distinguish an intentional prefix stop from
a producer failure without risking error masking. Concurrent teardown shares a
bounded deadline rather than paying one full timeout per edge. A later protocol
may stop producers early only after preserving any earlier substantive error.

INSERT commits only after successful statement completion and producer
quiescence. An error after partial consumption must roll back the statement.
SELECT may have streamed rows before a later error, following its existing
protocol; never label that execution successful.

The minimal wait graph is producer -> credit -> receiver -> missing head from
another producer. Stage startup must make every required producer runnable.
No consumer may monopolize the execution slots needed to start the producer
it waits on. Inject submission failure and a constrained worker pool to prove it.

## Compatibility and rollout

This is R3 design work: it changes distributed scope topology, ownership,
runtime expansion, and a material execution hot path. It requires its own
design review before implementation approval.

Encode ordered-gather capability and configuration explicitly in the pipeline
representation. Both cloning and remote encode/decode preserve it. Version 53
is the rollout gate. A node that cannot honor the ordering contract must
never silently decode it as ordinary Merge or MergeTop.

New plans activate only when all participating nodes support the capability;
otherwise use the existing compatible plan before execution. Mixed-version
fallback may retain today's performance and must be visible in EXPLAIN.
Never change algorithms after result publication to accommodate an old peer.
Topology/capability changes invalidate a cached prepared physical plan.

Retain existing row comparator, NULL/NaN/type rules, prepared provenance, and
spill codec. No table/catalog or persistent storage migration is proposed.
Spill is attempt-local and not resumable after restart. Rollback uses the
compatible planner path for new attempts after active generations quiesce.

Existing analyzer counters expose input/output rows and Top spill rows/bytes;
allocation accounts own actual allocation admission and peak accounting. Cleanup
timeout diagnostics include receiver/channel state. Dedicated stream-count,
spill-read, and prefix-stop metrics are not implemented in this revision; neither
external key merge passes nor producer early-stop is implemented. Do not infer
these metrics from `Batch.Size()`, or add a second estimated memory ledger.
All resources remain charged to the owning tenant.

The type-based guard retains the small fixed-width fast path. The runtime
resident path additionally retains fitting small varlen results, compacting dead
payload or migrating survivors on actual pressure. Ordered distributed producers
still use spill output; lazy writing bounds their rejected-input I/O, not all
candidate I/O. Updating a cluster cannot retrofit this runtime guard onto
old CN binaries: mixed-version fallback preserves protocol compatibility, while
the full payload safety guarantee requires all executors to carry this fix.

## Cost model and alternatives

Let F be fan-in, b the admitted input-window bytes, q the admitted queued bytes
per edge, and o the output-window bytes. One gather uses approximately
F*(b+q)+o plus O(F) cursor/heap state; allocation accounts enforce the actual
sum across the CN. None of these buffers grows with K.

For a flat merge, selection CPU is O(P + K log P); a balanced merge tree can
perform O(K log P) comparisons plus bounded prefetch/initialization work per
level. Its buffers and copies at every level must be measured. Flat input
consumption is K plus per-stream lookahead and finite prefetch, capped by
available rows. This is not an exact K-row network or disk-read claim.

The static activation estimate is deliberately about the pre-global-merge
candidate set, not the final K rows. The existing path remains preferable for
small inputs even when K itself is large relative to S. On 55, 1M input /
500K limit with approximately 146-byte rows ran in 1.15--1.28s through the
compatible path versus 2.82--2.94s when hierarchy was forced. At the issue-like
5% ratio, 10M input / 500K limit ran in 3.56--3.62s with bounded hierarchy
versus 21.19--21.49s on main. These measurements motivated, but do not replace,
the byte estimate and its conservative unknown-statistics fallback.

The issue-shaped 100M input / 5M limit INSERT completed in 111.57s on the
fixed build and produced exactly 5,000,000 distinct keys from 0 through
4,999,999. The same data and service configuration on main `6e5f82f568`
remained incomplete at 300s; canceling the client rolled the target table back
to zero rows. Thus the matched one-CN comparison establishes at least a 2.69x
speedup without extrapolating the 300s lower bound.

For 50M input and K=2.5M with 64-byte payload, global resident MergeTop needs
at least about 160 MB for that payload alone, before descriptors, keys, input
copies, dead area, and shuffle overlap. Streaming gather retains only windows.
For #27968, 4 KiB vector payloads remain bounded batches across the same path.
End-to-end improvement depends on how much time remains in local selection,
storage reads, and INSERT. No speedup multiplier is promised before measurement.

| Alternative | Decision |
|---|---|
| Restore resident MergeTop for every K | Reject as complete fix: single-allocation ceiling and variable-length replacement history remain |
| Raise threshold / trust row-width statistics | Reject as safety design: physical capacity and stale/unknown statistics differ |
| Global spill Top on unordered candidates | Possible compatible algorithm, but existing implementation writes all candidates and can repeat local payload I/O |
| Give existing MergeOrder a terminal LIMIT only | Insufficient: still receives/materializes every candidate before first output |
| Ordered gather plus byte-bounded payload reconstruction | Selected: uses order already established by producers, bounds global state and output batches while retaining accounted O(K) local keys |

## Validation and delivery sequence

Historical author measurements on 55 (`10.222.1.55`) used the same service
configuration and data for fixed/main comparisons. They predate the type-based
payload admission change and are not performance acceptance for that change:

- 1M input / K=500K stayed on the compatible path and completed in 1.286s,
  matching the main-path envelope; forcing the hierarchy here was rejected
  after it measured 2.82--2.94s.
- 10M input / K=500K completed in 3.56--3.62s versus 21.19--21.49s on main.
- 100M input / K=5M completed in 111.57s with exactly 5M distinct keys; main
  did not complete within 300s and cancellation left the INSERT target empty.
- ASC/DESC, NULL and duplicate keys, LIMIT 0 and LIMIT beyond cardinality,
  runtime parameters, expression fallback, cancellation, and a following
  healthy query were exercised. Removing the final global merge was also
  rejected by a black-box counterexample: parallel INSERT applied LIMIT per
  worker instead of globally.
- Focused package, topology, codec, allocation-generation, and race tests pass;
  static analysis reports zero issues. The five failures in the full compile
  package are identical Parquet fanout failures on the clean base commit.

### Revised-worktree validation (2026-09-07)

Base: `f14eb824df4c74fb341ebecd46d2da3358503a95`; rebased PR commit:
`76670d12eb71fe010c426908a5b82ab47a65b6eb`, plus the local fixes. Unlike the
historical run above, the current complete compile package passes.

- Complete Top, MergeTop and compile packages pass in normal mode. Top and
  MergeTop also pass under the race detector. Changed packages and the DML
  consumer pass `go vet`; the final service builds successfully.
- A 1 MiB allocator ceiling test executes K=8192 with varlen payload and no
  ordered-output hint. Independent exact survivor/order and multi-batch checks
  pass. MergeTop tests exercise interleaved varlen winners and an indivisible
  row exceeding the injected logical output window.
- Fixed-width allocation rejection exposed an 8192-byte selection leak: failed
  slice growth returned nil over the live owner. The Top wrapper now preserves
  the original slice on failure. The regression asserts error classification
  and zero account/pool ownership after cleanup. Small-K runtime spill disk
  rejection also asserts the disk resource error and zero disk/FD/memory usage.
- The existing forced multi-CN DML fixture now includes five-row varlen Top-N,
  a real remote physical plan, exact global OFFSET results and prepared reuse.
  The entire fixture passed twice in one process on 55 (20.60s including cluster
  startup, then 3.38s; the added subtest took 0.09s and 0.01s). Test executable
  SHA256: `0abd0ab7a055a5d81334d3ec2ae40701066949877d7c83581d30ade36a01db0f`.
- LIMIT BVT extends its existing data with small-K varlen and prepared queries;
  it adds no rows. Normal comparison passed 34/34 statements twice on the same
  two-CN instance, and the test table count was zero after teardown.
  Final service SHA256:
  `455a8209b59ec9619860fb4e7c31b3139ff76e49048615b50e2a132121e7deb7`.
- Test startup incident: restarting the launch fixture with its existing data
  failed CN admission before any SQL. Original data and logs were retained;
  a clean test-owned data directory reached SQL and ISCP readiness. This is not
  evidence of restart acceptance or a Top-N execution failure.

The following historical worktree record predates review 5127191041. Consult
the correction evidence below for current measurements. Mixed-version execution
and restart acceptance are not implied by unit tests or historical measurements.

1. Prove typed topology and a deterministic minimal gather, including shared
   receiver and runtime-DOP counterexamples, before large benchmarks.
2. Implement ordered fan-in, byte-bounded output, and teardown as one closure.
   Include encode/decode/version/clone tests and public multi-CN execution.
3. Follow up with variable-length key reclamation and a resident-to-external
   key-selection transition if workloads require K-independent local key state;
   do not attribute that unimplemented guarantee to this PR.
4. Run the combined implementation on 55 and review the complete change map.
   Functional partial delivery cannot be described as full #28285/#27968 closure.

| Counterexample/axis | Independent oracle |
|---|---|
| Interleaved sorted batches from two workers on one edge | Final topology rejects/repairs it before output; exact reference ordering |
| One slow/empty/failed producer; bounded scheduling slots | No premature output or fabricated EOF; injected release/error terminates |
| K=0/1, 16383/16384/16385, K>=S, parameter K, OFFSET overflow | Exact result and cardinality; reuse behaves like a fresh query |
| ASC/DESC, compound keys, NULLs, duplicates, NaN, expressions | Existing comparator semantics; full sort/reference with unique ties where required |
| N fixed, replacement count grows, increasing/decreasing payload lengths | Accounted physical peak remains within byte budget; same correct survivors |
| VECF32(1024) payload with narrow key | Byte-bounded multi-batch output succeeds despite a reduced per-allocation cap |
| Large variable-length sort keys | Accounted success or controlled resource error; external key selection is outside v1 |
| Pressure before/after candidate mutation and run publication | No lost/duplicate row; exact cleanup; external result equals resident reference |
| Disk/FD failure, partial/truncated spill, allocation rejection | Correct error class; zero terminal ownership; healthy following query |
| Full queue/held ACK, cancel, disconnect, stop racing real error | All owners quiesce; actual error is not masked; no producer drains unused output |
| Shared producers, SQL_CALC_FOUND_ROWS, DML consumer | Required work/count preserved; statement rollback after downstream failure |
| Old/new peers, prepared reuse after topology/capability change | Correct fallback/recompile; no silent ordered-to-unordered decode |
| Result identity and cleanup through SQL | COUNT and exact ordered/reference comparison; duplicate multiplicity; same-instance rerun |

Use injected small limits for UT/BVT boundaries, not huge fixtures. Preserve
existing useful tests and add only distinct proof. Run focused/package/race
evidence for changed owners and consumers, then SQL tests on 55 only.

Performance uses direct target SQL, without an unmeasured executing EXPLAIN in
front of it. Compare historical good, verified current main, and fixed build
under identical topology, DOP, rows, schema, data distribution, cache policy,
and resource limits. Use an ordered/interleaved repeated schedule to detect
noise rather than claiming stability from one run. Record build hashes and
data fingerprints. Compare 1M, 10M, 50M, and 100M with 1/2/3 CN where the
dimension adds evidence; vary narrow/wide rows and K/S, including high selectivity.

Acceptance requires both original scenarios to succeed and the narrow-row
50M/100M performance to recover to the historical-good envelope in matched
runs. Report the observed variance; any material regression (initial alert
threshold 5%, subject to baseline variance) requires investigation. Small K
and single-CN controls must also remain within the measured baseline envelope.
Gather input rows must follow prefix-plus-prefetch, and gather spill must be
zero. Local spill, total CPU, RSS, allocation peaks, I/O, and cleanup are
reported separately. If gains are below expectation, attribute the remaining
time before extending the design.

A finite suite cannot prove that no counterexample exists. Acceptance means
the stated invariants, reachable transitions, and mapped failure dimensions
have evidence; untested dimensions and known limits remain explicit.

## Review correction evidence (2026-09-07)

These checks apply to the corrections on reviewed head `7764c28e`, not the
historical implementation measurements below/elsewhere in this document.

Complete change-map closure against `e60b3eb3`:

| Owner / consumer group | Contract and evidence |
| --- | --- |
| Top state, accounting and tests | Actual winner admission; old/new migration ownership; bounded output; OFFSET and small-varlen reproducers; normal/race/failure/reuse tests |
| MergeTop stream, legacy path and tests | One head per ordered producer; exact bag prefix; byte/row output windows; terminal errors and cleanup; full package/race tests |
| Compile routing, scope placement/expansion, duplication, lifecycle tests | Compatible external fallback; preserve worker/CN ordering and allocation owner attachments; scope tests and final SQL topology/result checks |
| Remote encode/decode, write-back and client cleanup | Preserve ordered flags through the entire decoded execution envelope, including the newly added local edge; decoded DOP regression repeated 100 times under race; multi-CN SQL |
| Protocol constants/generated codec and process edges | Explicit version 53 capability; legacy zero values remain unordered; receiver end counts/reuse/backpressure; protocol and edge tests |
| DML and LIMIT BVT | Exact ordered values and payloads, prepared reuse, delete/cast behavior, external large OFFSET; isolated SQL fixtures |

Q1: resident, compacted and spilled batches have explicit owners. Migration
publishes only after successful copying; failure cleanup retains the old owner
and releases temporary keys, references and spill resources. Q2: ordered edges
have one real producer after DOP gathering; concurrent stage startup, existing
cancellation and terminal draining remain necessary, with no new wait primitive.
Q3: resident replacement history is bounded by admission/compaction, rejected
spill batches do not write payload, and large OFFSET avoids O(K) Top metadata.
Physical capacity is still charged; v1's accounted large-K key/reference state
is not claimed to be fully external. Human approval of the distributed design
and mixed-version deployment acceptance are not inferred from these tests.

- Review OFFSET reproducer: 4,194,304 bigint rows, LIMIT 1 OFFSET 4,194,303,
  160 MiB shared account; correct final value 4,194,303 and peak accounted
  bytes 103,374,880. Memory, spill-disk and spill-FD charges return to zero.
  The normal UT uses the minimum 16,385-row routing boundary; the 4M fixture
  is an explicit benchmark, not a permanently expensive unit test.
- Matched small-varlen Top benchmark: 128 batches of 8,192 ascending bigint
  keys with 64-byte varchar payload, LIMIT 1. Five samples per build, median
  main 7.682 ms versus fixed 7.738 ms; both write zero spill bytes. This
  replaces the review head's full-input payload writes without unaccounted
  retention or raising memory limits.
- Top, MergeTop and compile package tests pass. Resident compaction,
  partial/full-heap migration, cancellation/short-write cleanup and reuse
  have focused tests; full Top/MergeTop race checks pass.
- CI run 34069583657 on `7764c28e`: SCA found two `rowserrcheck` failures in
  the DML fixture. Explicit per-query `rows.Err()` checks fix them without
  suppressions. Matching golangci-lint 2.6.2 checks pass locally.
- That run's coverage failure is an actual `TestExecuteIteration1` timeout,
  not insufficient coverage. Its internal snapshot lookup uses LIMIT 1 with
  varlen payload and failed with `service LOCAL not found`; the snapshot
  polling helper hid that error and kept retrying. Byte-bounded local
  admission removes the unnecessary spill dependency. The unchanged test
  passes in 3.862 seconds, five repetitions in 19.031 seconds, and with
  `-short -tags matrixone_test -covermode=set -coverpkg=./pkg/...` in 3.942
  seconds. The full engine/test package also passes with that cross-package
  instrumentation in 130.732 seconds. The downstream Coverage merge job failed only because its UT
  producer failed. Remote CI for the eventual new commit remains required.

### Remote write-back correction found during large-scale acceptance

The three-CN 100M-row/5M-limit acceptance run exposed wrong result identity,
despite completing in about 150 seconds. The decoded remote Top was wrapped
by `appendWriteBackOperator` in an ordinary merge edge. That edge discarded
the ordered producer contract before runtime DOP expansion, allowing worker
batches to interleave on the wire. The coordinator cannot repair this while
performing a streaming merge.

The correction must preserve the ordered, single-producer edge when wrapping
an ordered Top. Runtime DOP expansion then creates the existing local ordered
gather before the write-back merge forwards its single stream. No new protocol
or ordering algorithm is needed. Acceptance requires a decoded-scope regression
including write-back and DOP expansion, and a fresh exact-result SQL rerun;
the timings of the incorrect runs are not performance acceptance evidence.

After preserving the write-back edge, final production binary SHA-256
`c593f182705aef2aa3d0609c600870584068bae4f0d6f5b5a02a5e069185cfe8`
passes the issue-shaped 100M/K=5M INSERT twice: **156.078 s and 145.954 s**.
Both outputs have count/distinct count 5,000,000, min/max 0/4,999,999,
sum 12,499,997,500,000 and payload byte count 640,000,000. Main at `e60b3eb3`
(binary `8fde3aa5c4c57b568b10926140f80d3f78fac5658069d6e4e633a756542c5386`)
did not complete in the 300-second observation window; its specific query
was canceled at about 324 seconds and returned after 328.25 seconds. The
target remained empty after cancellation. This is a censored baseline, not
a measured completion time or a reproduction of the historical 900s timeout.

Both builds used the same retained data, configuration and host 55 NVMe:
three CN services plus TN/logservice in one launch process, GOMAXPROCS=8,
8-worker scan scopes, bigint key and 128-byte varchar payload. This is not
the original three-host TKE deployment or a historical-good build comparison.
Run order was main then fixed then fixed; no cache-drop operation was used.
The source fingerprint is 100,000,000 rows, keys 0..99,999,999, sum
4,999,999,950,000,000. Final service process RSS high-water was 9,473,892 KiB;
this includes all co-located services and is not query-accounted memory.
Whole-query CPU/I/O counters and a full 1/2/3-CN scale matrix were not collected.

On the same final instance, fixed-width LIMIT 10 returns exact 0..9 twice
(0.504/0.420 s). The extended forced multi-CN DML test also passes twice in
one process, including a 100K-row/1K-limit exact key/payload regression.
Test-harness startup encountered an existing shared `/tmp` lock and was
rerun with a task-owned TMPDIR; that failed launch is not a test pass.

The final LIMIT BVT passes twice (35/35 statements each) against the same
final three-CN instance. Both teardown checks find zero `t1` tables and retain
the 100M performance source. Tester startup initially deleted the generated
performance fixture: its configuration loader ignores the supplied Java
property and reads `run.yml` from the working directory. The fixture was
recreated, the actual working-directory configuration now protects it, and
both final protected runs verify it survives. Generated expected results were
not used. The measured INSERT logs predate this reconstruction and are retained
separately; reconstructing test data is not counted as another performance run.

## References

- https://github.com/matrixorigin/matrixone/issues/28285
- https://github.com/matrixorigin/matrixone/issues/27968
- https://github.com/matrixorigin/matrixone/pull/27999
- [Memory accounting implementation](query_memory_control_and_attribution_impl.md)
- [PostgreSQL 17 Gather Merge](https://www.postgresql.org/docs/17/how-parallel-query-works.html)
