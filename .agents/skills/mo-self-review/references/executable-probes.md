# Executable probes

Use this method for R2/R3 closures (shared validation scope) that change the behavior
of concurrency, scheduling, caching, admission or budgets, or a read path, and when a
bot review keeps finding new transition bugs. R0 changes, such as comments or
mechanical renames in those files, don't need it.

The mo-bot reviewer does not stop at reading the code. It **runs probes**: real public
entry points, a storage fake gated per offset, budgets shrunk to a few units, and a
passing control next to every failing probe. On one fileservice PR, each review round found 1–4
new transition bugs this way. Running the same process before the push leaves the bot
nothing to find. A transition claim, finding or cleared cell, needs the cheapest
proof that settles it under the shared evidence rules: a probe, a qualifying existing
test or result at the owning boundary, or, when neither exists nor can be built, a
source trace under the first-principles proof marked "probe not built: <constraint>".
A probe through a public entry point is required where caller behavior or
reachability is still uncertain. Severity is calibrated as in the review contract.

## 1. Transition inventory (from the diff, not from memory)

Inventory the **whole PR delta against its base**, not only the hunks of the latest fix.
The bot re-audits every file each round; a path untouched since an earlier round is
still in scope.

For each changed function, list:

| Item | Question |
|---|---|
| **Producers** | Which units of work complete independently (runs, downloads, parts, workers, callbacks)? |
| **Consumers** | Who waits for them, and in what order? Is the consumer **ordered** while the producers are not? |
| **Holds and charges** | What does each unit hold (buffer charge, ref, conn, slot, goroutine, queue bytes)? **When is it released, and is that tied to the unit's own end or to the consumer reaching it?** |
| **Signals** | Which channel or flag does a waiter wait on: its own unit's, or an aggregate one ("all admitted", "started")? |
| **Exits** | Every `return` of the touched functions. Which postconditions should every successful exit share (cache updates, publication, release, metrics)? |
| **Policies** | Every policy flag the path must honor (cache skips, exact reads, per-object modes …). Does each **new** exit or join point check them? |
| **Lifecycle events** | Delete, close, cancel, scan end, idle timeout. Can each one reach work in **every** state (created, waiting-slot, waiting-conn, waiting-buffer, registered, reading, ended)? |
| **Check-then-act pairs** | Two lookups in different structures (cache, then registry) that a concurrent actor moves an item between. |
| **Cache tiers as sources** | Every tier a read can be served from (memory, disk, index-file, remote). What does the read do when the tier's bytes are short, missing or unreadable? Does a read whose contract promises the data fall through to the source and drop the bad entry, while still returning the caller's and the decoder's errors? |
| **Waits before registration** | Every wait (slot, conn, budget, path) a unit enters **before** it is registered where a lifecycle event can find it. Which signal wakes it on delete or close? |
| **Reentrancy** | Every callback, hook or observer the code runs. Can it call back into the same subsystem (submit, flush, close) while holding a slot, a lock or a turn that the nested call waits for? |
| **Scale inputs** | The supported maxima (for example the most columns or ranges one request can carry, the largest object, zero memory headroom). |

## 2. Probe matrix (derive it; do not reuse a remembered case list)

Cross the axes that apply to each item in §1:

- **Completion order:** every permutation of 2 producers ("first ends first", "second
  ends first"); with 3 or more, at least first, last and middle stalled.
- **Resource state:** free; connection pool at max; budget scaled to N units;
  measured headroom 0; queue at its byte limit.
- **Policy:** each flag from §1 on every entry point, including each point where a
  request joins work already queued or under way.
- **Lifecycle event × state:** fire each event while the unit is in each state,
  including every wait entered before registration (§1).
- **Tier fault:** for each cache tier, a truncated file, a missing file and a read
  error. Derive the expected result from the entry point's contract: a read that
  promises the data returns the source's bytes; a cache-only read reports a miss.
  Control: an error from the decoder or the caller is still returned.
- **Reentrancy:** each callback submits, flushes or closes while its own resources
  are held, with the capacity scaled to 1.
- **Scale:** the supported max input. Count goroutines, opens and charged bytes
  against the bound.

Prune a cell only when the code cannot reach it, or when it is equivalent to a cell
already covered (state the equivalence); never by convenience. Write down why each
cell was pruned.

## 3. Probe construction

Every probe:

1. **Drives a public entry point** (for example read, prefetch, delete, close,
   submit, shutdown), in the mode its oracle needs (§7): `-race` for a data-race or
   ownership oracle; normal mode for a timing, allocation or other
   instrumentation-sensitive oracle, such as a deadline.
2. **Synchronizes at the affected transition** so the test, not the scheduler, decides
   the order: one gate per unit, at the seam the change uses, such as a storage fake
   gated per offset, a completion callback, a worker hook or an allocator callback.
   Reuse the package's existing fakes and helpers.
3. **Asserts the affected invariants after every transition**, not only the final
   result. For example: charged bytes equal the units still running; holders per unit;
   backend opens (current, peak and total) with each byte read once; goroutines and
   workers against their bound; updates of the caller's cache tiers; queues back to 0
   once the work ends.
4. **Adds a bystander when the change shares a budget, pool or worker with other
   requests:** an unrelated request while the probe holds its stall, with the capacity
   sized so the bystander fits once the ended units are released. Its expected wait
   follows the admission contract: it must not wait when only ended units hold the
   capacity, and may wait while live units hold it all. The same request without the
   stall is its control. This is what catches pinning: "a completed unit stays charged
   and starves someone else".
5. **Pairs every failing probe with a control** in the same run that must succeed,
   such as a direct read that skips the cache, an exact read, or a live variant. If
   the control also fails, the comparison is inconclusive: the fixture or a path both
   share is broken. Check the shared path, or compare both on the base.
6. **Runs on the revisions its claim needs (§7).**
   - A **finding** is a probe that fails on the reviewed change, as first reviewed
     before any fix for it, while its control passes. When the claim is that the PR introduced the defect and the base supports
     the equivalent operation, the probe also passes on the base; for an entry point the PR
     adds, the source delta establishes where the behavior came from. A finding needs no
     fix to be reported.
   - A **fix** is proven when the finding's probe fails on the revision before the fix
     and passes on the fixed revision; record both results.
   - A probe that clears a matrix cell may pass on every revision: it shows the
     behavior is preserved.


## 4. Check-then-act windows

For each pair from §1, hold the request between its two lookups with a gate or a test
hook, not a sleep ([concurrency lifecycle](concurrency-lifecycle.md)). While it is
held, let the producer move the item (for example publish to the cache, then
unregister), confirm that the move happened, and release the request.

- The window is real when the specific wrong outcome follows, such as a second backend
  open for the same bytes. A deadline or cancellation error is not that outcome.
- In self mode, close it with a lookup order that cannot miss both structures (when
  the producer publishes before it unregisters, look in the registry first and the
  cache second), not with retries; the same gated probe must then pass.

A temporary sleep may help locate a window while exploring; it is not evidence. A rare
stress failure that runs in milliseconds is usually one of these windows. Do not settle
for "flaky": find the window, gate it, and fix it.

## 5. Exit and policy audit (mechanical)

- `awk` every `return` of each touched function. Each success exit runs the
  postconditions the function's contract requires of every success (for example
  `finishCacheUpdates()`), and each error exit the cleanup its contract requires.
- For each policy flag, grep its consumers. Each join point or exit added by the diff
  must check it the same way its siblings do.

## 6. Defect-class sweep

Go through [defect-classes.md](defect-classes.md). For each class that matches a §1
item, write the probe, cite existing evidence that proves its invariant under the
shared evidence rules, or record why the class cannot occur. The classes come from mo-bot findings on a fileservice PR. Three rounds in a row
were class **A**.

## 7. Validation and report

- **Revisions.** Run each probe on exactly the revisions its claim needs (§3.6): the
  reviewed change (the frozen PR head; in self-review, the full in-scope change,
  including staged, unstaged and untracked files); the PR's merge-base for a defect
  the PR introduces; the revision before a fix to prove the fix. Never assume `HEAD~`.
- **Isolation.** Run each revision other than the working tree in its own disposable
  checkout. Before testing, verify that the checkout holds exactly that revision or
  change: the in-scope paths, their contents and their deletions, and nothing else.
  Never alter the developer's checkout, branch, index or stash, and never build or
  test in a checkout another session is using. Parallel agents each get their own.
- Run each probe on the revisions of §3.6, in each completion order of its matrix cell.
  Choose the mode, the repetition budget and the package scope by the
  [race protocol](../../mo-dev/references/race-validation.md) and the
  [validation scope](../../mo-dev/references/validation-evidence.md#2-assign-risk-per-closure-not-per-pr).
- Save the **full output** to a file. Never filter failure output down to `ok`, `FAIL`
  or `---` lines: the error message, the line number and `WARNING: DATA RACE` are the
  evidence.
- In self mode, keep a probe as a regression test only when it proves a distinct behavior, and fold
  it into the existing table-driven test where it fits.
- Report the inventory, the matrix with pruned cells and their reasons, and for each
  finding: inputs and state → observed vs expected, its control, and its result on
  each revision run.

This method adds executable evidence to the change map, closure tracing and
`unhappy-path-audit` (Q1–Q3); it does not replace them.
