# Executable probes

Use this method when the diff changes concurrency, scheduling, caching, admission or
budgets, or a read path, or when a bot review keeps finding new transition bugs.

The mo-bot reviewer does not stop at reading the code. It **runs probes**: real public
entry points, a storage fake gated per offset, budgets shrunk to a few units, and a
passing control next to every failing probe. On one fileservice PR, each review round found 1–4
new transition bugs this way. Running the same process before the push leaves the bot
nothing to find. A finding requires a probe that fails; severity is calibrated as in
the review contract.

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
| **Cache tiers as sources** | Every tier a read can be served from (memory, disk, index-file, remote). What does the read do when the tier's bytes are short, missing or unreadable? Does it fall through to storage and drop the bad entry, while still returning the caller's and the decoder's errors? |
| **Waits before registration** | Every wait (slot, conn, budget, path) a unit enters **before** it is registered where a lifecycle event can find it. Which signal wakes it on delete or close? |
| **Reentrancy** | Every callback, hook or observer the code runs. Can it call back into the same subsystem (submit, flush, close) while holding a slot, a lock or a turn that the nested call waits for? |
| **Scale inputs** | The supported maxima (4,096 columns → about 2,048 ranges, a whole object, zero memory headroom). |

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

Prune by first principles (a combination the code cannot reach), never by
convenience. Write down why each cell was pruned.

## 3. Probe construction

Every probe:

1. **Drives a public entry point** (read, prefetch, delete, close),
   with `-race`.
2. **Gates storage per offset** so the test, not the scheduler, decides completion
   order: one channel per unit. Reuse the package's existing storage fakes and helpers.
3. **Asserts invariants after every transition**, not only the final result:
   - charged bytes equal the units still reading;
   - holders per unit;
   - backend opens: current, peak and total, with each byte read once;
   - goroutines started against the bound;
   - updates of the caller's cache tiers;
   - queue bytes back to 0 after the scans end.
4. **Adds a bystander:** an unrelated read with a tight deadline (≤ 2 s) while the probe
   holds its stall. This is what catches pinning: "a completed unit stays charged and
   starves someone else".
5. **Pairs every failing probe with a control** in the same run: a direct or bypass
   read that skips the cache, an exact read, or a live variant that must
   succeed. If the control also fails, the fixture is broken, not the code.
6. **Runs on the baseline and on the candidate (§7).** A probe for a finding fails on
   the baseline and passes on the fix; record both results. A probe that clears a
   matrix cell may pass on both: it shows the behavior is preserved. A cell may also be
   cleared by source evidence or retained results that close its contract, under the
   shared evidence rules.


## 4. Race-window widening (check-then-act)

For each pair from §1, insert a temporary `time.Sleep(5 * time.Millisecond)` between
the two lookups and run the probe:

- If it fails with the sleep, the window is real. Fix it with an order that closes it
  (for example, look in the registry first and the cache second when the producer
  publishes to the cache before it unregisters), not with retries.
- The fixed code must pass **with the sleep still in**. Then remove the sleep and
  confirm with `git diff`.

A rare stress failure that runs in milliseconds is usually one of these windows. Do
not settle for "flaky": find the window, widen it, and fix it.

## 5. Exit and policy audit (mechanical)

- `awk` every `return` of each touched function. Every success exit must run the same
  postconditions (for example `finishCacheUpdates()`), and every error exit the same
  cleanup.
- For each policy flag, grep its consumers. Each join point or exit added by the diff
  must check it the same way its siblings do.

## 6. Defect-class sweep

Go through [defect-classes.md](defect-classes.md). For each
class that matches a §1 item, either write the probe or record why the class cannot
occur. The classes come from mo-bot findings on a fileservice PR. Three rounds in a row
were class **A**.

## 7. Validation and report

- **Baseline.** Pick the revision the comparison needs: the PR's merge-base for
  behavior the PR introduces, or the known-bad revision for a fix made within the PR.
  Never assume `HEAD~`. Verify that it lacks the change under test (`git diff
  <baseline> HEAD -- <files>`). Check it out in a disposable `git worktree add --detach
  <baseline>`, copy in only the probe test files, and run there; never overwrite files
  in the active checkout, and don't use `git stash`, whose list every worktree shares.
  In zsh, loop over an explicit word list; an unquoted `$VAR` holding several names is
  not split.
- When the review is split into parallel agents (one per axis), give each its own
  `git worktree add --detach`, and never build or test in a checkout another session is
  using.

- Run each probe on the baseline and the candidate (§3.6), in both completion orders.
  Choose the mode, the repetition budget and the package scope by the
  [race protocol](../../mo-dev/references/race-validation.md) and the
  [validation scope](../../mo-dev/references/validation-evidence.md#2-assign-risk-per-closure-not-per-pr).
- Save the **full output** to a file. Never filter failure output down to `ok`, `FAIL`
  or `---` lines: the error message, the line number and `WARNING: DATA RACE` are the
  evidence.
- Keep a probe as a regression test only when it proves a distinct behavior, and fold
  it into the existing table-driven test where it fits.
- Report the inventory, the matrix with pruned cells and their reasons, and for each
  finding: inputs and state → observed vs expected, its control, and its baseline and
  candidate results.

This method adds executable evidence to the change map, closure tracing and
`unhappy-path-audit` (Q1–Q3); it does not replace them.
