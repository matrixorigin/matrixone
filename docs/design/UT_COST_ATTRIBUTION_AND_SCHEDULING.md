# UT cost attribution and scheduling

- Revision: 4, 2026-09-17; optional embedded prebuild narrowed to build-only cache warming; direct-binary consumption and a custom watchdog are deferred. Product addendum A2 is extracted to the separate product design, with sparse-workspace validation still open.
- Status: design and final infrastructure source review approved at the requested GPT-6 medium gate. Local validation below is complete; whole-suite Linux performance validation and any default scheduling promotion remain outside this delivery.
- Source baseline: `76e1a9bd9f75480ee76f686c43994f938a15a8d5`.
- Branch: `codex/test-infrastructure-20260917`.
- Owner: issue-to-PR task owner; this design defines the infrastructure follow-up to narrow fixture PR #29025. Implementation PR/owning issue links must be added when assigned.
- Implementation ownership: Aristotle owns only `optools/run_ut.sh` and new scheduler tests after design acceptance; main owns diagnostics and coordinates Makefile/native and readiness changes. The design agent owns this document only.
- Design session: `01a0aadb-93e0-76d1-a486-80a577ed5bdb`; requested review configuration: GPT-6 medium. Runtime model/effort attestation belongs in the task's invocation record, not an inference from this document.
- Evidence record: `/Users/xupeng/.codex/issue-to-pr/matrixone/race-ut-time-optimization-20260916/task.md`; E10/E11 and subsequent SQL timing are supplied in the design conversation.
- Relationship: narrows `UT-cost-20260917-1` to the approved infrastructure scope. Retains the single-runner/resource principles of `UT_EXECUTION_AND_FIXTURE_OPTIMIZATION.md`; exact baseline source takes precedence over historical descriptions in that document.

## Evidence and objective

E1 (#28956, run `35102230463`, job `104814317047`, head `35355c7ba9c4b871c781b95b5cb3fa34868b610f`) and E2 (#28885, run `35046292946`, job `104636925165`, head `f4691bfce68c7737579dfcde3b7b0a2c7140a821`) exhausted a 70-minute outer budget while heavy work remained. Both peaked near 16 GiB without OOM kills. These are censored runs, not complete wall-time controls.

E10/E11 attribute two long Arrow admission waits to the DML shared-cluster holder. E1's composite regression built/cloned its 18,193 rows quickly and completed HashDiff in about 1.37 seconds before a long apply interval. The relevant PICK/apply/helper source and chunk-boundary test are identical between E1's head and this baseline; staged updates already existed. Neither slow setup cardinality nor missing update staging is an established cause.

Admission waiting overlaps holder work. Removing or relocating a wait does not remove that work, and cumulative waiter duration is never a suite-savings estimate. Profiling of the actual apply interval remains with the task owner; this change does not duplicate archived timeline reconstruction.

Current-base profile supplied by the task owner: the required wrapper's exact composite subtest race run exited 0 after native prerequisites were validated; subtest 36.36s, parent 49.03s, package 52.961s. CPU samples total 59.11s, including ExternalCode 31.25s (reported as race runtime), `disttae.Transaction.deleteTableWrites` cumulative 5.52s/flat 0.63s, `Vector.GetType` 0.84s and `memclr` 2.27s. Artifacts: `/tmp/mo-dml-boundary-baseline-20260917.{json,cpu,stderr}`. CPU sample totals are not wall-time components. This supports material execution/transaction cost, not a finding that lazy staging would dominate savings; product changes are outside this infrastructure scope. This profile was reported by the task owner, not independently rerun by the design agent.

Objective: remove demonstrably repeated infrastructure preparation, repair existing optional build-only prebuild scheduling, and preserve trustworthy failure/lifecycle evidence. Measured whole-suite improvement, not timeout extension or a faster isolated case alone, determines performance success. Revision 4 does not claim binary consumption, avoided duplicate linking, or demonstrated whole-suite improvement.

## Scope and non-negotiable invariants

All existing cases, subtests, assertions, data, topology, negative paths, lifecycle boundaries, race instrumentation, tags, short mode, native mode, package working directories and package exclusions remain covered. Do not move cases to other suites, add skips/retries, weaken assertions, or replace synchronization with sleeps. No arbitrary data reduction: the composite PICK case retains three physical blocks, two 8,192-key chunks, and its untouched sentinels. A future smaller seam would need a separate proof of the same boundary; it is not part of this design.

Keep one runner, embedded package concurrency at its current cap of two, global complete-cluster admission, isolated engine execution, and current heavy/plan budgets. Keep `UT_PREBUILD_EMBEDDED=0` and `UT_OVERLAP_LIGHT=0` by default. No cluster daemon, dynamic resource controller, production branch optimization, or fixture-lifetime expansion.

The baseline Makefile defaults `UT_HARD_TIMEOUT` to 120m while `run_ut.sh` falls back to 70m. Align the script fallback with the existing Makefile default, with a regression check, without increasing the Makefile budget. Report this as consistency maintenance, never a speedup. Historical 70m failures remain historical evidence.

## Implementation boundaries

### 1. One native preparation owner

`Makefile`'s `ut` target already depends on `cgo`; `optools/run_ut.sh:run_tests` calls the phony `make cgo` again. The repeated invocation includes provenance preparation, recursive native builds and staging, even when compilation is incremental.

Keep Makefile as the native preparation owner for `make ut`. Its recipe passes an invocation-local prepared indication only after the prerequisite succeeds. The runner accepts the indication only after verifying the existing provenance for the checkout, host/target, accelerator and build mode. Missing/invalid evidence uses the existing authoritative `make cgo` path; direct script invocation retains preparation. Do not create a second stamp protocol, infer freshness from file existence, or skip the CGo wrapper smoke test. Source archives without reusable provenance retain the preparation fallback.

Files: `Makefile`, `optools/run_ut.sh`, existing native build/provenance contract tests and scheduler harness as applicable. Preserve the existing provenance helper's ownership contract. The claim is one avoided redundant preparation invocation, not an assumed full rebuild saving.

### 2. Bound optional build-only cache warming

Decision, approved by main under the user's authorization: retain `run_embedded_prebuild` as optional build-only cache warming and retain `go test` as the sole embedded execution owner. Remove the proposed direct-binary dispatch, execution manifests, `CLUSTER_PREBUILT_RUNNING`, private execution-report consumption and direct-execution-only heartbeat/grace branches. No custom watchdog is authorized in this infrastructure change.

After joining prebuild, always run the original complete-scope command through `run_ut_command`: `go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p "${package_parallel}" -timeout "${UT_TIMEOUT}m" -race ${package_scope}`, with the existing loader/CGo environment. Package selection, flags, package concurrency and caller-supported options stay unchanged. Go remains responsible for package working directories, TestMain, examples/fuzz seeds, no-test packages, exit status, its external timeout and output-drain handling. Do not execute test binaries for discovery.

Prebuild success, failure, or being disabled all lead to exactly one authoritative complete-scope `go test` execution. Prebuild failures are diagnostic, not test results; a runtime test failure never triggers a rerun. Keep original race/tags/native compilation inputs. Preserve build diagnostics before removing run-owned artifacts, only after children stop. A package producing no binary does not require a new execution manifest or synthetic terminal event.

Reason for narrowing: `-test.timeout` alone does not replace `go test` supervision. In Go 1.26.4, the internal alarm runs within `m.Run`; the outer process deadline is T + max(60s, T/10), and output/termination WaitDelay is max(5s, T/10). Direct `test2json` lacks that outer protection for initialization, TestMain teardown and inherited output descriptors. Recreating it adds ownership and timing machinery without demonstrated whole-suite benefit. Retaining the existing owner is the smallest safe correction. Existing engine/plan direct-binary paths are unchanged and outside this scope.

Replace FIFO child waiting in `run_embedded_prebuild` with completion-driven slot reclamation: a finished later child releases its slot even while an earlier child remains active. Reuse the established bounded completion mechanism from `run_plan_race_shards`, including portable-shell behavior; introduce no additional worker slots or test timing sleeps. Tests use explicit readiness/release barriers.

Files/functions: `optools/run_ut.sh` (`run_embedded_prebuild`, `start_embedded_prebuild`, `finish_embedded_prebuild`, embedded dispatch and termination), `optools/ut_embedded_schedule_test.go` and existing scheduler contracts. Prebuild remains opt-in (`UT_PREBUILD_EMBEDDED=0`). Describe it as cache warming, not binary reuse. Keep completion-driven slot reclamation and launch/report-open/cancellation ownership fixes; remove tests exclusive to the discarded direct-execution path without removing existing behavioral coverage.

### 3. Bound existing readiness cancellation

In `pkg/embed/operator.go:waitHAKeeperReadyLocked`, derive each five-second client attempt from the outer startup context and replace the unconditional one-second post-failure sleep with existing `waitStartupRetry`. Preserve retry cadence, outer timeout, readiness predicates, and successful client transfer/close ownership. This improves cancellation responsiveness; it is not a claimed healthy-startup speedup.

Add focused cases in `pkg/embed/operator_test.go` for cancellation during an attempt and retry waiting, successful transfer, and client cleanup. Do not retune the already-landed 100ms readiness or 500ms bootstrap retry defaults or alter HAKeeper health/heartbeat semantics.

### 4. Distinguish explicit release from inferred process exit

Extend `optools/summarize_ut_setup.py:setup_records` and `summarize_embedded_diagnostics` to retain timestamps and package terminal events, with regression coverage in `optools/ut_tools_test.go`.

Identify an admission generation by run/package invocation/PID/cluster/acquire sequence. A matching release is explicit evidence. If release is absent but an unambiguous package terminal event bounds the owning process, report `process-exit-inferred`, an upper bound on holder lifetime, and partial evidence. This does not prove a clean service shutdown or an exact release timestamp. With no terminal event, retain `unknown/open-at-capture`; ambiguous multi-process package reports must not be attributed to an arbitrary PID. Build failure without a started owner is not an owner exit. A release from an earlier generation cannot close a later one.

Preserve compatibility with existing summary consumers and add separate explicit/inferred/unresolved counts. Truncated records remain partial, and parser failures cannot override the runner's original failure. Label summed wait durations as overlapping diagnostic quantities. Do not add DML `TestMain` teardown just to obtain a release event: OS-exit lock release and successful service cleanup are different claims, and extra teardown adds work.

## Resource, cancellation and partial-failure contract

- The runner owns dispatch, helper process groups, final statuses and report publication; each helper owns its registered child groups and private outputs.
- Publish child ownership before acting on pending TERM, using the existing deferred-signal handoff pattern. Open reports before spawning; if a later open/spawn fails, stop and reap already-started children.
- Prebuild is bounded by existing slot counts; execution concurrency remains owned by the original `go test -p` command. Temporary build outputs belong to a unique run-owned directory. Preflight available disk and omit unsuccessful optional preparation without omitting the authoritative test command. Remove only owned artifacts after their users stop.
- On cancellation, stop admission, signal all owned groups, use bounded joins/KILL escalation, then merge completed or partial reports exactly once. Cleanup must fit inside the existing outer kill-after allowance; no report processing can wait on an active writer indefinitely.
- Reuse transactional report publication and its completion marker. Never rename the main report while a foreground writer still holds it open; preserve sources on publication failure.
- Ordinary test failure retains a nonzero suite result while remaining selected tests execute. Report/selection failure cannot become a green suite. No cancelled, truncated, zero-selection or surviving-process run counts as completed validation.
- A new invocation uses a new artifact namespace; it cannot consume prior-run binaries, state or partial reports.

## Validation and acceptance

| Risk | Required evidence |
|---|---|
| Stale/wrong native generation | Existing native provenance/build-contract checks; prepared, unprepared, invalid-stamp and direct-script harness paths; one preparation on a valid `make ut` path |
| Missing/duplicate tests or changed execution semantics | Prebuild success/failure/off each dispatches the same full-scope authoritative command exactly once; unchanged flags/native identity and package/test selection; runtime failures are nonzero and never retried |
| Slot leakage/head-of-line blocking | Deterministic blocked-first/finished-second harness; slot cap; failed build and report-open failure; identical complete package selection |
| Orphan children or lost reports | TERM at prebuild launch, ownership publication, build and join; report-open failure and children drained before cleanup; existing foreground-command cancellation/publication contracts |
| Readiness cancellation regression | Focused injected-client tests and relevant race validation; existing startup/rollback ownership tests |
| False holder attribution | Explicit release, unambiguous package exit, restarts, reused identities, multiple shard processes, missing terminal events and malformed/truncated JSON |
| Resource or runtime regression | Complete same-mode Linux race suite, stage/build timing, disk use, cgroup current/peak and OOM events; record runner, resolved workflow revision and cache conditions |

Keep source-identical existing tests and data. Compare full runtime identities and occurrence/terminal outcomes, not only pass totals. Require no dropped cases, duplicates, new skips, unfinished work or mode changes. The separate coverage workflow remains authoritative for statement coverage; compare normalized covered-block sets with matching instrumentation and investigate any loss. Add infrastructure regression tests without substituting them for existing behavioral coverage.

Reuse semantically valid evidence. A wrapper failure before tests is an environment prerequisite failure, not a test failure/pass or runtime sample. E1/E2 are not full-runtime controls. Compare the candidate against comparable complete baseline evidence and distinguish preparation, linking, process startup, holder work and queue waiting; never add nested case durations or overlapping waits into claimed savings.

Native duplicate-preparation removal must demonstrate the eliminated invocation and its measured cost. Cancellation/accounting improvements have correctness acceptance independent of speed. Optional prebuild repairs may merge while disabled, but promotion requires measured whole-suite wall-time improvement, preserved coverage and no resource regression on the constrained runner. For promotion retain the existing design's complete-run <=60m and verified peak <=90% of each applicable finite cgroup limit, with no OOM events. Report >=15% improvement as verified only with comparable complete evidence, consistent with the prior design; otherwise report preliminary or no demonstrated benefit. Do not change defaults to meet a timing claim.

## Deferred work and decision

Lazy online branch stage creation is **not authorized by this design**. It changes production execution/cleanup behavior and has not been shown to explain the expensive apply interval. Any proposal requires statement-level baseline/profile evidence, transaction/partial-DDL/cancellation cleanup proof, a separate product design decision and relevant public-path validation. Do not duplicate already-landed staged updates. Optional embedded direct-binary consumption and its custom watchdog are also deferred by revision 4; implement build-only preparation plus the original authoritative `go test` path instead.

Also deferred: shrinking boundary data, altering fixture lifetimes, raising parallelism, removing admission, cross-process fixture sharing, changing engine/plan waves, or extending timeout to claim improvement. Profiling may identify a later case or product correction; record it as a separate evidence-backed scope decision.

Decision: proceed with the four bounded infrastructure units above, preserving defaults and all existing case data/oracles. This approves implementation scope, not a claim that the historical whole-suite timeout is solved. Final implementation review must reconcile the complete diff, cancellation/resource evidence and measured results before delivery.

## Separate product design

The workspace-delete namespace guard is a separate product commit/PR, governed by `docs/design/WORKSPACE_DELETE_SCAN_GUARD.md` (product design revision 2/A3, with a measured sparse-workspace validation gate). This sibling document is delivered with the product PR, not required in the infrastructure commit. Add an external product PR/design link when assigned; absence of the sibling on an infrastructure-only branch is intentional. No product implementation approval is implied by this infrastructure design.

## Local delivery evidence

macOS/arm64, Go 1.26.4: full `optools` passed (50.666s), full `embed` normal/race passed (41.787s/61.698s); exact readiness-helper race stress passed 100 repetitions. Build-only authoritative-execution and cancellation contracts passed race repetitions 8/20 respectively. Native build/provenance contracts, shell syntax, vet and diff checks passed.

After committing clean native inputs and running `make -j8 cgo`, the real prepared-artifact path passed provenance verification in 0.51s without invoking make. The redundant incremental `make cgo` control took 4.46s. These are local preparation measurements, not a Linux whole-suite estimate. Source/review evidence is reusable across this documentation-only addition.
