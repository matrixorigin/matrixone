# Pull request CI scope

The PR entrypoint uses one `CI preflight` job to validate PR metadata, classify
the complete file list and, where needed, assign complementary BVT groups before
scheduling expensive jobs. Full/BVT runs use one preparation runner allocation
instead of three; UT/docs runs use one instead of two. Tests still run as complete
suites; this does not shard or select individual test cases.

| Changed files | Scope | Work performed |
| --- | --- | --- |
| Root README/README_CN/CONTRIBUTING/CHANGELOG Markdown, or Markdown/images under `docs/` | docs | Documentation whitespace check and PR metadata checks |
| Only `pkg/**/*_test.go`, optionally with the documentation above | ut | Existing race UT and static analysis |
| Only `.test`/`.sql`/`.result` under `test/distributed/cases/`, optionally with documentation | bvt | Shared build and both complementary BVT jobs |
| Everything else, including mixed UT+BVT changes | full | Existing complete CI pipeline, including merged coverage |

Go helpers, test fixtures, build scripts, dependencies, runtime configuration and
workflow changes deliberately retain full CI. Deleted paths are classified too;
renames consider both names. Missing/incomplete/capped file lists fall back to
full CI. A PR head or base that moves during classification fails that old run.
Set the repository Actions variable `CI_FORCE_FULL=true` and rerun CI to disable
all scope exemptions.

The classifier and required-check gates execute code checked out from the trusted
workflow SHA, never the PR head. The documentation job reads the PR diff without
running PR scripts. The separate scope contract workflow runs without secrets
and tests changes to the classifier before they reach the trusted base.

One `CI Required` summary job checks the existing required prerequisites for the
selected scope, including classification and PR validation. A skipped,
cancelled, failed or absent required job is not a pass. Its summary explicitly
states the scope and intentional omissions. Actual test results remain visible
under the reusable workflow calls named `... execution`. This replaces six
identical summary jobs with one runner allocation and gives the aggregate result
a distinct name rather than labeling it as an individual test's result.

Preflight publishes the original `pr_valid` and `scope` outputs plus both BVT
groups and their run/attempt generation. All BVT and coverage consumers take the
same generation from preflight. The required verifier rejects missing or
non-complementary groups. Invalid metadata and classification failures prevent
test producers from starting; the required check still fails. The 3.0-dev path
keeps metadata validation and its release workflows, and skips modern scope
classification and BVT planning.

The independent UT coverage and merged UT+BVT coverage jobs run only for full
runs. Scoped runs explicitly report coverage as not evaluated; they do not
fabricate missing profiles or reuse profiles from a different commit. BVT's
existing instrumented build and incidental profiles are unchanged. Removing
tests can still reduce coverage, so test-only exemptions do not establish that
coverage has stayed constant. Use a full run when reviewing that risk.
Existing 3.0-dev routing is unchanged.

Branch protection must require `CI Required` from GitHub Actions in place of the
six former aggregate check names. Preserve the existing strict/up-to-date setting
and unrelated protection rules. Because the entrypoint uses `pull_request_target`,
the new check appears on runs triggered after this change lands in the target
branch. Verify a new run emits `CI Required`, then switch protection; until the
switch, the old required names can remain pending. Old workflow runs do not emit
the new name and need a new PR event after rollout. No CI-repository change is
required. Test selection and prerequisite validation are unchanged.

## Cancellation and superseded runs

The entrypoint cancels older runs in the same head-repository/branch concurrency
group when a new run arrives. `CI Required` deliberately retains `always()`:
failed or cancelled prerequisites must reach the strict verifier instead of
turning the only required check into a successful skip. Its five-minute job
timeout bounds execution, not runner queue time.

An `always()` summary can survive ordinary cancellation and hold the concurrency
group while queued for a runner. Run 34575985362 demonstrated this: producers
were cancelled, its summary stayed queued, and successor 34578155424 remained
pending with zero jobs. `Release superseded CI runs` runs independently on the
`MatrixOne ALL CI` workflow's `requested` event, outside that concurrency group.
It identifies older active runs by workflow ID, event, head repository ID and
branch, requests normal cancellation, and polls for up to two minutes so process
cleanup and bounded diagnostic uploads can finish. Only remaining eligible runs
receive GitHub's force-cancel API, which bypasses `always()`. All candidates share
one grace period. No candidates means no polling delay.

The controller checks run attempts and start times again before mutation so a
delayed event does not target a newer rerun. It stops if its successor finishes
or changes attempts. A 409 from normal cancellation triggers a state recheck and
the same grace period if the target is still eligible; permission and transport
errors fail visibly. API acceptance is not a guarantee that runner cleanup has
finished, and check/cancel requests are not an atomic transaction.

This controller uses only its default-branch workflow revision and read-only
checkout credentials; `actions: write` is limited to the independent workflow.
It does not execute PR code, read artifacts, or publish successful check results.
The strict verifier and branch-protection requirements remain intact. A cancelled
current run still needs a successful rerun before it is eligible to merge.

GitHub emits `requested` only for new runs, not reruns. After this workflow is on
the default branch, an operator can recover a pending rerun or a run predating
deployment through `workflow_dispatch` on the default branch with
`successor_run_id` set to the newer waiting MatrixOne ALL CI run. That path applies
the same identity, ordering and attempt checks; it is not an arbitrary run killer.
A manually cancelled run with no successor is outside automatic recovery scope.

The independent controller still needs a control runner and GitHub API
availability. Its six-minute execution timeout and two-minute grace do not bound
runner queue delay. Platform scheduling and forced-cancellation propagation need
a post-merge test with an older cancelled run and a newer pending run; local
tests exercise mocked API state transitions, not GitHub's scheduler.

## Lightweight control runner pool

Preflight, `CI Required` and the cancellation controller share the repository
variable `CI_CONTROL_RUNNER_LABEL`. If unset or empty, it resolves to
`ubuntu-latest`. This PR does not provision a runner or change repository variables.
Setting a nonexistent label will queue jobs indefinitely; there is no automatic
fallback after a configured pool stops serving work.

An operator should first provision and verify a separate, repository-accessible
Linux pool with a unique label, Git, Bash, and support for the checked-in checkout
and github-script action runtimes. Set `CI_CONTROL_RUNNER_LABEL` to that label
only after a smoke job is picked up. Use ephemeral isolated workers; this pool
handles trusted control code, including metadata-validation credentials and the
cancellation controller's Actions write token. Do not reuse a persistent worker
that executes untrusted PR tests. UT/BVT pools and the PR documentation-diff job
do not consume this variable.

Roll out by checking runner assignments for preflight, the summary and the
cancellation controller, then compare queue times. Clear the variable to restore
the hosted default. Separate capacity avoids contention with test jobs but still
requires its own availability; merging preparation jobs alone saves allocations,
not a guaranteed number of minutes. Run 34571127635, where producers had already
failed and only the summary awaited a hosted runner, is a capacity case rather
than a superseded-run cancellation case.

References: [workflow cancellation](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-cancellation),
[force cancellation](https://docs.github.com/en/rest/actions/workflow-runs#force-cancel-a-workflow-run),
[workflow_run events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run).

Local contract validation: `node --test .github/ci/*.test.cjs`.
