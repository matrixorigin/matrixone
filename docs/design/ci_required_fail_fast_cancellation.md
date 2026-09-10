# CI Required fail-fast cancellation

## Scope

MatrixOne PR CI deliberately runs independent jobs in parallel. Once a job that
feeds the `CI Required` gate has reached a terminal failure, that workflow run
cannot become green, but unrelated required jobs may continue occupying runners
for tens of minutes.

Add a repository-owned watchdog that runs every five minutes, inspects active
`MatrixOne ALL CI` pull-request workflows, and cancels a run after observing a
terminal failure in a job that contributes to `CI Required`. This changes CI
control-plane behavior only; it does not change MatrixOne product or test
semantics.

## Contract

The watchdog must satisfy these invariants:

1. It considers only in-progress runs of `.github/workflows/entrypoint.yaml`
   triggered by `pull_request_target`.
2. It cancels only after the Actions API reports a terminal unsuccessful
   conclusion for an explicitly authorized job in the union of the triggering
   revision's `CI Required` dependency closures.
3. A failure in Shared Build, Upgrade, Utils, CodeQL, TKE, labels, or another
   non-gating workflow never triggers cancellation.
4. It does not treat `skipped` as failure. Jobs omitted by the `docs`, `ut`, or
   `bvt` scope are intentionally skipped, and the watchdog does not need to
   recompute the scope to distinguish them.
5. It skips `3.0-dev`, where the `CI Required` gate is disabled. The triggering
   workflow writes immutable PR, base ref, base SHA, head SHA and cancellation
   policy version into a machine-readable run name. Missing, malformed or
   unsupported metadata grants no cancellation authority.
6. A decision is bound to `run_id + run_attempt`. Every inspected job and the
   final workflow status must still belong to that attempt. A completed run or
   changed attempt is left unchanged; a cancellation conflict is reported as a
   race rather than retried.
7. One scan failure must be visible. The workflow reports API/association errors
   and fails after safely processing other runs; it never converts incomplete
   information into a cancellation decision.

Cancellation is expected to make `CI Required` fail or cancel. That result is
already inevitable once a required dependency has failed. A new PR commit has a
new run ID. Manual reruns retain a run ID but increment `run_attempt`, so the
watchdog must reject a decision made from an older attempt.

## Required-job ownership

The scope-to-caller contract remains owned by
`.github/ci/change-scope.cjs`. The watchdog derives the union of logical
required jobs from that module, then authorizes exact Actions display names in
policy version `v1`:

| Logical dependency | Actions job match |
| --- | --- |
| PR validation | exact `CHECK PR VALID` |
| scope classification | exact `Classify CI changes` |
| documentation check | exact `Documentation whitespace check` |
| BVT planning | exact `Plan complementary BVT groups` |
| MatrixOne UT/SCA caller | exact Ubuntu UT and Linux/arm64 SCA job names |
| UT coverage caller | exact eligibility and Ubuntu coverage job names |
| Compose BVT caller | exact active Compose + Proxy BVT job name |
| Standalone BVT caller | exact active Compose + Pessimistic BVT job name |
| merged coverage caller | exact merged Coverage job name |

Reusable-workflow children include the caller prefix in the Actions jobs API,
but a prefix alone grants no authority. Disabled, future and unknown children
are ignored. Each authorized external job is also bound to its referenced
workflow path and an explicitly supported immutable `referenced_workflows.sha`
reported by the workflow-run API. A new MatrixOrigin/CI revision therefore
pauses cancellation for that caller until its propagation behavior and names
are reviewed and added to the policy. It cannot silently inherit authority by
retaining a prefix.

For local jobs, the watchdog compares the `entrypoint.yaml` and
`change-scope.cjs` Git blob identities at the run's immutable base SHA with the
current default-branch policy blobs. An old or changed gate is skipped. This
lets ordinary product commits share the policy while preventing a current name
mapping from being applied to a run created by a different gate revision.

Terminal conclusions that make a required caller irrecoverable are `failure`,
`cancelled`, `timed_out`, `action_required`, `startup_failure`, and `stale`.
`success`, `neutral`, and `skipped` do not trigger the watchdog. A workflow
already being cancelled may be observed once; the final status re-check and
conflict handling make that harmless.

## Execution and trust boundary

The scheduled workflow runs from the default branch every five minutes and may
also be dispatched manually in dry-run mode. It checks out only `.github/ci`
from the trusted default-branch revision with persisted credentials disabled.
No pull-request code is loaded or executed. The PR entrypoint uses a strict run
name of the form:

```text
CI_REQUIRED/v1 pr=<number> base=<ref> base_sha=<40-hex> head_sha=<40-hex>
```

The watchdog requires an exact parse, checks that `head_sha` equals the run API
field, rejects `3.0-dev`, and uses `base_sha` for the local policy-blob check.
Because these values are evaluated by the trusted `pull_request_target`
workflow at trigger time, later PR retargeting cannot change the decision.

The token permissions are limited to `contents: read`, `pull-requests: read`,
and `actions: write`. The last permission is needed only for
`cancelWorkflowRun`. The script cannot push commits, edit PRs, or mutate issues.

Existing runs without `CI_REQUIRED/v1` metadata are intentionally not
cancelled. This is a rollout boundary: the watchdog does not reconstruct a
trigger-time base from the PR's current base, because retargeting can preserve
the head SHA. The recorded PR number is diagnostic; it is not used to replace
the immutable base metadata.

## Unhappy paths and bounds

- API pagination covers all currently in-progress entrypoint runs and their
  latest-attempt jobs. The number of in-progress runs is bounded by active
  repository workflow executions and runner capacity; completed history is not
  scanned.
- Runs are inspected sequentially to bound mutation rate and avoid cancellation
  bursts. The scheduled workflow has a single non-preempting concurrency group,
  so scans cannot overlap.
- A jobs-list or policy-validation API error records the run as an error and
  skips cancellation. A final nonzero result exposes degraded watchdog
  operation.
- Jobs are queried with `filter: latest`, filtered again by their
  `run_attempt`, and followed by a final `getWorkflowRun` comparison of status
  and attempt. GitHub's cancel endpoint accepts only `run_id`, not an attempt
  precondition. A new attempt beginning in the final request interval remains
  a platform-level residual race; the script neither retries a conflict nor
  claims atomic compare-and-cancel. GitHub normally requires the prior run to
  complete before a rerun, making the checked state transition the expected
  protection rather than a timing assumption.
- GitHub schedule delays increase wasted runner time but cannot create a false
  cancellation. The expected detection delay is zero to roughly five minutes,
  plus scheduler queueing.
- The watchdog does not detect a merely slow or hung job before GitHub gives it
  a terminal failure. Existing job timeouts remain the authority for hangs.

## Alternatives

Adding `fail-fast` inside each reusable workflow cannot cancel sibling reusable
workflow calls owned by the parent entrypoint. Wiring every job through a long
`needs` chain would serialize independent tests and increase healthy latency.
Duplicating an `if: success()` dependency graph in the entrypoint would couple
all suites and make the coverage/fallback graph harder to reason about.

A periodic repository-owned controller preserves healthy parallelism and acts
only after the aggregate result is already irrecoverable. A five-minute cadence
uses GitHub's minimum practical schedule interval and avoids a persistent
external service.

## Validation and rollout

Pure Node tests cover required and ignored exact job names, every terminal
conclusion, scope-related skips, malformed/old/retargeted run metadata, local
and referenced-workflow policy drift, pagination with attempt changes,
completion/cancellation races, API failure reporting, dry-run behavior, and the
workflow's permissions/schedule/trusted checkout. The live cancellation API is
not invoked from the feature branch.

After merge, first run the workflow manually with dry-run enabled and inspect
its summary. Scheduled runs then enforce cancellation automatically. Actions
audit history identifies the watchdog run and the failed required job that
caused each cancellation.
