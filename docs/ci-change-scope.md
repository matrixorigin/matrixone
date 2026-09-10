# Pull request CI scope

The PR entrypoint classifies the complete file list before scheduling expensive
jobs. Tests still run as complete suites; this does not shard or select individual
test cases.

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
PR base SHA, never the PR head. The documentation job reads the PR diff without
running PR scripts. The separate scope contract workflow runs without secrets
and tests changes to the classifier before they reach the trusted base.

One `CI Required` summary job checks the existing required prerequisites for the
selected scope, including classification and PR validation. A skipped,
cancelled, failed or absent required job is not a pass. Its summary explicitly
states the scope and intentional omissions. Actual test results remain visible
under the reusable workflow calls named `... execution`. This replaces six
identical summary jobs with one runner allocation and gives the aggregate result
a distinct name rather than labeling it as an individual test's result.

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
required. Test selection, prerequisite validation and cancellation behavior are
unchanged.

Local contract validation: `node --test .github/ci/change-scope.test.cjs`.
