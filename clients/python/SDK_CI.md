# SDK CI

The trusted repository PR entrypoint calls the reusable root workflow
`.github/workflows/python-sdk.yml`. It is the sole automatic SDK PR owner on
supported target branches once deployed there. Python-only PRs and edits to that
workflow (plus ordinary docs) skip kernel build, UT, coverage, BVT, upgrade and
utils jobs. Mixed SDK/kernel changes still run kernel CI and the SDK matrix.
The existing `CI Required` aggregate enforces the SDK call's result, including
failures, cancellations and missing results. SDK-only skips do not provide kernel
coverage or online SDK certification.

The supported SQLAlchemy minimum is **1.4.54**. An exact 1.4.54 cell
runs the full offline contracts and asserts the installed version; the Python
3.8 installed-core cell also pins that minimum. SQLAlchemy 1.4.24 fails verified
escaped expanding-bind and cancelled-connection cleanup contracts.

Offline contracts run on Python 3.10/3.12 with SQLAlchemy 1.4/2.0 and Python
3.11/3.12 with SQLAlchemy 2.1 (which requires Python >=3.11). An additional Python
3.12 cell fixes SQLAlchemy at 2.0.43 with PyMySQL 1.2.3 and aiomysql 0.3.2
for pre-ping and binary binding, asserting all three distribution versions. Each cell runs
`scripts/run_contracts.py`, which records source/dependency provenance and rejects
failures, errors, skips, empty selections and source changes during execution.
Analytics offline checks run separately; their online-only skips do not weaken
the required offline contracts. Each cell also builds and validates wheel/sdist
artifacts. The Python 3.12/SQLAlchemy 2.0 range cell checks fatal Python errors
with flake8 (`E9,F63,F7,F82`); this is not a full style/type-check gate.

Python 3.8/3.9 have separate installed-core smoke checks: install the package,
then import synchronous/asynchronous clients from outside the source directory
without optional analytics dependencies. These checks do not certify the full
offline suite or online behavior on those runtimes. Installing the package
registers its dialect entry points; SQLAlchemy's asyncio extra supplies the async
runtime.

The online job runs only when an operator invokes `workflow_dispatch` with
`online` enabled. It requires an isolated self-hosted runner labelled
`matrixone-sdk-test` and both explicitly configured target environments:

| Environment | Target identity |
| --- | --- |
| `matrixone-sdk-4.2.5` | Released server; `MO_SDK_EXPECT_VERSION=4.2.5` |
| `matrixone-sdk-current-source` | Source build; set its expected version and exact build revision |

Configure each environment independently:

- Variables `MATRIXONE_HOST`, `MATRIXONE_PORT`, `MATRIXONE_USER` and
  `MATRIXONE_DATABASE`, plus the `MATRIXONE_PASSWORD` secret. The workflow maps
  these to the matching `MO_HOST`, `MO_PORT`, `MO_USER`, `MO_DATABASE` and
  `MO_PASSWORD` aliases for the contract runner.
- Variable `MO_SDK_EXPECT_VERSION` for both targets and
  `MO_SDK_EXPECT_REVISION` for the current-source target. The runner requires
  these identities and the contracts verify the connected server, rather than
  trusting the matrix name.
- Variables `MO_SDK_EXPORT_DIR` and `MO_SDK_EXPORT_LOCAL_DIR`: respectively the
  server and runner views of the same shared export directory. Configure both
  explicitly for the export round-trip contracts.
- For extended analytics checks, configure `MO_SECOND_CN_PORT` and
  `MO_ANALYTICS_S3_ENDPOINT` plus the required fixture services. For transport
  tests, configure `MO_TLS_CA`, `MO_TLS_PORT` and `MO_PLAIN_PORT`.

`run_contracts.py` owns connection, identity and export configuration validation;
missing configuration fails with the required variable names. The workflow checks
`MATRIXONE_DATABASE` separately because the extended suite requires it. Required
online contracts must run without skips; the extended integration/analytics suite
runs separately and reports its skips. Artifacts include the required contracts'
JUnit and provenance plus extended-suite JUnit, named by server target. A run with
extended-suite skips must not be reported as complete online certification.
Connection selection uses the shared `OnlineTestConfig`; fixture-owned databases
retain their overrides while using the configured host and credentials.

This change provisions no runner, environment, server or credentials. Until both
targets are configured and successfully dispatched, the online matrix is an
execution capability, not evidence that either deployment has passed.

Offline PR jobs use immutable head-repository/SHA checkout without persisted
credentials, receive no inherited secrets, run only on hosted runners, and have
bounded timeouts. The explicit fork-head checkout opt-in is limited to these
read-only `pull_request_target` jobs. Online credentials remain isolated behind
manual dispatch. Fork-aware SDK concurrency does not cancel unrelated PRs or the
caller; artifacts distinguish source SHA, run, attempt and matrix cell.

Push checks on main/develop and manual dispatch remain supported. Dependency
stacks targeting branches outside the PR entrypoint's branch filters do not
exercise the automatic aggregate; verify live SDK-only routing after rollout.

The separate Iceberg module-tidy workflow uses GitHub's native path filter to
omit SDK-only and docs-only changes while retaining Go production/test and build
inputs. This is a best-effort optimization, separate from the SDK classifier's
conservative inventory fallback: GitHub's native diff limits can omit matching
files on very large PRs. See the [GitHub path-filter limits](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#git-diff-comparisons).
