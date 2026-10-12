# SDK CI

The trusted repository PR entrypoint calls the reusable root workflow
`.github/workflows/python-sdk.yml`. It is the sole automatic SDK PR owner on
supported target branches once deployed there. Python-only PRs (plus ordinary
docs) skip kernel build, UT, coverage, BVT, upgrade and utils jobs, while running the installed-package offline suite and wheel/sdist
validation on Python 3.10/3.12 with SQLAlchemy 1.4/2.0 and Python 3.11/3.12
with SQLAlchemy 2.1 (which requires Python >=3.11). Installing the package
registers its dialect entry points; installing SQLAlchemy's asyncio extra supplies
the async runtime. The Python 3.12/SQLAlchemy 2.0 cell also checks fatal Python
errors with flake8 (`E9,F63,F7,F82`); this is not a full style/type-check gate.
Python 3.8/3.9 qualification and optional-extra contract cleanup
remain separately tracked; this matrix does not certify those runtimes.

The online job runs only when an operator invokes workflow_dispatch with `online`
enabled. It requires an isolated self-hosted runner labelled `matrixone-sdk-test`
and the `matrixone-sdk-integration` environment. Configure MATRIXONE_HOST, PORT,
USER and DATABASE as environment variables and MATRIXONE_PASSWORD as an environment
secret. For analytics, also configure the second CN and local S3 endpoint, plus
client/server shared file paths. For transport tests, configure MO_TLS_CA,
MO_TLS_PORT and MO_PLAIN_PORT. Existing online tests may require additional fixture
services/settings; their failures are surfaced rather than suppressed. Connection
selection uses the shared `OnlineTestConfig`; fixture-owned databases retain their
overrides while using the configured host and credentials. Pure configuration
validation runs offline and restores environment overrides before the next case.

JUnit results report failures, errors and skips. A run with skips must not be
reported as full online certification. No runner/environment/credentials have been
provisioned by this change, and no production deployment is selected implicitly.

Mixed SDK/kernel changes still run kernel CI and the SDK matrix. The existing
`CI Required` aggregate enforces the SDK call's result, including failures,
cancellations and missing results. SDK-only skips are intentional and do not
provide kernel coverage or online SDK certification.

Offline PR jobs use immutable head-repository/SHA checkout without persisted
credentials, receive no inherited secrets, run only on hosted runners, and have
a 30-minute timeout. The explicit fork-head checkout opt-in is limited to this
read-only `pull_request_target` job. Online credentials remain isolated behind
manual dispatch. Fork-aware SDK concurrency does not cancel unrelated PRs or the
caller; artifacts distinguish source SHA, run, attempt and matrix cell.

Push checks on main/develop and manual dispatch remain supported. Dependency
stacks targeting branches outside the PR entrypoint's branch filters do not
exercise the automatic aggregate; verify live SDK-only routing after rollout.
