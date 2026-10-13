# SDK CI

Repository Actions discovers `.github/workflows/python-sdk.yml` at the repository
root. Python-only PRs run the installed-package offline suite and wheel/sdist
validation on Python 3.10/3.12 and SQLAlchemy 1.4/2.0/2.1. Installing the package
registers its dialect entry points; installing SQLAlchemy's asyncio extra supplies
the async runtime. Python 3.8/3.9 qualification and optional-extra contract cleanup
remain separately tracked; this matrix does not certify those runtimes.

The online job runs only when an operator invokes workflow_dispatch with `online`
enabled. It requires an isolated self-hosted runner labelled `matrixone-sdk-test`
and the `matrixone-sdk-integration` environment. Configure MATRIXONE_HOST, PORT,
USER and DATABASE as environment variables and MATRIXONE_PASSWORD as an environment
secret. For analytics, also configure the second CN and local S3 endpoint, plus
client/server shared file paths. For transport tests, configure MO_TLS_CA,
MO_TLS_PORT and MO_PLAIN_PORT. Existing online tests may require additional fixture
services/settings; their failures are surfaced rather than suppressed.

JUnit results report failures, errors and skips. A run with skips must not be
reported as full online certification. No runner/environment/credentials have been
provisioned by this change, and no production deployment is selected implicitly.
