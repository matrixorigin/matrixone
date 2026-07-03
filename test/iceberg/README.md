# MatrixOne Iceberg external test runbook

This directory contains scenario templates and environment templates for the
remaining external Iceberg tests:

- `ICE-TEST-124`: credential vending
- `ICE-TEST-130`: public dataset read-only checks
- `ICE-TEST-131..134`: Tier C sandbox catalog checks
- `ICE-TEST-135`: NESR demo checks
- `ICE-TEST-156..159`: real-file golden checks

The templates are intentionally not marked complete in the test plan. A test is
complete only after a real environment runs the profile and uploads a redacted
report artifact.

## Files

| file | purpose |
| --- | --- |
| `external_profiles.env.example` | Fillable environment template for all external profiles. |
| `credential_vending_scenarios.example.json` | Credential vending read/refresh and expired credential fail-fast scenarios. |
| `tier_b_public_dataset_scenarios.example.json` | Public dataset count/filter/projection scenarios. |
| `nyc_tlc_perf_queries.example.sql` | Optional NYC TLC performance query set for local benchmark runs. |
| `tier_c_sandbox_scenarios.example.json` | Polaris/Open Catalog/Gravitino REST, remote signing, server planning and capability scenarios. |
| `tier_d_nesr_scenarios.example.json` | NESR publish/KPI/residency scenarios. |
| `golden_real_scenarios.example.json` | Real-file timestamp, bucket/truncate, field-id and row ordinal golden scenarios. |

## Local structural gates

Run these without external credentials:

```bash
make test-iceberg-e2e-local
make test-iceberg-external-templates
make test-iceberg-readiness
```

`test-iceberg-e2e-local` is the PR-facing Iceberg correctness smoke. It builds
`mo-service`, starts local Nessie + MinIO, seeds tiny deterministic Iceberg
tables without Spark, starts a single-CN MatrixOne, and verifies catalog DDL,
mapping DDL, namespace/table discovery, append read/time travel, partition
filtering, append write, and merge-on-read DELETE apply. It does not require
GitHub secrets and does not contact cloud object storage. The job is initially
advisory/non-blocking; after enough stable PR runs it can become a required
check.

`test-iceberg-external-templates` validates JSON structure, external runner
compatibility, and the golden-real cross-engine scenario schema. It also checks
that the golden-real template covers `ICE-TEST-156..159`. The same gate runs a
local external-runner selftest that exercises success comparison,
`expect_error_contains`, report artifact validation, coverage validation, and
credential/object-path redaction.
`test-iceberg-readiness` writes:

- `external_readiness.md`
- `external_readiness.json`

Profiles marked `not ready` must remain unchecked in the test plan.

## Running a real profile

The profiles below are test-team-owned external matrices. They are intentionally
not part of the development PR workflow. Use them for Spark/Trino/public dataset/cloud
credential coverage, not for the lightweight PR E2E gate.

1. Copy the environment template to a private file:

   ```bash
   cp test/iceberg/external_profiles.env.example /tmp/iceberg_external_profiles.env
   ```

2. Fill only the profile you are about to run. Do not commit the filled file.

3. Load it:

   ```bash
   set -a
   source /tmp/iceberg_external_profiles.env
   set +a
   ```

4. Run preflight:

   ```bash
   MO_ICEBERG_CI_PROFILE=<profile> make test-iceberg-preflight
   ```

5. Run the profile:

   ```bash
   MO_ICEBERG_CI_PROFILE=<profile> make test-iceberg-nightly
   ```

6. Verify artifact structure and redaction when rerunning manually:

   ```bash
   make test-iceberg-artifact
   ```

7. Verify that the artifacts cover the expected `ICE-TEST` ids when rerunning manually:

   ```bash
   MO_ICEBERG_CI_PROFILE=<profile> make test-iceberg-external-coverage
   ```

   For a full release gate, set:

   ```bash
   MO_ICEBERG_CI_PROFILE=nightly make test-iceberg-external-coverage
   ```

## Profiles

| profile | tests | command |
| --- | --- | --- |
| `credential-vending` | `ICE-TEST-124` | `MO_ICEBERG_CI_PROFILE=credential-vending make test-iceberg-nightly` |
| `tier-b` | `ICE-TEST-130` | `MO_ICEBERG_CI_PROFILE=tier-b make test-iceberg-nightly` |
| `tier-c` | `ICE-TEST-131..134` | `MO_ICEBERG_CI_PROFILE=tier-c make test-iceberg-nightly` |
| `tier-d` | `ICE-TEST-135` | `MO_ICEBERG_CI_PROFILE=tier-d make test-iceberg-nightly` |
| `golden-real` | `ICE-TEST-156..159` | `MO_ICEBERG_CI_PROFILE=golden-real make test-iceberg-golden-real` |

## NYC TLC Tier B public dataset

The local Tier B seed uses the official NYC TLC yellow taxi Parquet file:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

It downloads the file, writes an Iceberg v2 table through Spark into the local
Nessie + MinIO catalog, creates the MatrixOne external-table mapping when
`MO_ICEBERG_MO_SQL_CMD` is set, and writes a reusable env file:

```bash
make dev-seed-iceberg-tier-b-nyc-tlc
set -a
source etc/launch-minio-local/tier-b/tier_b_nyc_tlc.generated.env
set +a
MO_ICEBERG_CI_PROFILE=tier-b make test-iceberg-nightly
```

The default seed uses `MO_ICEBERG_NYC_TLC_ROW_LIMIT=200000` so the local smoke
run stays small. Set `MO_ICEBERG_NYC_TLC_ROW_LIMIT=0` before seeding to import
the full month and use `nyc_tlc_perf_queries.example.sql` as the starting point
for performance comparison. Recommended comparisons are:

- MatrixOne Iceberg external table vs Spark over the same Iceberg snapshot.
- MatrixOne Iceberg external table vs a MatrixOne native imported copy.
- Cold metadata cache vs warm metadata cache.
- One month vs multiple months as the dataset grows.

When using the MySQL client, keep the setup command and comparison command
separate so headers do not enter checksums:

```bash
MO_ICEBERG_MO_SQL_CMD='mysql -h 127.0.0.1 -P 6001 -u root -p111 -e {sql}' \
MO_ICEBERG_MO_QUERY_SQL_CMD='mysql -h 127.0.0.1 -P 6001 -u root -p111 --batch --raw --skip-column-names -e {sql}' \
make dev-seed-iceberg-tier-b-nyc-tlc
```

## Expected-error scenarios

The external runner supports `expect_error_contains`. These negative scenarios
pass only when the query fails and the output contains the expected error text.

Current templates use it for:

- expired credential fail-fast in `ICE-TEST-124`
- KSA residency denial in `ICE-TEST-135`

## Artifact requirements

Every successful scenario directory must contain:

- `metadata.json`
- `diff.json`
- `summary.md`
- one raw output file per engine, for example `mo.out`, `spark.out`, `official.out`

Artifacts are checked by `make test-iceberg-artifact`; this also scans for
unredacted credentials and signed URL material.

Coverage is checked by `make test-iceberg-external-coverage`. It writes:

- `external_artifact_coverage.md`
- `external_artifact_coverage.json`

The coverage gate fails when an expected test id has no successful artifact. By
default it derives the expected ids from `MO_ICEBERG_CI_PROFILE`; override it
with `MO_ICEBERG_EXTERNAL_EXPECTED_TESTS="ICE-TEST-124 ICE-TEST-130"` for a
targeted rerun.

`make test-iceberg-nightly` runs both artifact and coverage checks
automatically for external profiles that produce reports. The standalone targets
are useful when validating an uploaded artifact directory or rerunning a single
profile locally.
