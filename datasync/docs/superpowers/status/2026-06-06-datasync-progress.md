# Datasync Progress

Date: 2026-06-06

## Requirement Summary

The tool must:

- Export data from multiple MatrixOne tenants.
- Support multiple databases per source tenant.
- Export each table separately using `mo-dump -csv`.
- Read a config file that selects tenants/databases and excludes specific tables.
- Produce per-table results including row counts, timing, attempts, status, and errors.
- Retry failed export/import operations.
- Keep export/import idempotent.
- Import into a target MatrixOne database with configurable source-to-target database mapping.
- Use overwrite import semantics.
- Use `mysql source` for imports.

## Design Decisions

- Implement as a standalone Go CLI in this repository.
- Call the existing `mo-dump` binary as an external process.
- Keep `mo-dump` unchanged.
- Use YAML config.
- Use one table task directory per exported table.
- Let `datasync` create and select target databases because table-level `mo-dump -csv -tbl` SQL does not include `CREATE DATABASE` or `USE`.
- Rely on generated table SQL containing `DROP TABLE IF EXISTS` for overwrite import idempotency.

## Local MatrixOne Findings

Validated against MatrixOne on `127.0.0.1:6001`:

- Tenant login format `account:user` works, for example `dsync_src_a:admin`.
- `mo-dump -csv -db <db> -tbl <table>` generates a table-level SQL file and a CSV file.
- The generated SQL includes `DROP TABLE IF EXISTS`, `CREATE TABLE`, and `LOAD DATA LOCAL INFILE`.
- The generated SQL contains an absolute CSV path.
- Re-running the generated SQL with `mysql --local-infile=1 ... source <table>.sql` overwrites the table and preserves idempotent row counts.

## Completed Code

Completed in the current workspace:

- CLI skeleton:
  - `cmd/datasync/main.go`
  - `README.md`
- CLI mode selection:
  - `-mode sync` runs export followed by import and is the default.
  - `-mode export` runs export only and marks import status as skipped.
  - `-mode import` skips export and imports SQL/CSV files from the existing run directory selected by `-run-id`.
- Config loading and validation:
  - `internal/config/config.go`
  - `internal/config/config_test.go`
  - `configs/example.yaml`
- Duplicate YAML keys are rejected for custom top-level and `retry` mappings.
- MatrixOne database client:
  - `internal/db/db.go`
  - `internal/db/db_test.go`
- Table task planner:
  - `internal/plan/plan.go`
  - `internal/plan/plan_test.go`
- Retry helper:
  - `internal/retry/retry.go`
  - `internal/retry/retry_test.go`
- Report writer:
  - `internal/report/report.go`
  - `internal/report/report_test.go`
  - Per-table reports include exported CSV data file size in bytes.
- Runner:
  - `internal/run/runner.go`
  - `internal/run/runner_test.go`
- App orchestration:
  - `internal/app/app.go`
  - `internal/app/app_test.go`
- Opt-in local MatrixOne integration test:
  - `tests/integration/datasync_local_test.go`

## Current Verification

Passing on local `main`:

```bash
rtk go test ./...
rtk go vet ./...
rtk go mod tidy -diff
rtk proxy env DATASYNC_INTEGRATION=1 go test ./tests/integration -v
rtk go run ./cmd/datasync -version
```

Current test count reported by `rtk go test ./...`: 61 tests across 9 packages. The integration test is skipped unless `DATASYNC_INTEGRATION=1` is set.

## Not Yet Implemented

The following planned tasks are not complete:

- Import-only resume from an arbitrary report file outside the configured `output_dir`.
- Final verification and polish.

Stashes:

- Historical stashes still exist:
  - `stash@{0}`: `task4-plan-wip-stopped-before-merge`
  - `stash@{1}`: `task3-db-client-wip-before-task2-review`
- The planner code itself has now been merged into `main`; the Task 4 stash is retained only as historical WIP unless explicitly cleaned up.

## Branch State

- Current branch: `main`.
- Current `main` HEAD: `0a662ea merge datasync planner progress`.
- Branch `datasync-implementation` has no commits ahead of `main`.
- The linked worktree `.worktrees/datasync-implementation` has been removed.
- Requested stop point has been applied: current progress is merged to local `main`, and active implementation tasks are stopped.
