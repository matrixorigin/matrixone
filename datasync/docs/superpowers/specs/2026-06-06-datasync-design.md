# Datasync Design

Date: 2026-06-06

## Goal

Build a standalone `datasync` CLI that exports data from multiple MatrixOne tenants and imports it into a target MatrixOne tenant or instance. The tool orchestrates `mo-dump -csv` for table-level export, uses `mysql source` for import, supports retries, and records a per-table result report.

## Confirmed Runtime Behavior

Local validation against MatrixOne on `127.0.0.1:6001` confirmed these behaviors:

- MatrixOne tenant login supports `account:user`, for example `dsync_src_a:admin`.
- `mo-dump -csv -db <db> -tbl <table>` emits a table-level SQL file that includes:
  - `SET foreign_key_checks = 0`
  - `DROP TABLE IF EXISTS <table>`
  - `CREATE TABLE <table>`
  - `LOAD DATA LOCAL INFILE '<absolute csv path>'`
  - `SET foreign_key_checks = 1`
- The table-level SQL does not include `CREATE DATABASE` or `USE <db>`.
- The CSV file is created in the process working directory.
- The `LOAD DATA LOCAL INFILE` path in the generated SQL is absolute.
- Re-running the same table SQL with `mysql --local-infile=1 ... source <table>.sql` is idempotent for overwrite imports because the SQL contains `DROP TABLE IF EXISTS`.

These facts define the implementation boundary: `mo-dump` owns table SQL and CSV generation; `datasync` owns tenant/database/table selection, target database creation, import context, retries, and reporting.

## Implementation Choice

Implement `datasync` as a Go CLI in this repository. It will call the existing `mo-dump` binary as an external process rather than modifying `mo_dump`.

Reasons:

- Go fits long-running CLI orchestration, concurrent table jobs, structured error handling, and single-binary deployment.
- The MatrixOne and MySQL ecosystem already uses Go drivers and tooling.
- Keeping `mo-dump` unchanged preserves its focused role as a table/database dump utility.
- External process orchestration lets `datasync` consume future `mo-dump` versions without copying dump logic.

## Configuration

The tool reads a YAML configuration file.

Example:

```yaml
mo_dump_path: /Users/liubo/Workspace/Projects/00-MatrixOrigin/mo_dump/mo-dump
mysql_path: /usr/local/mysql/bin/mysql
output_dir: ./runs
parallelism: 2
retry:
  max_attempts: 3
  backoff: 2s

source:
  host: 127.0.0.1
  port: 6001
  password: "111"
target:
  host: 127.0.0.1
  port: 6001
  password: "111"

databases:
  - source:
      name: tenant_a_db1
      user: dsync_src_a:admin
      database: dsync_a_db1
    target:
      name: target_a
      user: dsync_target_a:admin
      database: dsync_target_a1
    include_tables:
      - t_keep
      - t_skip
    exclude_tables:
      - t_skip
```

Rules:

- Top-level `source` and `target` entries are optional connection defaults for `name`, `host`, `port`, `user`, and `password`.
- Each `databases` entry supplies `source.database` and `target.database`, and may override any inherited source or target connection field.
- If a `databases` entry cannot be completed from its own fields plus top-level defaults, that entry is ignored.
- If `include_tables` is configured, only discovered ordinary tables listed there are candidates.
- If `include_tables` is omitted or empty, all discovered ordinary tables are candidates.
- `exclude_tables` removes matching tables from the candidate set.
- If `exclude_tables` is omitted or empty, no tables are removed.
- Passwords may be supplied directly in the config or as environment references using `${ENV_NAME}`. Missing referenced environment variables are configuration errors.

## Data Flow

The CLI accepts a config file and an explicit run mode:

- `-mode sync` is the default and performs export followed by import.
- `-mode export` performs table discovery and export only, then writes reports with import status marked as skipped.
- `-mode import` uses `-run-id` to select an existing run directory, skips `mo-dump`, verifies each task SQL/CSV file exists, and imports those files into the target databases.
- `-cleanup-export-after-import` is optional and only applies to `sync` mode; when set, each successfully imported table deletes its export directory after report metrics have been captured.

1. Load and validate configuration.
2. Create a run directory:
   `runs/<run_id>/`
3. For each configured database sync entry:
   - Connect to the source MatrixOne tenant.
   - Discover ordinary tables using MatrixOne metadata.
   - Apply `include_tables`, then remove `exclude_tables`.
   - Build one task per remaining table.
4. For each table task:
   - Count source rows with `SELECT COUNT(*) FROM <db>.<table>`.
   - Export into a stable table directory:
     `runs/<run_id>/exports/<source_name>/<source_db>/<table>/`
   - Run:
     `mo-dump -csv --local-infile=true -u <user> -p <password> -h <host> -P <port> -db <source_db> -tbl <table> > <table>.sql`
   - Verify the SQL and CSV files exist.
   - Ensure the target database exists:
     `CREATE DATABASE IF NOT EXISTS <target_db>`
   - Import using `mysql --local-infile=1`, connected to the target database, with:
     `source <table>.sql`
   - Count target rows with `SELECT COUNT(*) FROM <target_db>.<table>`.
   - Compare target rows with source rows. A mismatch is an import failure and participates in the import retry loop.
   - If `-cleanup-export-after-import` is set in `sync` mode and the table import succeeds, delete that table's export directory after capturing SQL/CSV paths and CSV file size.
   - Record task status and metrics.
5. Write reports:
   - `runs/<run_id>/report.json`
   - `runs/<run_id>/report.csv`

## Idempotency

Export idempotency:

- A table export writes into a dedicated task directory.
- On export retry, the task directory is removed and recreated before invoking `mo-dump`.
- A completed export is recognized only after the SQL file and expected CSV file are present and the command exited successfully.

Import idempotency:

- Target database creation uses `CREATE DATABASE IF NOT EXISTS`.
- Table import directly sources the `mo-dump` table SQL.
- The generated SQL already includes `DROP TABLE IF EXISTS`, so repeated imports replace the table and load the CSV again.
- The CSV path in the SQL is absolute, so imports remain valid as long as the run directory is retained.

The first version will use overwrite import semantics only. Append or merge imports are intentionally out of scope because they require table-specific keys or conflict rules.

## Retry Behavior

Each table task has separate retry loops for export and import.

- `max_attempts` controls total attempts, including the first attempt.
- `backoff` is the base delay between attempts.
- Export retry cleans the table task directory before retrying.
- Import retry re-runs the same table SQL. This is safe because the SQL drops and recreates the table.
- Import retry includes the target row count check. A source/target row count mismatch retries the import and is reported as a table failure if all attempts are exhausted.
- Export cleanup is disabled by default. When enabled, it only removes successful table export directories in `sync` mode; failed tables, `export` mode, and `import` mode retain export files.
- Failed tasks are recorded in the report with the final error message and attempt count.
- The CLI exits non-zero if any table task fails.

## Reporting

Each table report entry includes:

- Run ID
- Source tenant name
- Source connection host and port
- Source database
- Source table
- Target database
- SQL file path
- CSV file path
- CSV file size in bytes
- Source row count
- Target row count
- Export status
- Import status
- Export started at and finished at
- Import started at and finished at
- Export duration
- Import duration
- Export attempts
- Import attempts
- Error message, if any

The summary includes total tasks, succeeded tasks, failed tasks, total source rows, total target rows, total duration, and report paths.

## Testing Plan

Automated unit tests:

- Config parsing and validation.
- Source database to target database mapping.
- Exclude table filtering.
- Task directory path generation.
- Retry behavior for successful and failing functions.
- Report generation in JSON and CSV formats.

Integration tests against local MatrixOne:

- Create source tenants `dsync_src_a` and `dsync_src_b`.
- Create target tenant `dsync_target`.
- Create multiple source databases and tables.
- Verify excluded tables are not exported or imported.
- Verify database mapping works.
- Verify target tables are overwritten when old data exists.
- Verify repeated import of the same table is idempotent.
- Verify source and target row counts match.
- Verify failures are reported and return a non-zero exit code.

Integration tests should use unique names or clean up with `DROP ACCOUNT IF EXISTS` / `DROP DATABASE IF EXISTS` so they can be run repeatedly.

## Out of Scope

- Transforming table schemas or data.
- Append or merge import semantics.
- Cross-version compatibility fixes inside `mo-dump`.
- Moving exported runs after creation, because generated SQL contains absolute CSV paths.
- Incremental sync or change data capture.
- Exporting views as data tables. The first version migrates ordinary tables only.

## Current Implementation Status

Updated: 2026-06-06

Implemented:

- Go module and `cmd/datasync` CLI skeleton.
- CLI flags: `-config`, `-mode sync|export|import`, `-run-id`, `-cleanup-export-after-import`, and `-version`.
- YAML configuration loading and validation.
- Example configuration at `configs/example.yaml`.
- MatrixOne database client helpers for DSN generation, connection, ordinary table discovery, row count, database creation, and identifier quoting.
- Table task planner.
- Retry helper.
- JSON/CSV report writer.
- Export/import runner with retries, stable task paths, row-count comparison, `sync` mode, `export` mode, and import-only mode from an existing run directory.
- App-level orchestration for discovery, planning, running, and report writing.
- Opt-in local MatrixOne integration test.

Verified:

- `go test ./...` passes.
- `go vet ./...` passes.
- `go mod tidy -diff` is clean.
- `DATASYNC_INTEGRATION=1 go test ./tests/integration -v` passes locally.
- `go run ./cmd/datasync -version` prints `dev`.

Not yet implemented:

- Import-only resume from an arbitrary report file outside the configured `output_dir`.
