# datasync

`datasync` exports selected MatrixOne tenant tables with `mo-dump -csv` and imports them into mapped target databases with `mysql source`.

## Build

```bash
rtk go build ./cmd/datasync
```

## Run

```bash
rtk go run ./cmd/datasync -config configs/example.yaml
```

The default mode is `sync`, which exports each selected source table and then imports it into the mapped target database.

```bash
rtk go run ./cmd/datasync -config configs/example.yaml -mode sync
rtk go run ./cmd/datasync -config configs/example.yaml -mode export
rtk go run ./cmd/datasync -config configs/example.yaml -mode import -run-id <existing-run-id>
rtk go run ./cmd/datasync -config configs/example.yaml -mode sync -cleanup-export-after-import
```

`-mode export` only writes the run export files and reports. `-mode import` skips `mo-dump` and imports SQL/CSV files from the existing run directory selected by `-run-id`.

Use `-run-id <id>` to choose a stable run directory under `output_dir`.

The config uses optional top-level `source` and `target` entries for shared connection defaults: `name`, `host`, `port`, `user`, and `password`. Each `databases` entry has its own `source.database` and `target.database`, and may override any inherited connection field. If a database entry cannot be completed from its own fields plus the top-level defaults, that entry is ignored.

Table filtering is evaluated per database:

- If `include_tables` is configured, only discovered tables listed there are candidates.
- If `include_tables` is omitted or empty, all discovered ordinary tables are candidates.
- `exclude_tables` is then removed from the candidate set.
- If `exclude_tables` is omitted or empty, nothing is removed.

After each import, `datasync` compares the target row count with the source row count. A mismatch is treated as an import failure and retried according to `retry.max_attempts`.

By default, exported SQL/CSV files are retained after sync imports. Use `-cleanup-export-after-import` in `sync` mode to delete each table's export directory after that table imports successfully. Failed tables, `export` mode, and `import` mode always keep export files.

Use `-help` for detailed command-line usage and examples.

## Local MatrixOne Integration Test

The integration test expects MatrixOne on `127.0.0.1:6001` with system user `dump/111`, plus working `mysql` and `mo-dump` binaries.

```bash
rtk proxy env DATASYNC_INTEGRATION=1 go test ./tests/integration -v
```

Override binary paths with `DATASYNC_MYSQL` and `DATASYNC_MO_DUMP`.
