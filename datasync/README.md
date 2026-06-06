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
```

`-mode export` only writes the run export files and reports. `-mode import` skips `mo-dump` and imports SQL/CSV files from the existing run directory selected by `-run-id`.

Use `-run-id <id>` to choose a stable run directory under `output_dir`.

## Local MatrixOne Integration Test

The integration test expects MatrixOne on `127.0.0.1:6001` with system user `dump/111`, plus working `mysql` and `mo-dump` binaries.

```bash
rtk proxy env DATASYNC_INTEGRATION=1 go test ./tests/integration -v
```

Override binary paths with `DATASYNC_MYSQL` and `DATASYNC_MO_DUMP`.
