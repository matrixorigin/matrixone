# mo-tool Object And Checkpoint Usage

`mo-tool` provides offline tools for inspecting MatrixOne checkpoint and object
files. It can read from a local data directory, or from a remote MatrixOne
fileservice such as S3 or MinIO.

## Build

```bash
go build -o mo-tool ./cmd/mo-tool
```

You can also run it directly during development:

```bash
go run ./cmd/mo-tool --help
```

## Local Checkpoint Usage

Read checkpoint files from a local MatrixOne data directory:

```bash
./mo-tool ckp info ./mo-data
./mo-tool ckp view ./mo-data
```

If no directory is provided, the current working directory is used:

```bash
./mo-tool ckp info
./mo-tool ckp view
```

The checkpoint viewer lists checkpoint entries and lets you drill down into
tables, object ranges, and object data.

Inside table detail view:

- Press `Enter` on a row to open the physical object view for that object.
- Press `L` to open a logical table view with tombstones applied to table rows.

## Offline Table Schema And CSV Export

`mo-tool ckp` can reconstruct table schema and export logical table rows directly
from checkpoint data.

Show the `CREATE TABLE` statement for a table at the latest checkpoint:

```bash
./mo-tool ckp show-create-table --table-id=272387 ./mo-data
```

Show the schema at a specific snapshot timestamp:

```bash
./mo-tool ckp show-create-table \
  --table-id=272387 \
  --ts=1780574341899433000:0 \
  ./mo-data
```

Dump a logical table view to CSV:

```bash
./mo-tool ckp dump --table-id=323820 ./mo-data
./mo-tool ckp dump --table-id=323820 -o workflow_cases.csv ./mo-data
```

The CSV dump:

- applies tombstones at the selected checkpoint snapshot
- excludes hidden system columns
- uses catalog metadata from `mo_tables` and `mo_columns`
- streams row output instead of materializing the full CSV in memory first

If catalog metadata cannot be resolved exactly, the command fails instead of
guessing from raw physical object columns.

## Local Object Usage

Read a single local object file:

```bash
./mo-tool object info ./mo-data/<object-name>
./mo-tool object view ./mo-data/<object-name>
```

Replace `<object-name>` with the object path shown by the checkpoint viewer or
stored in checkpoint metadata.

## Remote Fileservice From MatrixOne Config

The recommended remote workflow is to reuse the MatrixOne service TOML file.
This avoids manually copying bucket, endpoint, key prefix, and credential
settings.

For example, with a MinIO launch config:

```bash
./mo-tool ckp info \
  --fs-config etc/launch-minio-local/tn.toml

./mo-tool ckp view \
  --fs-config etc/launch-minio-local/tn.toml
```

By default, `mo-tool` uses the fileservice configured in
`[tn.Txn.Storage].fileservice`. If that field is absent, it uses `SHARED`.

To select a specific fileservice:

```bash
./mo-tool ckp view \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED
```

The same fileservice options work for object inspection:

```bash
./mo-tool object info <object-name> \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED

./mo-tool object view <object-name> \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED
```

When checkpoint data is opened from a remote fileservice, object drill-down from
the checkpoint viewer reuses the same fileservice. This means checkpoint meta,
checkpoint data objects, and table data objects are all read from the same S3 or
MinIO backend.

The logical table view also uses the same fileservice and applies tombstones at
the selected checkpoint snapshot timestamp.

The same remote options work for schema lookup and CSV export:

```bash
./mo-tool ckp show-create-table \
  --table-id=272387 \
  --fs-config etc/launch-minio-local/tn.toml

./mo-tool ckp dump \
  --table-id=323820 \
  --fs-config etc/launch-minio-local/tn.toml
```

## Remote S3 Or MinIO With Inline Arguments

You can also provide object storage settings directly on the command line.

MinIO example:

```bash
./mo-tool ckp view \
  --backend MINIO \
  --s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,key-prefix=server/data,key-id=minio,key-secret=minio123
```

AWS S3 example:

```bash
./mo-tool ckp info \
  --backend S3 \
  --s3 bucket=my-mo-bucket,region=us-east-1,key-prefix=prod/mo-data
```

Object viewer with inline S3 arguments:

```bash
./mo-tool object view <object-name> \
  --backend S3 \
  --s3 bucket=my-mo-bucket,region=us-east-1,key-prefix=prod/mo-data
```

Supported `--s3` keys include:

- `bucket`
- `endpoint`
- `region`
- `key-prefix`
- `key-id`
- `key-secret`
- `session-token`
- `role-arn`
- `external-id`
- `shared-config-profile`
- `no-default-credentials`
- `no-bucket-validation`
- `is-minio`
- `cert-files`

Credentials can also come from the default AWS credential chain when supported
by the selected backend and when `no-default-credentials` is not set.

## Catalog Layout Compatibility

Checkpoint catalog objects are not completely stable across MatrixOne branches.
In particular, older `3.0-dev` generated data may differ from newer builds in
the layout of `mo_tables` and `mo_columns`.

Current tool behavior:

- it first uses actual column names from checkpoint catalog rows when present
- if the checkpoint only exposes generic `col_N` headers, it infers the system
  table layout from the observed column count
- it supports both:
  - current layout, which includes `mo_tables.rel_logical_id` and
    `mo_columns.attr_has_generated` / `mo_columns.attr_generated`
  - older `3.0-dev` layout, which does not include those columns

For `mo_columns`, CSV extraction uses:

- `attnum` for SQL column order
- `att_seqnum` for physical object column position

This matters for tables with hidden columns, index-related internal columns, or
wide schemas. Without `att_seqnum`, visible values can shift to the wrong CSV
column.

In practice, you do not need a separate flag for `3.0-dev` generated data. The
tool auto-detects the known layout variants and falls back to the matching
built-in schema for system tables when needed.

Example with older checkpoint data:

```bash
./mo-tool ckp show-create-table --table-id=2 ./mo-data
./mo-tool ckp dump --table-id=323820 ./mo-data
```

If a checkpoint comes from an unknown future layout and neither named columns
nor known-width inference matches, schema reconstruction can still fail closed.
That is intentional.

## Important Path Rules

For remote checkpoint inspection, `key-prefix` should point to the MatrixOne
data root, not to the checkpoint directory itself.

Correct:

```text
s3://my-bucket/prod/mo-data/
  ckp/
  <table-data-objects>
```

Use:

```bash
--s3 bucket=my-bucket,key-prefix=prod/mo-data,...
```

Do not use:

```bash
--s3 bucket=my-bucket,key-prefix=prod/mo-data/ckp,...
```

Checkpoint metadata stores object names relative to the fileservice root. If the
fileservice root is pointed directly at `ckp/`, checkpoint metadata may be found
but table data object drill-down will fail.

## Command Summary

Checkpoint commands:

```bash
./mo-tool ckp info [directory] [--fs-config FILE] [--fs-name NAME]
./mo-tool ckp view [directory] [--fs-config FILE] [--fs-name NAME]
./mo-tool ckp dump --table-id ID [directory] [--ts PHYSICAL:LOGICAL] [-o FILE]
./mo-tool ckp show-create-table --table-id ID [directory] [--ts PHYSICAL:LOGICAL]
./mo-tool ckp info --backend S3 --s3 key=value,...
./mo-tool ckp view --backend MINIO --s3 key=value,...
./mo-tool ckp dump --table-id ID --backend S3 --s3 key=value,...
./mo-tool ckp show-create-table --table-id ID --backend S3 --s3 key=value,...
```

Object commands:

```bash
./mo-tool object info <object-file> [--fs-config FILE] [--fs-name NAME]
./mo-tool object view <object-file> [--fs-config FILE] [--fs-name NAME]
./mo-tool object info <object-name> --backend S3 --s3 key=value,...
./mo-tool object view <object-name> --backend MINIO --s3 key=value,...
```

Local mode treats paths as local filesystem paths. Remote mode treats object
names as paths relative to the selected fileservice root.
