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

- Press `Enter` on a row to open the physical object view for that object range.
- Press `L` to open a logical table view with tombstones applied to table rows.

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
./mo-tool ckp info --backend S3 --s3 key=value,...
./mo-tool ckp view --backend MINIO --s3 key=value,...
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
