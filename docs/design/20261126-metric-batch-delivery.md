# Metric batch delivery after an unknown commit outcome

The metrics exporter can receive a MySQL error or lose its connection after
sending a write. The TN may still commit that write. The exporter then saves
the same CSV payload, and the ETL job can insert the batch again. Both metric
tables accept duplicate rows. The problem affects the initial SQL write and
every subsequent attempt to replay a retained file.
[Issue #33256](https://github.com/matrixorigin/matrixone/issues/33256)

## Batch identity and transaction

The writer factory assigns one random UUID to a batch for either
system_metrics.metric or system_metrics.sql_statement_cu. The SQL writer and
the CSV filename carry this UUID. SQL-disabled writers also name their CSV
with a batch UUID. The filename retains the existing path and CSV body layout.

Both write paths insert the UUID into the primary key of
system_metrics.metric_batch_receipt and load the rows in the same transaction.
Replay splits LOAD statements at CSV record boundaries using the connection's
packet limit. Partial loads are rolled back together with the receipt.
The operation deadline covers CN discovery, connection acquisition, SQL and
the wait for COMMIT. An interrupted COMMIT leaves the file available for
receipt-based adjudication. A connection error or operation deadline marks
that pool for CN reselection even if the next replay runs minutes later.
A rollback removes both effects. When the insert or commit reports a
duplicate key, the writer checks the receipt from a new SQL statement after
releasing the failed transaction. A visible receipt proves that a previous
attempt committed. Timeouts, conflicts without a visible receipt, and failed
receipt reads leave the file available for retry. Only a confirmed commit or
a visible committed receipt permits deletion. If deletion fails, the next
importer uses the same UUID and can safely retry deletion.

This fits the existing 4.2-dev file transaction. On that branch several
bounded LOADs can share one receipt and commit after a clean end of file.
[PR #32991](https://github.com/matrixorigin/matrixone/pull/32991)

## Retention and upgrade

A successful first-write acknowledgment means the sole producer of that
batch will never publish its fallback file. The producer can delete the
receipt at that point. A failed receipt delete leaves the receipt in place
and still counts as a successful delivery. The normal SQL path adds a
transaction and a receipt insert and delete per batch.

Keep receipts for file-backed batches, and orphaned receipts from failed
cleanup, until there is a cleanup mechanism that can prove the batch cannot
be replayed. Both a delayed SQL writer publishing a fallback and an importer
that already read a deleted file can outlive a listing or delete operation.
Age-based reclamation can therefore cause duplicates. Receipt storage
grows with file-backed batches and failed receipt deletions. Operators can
query created_at to inspect that growth.

Fresh bootstrap creates the receipt table in the system account. Shutdown
upgrade creates it through an idempotent cluster entry in the pending
4.0.7 catalog version. The two metric tables keep their existing columns
and CSV column order. ETL retains incompatible versioned CSV before opening
a SQL transaction. Legacy TAE files keep their existing import path. The
SQL writer has only ever used CSV for fallback.

## Legacy CSV during a shutdown upgrade

A legacy CSV filename is the same whether the producer wrote directly to
files or fell back after a SQL error. Even a file-only batch may already
have been imported by the old ETL job if file deletion failed. The new
importer leaves unversioned CSV in place and reports the path for
reconciliation. A new batch UUID cannot establish the outcome of an old
transaction.

Keep the old importer stopped once the legacy backlog is inventoried. Stop
all old producers and take the final inventory before starting the upgraded
cluster. Leave the files untouched during the transition. A drain by the old
importer could commit and then lose its response or fail to delete the file,
leaving a subsequent old ETL run free to insert the same batch again.

After upgrade, operators can recover a verified undelivered batch using
the new importer. Record the original object key, target key and a fresh
lowercase UUID once. Publish the unchanged CSV in the same table directory
as `<original-stem>-batch-<uuid>.csv`. For example, on a local fileservice,
with an already recorded UUID and with `source` set to the verified file:

```sh
mv -n -- "$source" "${source%.csv}-batch-$batch_uuid.csv"
```

On object storage, copy to the recorded key and keep the source until the
receipt is visible. Use the same target key and UUID if the copy result
is uncertain. A receipt can be checked with
`SELECT batch_id FROM system_metrics.metric_batch_receipt WHERE batch_id = '<uuid>'`.
Once the receipt is visible, archive any remaining legacy source outside
the scanned directory. If the batch was already delivered, archive it
without publishing a versioned file. Leave unresolved files in place.
Only CSV that matches the current table schema can use this procedure.

A batch can still be lost if its producer crashes before publishing a
fallback for a failed write. Persisting the payload before the initial SQL
attempt would add file I/O to every delivery and is outside this change.
Recovery of already published versioned files uses the database receipt
after a process restart.
