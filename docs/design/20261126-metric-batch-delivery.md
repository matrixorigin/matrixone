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
4.0.7 catalog version. Older files without a batch UUID may include batches
committed by a previous binary. ETL leaves those files in place for
reconciliation and continues to the next file. It also retains incompatible
versioned files before opening a SQL transaction. The two metric tables
keep their existing columns and CSV column order.

A batch can still be lost if its producer crashes before publishing a
fallback for a failed write. Persisting the payload before the initial SQL
attempt would add file I/O to every delivery and is outside this change.
Recovery of already published versioned files uses the database receipt
after a process restart.
