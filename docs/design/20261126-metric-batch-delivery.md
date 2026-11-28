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
with a batch UUID. The table directory and CSV body layout stay the same.
New files use `@<uuid>.csv`, with the full 16-byte UUID encoded as 22
unpadded URL-safe Base64 characters. The 27-byte name avoids growing the
object key and leaves room for the disk-cache suffix. Each new file starts
with a 16-byte publisher UUID and an 8-byte big-endian sequence, followed
by the original CSV. The producer assigns a sequence when it starts a
file write, including in file-only mode. Retries of a file write reuse the
same key, header and payload. Each writer factory uses a separate publisher
identity per account and table. A restart uses a new publisher identity.

Both write paths insert the UUID into the primary key of
system_metrics.metric_batch_receipt and load the rows in the same transaction.
Replay splits LOAD statements at CSV record boundaries using the connection's
packet limit. A record too wide for a single inline LOAD uses a prepared
INSERT in that same transaction. Large field values are split at UTF-8
boundaries and concatenated in SQL when a single parameter would exceed
the connection limit. Inline LOAD encodes backslashes according to the
writer connection's SQL mode. Replay reads that mode with the packet
limit and uses it to size each statement. Partial loads are rolled back
with the receipt.
Replay observes task cancellation throughout the transaction. CN discovery,
connection acquisition, each SQL statement and COMMIT each get a fresh
10-second deadline, so a large file can finish while its statements make
progress. Each exporter process admits one file replay at a time and allows
two connections in its shared pool, leaving capacity for foreground writes.
The synchronous first write retains its 10-second total budget.
An interrupted COMMIT leaves the file available for receipt-based
adjudication. A connection error, operation timeout or rolling-restart
refusal marks the pool for CN reselection even if the next replay runs
minutes later.
A rollback removes both effects. When the insert or commit reports a
duplicate key, the writer checks the receipt from a new SQL statement after
releasing the failed transaction. A visible receipt proves that a previous
attempt committed. Timeouts, conflicts without a visible receipt, and failed
receipt reads leave the file available for retry. For a sequenced file,
its delivery must also be recorded in the progress table before deletion.
If deletion fails, the next importer can safely retry deletion.

This fits the existing 4.2-dev file transaction. On that branch several
bounded LOADs can share one receipt and commit after a clean end of file.
[PR #32991](https://github.com/matrixorigin/matrixone/pull/32991)

## Retention and upgrade

A successful first-write acknowledgment means the sole producer of that
batch will never publish its fallback file. The producer can delete the
receipt at that point. A failed receipt delete leaves the receipt in place
and still counts as a successful delivery. The normal SQL path adds a
transaction, a receipt insert and delete, and a write to the origin
fence per batch.

The replay transaction records a delivered sequence in
`system_metrics.metric_file_progress` along with the LOAD and UUID receipt.
When the first write already committed, replay records the sequence after
a fresh read of that receipt under the replay fence. The permanent prefix
row serializes replays in pessimistic mode and makes concurrent progress
updates conflict in optimistic mode. A paused importer must check progress
inside this transaction, even if it read the payload before another
importer finished.

After progress commits, a second short transaction retires the UUID
receipt before the source file is deleted. That transaction and the
original SQL attempt write a separate fence row. An older optimistic
first write therefore cannot commit after retirement removes the UUID it
would otherwise have conflicted with. The original write never locks the
replay prefix row, and holds its own fence only at commit. A failed
retirement leaves the CSV available for retry. Progress also deduplicates
the original bytes if an upload retry recreates the deleted object.

One prefix row and one first-write fence row cover consecutive
deliveries for a publisher. Out-of-order deliveries use one row per
disjoint delivered interval beyond the prefix, and intervals coalesce
when gaps are filled. Gaps may correspond to delayed or failed uploads.
Retirement never infers completion from time or from an absent object.
An original SQL receipt that is still awaiting fallback keeps its UUID
until that file is replayed. Unsequenced `!<uuid>.csv` and manually
reconciled `-batch-<uuid>.csv` use their UUID receipts, since they have no
publisher sequence to retire by.

Fresh bootstrap and the idempotent 4.0.7 cluster upgrade create both
tables in the system account. The two metric tables keep their existing columns
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
UUID once. Publish the unchanged CSV in the same table directory. Compute
the compact basename from the recorded UUID:

```sh
python3 -c 'import base64, sys, uuid; print("!" + base64.urlsafe_b64encode(uuid.UUID(sys.argv[1]).bytes).decode().rstrip("=") + ".csv")' "$batch_uuid"
```

Record the full target key before publishing. In a local fileservice, set
`source` and `target` to the recorded paths and run `mv -n -- "$source" "$target"`.
The importer also accepts filenames ending in `-batch-<uuid>.csv`.

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
