# Stopped upgrade from 4.2 to catalog 4.0.7

Before starting any new-version service, connect to the running 4.2 deployment
as a system administrator and check for CDC tasks across all accounts:

```sql
SELECT account_id, task_id, task_name, state
FROM mo_catalog.mo_cdc_task
ORDER BY account_id, task_id;
```

The current CDC executor cannot safely continue a 4.2 task: 4.2 did not record
the source generation and target identity needed to validate its watermark.
This includes paused and failed tasks. Securely export any required task
configuration and watermarks, decide how to rebuild the target, and remove all
such tasks using 4.2 before upgrading. Log in as an administrator of each task's
own account and run `DROP CDC TASK <name>` there; a system-account session does
not drop another account's task. `DROP CDC` removes task metadata and
watermarks. Return to the system account and confirm the query above is empty,
stop and drain every old CN, and
take a verified backup of the complete stopped deployment before starting the
new version. Recreate CDC after upgrading against a clean or independent
target; the old watermark does not prove that target is safe to resume.

Enable automatic catalog upgrade for this stopped migration. If it was disabled,
restore the setting after the upgrade. The per-object upgrade input limit
defaults to 64 MiB; if startup reports that an object exceeds it, provision
sufficient memory and set the same higher `cn.upgrade-catalog-object-limit` on
all upgrading CNs before retrying. This limit does not bound total engine memory.

The new CN also rejects startup if tasks remain. This is a last safety check,
not a replacement for the preflight: by then a new logservice may already have
written log entries that 4.2 cannot read. If that happens, restore the complete
pre-upgrade backup before restarting 4.2; do not assume the old binary can
restart directly. An interrupted upgrade may already have committed earlier
catalog work, and the check prevents subsequent work rather than undoing it.

System-account and cluster snapshot/PITR restores are rejected before changes
when the historical CDC watermark catalog lacks the columns required by the
current version. Restoring that older catalog would also break newly created CDC
tasks. Keep these historical backups and use a compatible recovery procedure;
the upgrade itself does not require restoring them. The schema check does not
quiesce running CDC or prove that restored task metadata matches an external
target. Stop and drain CDC and plan target reconstruction before restoring
account or cluster snapshots/PITR; do not resume from an unverified watermark.

Historical Iceberg snapshots and PITR also require special care. The restore
preflight rejects historical metadata that cannot be rebound to the target
account or recreated objects. Database and table restores reject Iceberg tables
that would otherwise be omitted or overwritten. These checks do not establish
that concurrent Iceberg operations have stopped. Until restore is coordinated
with all in-flight Iceberg operations, stop and drain Iceberg workloads before
restoring a historical snapshot or PITR. Verify with the remote catalog that
any commits with an unknown outcome have resolved; a network error can leave a
remote commit pending after its local operation exits. Do not treat an empty
metadata or job table as proof that remote writes have finished.

## Ambiguous snapshot ownership

Some old system table snapshots can have names also used by branch protection.
If the upgrade reports undecided snapshots, it prints their complete preimages,
including the snapshot UUID and the branch edge (or `null`). An administrator
must determine each snapshot's ownership from independent records; the name
alone does not prove ownership. If ownership cannot be established, keep the
upgrade stopped rather than guessing.

Stop and drain all CNs. Create a UTF-8 newline-delimited JSON file with this
header on its first line:

```json
{"format":"matrixone-catalog-snapshot-decisions-v1","target":"4.0.7"}
```

For each undecided snapshot, copy its entire JSON preimage from the diagnostic
to a separate line and add `"decision":"user"` or `"decision":"branch"`.
Preserve every field and its exact value, including explicit `null` values and
the entire edge object. Decimal IDs and timestamps are strings. Sort records
by snapshot name, with no duplicate names. For example, the following single
line illustrates the format only; replace every value with the reported
preimage and the administrator's independently verified decision:

```json
{"catalog_account_id":"0","snapshot_id":"01a17d34-05f1-752c-acbd-52c7a8fe04fe","name":"__mo_branch_42","timestamp":"123","level":"table","account_name":"sys","database_name":"db","table_name":"t","object_id":"9","prior_kind":"user","edge":{"table_id":"42","parent_id":"9","clone_ts":"123","creator":"0","level":"table","table_deleted":false},"decision":"branch"}
```

With the CNs still stopped, publish the validated file using the new binary and
a CN configuration pointing at the deployment's shared file service:

```sh
mo-service -cfg /path/to/cn.toml -catalog-upgrade-decisions /path/to/decisions.ndjson
```

The command publishes `SHARED:/catalog-upgrade/4.0.7/decisions.ndjson`. If
publication fails, repair the file and rerun the command before restarting the
CNs. On restart the upgrade rechecks the preimages; a changed snapshot or edge
is rejected instead of applying a stale decision. Diagnostics are bounded to
128 undecided snapshots at a time. If the next restart reports more, stop the
CNs again, append the newly verified records to the existing file in name
order, and republish the complete file; retain earlier records.
