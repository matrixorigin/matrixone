# Stopped upgrade from 4.2 to catalog 4.0.7

This procedure upgrades a stopped 4.2 release directly to current main.
Intermediate development versions, mixed-version operation and downgrade are
outside this upgrade contract. Existing data and snapshots in 4.2 remain in scope.

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
watermarks. Return to the system account and confirm the query above is empty.
Complete the snapshot preflight below while the 4.2 system account is still
accessible, then stop and drain every old CN and take a verified backup of the
complete stopped deployment before starting the new version. Recreate CDC after upgrading against a clean or independent
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
An administrator must determine each snapshot's ownership from independent
records; its name alone does not prove ownership. If ownership cannot be
established, keep the upgrade stopped rather than guessing.

Before stopping the 4.2 CNs, stop snapshot and branch operations, including
background jobs that modify them. Keep them stopped until the old deployment
is shut down. From the new source tree, use a system-account connection to
produce a complete template:

```sh
optools/upgrade-4.2-preflight.sh decisions.ndjson \
  --defaults-extra-file=/secure/path/mysql-client.cnf
```

The MySQL option file must be readable only by the administrator and contain
the connection details and credentials; configure TLS according to the source
deployment. The script requires a MySQL-compatible client, `jq` and GNU
coreutils. It reports the CDC task count and writes a private file without
overwriting an existing path. Its temporary disk use is proportional to the
number and size of snapshot and branch records; `TMPDIR` chooses the scratch
filesystem. It scans both source catalogs, and its 16 MiB sort setting does not
bound database-server or total process memory. Failures before publication leave
no output file. If interruption races with publication, a complete file may
already exist: inspect it before proceeding. Choose a new path when regenerating.

Review each template record by UUID and replace its `"decision":null` with
`"user"` or `"branch"`. Do not change the remaining fields or their order in the
file; do not automatically classify records by name. A template containing
only the header needs no decisions. The header is:

```json
{"format":"matrixone-catalog-snapshot-decisions-v1","target":"4.0.7"}
```

Preserve every field and its exact value, including explicit `null` values and
the entire edge object. Decimal IDs and timestamps are strings. Records are
sorted by snapshot name, with no duplicate names. For example:

```json
{"catalog_account_id":"0","snapshot_id":"01a17d34-05f1-752c-acbd-52c7a8fe04fe","name":"__mo_branch_42","timestamp":"123","level":"table","account_name":"sys","database_name":"db","table_name":"t","object_id":"9","prior_kind":"user","edge":{"table_id":"42","parent_id":"9","clone_ts":"123","creator":"0","level":"table","table_deleted":false},"decision":"branch"}
```

Stop and drain all CNs. Publish the completed file using the new binary and
a CN configuration pointing at the deployment's shared file service:

```sh
mo-service -cfg /path/to/cn.toml -catalog-upgrade-decisions /path/to/decisions.ndjson
```

The command publishes `SHARED:/catalog-upgrade/4.0.7/decisions.ndjson`. If
publication fails, repair the file and rerun the command before restarting the
CNs. The publisher rejects incomplete or invalid decisions before writing. On
restart the upgrade rechecks the preimages; a changed snapshot or edge is
rejected instead of applying a stale decision. If preflight was not run, the
startup diagnostic reports at most 128 undecided snapshots per attempt; use a
restored 4.2 backup to run the complete preflight, or assemble and republish
the complete ordered decision file from the diagnostics while all CNs remain
stopped. Preserve earlier decisions when retrying after partial progress.

## Retry and resource limits

A successful migration preserves the logical identities and exact-object grants
of existing information-schema objects. Restarting or repeating an upgrade on an
already-current account does not rebuild those objects. Failed transactions roll
back together; work committed by earlier transactions remains and is checked
again on retry.

The page/object limit bounds retained SQL result payload, not query execution,
index construction or total transaction memory. Login upgrades with a supplied
transaction keep that transaction's atomicity and aggregate workspace. Foreign-key
reconciliation currently scans the directory per child table; a large FK catalog
can require substantial upgrade time and repeated scanning after interruption.
