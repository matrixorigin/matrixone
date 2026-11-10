# Native Rebase Logical-DML History

Revision4, 2026-11-10. WAL component qualified in071253; runtime and optional
checkpoint14 passed11 roots including real SIGKILL WAL/checkpoint recovery in
080304. GC checkpoint composition and actual backup checkpoint rewrite passed4
targeted roots in081830, including legacy-only/mixed metadata. These are metadata
preservation tests, not a full restored-database proof. Production R2 stays off;
full R2 is not qualified.

## Observed Failure And Invariant

The checkpoint recovery control in063127 and TN diagnosis064121 show a physical
flush advancing the reused dirty-table replay watermark past unchanged F.
Deleting that replay check is unsafe: a true post-F write may already be wholly
inside a checkpoint. Keep the existing allocator-reset watermark unchanged.

Rebase needs its own logical-write history. A committed logical write after F
must prevent retirement; flush/merge alone must not advance that history. Pending
writers must participate until their final commit/rollback. WAL and checkpoint
recovery must preserve the same distinction, including file-import user DML.

## WAL Component

Keep the ordinary dirty-tree encoding and default WAL V5 bytes unchanged. Add a
V6 codec, selected only by an explicit transaction-memo opt-in. Append a bounded
logical-write extension after the historical memo: one known flag, uint32 count,
then(database ID,table ID) pairs. Each pair must be a unique existing dirty-table
identity. Count cannot exceed that memo's dirty-table count. An empty known set
means no logical writes; unknown legacy history is a different state. Re-encoding
legacy metadata must not promote unknown to known-empty.

The memo owns its optional set for exactly one transaction. It is not a global
cache; memory and WAL bytes are O(dirty tables), with no row payload or background
worker. Reads clear prior classification and reject truncation, duplicate IDs,
wrong database identities, invalid flags/counts and unknown-with-entries.

The V4/V5 codecs remain registered. New binaries read old records; no old binary
is asked to read V6. This is shutdown-upgrade compatibility, not a new in-cluster
protocol/capability gate. No caller enables V6 until its runtime/physical-format
activation path is explicit and separately qualified. Do not change CurrVer.

## Runtime Ownership

The normal transaction owner opts in via a default-off TAE/TN configuration. Mark only
actual CN user writes, including tombstone/file-import paths, rather than every
dirty table. Flush/merge retain their existing maintenance classification. On
prepare register a pending logical writer; on final commit advance a separate
committed upper bound; on final rollback remove the pending writer. Keep unknown
outcomes fail-closed and reuse existing transaction termination owners.

The existing allocator-reset watermark is independent and unchanged. Only a
registered pending writer's final commit advances the Rebase bound; rollback
removes its identity without advancing the bound. Unknown/duplicate completions
cannot invent writes. The EXPECT guard explicitly refuses disabled history.

## Opt-in Checkpoint14

Keep CheckpointCurrentVersion13. Only a catalog opened with history enabled
writes14. The ordinary range index remains SchemaData. A separate metadata
checkpoint sub-schema(CkpMetaStart, ordinal0) stores db ID, table ID and committed
logical bound, in bounded8192-row blocks. Its initial(0,0,end) coverage row is
mandatory even for an empty catalog. All catalog tables, including empty tables,
are visited. A concurrently advanced bound is capped at checkpoint end, a safe
upper bound rather than falsely asserting absence of an earlier write.

Incremental and global collectors use the same representation. Reconstruct the
catalog first, then merge each checkpoint's bounds monotonically before WAL
replay/SQL ingress. IDs, not names, determine identity. Legacy checkpoint13 or a
missing table identity supplies a conservative end floor, never zero/known-empty.
Checkpoint14 must have its metadata and a coverage timestamp matching its entry;
invalid identities, duplicate tables, NULLs, malformed types or future bounds
fail recovery. Bounds for already GCed tables are ignored by ID. Old pre-cutover
snapshots can conservatively require retry; snapshots fixed after recovery use
the restored bounds normally.

Backup checkpoint rewriting copies the metadata and caps its bounds/coverage at
the restored end. The ordinary snapshot backup collector may retain an upper
bound at its own checkpoint end: conservative rejection is preferable to losing
logical-write protection. No old binary opens a14 checkpoint or V6 WAL. Reopening
with history disabled cannot admit EXPECT; re-enabling after legacy checkpoints
uses conservative floors. No global format default, generic coordinator,
catalog row UPDATE per user DML, or directory rescan substitutes for protection.

GC checkpoint compaction must preserve the same bounds and advertise its actual
format, rather than writing a13 index and losing14 metadata. Merge input bounds
monotonically by table ID/database identity; a legacy input contributes an end
floor to every known table. All-legacy compaction stays13. A14 input missing its
metadata, outside the output coverage or with mismatched table ownership fails
before publishing the compacted index. The checkpoint writer is the single owner
of its sinker and optional metadata on success and error paths.

## Validation

First test V5 unchanged defaults, opt-in V6 roundtrip, legacy unknown, malformed
extensions and parser reuse, under vet/linked Linux tests. Then qualify actual
logical/maintenance tagging, pending/commit/rollback and checkpoint persistence.
Finally rerun the existing WAL/checkpoint SIGKILL controls, real physical
flush/merge, late no-PK/file-import writes and representative publication order.
The new codec alone is not R2 recovery completion or a performance result.
