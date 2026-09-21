# Sirius TAE recovery foundation

Design version: 1. Tracking: #28966. This is the storage prerequisite for the
approved embedded Sirius migration; it does not enable an execution route.

## Contract and delivery boundary

An explicitly owned local TAE process restores durable read protection before
checkpoint replay or the first cleaner opportunity. Its replayed lease manager
becomes available only after the complete TAE storage constructor succeeds.
Failure aborts unpublished ownership, rolls storage back, and releases the local
directory lock after rollback. Existing Flight records remain protected until
external execution is reconciled; embedded records may be released only when
the caller has established that the prior owning process is gone.

The internal `WithSiriusLeaseManagerBroker` option is the opt-in. A nil broker
installs no bootstrap, performs no journal replay, and does not change ordinary
standalone or distributed TN behavior. Non-standalone opt-in is rejected. No
production launcher invokes this option in this PR. Explicit recovery-only
helpers support deterministic restart tests without publishing a CN manager.

## Local ownership and persistent identity

All write-mode TAE opens acquire a descriptor-owned nonblocking lock on a
separate owner file before opening the existing POSIX record-lock file. This
rejects both same-process and cross-process duplicates, while preserving the
legacy record lock for older executables. Closing a failed duplicate never
opens/closes the legacy inode and therefore cannot release the first owner's
record lock. Close is idempotent and releases the legacy descriptor before the
owner descriptor. Lock files are never unlinked.

The generic pre-GC hook receives a lazy generation callback. Only the explicit
Sirius hook calls it while the DB owns the directory lock; ordinary opens and
generic hooks neither create a generation token nor read `/etc/machine-id`.
Opted-in recovery requires host identity and fails closed if it is unavailable.

A 32-byte token is written and synced before atomic no-replace publication,
then its directory is synced. One fixed temporary name bounds interrupted-write
residue and is replaced under exclusive ownership on retry. Reads are bounded
before allocation; malformed existing tokens are rejected without replacement.
The hash covers the token, canonical directory, and host identity. The published
broker identity covers that hash and the exact shard/replica; journal names stay
shard-stable for recovery. Token hashes identify a directory, not an external
lease or remote fencing epoch.

## Activation blocker: shared namespace authority

The journal coordinator and broker provide exclusion only inside one process.
The local directory lock excludes owners of that directory only. Neither
protects against another TN using a different directory and the same shared
FileService namespace. FileService `Write` is not an atomic election primitive:
current LocalFS and S3 implementations check existence before publication.
There is deliberately no shared authority JSON marker in this foundation.

Before a production launcher can enable this option, the following PR must
establish exclusive namespace enrollment before any competing TN can open it,
and retain that authority through query drain and storage shutdown. An atomic
marker create alone is insufficient: an ordinary TN could already have passed
an absent-marker check. Until this protocol and its restart tests exist, direct
TAE activation must remain unavailable. Cross-host relocation, rolling
replacement, CN topology monitoring, and capability renewal are deferred.

## Verification

Focused tests cover publication after success, abort/retry, replacement sealing,
capacity and replica checks, generic hooks without host identity, pre-GC
rollback, bounded record reads, same-process and subprocess lock exclusion,
legacy writer compatibility, stable/copy-bound generations, malformed tokens,
interrupted publication, and stale embedded versus unreconciled Flight reads.
Run owning packages normally and with the race detector. No SQL BVT or GPU run
can prove a new public behavior here because no execution route is enabled.
