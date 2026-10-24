# Revision 4 implementation and verification

## Range and decision

Official main was freshly verified at
`6fb6531432aaec8c1fbd98a30364941559e0e1d6` on 2026-10-25. The replacement
starts from that tree and preserves both original PR histories as ancestors;
neither branch requires a force push. The exact revision-4 design and its
[explicit human decision](spill_cleanup_local_lifecycle_v4_approval.md) remain
separate from GitHub reviewers' implementation approval.

This is one consolidated implementation for #30099 and #30102, not two
independently evolving cleanup mechanisms. No background cleanup package,
workers, enqueue callers, or global file-count admission policy are added to
the effective main-based diff.

## Change map and ownership

| Closure | Owner, transition and risk | Required proof |
| --- | --- | --- |
| Local anonymous files | LocalFS/LocalETLFS publish only after unlink; unpublished failure closes FD. R2 local filesystem contract. | Real anonymous lifetime, create/unlink/close failure and original errors. |
| Scratch root | One local-service holder; exclusive external lease before startup cleanup; healthy -> fenced is permanent. R3 concurrency/restart/path boundary. | View sharing, zero-byte fence, already-admitted creation, blocked unlink, initialization/close race, aliases, symlinks, traversal, startup errors, private child-process restart. |
| Disk ledger | Shared token core becomes terminal-abandoned without decrementing query/CN counters. R3 exactly-once accounting and GC. | Copied token cannot refund/grow/reconcile; generation close/GC does not refund or retain query. |
| GROUP/DISTINCT | Bucket, popped reload and local intermediate retain destruction ownership until synchronous free. R2 operator ownership. | Reload success/error, rollback, failed sort run, second merge failure, Reset/Free and pool release. |
| HashBuild | Unpublished bundle -> complete move-only payload; terminal bundle cleanup closes FD and releases optional memory, abandoning only surviving disk bytes. R3 generation/handoff. | Valid/invalid Prepare, cancellation, Reset/Free/pool release, writer cache, healthy/failing publication and terminal handoff. |
| JOIN payload/engine | One terminal SpillFile.Close covers FD close, unlink and finish; clear callbacks even after failure. R3 concurrent terminal ownership. | Concurrent close, already-gone path, rejected row metadata, reader close/finish, scatter/re-spill failure, constructor/account rejection and actual HASH/DEDUP/RIGHT DEDUP consumers. |
| Service startup | clearSpillFiles publishes only a successfully leased/cleaned root. R2 dependent consumer. | Original list error, canceled startup and unpublished ETL/counters. |

Q1: each resource has one terminal owner; failed deletion leaves a physical
file and an abandoned charge, not a retained query callback. Q2: root state
locks are not held across I/O; cancel/fenced admission remain independent of a
blocked unlink. An OS unlink itself has no synthetic timeout. Q3: root state is
bounded by configured roots, including one lease FD retained per fenced root
until process exit, rather than by ended queries/files.

Normal shutdown still requires the established caller ordering: drain all
operator/raw-FD owners before LocalFS/LocalETLFS.Close. Root operation counters
cover in-flight service I/O and initialization; they do not make an arbitrary
raw *os.File close observable. A fenced lease is never released by service
replacement. Upgrade/rollback must stop the old binary first; mixed-version
shared-root access is unsupported.

## Prior-oracle preservation

Queue-specific eventual retry assertions are deliberately replaced by the
approved fence/restart contract; the physical ownership assertions remain.

| Original distinct oracle | Retained revision-4 scenario |
| --- | --- |
| Popped/discarded GROUP buckets and zero-byte sort run | TestGroupTerminalDiscardedBuckets |
| Lost DISTINCT intermediate after the second merge fails | TestGroupTerminalDistinctMergeIntermediate |
| Prepared GROUP/MergeGroup Reset/Free and pooled Group | TestGroupTerminalTeardownAndPoolRelease |
| Bundle teardown, direct pool release, consumer handoff and optional writer cache | TestHashBuildTerminalTeardownAndHandoff; TestHashBuildTerminalRemovalReleasesWriterCache |
| Invalid direct-input Prepare preserves prior generation | TestHashBuildTerminalPrepareInvalidInputPreservesGeneration |
| Terminal JoinMap Free/FreeMemory, partial payload, missing physical file and callbacks | TestSpillPayloadTerminalCleanupDropsAllQueryReferences; existing TestAccountedSpillFileOwnership |
| Concurrent SpillFile close and removal-before-disk-completion | TestSpillFileTerminalConcurrentClose |
| Writer, handed-off file, reader, finish and queue cleanup | TestJoinTerminalOwnershipBoundaries |
| Rejected reader and build payload | TestJoinTerminalReaderRejectsInvalidFileAndReportsCleanup; TestJoinTerminalOwnershipBoundaries/rejected |
| Ended query/engine collection while deletion is unavailable | TestJoinTerminalWriterDoesNotRetainQuery; TestExecutionSpillDiskAbandonDoesNotRetainGeneration |
| Scatter partial failure and re-spill queue-limit failure | TestJoinTerminalScatterPreservesUpstreamAndCleanupErrors; TestJoinTerminalRespillQueueFailureCleansTemporaryOwners |
| Each real consumer's account/constructor/scatter failure and retained Reset/Free/pool boundary | TestHashJoinTerminalCleanupBoundaries; TestDedupJoinTerminalCleanupBoundaries; TestRightDedupJoinTerminalCleanupBoundaries |
| Obsolete global metadata admission / retry-progress policy | TestSpillRootFencesEveryViewAfterUnlinkFailure; TestSpillRootAdmittedCreationCleansAfterConcurrentFence; no global capacity change |

The earlier queue-progress, pending-list and retry-count assertions are not
valid for revision 4. Removing those assertions does not authorize removing
their malformed-input, cancellation, physical-file, accounting, generation,
consumer or GC oracles.

## Verification environment and evidence

Darwin/arm64, Go 1.27.1, local CPU native build through the repository Makefile;
tests use the repository mo-cgo-test wrapper with matching artifact provenance.
No shared/production environment, manual CI dispatch or resource-policy override
was used. Tests use small files, deterministic barriers and explicit private
budgets for injected lifecycle failures.

Initial complete owning-package normal run passed fileservice, process,
message, spillutil, hashbuild, hashjoin, rightdedupjoin and group. Dedup's
TestDedupLogicalDiagnostic threshold=2 cases failed because this host's free
disk is below the existing 16-GiB reserve and its auto spill cap becomes one
byte. A Go overlay restoring every changed Go input and excluding every new Go
file reproduced those same failures against exact official main, with the same
requested byte counts and limit=1. This is baseline evidence, not a pass or a
waiver of the diagnostic assertions.

Three focused mo-service startup tests passed. The initial focused root race
run passed, including the real private child-process lease/restart test.
Parallel race/vet builds were interrupted before execution as local disk
headroom approached exhaustion; their generated temporary build directories
were removed after all child compilers stopped. Those interrupted runs are not
validation passes. Subsequent verification is serialized per package.

Final focused race stress, complete owning-package race, final static analysis
and final post-edit package results are pending at this record's current stage.

The serialized measurement run passed the new process abandon/GC cases and
message concurrent-close/payload cases under race. Its fileservice link failed
with an explicit `rewriting uuid ... no space left on device`; the same attempt
also reported a vet `weak` import-resolution error, which remains unverified
until a clean resource-sufficient rerun. Spillutil returned no terminal test
result, and the following Group build was deliberately stopped. Neither is a
pass. The host subsequently had about 460 MiB free. Further builds were stopped
instead of consuming the remaining operating-system headroom. No global Go
build cache was removed without a separate human choice.

Two final self-review fixes therefore have **no post-edit runtime evidence**:
fence immediately retains the lease even if the failed service is dropped
without Close, and an initialization/Close race retires directory FDs created
by late startup deletion. Their named regressions are present, but the initial
root race pass predates these changes. The current delivery review decision is
**BLOCKED on resource-sufficient final validation**, not PASS. No code has been
pushed at this checkpoint.

## Non-claims

No Linux runtime, SQL/BVT/large-data workload, live CN restart, shared-cluster
or production fault injection was run. The child-process test proves local OS
lease exclusion and post-stop directory cleanup, not whole-CN recovery. The
design intentionally trades spill availability for containment after a failed
unlink and may require an operator-controlled CN restart. No performance
improvement or unchanged throughput is claimed.
