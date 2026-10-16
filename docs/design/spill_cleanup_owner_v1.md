# Bounded spill cleanup ownership, revision 1

Status: proposed for review; no design approval is asserted. This revision
addresses matrixone PR #30099 review 5542718835 and #30102 comment 4290864401.

## Scope and owner

Reuse `pkg/common/spillcleanup`, the existing process-scoped terminal owner,
not a new generic task service. Group (including distinct runs), JOIN writers
and HashBuild reserve one metadata slot **before** creating a named file.
All file-to-file/payload handoffs move that same reservation; they do not obtain
new capacity. A file-service reference, generated name, bounded accounting
tokens and removal callback may survive query teardown. Operators, batches,
writer buffers and query callbacks may not.

## Admission and bounds

The process hard limit is 16,384 active plus pending file reservations, shared
across queries and CN service instances in that process. It is independent of
disk bytes and open FD limits; zero-byte files consume a slot. This is a count
bound, not a claim to measure all Go heap bytes retained by arbitrary file
service implementations. Production retains borrowed, shared file services and
generated bounded-size names. The limit allows 128 simultaneous 128-file
repartition footprints but intentionally does not promise all spill workloads
can complete. It is a conservative safety ceiling, not a performance tuning
result; changing it requires a separate capacity decision/review.

When full, `Reserve` returns `ErrCapacity` before `CreateFile`, without waiting
or retrying as memory reclamation. No file is created and no cleanup callback is
dropped. Failed creation returns the slot; creation followed by cancellation,
write/allocation failure, reset or free keeps it until physical deletion succeeds
(including already-not-found). Memory/generation teardown and closing the FD do
not return the slot. Nonempty files also keep their independent disk charge.
The reservation atomically permits only one terminal enqueue and one release.
The unreserved callback enqueue API is removed.

At most 16,384 admitted files can contribute retry tasks. Up to four completed
callback frames can briefly retain already-released metadata while the slots
are reused. Pending slice slots are cleared on removal. Each HashBuild/message
payload is split into per-file callbacks, so one blocked file cannot lock all
its siblings behind a bundle callback.

## Retry and blocking

At most four process-wide workers execute callbacks. They hold no queue lock
while doing I/O; failures are eligible again after one second and healthy tasks
can bypass delayed retries. Workers exit when idle and restart on new work.
No per-attempt goroutine or timeout replacement is spawned. One non-returning
filesystem call consumes one worker; other workers continue independently.
If all four calls never return, cleanup stops progressing, ownership is kept,
and new file admission eventually fails closed at the hard limit. Recovery then
requires the filesystem/OS call to return or an operator-controlled process
restart; the design does not pretend Go can cancel a stuck unlink syscall.
Existing synchronous teardown removal attempts remain synchronous. This change
does not promise bounded teardown latency for a hung OS/filesystem call.

## Shutdown and restart

Query/generation close is not service shutdown: detached retries continue and
retain the file service until deletion. The queue is process-scoped, has no
durable journal, and does not block process exit trying to drain an unavailable
filesystem. All reservations/callbacks disappear with the process, not with a
query. Named files remain in the existing `LOCAL/__spill` scratch namespace.

On restart, `createFileService` calls the existing `clearSpillFiles` before
service initialization. This revision propagates deletion failures instead of
ignoring them: service startup must not accept new spill work over undeleted
prior-incarnation files. Successful startup cleanup is the recovery boundary;
a listing/deletion error fails startup and must be corrected/retried by the
operator. This assumes exclusive ownership of each service's LOCAL spill root,
as before; shared roots between simultaneously live processes are unsupported.
No new directory, journal, deployment action or background environment reset is
introduced.

## Verification contract

- Red control: a blocked callback prevents independent cleanup in the old queue.
- Reserve exhaustion and concurrent duplicate enqueue/release; all four workers
  blocked without replacement goroutine growth; recovery drains retained work.
- Real Group files with 1-byte disk/1-FD caps: cancellation before first write,
  writer-init failure, nonempty file, exhausted admission and recovery; the
  budget-less compatibility path obeys the same limit.
- JOIN failed creation, empty cancellation, detached writer and payload handoff;
  HashBuild pre-write admission, teardown and payload handoff.
- Startup deletion failure and successful retry on a private local filesystem.
- Normal and race-focused Go tests, plus relevant owning-package suites where
  local resources allow. Environment failures remain failures, not passes.
