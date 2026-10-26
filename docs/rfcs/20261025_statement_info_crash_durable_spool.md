# Issue #30812: crash-durable statement_info audit spool
- Status: in progress
Revision 5, 2026-10-26. Owner: observability (motrace).
Tracking: [issue #30812](https://github.com/matrixorigin/matrixone/issues/30812); implementation [PR #31130](https://github.com/matrixorigin/matrixone/pull/31130) (the PR links this RFC revision).
Revision 5 reverts the revision-4 adaptive gather window after paired before/after sysbench measurement showed it net-negative (revision 4, reverted: adaptive 1 ms linger before crowded leaders' Sync), and records the OLTP scaling law with the honest residual regression bound.
Revision 3 replaced segment-oriented retirement with record→batch ownership tracking and a contiguous-commit export watermark (round-2 review: a batch's acknowledgement cannot cover records sealed into the same segment but owned by a later, still-pending batch), converted spool quota to charge-on-write (round-2 review: a failed Sync must not uncharge bytes that are on disk), and counts quarantined poison files in startup size accounting (round-2 review: the cap must survive restarts). Revision 2 (per-file fsync confirmation, per-segment acks, quote-aware torn-tail/poison — kept) superseded Revision 1 (pre-review implementation).

## Gate, classification and review record

This design document is the mandatory design-first artifact. The change is labeled a fix, but its real scope is a durability feature: ~1,300 production lines added, persistent on-disk state with a recovery protocol, two background workers (replay loop, janitor), a per-statement fsync on the report hot path, and new configuration. Triggers (size: ≥500 production lines; complexity: persistent state, failure-recovery protocol, background long-lived worker, hot-path capacity impact, configuration semantics) are recorded for the review record.

```text
Change scope: whole feature — statement_info crash-durable spool (issue #30812, PR #31130)
Trigger: ≥500 production lines; persistent state; recovery protocol; background workers; hot-path impact; config semantics
Design: docs/rfcs/20261025_statement_info_crash_durable_spool.md, status in progress, revision linked from PR body
Blocking findings: none open after revision 3 repairs (rev2: F1 fsync protocol, F2 ack protocol, F4 torn-tail/poison;
                    rev3: batch/segment misalignment → ownership watermark; quota charge-on-write; poison boot accounting)
Decision log: at-least-once ceiling (no dedup key); rejected durable ack markers on the regular path (evidence below);
              in-memory acks + ack-lag horizon; parenting records to batch ids, contiguous-commit watermark;
              poison = quarantine, never delete, never block replay, counted against the cap at every boot
Decision: PENDING — design phase of PR #31130, recorded approval required before implementation sign-off
Implementation deviations: revision 3 repairs pending design re-approval
```

## Problem, evidence and invariants

`system.statement_info` is the documented statement audit surface. Its pipeline is asynchronous: `StatementInfo.Report` → collector → per-type buffer → batch generation every trace-export-interval (default 15s) or size threshold → `ContentWriter` → `SQLFlusher` (`LOAD DATA INLINE`, one atomic statement per batch; on SQL error a silent CSV-file fallback merged by a ~4 hour cron). Everything before the final write is process memory. An abrupt CN exit (SIGKILL, crash, power loss) destroys the whole staging window.

Observed contract violation (issue #30812), reproduced on upstream main with a 2-CN rig: 50 `INSERT`s committed successfully on CN2 (rows durably readable from CN1 after `kill -9` on CN2); `system.statement_info` shows **0** matching rows immediately after kill and still **0** after 65s. The audit record of a committed statement is lost even though the statement's own data committed.

**Invariant (goal):** once a statement's report has been accepted by the trace pipeline (after the existing report filters), its final export row survives an abrupt process exit on the same node and is eventually visible in `system.statement_info`. Guarantees are **at-least-once** with an explicitly bounded duplicate window (below).

**Non-goals:** logs and errors tables (untouched); cross-node spooling (the spool directory is node-local; a replacement node without it cannot replay — same loss class as before for that topology); dedup or exactly-once (impossible without a dedup key in `statement_info`); covering records the collector itself drops under overload (pre-existing lossy path, unchanged); making the silent CSV fallback timely (out of scope, noted for operators).

**Measurable success criteria:** with the PR-head binary on a deterministic 2-CN rig, 3 rounds of 50 committed INSERTs + `kill -9` yield 50/50 audit rows (FINAL and after a 45s +replay recheck); graceful-exit/restart recovers the pre-shutdown pipeline-gap window; spool on/off throughput is reported honestly (post-repair measurement: see §Performance).

**Owners:** `statementInfoSpool` owns the directory, files, segments, replay and janitor loops. `spoolFsyncGroup` owns the active file handle. The collector, buffers and `ContentWriter` keep their existing ownership; the spool only observes them through two seams (`ReportStatement` hook, buffer `GetBatch` rotation + request wrap). Lock order: `statementInfoSpool.mux` > `spoolFsyncGroup.mu`.

## Standards, precedent and alternatives

Precedent for the mechanism (not copied by reputation): write-ahead staging with **group commit** is the standard shape of durability buffering (pebble's `commitPipeline` batches syncs across writers; bbolt/etcd amortize fdatasync per freelist-and-data flush; Kafka producer `linger.ms`/`batch.size` amortizes broker RPCs). The spool borrows exactly one idea: *many logical records, one fsync, per-file confirmation tracking*. MatrixOne deviations from that precedent: records are CSV export lines (no custom binary format, no length prefix) because the replay path replays THE SAME bytes the exporter would have loaded (byte-identical emission from the same `FillRow` + `encoding/csv` writer); and the destination is SQL (`LOAD DATA INLINE`), not a local index.

Alternatives compared against the status quo:

- **(A) Synchronous per-statement SQL insert** into `statement_info` at statement end. Loses: couples statement latency and correctness to audit-destination availability; contradicts the async trace architecture (queue drops exist exactly to protect the SQL path); turns audit into a per-statement distributed transaction.
- **(B) Persisted ack markers on the regular export path** (durably mark each spool segment exported when its batch commits, matching the replay-side tombstones). **Rejected with evidence:** on the graceful/`777000x` verification legs an export batch was observed to commit while earlier spooled records were still draining through the collector (2s+ gap between the batch's commit and the record's availability). Persisting the ack before the ack-lag horizon lets a kill in that window turn *committed-but-unexported* records into replay-skipped, permanently lost rows. In-memory acks + horizon keep the worst case a bounded duplicate, never a silent loss. (Replay-side tombstones stay: they guard the kill-between-replay-commit-and-removal window, which has no such tradeoff.)
- **(C) Status quo (accept the loss):** the documented audit surface cannot attribute committed DML after abrupt CN exit — the exact defect reported in the issue.

Selected: node-local WAL-style spool with per-record group-commit fsync, record-ownership watermark retirement, boot replay through the SQL path only.

## Logical architecture

### Durable append / group commit (repair F1)

`spoolFsyncGroup` serializes appends to the active segment file behind a `spoolSyncFile{Write,Sync,Close}` seam (production: `*os.File`; tests: fault-injecting stubs). Confirmation is tracked **per file as a byte width** (`confirmedWidth`), initialized to the file's size at adoption (re-opened content was confirmed by its previous writer), never as a generation counter: the revision-1 protocol compared generations whose zero-equal initial state made the wait loop vanish entirely, so records sat un-fsynced until seal (review finding F1, proven by its probe).

`append(line)`:

1. Under `mu`: write the line (single `Write`; a crash can only tear the tail), snapshot its end offset.
2. Wait until `confirmedWidth ≥ end`. The appender that finds no Sync in flight becomes the leader: snapshot `target = fileSize`, drop `mu`, `Sync()`, and on success raise `confirmedWidth` to `target` — **not** to a possibly newer `fileSize` (a write racing the Sync is not covered by it). Followers share the leader's Sync (group commit; under burst load the queued records are confirmed by exactly one following Sync).
3. A waiter whose `sealGen` changed while waiting returns success: the rotation's seal Sync already confirmed its write on that file.
4. A Sync failure settles its generation and is surfaced at the append (pre-seal), one error consumed per waiter; `appendRecord` downgrades that record to the pre-spool memory-only behavior and logs rate-limited (1/min), meters `spool_total{action="error"}`. The statement path is never broken. Quota keeps charging the written bytes (charge-on-write; see §Resource bounds).

`sealFileLocked()` (rotation, shutdown): waits out an in-flight Sync, performs its own Sync covering the whole file, bumps `sealGen`, broadcasts, closes, resets per-file state. First append after boot and after every rotation therefore always pays a fresh fsync on the new file.

### Record ownership and the export watermark (repairs F2, batch/segment misalignment)

Rotation still happens at `GetBatch`: `spooledItemBuffer.GetBatch` rotates the active segment BEFORE the buffer drain generates the batch and wraps the batch in `spoolBatchRequest`, so a kill after rotation keeps everything reported so far durable on disk. Rotation over an empty segment produces no segment. **But rotation boundaries carry no ownership meaning**: `bufferHolder.getGenerateReq` detaches a buffer BEFORE asynchronous generation, so `GetBatch` (and with it the seal) runs arbitrarily later than the records' arrival. A sealed segment holds the detached buffer's records AND records already appended for the next, still-pending batch — revision 2's per-segment ack ("segment == its batch") therefore still loses those next-batch records when a later batch satisfies the lag (round-2 probe: batches A and C commit while B stays pending ⇒ the segment holding B's records was deleted). Retirement must follow the exported *records*, never sequence numbers, newer-batch counts, or elapsed time.

Revision 3 protocol — every record is parented to exactly one batch, and only committed parenthood retires bytes:

- Each appended record takes a spool record sequence; `spoolStatementInfo` stamps it onto the `StatementInfo` before the collector hand-off (write happens-before the queue transfer, same goroutine).
- Each collector buffer instance for the pipe (one future batch) allocates a batch id `K` from a spool-local monotonic counter on its first `Add` (empty buffers never consume ids, so no ghost id can stall the watermark). `spooledItemBuffer.Add` parents the record: `(recordSeq → K)` folds into the owning segment's counters — `taggedCount++`, `maxTag = max(maxTag, K)`. Tags and acks share one id space, so coverage is exact regardless of generation/detach reordering: record exported ⟺ its owning batch committed.
- On batch commit `spoolBatchRequest.Handle` calls `ackBatch(K)`: the committed-id set gains `K` and the export **watermark `W`** advances to the highest id with every id ≤ it committed (contiguous completion — C committing while B is pending does NOT advance past B). A failed `Handle` changes nothing: the stall itself retains every segment owning uncommitted records, for restart replay.
- Segment removal (in-order head-of-line over the sealed ranges) requires ALL of:
  1. `W ≥ seg.maxTag` — every record parented into this file belongs to a committed batch. (A record with tag > W still owns durable bytes here; deleting would repeat revision-1's loss.)
  2. `fullyTagged` (`taggedCount == recordCount`) **or** age ≥ `ackLagDuration` — distinguishes real stragglers (records queued between report and `Add`; bounded by queue residence, far below the duration) from the pre-existing collector-overload drop class (never Added; acknowledged lossy, released only by the duration leg).
  3. the ack-horizon (unchanged): `ackLagSegments` newer settled segments **or** age ≥ `ackLagDuration` — keeps the duplicate window bounded.
- All lifecycle state is memory-only: a kill loses `W`, the committed set and the per-segment counters; every segment still on disk then replays at-least-once. A segment on disk at kill-time is exactly one whose records committed inside the horizon (or not at all) ⟹ the duplicate window is unchanged from revision 2 (observed live: `COUNT=100 / COUNT(DISTINCT statement)=50` for one 50-row round — the horizon batches, never a third copy).
- The ownership seam is narrow: one uint64 field on `StatementInfo` (never serialized/compared), one `Add` hook on the existing wrapper. The collector, aggregator and `ContentWriter` keep their semantics.

### Boot replay, torn-tail and poison (repair F4)

Boot scan registers every leftover `*.spool` as an orphan (oldest first) alongside durable tombstones `<seg>.spool.acked` (written by the replay after its commit, fsync + directory fsync; stray markers purged). `replayOnce` writes each orphan's content through the **SQL path only** (`writeSpoolContent` → the same `LOAD DATA INLINE`, byte-identical SQL, same pool, same metrics as the exporter's SQL flush; **no** silent CSV fallback — that path reports success while deferring visibility to the ~4h merge cron and loses the boot-time "destination not ready" retry, observed on the rig as `invalid state SQL writer's DB conn not ready`). Retry every 20s until the orphans drain; after each replayed segment commits, its tombstone is written, then the file removed — a kill between commit and removal cannot re-insert.

Recovery of crashed content is **quote-aware**, because a CSV field may itself contain newlines (statement texts do), so "the last newline" is not a record boundary (finding F4: truncating at the last newline kept an unterminated quoted fragment and rejected the whole segment's load):

1. `lastCompleteCSVRecordEnd` scans strict `encoding/csv` quoting rules (`"` special only at field start, `""` escape, `\n` terminator with `\r` tolerated) and yields the offset just past the last COMPLETE record. Everything beyond is the torn remnant of an append that was in flight at the kill — never fsync-confirmed as a complete record — and is dropped (log + `spool_total{action="drop"}`).
2. The surviving prefix is fully parsed with `encoding/csv` before loading. A parse failure is **poison**: no torn append can damage a complete record boundary mid-file, so this means media corruption or a foreign write. The segment is renamed to `<name>.spool.poison` (excluded from future orphan scans), logged at error level, metered as drop, counted against the size bound until an operator deletes it — and replay **skips it and continues** with later segments (quarantine never head-of-line blocks, and its records are loudly, not silently, lost). Transient failures (destination not ready, read error) still stop the pass to preserve order for healthy segments.

### State machine (per segment)

`active` → (rotation) → `sealed` → (all records parented, `W ≥ maxTag`, horizon passed) → removed; an export stall or a kill/shutdown with an under-covered segment → boot `orphan` → (SQL commit → tombstone → removed) / (kill mid-removal with tombstone) → `exportedPendingRemove` → removed without re-export / (poison) → `.poison` quarantine (retained, counted against the cap at every boot, never replayed).

### Resource and operational bounds

- Disk: `statement-info-spool-max-size` (default 1 GiB) across all files the directory holds — segments, tombstones (empty) and quarantined poison files; overage drops NEW spool records (statement pipeline unchanged), metered + rate-limited log. One segment ≈ one export cycle (≈ buffer threshold or 15s of traffic). **Charge-on-write accounting:** the append path reserves the record's bytes before writing and converts the reservation into charged bytes as the `Write` lands — an fsync failure keeps the charge (the bytes exist on disk; only bytes never written release the reservation), and startup accounting walks EVERY file in the directory (`.poison` included, excluded only from replay), so the bound holds across I/O errors and restarts alike (round-2 reviews: a Sync failure used to uncharge on-disk bytes; quarantined bytes used to vanish from accounting at boot).
- I/O: worst case one fsync per statement, amortized across concurrent statements by group commit; one fsync-batch per rotation; replay adds one fsync+dirsync per segment tombstone.
- Goroutines: replay loop + janitor (10s) only; both stop at `Shutdown`.
- Memory: appends use a pooled 1 KiB scratch buffer per record render; no per-record heap state.
- Backpressure/failure containment: full spool → degrade to memory-only (pre-change behavior); broken disk → error path per append (rate-limited); poison → quarantine, replay continues.

### Configuration contracts

`observability.disable-statement-info-spool` (default `false`), `observability.statement-info-spool-dir` (default `<data-dir>/statement_info_spool`, wired in `cmd/mo-service/main.go`), `observability.statement-info-spool-max-size` (default `1<<30`). Metrics follow the existing family: `mo_trace_mologger_spool_total{action="write"|"replay"|"drop"|"error"}` (sibling of `mo_trace_mologger_buffer_*`).

## Upgrade, mixed-version, rollback, operations

- **Upgrade:** additive, on-by-default per CN. No catalog or wire change; the spool directory is created lazily.
- **Mixed-version:** CNs on old and new binaries keep per-node spools or none; no interaction. The destination table content is identical in shape (at-least-once duplicates possible horizon-wide after a kill on new CNs, exactly-once-lossy as before on old CNs).
- **Downgrade / rollback:** set `disable-statement-info-spool = true` (in-memory pipeline becomes bit-identical to pre-change). Or downgrade the binary: leftover `*.spool`(`.poison`/`.acked`) files are never read by old binaries; operators may delete the directory. No migration.
- **Failure containment:** the feature can be disabled at runtime-config-reload boundaries without draining; disabling never loses already-persisted segments (they replay on next boot with the feature enabled).
- **Observability/diagnosis:** metrics above; key logs: "spool is full, drop records" (raise max-size or investigate export stalls), "quarantine as poison" (operator forensics; never auto-deleted), "replay segment failed, retry later" (destination not ready; normal at boot), "replayed segment". Alerting suggestion: sustained `action="drop"` > 0.
- **Security / tenancy:** spool files contain statement text with literals — the same exposure class as the existing ETL CSV staging directory; node-local `0644/0755`; no new network surface; a CN's spool holds only statements that CN served (same mixing class as the destination table itself).

## Performance and capacity model

Cost added to the report hot path: one row render (shared with export, plan JSON marshaled once and cached — pinned by UT), one CSV encode (~pooled 1 KiB), one `Write` + shared `Sync`. Serial statements pay one fsync each; concurrent statements share fsyncs (group commit — under load the queue behind an in-flight Sync is confirmed by exactly one more Sync; pinned by UT).

**Measured OLTP impact (sysbench `oltp_read_write`, 8 tables × 100k rows prepared once, 16/32/64 threads, 90 s legs, exact PR-head binary, spool toggled on CN2; rig shared overlay disk, fsync measured 1.125 ms/op):** final ON ≈349 TPS avg at 32 threads (382.8/314.9) vs OFF ≈620 avg (660.0/580.9, −43.8%), ON 316.1 vs OFF 554.8 at 16 (−43.0%), ON 341.7 vs OFF 476.0 at 64 (−28.2%); single-client inserts 163/s vs 167/s (−2.4%). Mechanism: statement rate on slow storage caps at `fsync_rate × achieved_sharing` (≈890 sync/s × ~7 on this rig) and every statement adds up to ~fsync-scale latency that multiplies over ~20 statements per OLTP txn at fixed concurrency; the 32→64 narrowing is sharing growing with arrival rate. A revision-4 gather window (adaptive 1 ms linger) was implemented and measured ≈290 TPS at 32 — net-negative at fixed concurrency (the +L/2 per-statement latency dominates the group gain once sessions are compute-bound) — and REVERTED (revision 5).

**Honest scaling law:** the residual regression is proportional to the spool volume's fsync latency. ≈500 µs ⇒ ≈2× the final ON numbers above; ≈100 µs (local-NVMe class) ⇒ ≈10×, i.e. single-digit-% regression class at the same concurrency. This rig's shared overlay disk is worst-case-ish; per-statement audit durability has an irreducible fsync-share cost on any storage. The remaining lever for a storage-bound deployment is the documented `disable-statement-info-spool` feature gate.

Capacity: head-of-line blocked removals (stuck export) grow the directory until the 1 GiB cap, then degrade to memory-only with metric+log — blast radius bounded to audit durability, never to serving SQL.

## Verification mapping

Cheapest deterministic check per invariant (fault injection, fake clocks, in-memory files — no sleeps where injection works):

| Invariant | Check |
|---|---|
| append never returns un-fsynced (incl. first append after boot/rotation) | `TestSpoolFsyncGroupAppendSurfacesSyncError` (Sync-failing file, error surfaces pre-seal), `TestSpoolFsyncGroupAppendSyncFailsOnRealPipe` (EINVAL real fd), `TestSpoolFsyncGroupFreshFileRequiresNewSync`, `TestStatementInfoSpoolAppendFsyncBeforeAck` (Sync counted before seal AND before ack) |
| group commit amortizes + racing write not covered by in-flight Sync | `TestSpoolFsyncGroupCommitSharesSync` (3 records, exactly 2 Syncs) |
| waiter across seal succeeds once, via seal Sync | `TestSpoolFsyncGroupWaiterAcrossSealSucceeds` (gated Sync, channels) |
| spool error path degrades, no panic, written bytes stay charged | `TestStatementInfoSpoolAppendSyncFailureDegrades` (Sync failure: recorded charge retained, rate-limit, no panic; quota control still bounded), `TestSyncErrorMustNotUnchargeWrittenBytes` (round-2 probe: repeated Sync failure over a real file, disk usage tracks accounting), `TestStatementInfoSpoolErrorPaths` |
| retirement follows exported records; misalignment cannot lose a pending batch | `TestBatchCommitMustCoverEverySpoolRecord` (round-2 probe verbatim: A=true, B=false, C=true ⇒ the A/B file survives), `TestStatementInfoSpoolWatermarkOutOfOrderFuzz`, `TestStatementInfoSpoolAckUnknownBatchSettlesNothing`, `TestStatementInfoSpoolPipelineGapReplayed` (crash ⇒ replay lands the record) |
| failed/stalled batch retains everything it owns | `TestStatementInfoSpoolStalledWatermarkKeepsSegments` (failure ⇒ no watermark progress ⇒ files stay for restart replay) |
| torn tail at real CSV boundaries | `TestLastCompleteCSVRecordEnd` (12-case table), `TestRecoverSpoolSegmentContent` (multiline quoted probe + parse-cleanliness), `TestStatementInfoSpoolReplayDropsTornTail` |
| poison quarantined, never blocks replay, counted at every boot | `TestStatementInfoSpoolReplayPoisonSkippedNotBlocking`, `TestPoisonBytesSurviveQuotaRestart` (round-2 probe verbatim: 90 poison bytes +100-byte cap ⇒ only 10 free) |
| replay semantics: order, retry, tombstone, no-dup, no CSV fallback | `TestStatementInfoSpoolCrashRecoveryReplay`, `…ReplayFailureKeepsSegment`, `…ExportedPendingRemove`, `…AckMarkerPreventsReplayDup`, `…StartReplayRetryLoop`, `TestWriteSpoolContent` |
| bounds/janitor/lifecycle | `TestStatementInfoSpoolMaxSizeBound`, janitor/removal subtests, `TestStatementInfoSpoolDoubleClose`, lifecycle suite, `BenchmarkStatementInfoSpoolRecord` |
| end-to-end (live, exact PR-head binary, sha256 recorded) | 2-CN rig: 3× kill −9 rounds ×50 INSERTs FINAL 50/50 + 45s recheck; graceful restart ⇒ pipeline-gap recovery; post-flush ⇒ nothing re-inserted; inside-horizon kill ⇒ bounded dup; concurrent-burst kill leg (8 sessions × 25, kill mid-burst ⇒ `COUNT ≤ 2× DISTINCT`, `DISTINCT == committed`); failed-DML control; spool on/off perf |

Coverage gate for the package stays >75%; `gofmt`/`go vet`/staticcheck U1000 clean.

## Acceptance criteria, drawbacks, open questions

Acceptance: all checks above at the PR head; live evidence comment with binary sha256; reviewers' sign-off on this revision; then implementation review re-runs against this design.

Drawbacks (accepted, recorded): at-least-once duplicates inside the horizon after a kill (no dedup key exists); page-cache-class durability only until each fsync (by contract: append returns only after fsync); node-local recovery only; poison quarantine requires operator cleanup while counting toward the cap; a stuck export stalls segment retirement until the cap (observable, bounded).

Open questions (non-blocking): whether poison files should expire automatically (owner: observability; decision point: next observability review); whether ack-lag should be operator-tunable beyond the derived default (no knob until a need is evidenced).

## Decision log

- At-least-once ceiling accepted: `statement_info` has no PK/dedup key; exactly-once would require schema change + dedup — rejected as disproportionate for an audit surface.
- Durable ack markers on the regular export path REJECTED (alternative B) with live evidence (grace/777000x legs: batch commit preceding still-draining records by 2s+); replay-side tombstones kept (no such tradeoff there).
- In-memory acks + derived ack-lag horizon (segments + duration) accepted; kill cost = bounded duplicate window, observed live as one most-recent batch.
- Per-segment acks + in-order head-of-line removal over out-of-order completion (revision 2, review F2), SUPERSEDED in revision 3: even per-segment ownership was wrong — seal boundaries drift from batch boundaries (detached-buffer generation). Retirement now follows record→batch parenthood + contiguous-commit watermark (round-2 review); the horizon/dup window is unchanged, and batch failure/stall retains everything owned for restart replay with no explicit failed-flag.
- Per-file byte-width fsync confirmation over generation counters (revision 2, review F1).
- Charge-on-write quota accounting: written bytes stay charged across fsync failures; quarantined poison bytes counted at every boot (revision 3, round-2 reviews).
- Quote-aware CSV record recovery + poison quarantine (revision 2, review F4; fail-loud, never auto-delete, never block replay).
- Import-cycle constraint: the replay SQL flush duplicates `etl.SQLFlusher.FlushBuffer`'s SQL byte-for-byte on the `db_holder` pool instead of importing `etl` (`etl`'s tests import `motrace`; the import must not exist in either direction for test builds). Kept under a comment with the identity contract; a follow-up extraction shared by both is welcome but not blocking.
- Adaptive gather window: implemented, measured pre/post on identical sysbench legs, and REVERTED as net-negative (revision 5; +L/2 per serial statement at fixed concurrency outweighs the group-size gain in the compute-bound OLTP regime; ≈290 vs ≈349 TPS at 32 threads on the rig's slow disk). Recorded so the idea is not re-litigated without materially new evidence (e.g. a different WAL write path or an fsync-batched report design).
- OLTP residual regression accepted and bounded (performance review): −43% TPS class at 16–32 threads on a 1.125 ms-fsync shared disk, mechanism-explained and inverse-proportional to fsync latency (single-digit-% class on local NVMe), with the documented opt-out for storage-bound deployments. The alternative — silently weakening the kill-durability contract — is rejected.
