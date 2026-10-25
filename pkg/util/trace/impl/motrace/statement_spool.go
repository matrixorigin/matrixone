// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package motrace

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// This file implements the crash-durable spool (write-ahead staging) for
// system.statement_info records. The owning design document is
// docs/rfcs/20261025_statement_info_crash_durable_spool.md (issue #30812).
//
// Background: statement records are staged in memory by the collector and are
// flushed to the destination every trace-export-interval (default 15s), or
// when the buffer size threshold is reached. An abrupt process exit (SIGKILL,
// crash, node power loss) in that window destroys every staged record, so a
// committed DML which had returned success to its client ends up with no row
// in the documented audit surface (system.statement_info). Reported as
// issue #30812.
//
// Design
//
//   - At statement end (ReportStatement, after the existing report filters and
//     before the hand-off to the collector) the final export row is rendered
//     once, encoded in the exact CSV export format, and appended to the
//     process-local spool file; the append returns only after an fsync of the
//     CURRENT file covered the record (group commit under spoolFsyncGroup:
//     concurrent statements share one fsync; confirmation is tracked as a
//     per-file byte width, so the first append after boot and after every
//     rotation always pays for a fresh fsync). From this point the record
//     survives a process kill.
//   - The spool is a sequence of segment files. The statement exporter rotates
//     the active segment whenever an export batch is generated (one batch ==
//     one segment of spooled records). When an export batch is durably
//     committed its segment is acked in memory; a segment becomes removable
//     once it settled AND the ack-lag guard cleared it.
//   - Retirement follows the confirmed completion of the segment's own
//     records, never sequence numbers: MOCollector runs multiple doExport
//     workers, so batch completion order is not guaranteed. ackSegment(seq)
//     settles only that segment's batch; removal walks the sealed segments in
//     order and stops at the first unsettled one (a segment awaiting its own
//     ack, or a failed one, head-of-line blocks newer removals — bounded by
//     the spool size cap).
//   - A segment sealed at an export tick whose buffer drain produced no batch
//     holds records that were in flight between the spool append and the
//     collector. No export request exists for it (flagged noBatch); its
//     records ship inside a later batch within one aggregation window, so it
//     becomes removable through the ack-lag guard alone.
//   - At process start, every spool segment still on disk is replayed through
//     the SQL export path (the same LOAD DATA INLINE destination the exporter
//     uses) and removed only after the replayed content is committed at the
//     destination. A segment is never removed before its content is confirmed;
//     a flush failure keeps the segment for the next retry, there is no
//     silent CSV fallback on replay (see writeSpoolContent).
//   - Replay is quote-aware CSV recovery (see recoverSpoolSegmentContent): a
//     crash can only tear the tail record (records are single-Write lines),
//     but a quoted CSV field may itself contain newlines, so truncation stops
//     at the end of the last COMPLETE record, not the last newline byte. The
//     surviving prefix is fully parsed before loading; a segment whose
//     complete prefix does not parse is quarantined (<name>.spool.poison)
//     and skipped without blocking the replay of later segments.
//   - The size of the spool directory is bounded; once the bound is exceeded
//     newly reported records are dropped from the spool (the in-memory
//     pipeline keeps its old behavior) and the loss is logged and metered.
//
// Failure semantics (explicitly documented):
//   - At-least-once: a record that reached the spool is eventually exported,
//     even across an abrupt process exit, as long as the local spool directory
//     survives (same-node restart; a replacement node without the directory
//     cannot replay it).
//   - Duplicates stay bounded: a process kill loses the in-memory ack state,
//     so on restart the replay re-exports every segment that was acked but
//     not yet removed when the process died. With the ack-lag guard that is
//     at most the export batches committed inside the last ack-lag horizon
//     (aggregation window + export interval + margin, typically one or two
//     batches). Their rows are re-inserted; statement_info has no dedup key,
//     so at-least-once is the strongest guarantee achievable here. The
//     horizon never resurrects older batches: acked segments older than the
//     horizon are removed in run, so nothing remains to replay.
//   - A segment is acked only when its own export batch is confirmed
//     committed; the ack is then guarded by the ack lag (see ackLagSegments)
//     before the segment is removed, which also covers records that were
//     still draining through the collector pipeline when their batch fired:
//     such a late record is replayed from its own, still un-removed segment
//     (or from the noBatch segment holding it). No ack state is persisted
//     ahead of that guard, so a kill can never turn an un-exported record
//     into a permanently lost one.
//   - A poison segment (mid-file CSV corruption, never producible by a torn
//     append) is quarantined for forensics and keeps occupying the size
//     bound; its records are lost. This is a deliberate fail-loud choice.
//   - Records discarded by the in-memory pipeline under extreme overload
//     (collector queue drops) remain lossy, exactly as before; the spool only
//     covers the at-rest buffering window.
//
// The spool only stages statement_info records (the audit surface); logs and
// errors are untouched.
type statementInfoSpool struct {
	dir     string
	maxSize int64

	// ackLagSegments tells how many newer export batches must be acked before
	// an acked segment is removed. Aggregation may delay the export of a
	// record until one export cycle later, so a segment is only safe to drop
	// after ackLagSegments batches newer than its own have been committed.
	ackLagSegments uint64

	// ackLagDuration is the time-based fallback of ackLagSegments: an acked
	// segment older than this duration is removed even if no newer batches
	// have been generated (covers the cluster-going-quiet case).
	ackLagDuration time.Duration

	// replayRetryInterval controls how often left-over replay segments are
	// retried while the destination (table / db connection) is not ready yet.
	replayRetryInterval time.Duration
	// janitorInterval controls how often removable segments are cleaned.
	janitorInterval time.Duration

	// now, for tests
	now func() time.Time

	mux sync.Mutex
	// active is the segment currently being appended.
	active *spoolSegment
	// fg groups concurrent appends around shared fsyncs; it owns the active
	// file handle.
	fg *spoolFsyncGroup
	// sealed holds rotated segments that still wait for their export ack,
	// keyed by seq, oldest first.
	sealed    map[uint64]*spoolSegment
	nextSeq   uint64
	totalSize int64

	// orphan segments recovered from the previous process run, replayed in
	// sequence order.
	orphans []*spoolSegment

	// drop counters (under mux)
	droppedRecords int64
	lastDropLog    time.Time
	lastErrLog     time.Time

	// replay lifecycle
	replayStop  chan struct{}
	replayDone  chan struct{}
	janitorStop chan struct{}
	janitorDone chan struct{}
	closeOnce   sync.Once
	// started marks startReplay/startJanitorOnly ran; close() must not wait
	// for jobs which never started.
	replayStarted  bool
	janitorStarted bool

	// closed marks the spool as unusable (Shutdown)
	closed bool
}

const (
	spoolFilePrefix = "statement_info_"
	spoolFileSuffix = ".spool"
)

// spoolSyncFile is the file seam of spoolFsyncGroup: *os.File in production,
// a Sync-counting/-failing stub in fault-injection tests.
type spoolSyncFile interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// spoolFsyncGroup serializes appends to the current spool segment file and
// amortizes fsyncs across concurrent appenders (group commit). An appender
// returns only after an fsync of the CURRENT file confirmed its record, so
// the fsync-per-record durability contract from the design holds while
// concurrent statements share one fsync instead of serializing on it.
//
// The protocol is per-file: confirmation is a byte width (confirmedWidth),
// not a generation number, so the first append after boot and the first
// append after every rotation must lead (or join) a fresh Sync on the file
// they just wrote to — a generation counter that happens to compare equal on
// a fresh file would silently skip the fsync (round-1 review defect).
//
// Lock order: statementInfoSpool.mux > spoolFsyncGroup.mu.
type spoolFsyncGroup struct {
	mu   sync.Mutex
	cond sync.Cond

	f spoolSyncFile
	// fileSize counts bytes written into the current file.
	fileSize int64
	// confirmedWidth is the prefix of the current file a completed Sync is
	// known to have covered. It starts at the file's size at adoption
	// (re-opened content was confirmed by its original writer) and is raised
	// only to the fileSize snapshot taken BEFORE the Sync started: a write
	// racing an in-flight Sync is not covered by that Sync.
	confirmedWidth int64
	// syncing marks one appender leading a Sync.
	syncing bool
	// sealGen is bumped by sealFileLocked after its final Sync confirmed the
	// whole file, right before closing it. An append waiter whose sealGen
	// changed under it returns success: the seal Sync covered its write.
	sealGen uint64

	// syncErr keeps a pending fsync error to report back to an appender.
	syncErr   error
	terminate bool
}

func (g *spoolFsyncGroup) init() {
	g.cond.L = &g.mu
}

// adoptLocked makes f the group's current file. Content already present in f
// (a re-opened segment) was confirmed by its previous writer, so the
// confirmed width starts at the file's size: only NEW bytes must be fsynced.
// Call with g.mu held, with no Sync in flight.
func (g *spoolFsyncGroup) adoptLocked(f spoolSyncFile, size int64) {
	g.f = f
	g.fileSize = size
	g.confirmedWidth = size
}

// append writes line to the current file and blocks until an fsync of that
// file confirmed it (its own or a shared one). The durability contract:
// append must never leave a record un-fsynced on the file that holds it.
func (g *spoolFsyncGroup) append(line []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.terminate || g.f == nil {
		return 0, moerr.NewInternalErrorNoCtx("statement_info spool file is closed")
	}
	myGen := g.sealGen
	n, err := g.f.Write(line)
	if err != nil {
		return n, err
	}
	g.fileSize += int64(n)
	end := g.fileSize
	for g.confirmedWidth < end {
		if g.terminate || g.sealGen != myGen {
			// The file I wrote to was sealed (or the group is going down
			// after sealing it): the seal's final Sync confirmed my write.
			break
		}
		if !g.syncing {
			// lead the fsync: it confirms everything written up to target.
			// A write landing while the Sync runs is NOT covered by it and
			// keeps confirmedWidth behind fileSize.
			target := g.fileSize
			f := g.f
			g.syncing = true
			g.mu.Unlock()
			syncErr := f.Sync()
			g.mu.Lock()
			g.syncing = false
			if syncErr != nil && g.syncErr == nil {
				g.syncErr = syncErr
			}
			// Settle the generation even on error: the Sync attempt for
			// these bytes is finished, so waiters must leave the loop and
			// read the error from syncErr instead of re-leading a failing
			// disk forever.
			if target > g.confirmedWidth {
				g.confirmedWidth = target
			}
			g.cond.Broadcast()
		} else {
			g.cond.Wait()
		}
	}
	if err := g.syncErr; err != nil {
		g.syncErr = nil
		return n, err
	}
	return n, nil
}

// sealFileLocked sync-confirms and closes the current file. Every record
// appended before sealFileLocked acquired the lock is durable before the file
// is closed. Call with g.mu held.
func (g *spoolFsyncGroup) sealFileLocked() (int64, error) {
	if g.f == nil {
		return 0, nil
	}
	// wait out an in-flight fsync: the appender leading it is covered by the
	// seal Sync below, and nobody else can be mid-write while we hold the lock.
	for g.syncing {
		g.cond.Wait()
	}
	size := g.fileSize
	if err := g.f.Sync(); err != nil && g.syncErr == nil {
		g.syncErr = err
	} else if err == nil {
		g.confirmedWidth = size
	}
	// Wake append waiters of this file before the handle goes away: the Sync
	// above covered their writes, so they must observe the generation bump
	// and return success (an error is reported through syncErr instead).
	g.sealGen++
	g.cond.Broadcast()
	var closeErr error
	if err := g.f.Close(); err != nil {
		closeErr = err
	}
	g.f = nil
	g.fileSize = 0
	g.confirmedWidth = 0
	return size, closeErr
}

// close terminates the group: no further appends are accepted.
func (g *spoolFsyncGroup) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.terminate = true
	g.cond.Broadcast()
}

// spoolSegment describes one sealed or active spool segment file.
type spoolSegment struct {
	seq    uint64
	name   string
	size   int64
	sealed time.Time
	done   bool
	// failed marks a segment whose export failed at least once. It is kept
	// for the restart replay, same as a never-acked segment.
	failed bool
	// noBatch marks a segment sealed at an export tick whose buffer drain
	// produced no batch (GetBatch returned nil or a non-export shape): the
	// segment has records on disk but no export request exists for it, so no
	// ack can ever arrive. Its records demonstrably ship inside a later
	// batch (they were in flight between the spool append and the
	// collector, and ship within one aggregation window), so the segment
	// becomes removable through the ack-lag guard alone.
	noBatch bool
	// exportedPendingRemove is set once the segment's content has been
	// confirmed exported — either by a replay in this process run, or by an
	// earlier run which left a replay tombstone next to the segment file
	// (see spoolAckFileSuffix) — while the segment file still exists: the
	// next replay pass removes the file without re-exporting it, so no
	// exported row is duplicated.
	exportedPendingRemove bool
}

func spoolSegmentFileName(seq uint64) string {
	return fmt.Sprintf("%s%016d%s", spoolFilePrefix, seq, spoolFileSuffix)
}

// spoolAckFileSuffix extends a segment file name to the replay tombstone: a
// durable proof, written right after a replayed batch was committed at the
// destination, that the file content above it must not be re-exported. A
// kill between the replay commit and the segment removal would otherwise
// re-insert the batch on the next boot. Ack state of the regular export
// path is intentionally NOT persisted (see ackSegment).
const spoolAckFileSuffix = ".acked"

func spoolAckFileName(segName string) string {
	return segName + spoolAckFileSuffix
}

// writeSpoolAckMarker durably marks one segment as exported: the marker file
// is written and fsynced, then the directory is fsynced so the file name
// itself survives a crash. One marker costs one fsync per export batch
// (one batch spans many records), which is negligible next to the batch's
// own LOAD DATA INLINE.
func writeSpoolAckMarker(dir, segName string) error {
	path := filepath.Join(dir, spoolAckFileName(segName))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return moerr.ConvertGoError(context.Background(), err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return moerr.ConvertGoError(context.Background(), syncErr)
	}
	if closeErr != nil {
		return moerr.ConvertGoError(context.Background(), closeErr)
	}
	d, err := os.Open(dir)
	if err != nil {
		return moerr.ConvertGoError(context.Background(), err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return moerr.ConvertGoError(context.Background(), err)
	}
	return nil
}

// parseSpoolSegmentFileName extracts the segment sequence from a spool file
// name. Return ok == false if the name is not a spool segment file.
func parseSpoolSegmentFileName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, spoolFilePrefix) || !strings.HasSuffix(name, spoolFileSuffix) {
		return 0, false
	}
	mid := name[len(spoolFilePrefix) : len(name)-len(spoolFileSuffix)]
	seq, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// defaultStatementInfoSpoolMaxSize bounds the disk usage of the spool for
// direct users of this package; the production wiring passes the value from
// the observability configuration.
const defaultStatementInfoSpoolMaxSize = int64(1) << 30 // 1 GB

// newStatementInfoSpool opens (creating if needed) the statement_info spool
// rooted in dir. Any segment left by a previous process run is registered as
// an orphan; orphans are replayed by startReplayLoop.
//
// maxSize bounds the total disk usage of the spool. ackLagSegments,
// ackLagDuration guard the segment removal against pending aggregation.
func newStatementInfoSpool(dir string, maxSize int64, ackLagSegments uint64, ackLagDuration time.Duration) (*statementInfoSpool, error) {
	if maxSize <= 0 {
		maxSize = defaultStatementInfoSpoolMaxSize
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, moerr.ConvertGoError(context.Background(), err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, moerr.ConvertGoError(context.Background(), err)
	}
	s := &statementInfoSpool{
		dir:                 dir,
		maxSize:             maxSize,
		ackLagSegments:      ackLagSegments,
		ackLagDuration:      ackLagDuration,
		replayRetryInterval: defaultReplayRetryInterval,
		janitorInterval:     defaultJanitorInterval,
		now:                 time.Now,
		fg:                  &spoolFsyncGroup{},
		sealed:              make(map[uint64]*spoolSegment),
		replayStop:          make(chan struct{}),
		replayDone:          make(chan struct{}),
		janitorStop:         make(chan struct{}),
		janitorDone:         make(chan struct{}),
	}
	s.fg.init()
	var maxSeq uint64
	hasSegments := false
	// ackedNames are the persistent ack markers left by a previous run: the
	// segment's export batch was committed at the destination, so the segment
	// must be removed without re-exporting it. Two passes: the marker file
	// name sorts after its segment file name, so the map must be complete
	// before any orphan is built.
	ackedNames := make(map[string]bool)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), spoolAckFileSuffix) {
			ackedNames[e.Name()] = true
		}
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseSpoolSegmentFileName(e.Name())
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		orphan := &spoolSegment{
			seq:    seq,
			name:   e.Name(),
			size:   info.Size(),
			sealed: s.now(),
			// an orphan whose ack marker survived never re-exports, it is
			// only awaiting removal.
			exportedPendingRemove: ackedNames[spoolAckFileName(e.Name())],
		}
		s.orphans = append(s.orphans, orphan)
		s.totalSize += info.Size()
		if seq >= maxSeq {
			maxSeq = seq
		}
		hasSegments = true
	}
	// drop markers whose segment file is already gone: a leftover marker
	// without a segment carries no information and would linger forever.
	for name := range ackedNames {
		segName := strings.TrimSuffix(name, spoolAckFileSuffix)
		if _, err := os.Stat(filepath.Join(dir, segName)); os.IsNotExist(err) {
			if rmErr := os.Remove(filepath.Join(dir, name)); rmErr != nil && !os.IsNotExist(rmErr) {
				logutil.Warn("statement_info spool remove stray ack marker failed",
					zap.String("file", name), zap.Error(rmErr))
			}
		}
	}
	// replay the orphans oldest first: spool records must not be reordered
	// across segment boundaries.
	sort.Slice(s.orphans, func(i, j int) bool { return s.orphans[i].seq < s.orphans[j].seq })
	if hasSegments {
		s.nextSeq = maxSeq + 1
	} else {
		s.nextSeq = 1
	}
	if err := s.openActiveLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

const (
	defaultReplayRetryInterval = 20 * time.Second
	defaultJanitorInterval     = 10 * time.Second
)

// openSegmentFile opens one segment file and returns it with its size.
func openSegmentFile(dir string, seq uint64) (*os.File, int64, error) {
	fp := filepath.Join(dir, spoolSegmentFileName(seq))
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, moerr.ConvertGoError(context.Background(), err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, moerr.ConvertGoError(context.Background(), err)
	}
	return f, info.Size(), nil
}

// openSpoolSegmentFile is the segment-file factory used by the spool, a
// variable so fault-injection tests can wrap the returned file (Sync
// failure / Sync counting) without monkey-patching the filesystem.
var openSpoolSegmentFile = func(dir string, seq uint64) (spoolSyncFile, int64, error) {
	return openSegmentFile(dir, seq)
}

// openActiveLocked creates the next active segment file.
func (s *statementInfoSpool) openActiveLocked() error {
	f, size, err := openSpoolSegmentFile(s.dir, s.nextSeq)
	if err != nil {
		return err
	}
	s.fg.mu.Lock()
	s.fg.adoptLocked(f, size)
	s.fg.mu.Unlock()
	s.active = &spoolSegment{
		seq:    s.nextSeq,
		name:   spoolSegmentFileName(s.nextSeq),
		size:   size,
		sealed: s.now(),
	}
	s.nextSeq++
	return nil
}

// appendRecord makes one already-encoded record line durable.
//
// The write is a single Write call, so process termination can only leave
// whole lines behind. The fsync per call bounds the loss window of a process
// kill to "the statement whose report is in flight right now" and keeps the
// audit-durability contract independent of OS page-cache behavior.
//
// On any failure the record falls back to the pre-spool (in-memory only)
// behavior; the failure must never break the statement path.
func (s *statementInfoSpool) appendRecord(line []byte) {
	if len(line) == 0 {
		return
	}
	s.mux.Lock()
	if s.closed {
		s.mux.Unlock()
		return
	}
	if s.totalSize+int64(len(line)) > s.maxSize {
		s.droppedRecords++
		v2.TraceMOLoggerSpoolDropCounter.Inc()
		now := s.now()
		if now.Sub(s.lastDropLog) > time.Minute {
			s.lastDropLog = now
			logutil.Error("statement_info spool is full, drop records from spool (kept in memory only)",
				zap.Int64("dropped records accumulates", s.droppedRecords),
				zap.Int64("spool total size", s.totalSize),
				zap.Int64("spool max size", s.maxSize))
		}
		s.mux.Unlock()
		return
	}
	if s.active == nil {
		if err := s.openActiveLocked(); err != nil {
			s.mux.Unlock()
			v2.TraceMOLoggerSpoolErrorCounter.Inc()
			s.logErrRateLimited("statement_info spool open active segment failed", err)
			return
		}
	}
	// reserve the quota before the (possibly shared) fsync wait so concurrent
	// appenders observe a tight bound; undo on failure.
	s.totalSize += int64(len(line))
	s.mux.Unlock()

	n, err := s.fg.append(line)
	if err != nil {
		s.mux.Lock()
		s.totalSize -= int64(len(line))
		if s.totalSize < 0 {
			s.totalSize = 0
		}
		s.mux.Unlock()
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		s.logErrRateLimited("statement_info spool append record failed", err)
		return
	}
	_ = n
	v2.TraceMOLoggerSpoolWriteCounter.Inc()
}

// logErrRateLimited logs an I/O error at most once a minute: a broken spool
// disk must not turn every statement into a log line.
func (s *statementInfoSpool) logErrRateLimited(msg string, err error) {
	now := s.now()
	if now.Sub(s.lastErrLog) > time.Minute {
		s.lastErrLog = now
		logutil.Error(msg, zap.Error(err))
	}
}

// sealForExport rotates the active segment: the current active segment becomes
// a sealed segment that maps 1:1 with the export batch being generated, and a
// fresh active segment is opened. Returns the sealed segment; nil if the
// active segment holds no record. The file swap is atomic against appends,
// so a record can neither land in a sealed file after its batch was captured
// nor fall between two files.
func (s *statementInfoSpool) sealForExport() *spoolSegment {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed || s.active == nil {
		return nil
	}
	s.fg.mu.Lock()
	if s.fg.f == nil || s.fg.fileSize == 0 {
		s.fg.mu.Unlock()
		return nil
	}
	size, sealErr := s.fg.sealFileLocked()
	if sealErr != nil {
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		logutil.Error("statement_info spool close active segment failed", zap.Error(sealErr))
	}
	var f spoolSyncFile
	var nextSize int64
	openErr := error(nil)
	f, nextSize, openErr = openSpoolSegmentFile(s.dir, s.nextSeq)
	if openErr != nil {
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		logutil.Error("statement_info spool open new segment failed", zap.Error(openErr))
	}
	if openErr == nil {
		s.fg.adoptLocked(f, nextSize)
	}
	s.fg.mu.Unlock()

	seg := s.active
	seg.size = size
	seg.sealed = s.now()
	s.sealed[seg.seq] = seg
	if openErr != nil {
		// the next append reopens lazily; meanwhile the rotated segment is
		// on disk and tracked, so no record is stranded.
		s.active = nil
	} else {
		s.active = &spoolSegment{
			seq:    s.nextSeq,
			name:   spoolSegmentFileName(s.nextSeq),
			size:   nextSize,
			sealed: s.now(),
		}
	}
	s.nextSeq++
	logutil.Debug("statement_info spool seal segment",
		zap.Uint64("seq", seg.seq), zap.Int64("size", seg.size))
	return seg
}

// ackSegment marks the export batch of one segment as committed, then removes
// every segment that is safely covered. Also see markExportFailed for the
// failure case.
//
// The ack settles ONLY the segment whose own batch committed: MOCollector
// runs multiple doExport workers, so the completion order of consecutive
// batches is not guaranteed — settling every older segment here could remove
// a segment whose own batch is still in flight, turning its records into a
// permanent loss on a following kill.
//
// The ack is deliberately in-memory only: persisting it ahead of the ack lag
// would let a kill between the batch commit and the lag horizon turn records
// that were still draining through the collector pipeline into un-replayed,
// permanently lost rows (see the file header). The kill costs at most the
// re-export of the batches inside the horizon.
func (s *statementInfoSpool) ackSegment(seq uint64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if seg, ok := s.sealed[seq]; ok && seg != nil {
		seg.done = true
	} else {
		// Either a duplicate Handle completion or the segment was already
		// removed; both are benign, but neither may settle another segment.
		logutil.Debug("statement_info spool ack for unknown segment", zap.Uint64("seq", seq))
	}
	s.collectRemovableLocked()
}

// markNoBatch flags a sealed segment whose export tick produced no batch for
// it: no request was generated, so no ackSegment will ever reference it. Its
// removal relies on the ack-lag guard alone (see collectRemovableLocked).
func (s *statementInfoSpool) markNoBatch(seq uint64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if seg, ok := s.sealed[seq]; ok && seg != nil {
		seg.noBatch = true
	}
}

// markExportFailed keeps the segment; its content is replayed after restart,
// or by the periodic retry. The exporter itself already logged the failure.
func (s *statementInfoSpool) markExportFailed(seq uint64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if seg, ok := s.sealed[seq]; ok && seg != nil {
		seg.failed = true
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		logutil.Warn("statement_info spool keeps segment after export failure, wait for replay",
			zap.Uint64("seq", seq))
	}
}

// collectRemovableLocked removes the head of the sealed list while every
// segment in it (a) settled — its own export batch committed (done) or it
// never had one (noBatch) — (b) is not failed, and (c) clears the ack-lag
// guard: followed by ackLagSegments newer acked segments or older than
// ackLagDuration. Removal stays in sequence order: a segment waiting for its
// own ack (or a failed one) head-of-line blocks all newer removals until it
// settles or the process restarts, which the spool size bound keeps finite.
func (s *statementInfoSpool) collectRemovableLocked() {
	now := s.now()
	seqs := make([]uint64, 0, len(s.sealed))
	for seq := range s.sealed {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		seg := s.sealed[seq]
		if seg == nil || seg.failed {
			return
		}
		if !seg.done && !seg.noBatch {
			// still waiting for its own batch's ack: a gap stops the
			// cleanup, later segments wait for the next round.
			return
		}
		if !s.lagSatisfiedLocked(seqs, seq, now) {
			return
		}
		s.removeSegmentLocked(seg)
	}
}

// lagSatisfiedLocked checks the aggregation lag guard for one settled
// segment: ackLagDuration reached, or ackLagSegments newer batches have
// committed. A noBatch segment relies on this guard alone: its records were
// in flight between the spool append and the collector when the export tick
// generated no batch for them, and they ship inside a later batch within one
// aggregation window — which the newer-acked count (or the duration fallback
// of a cluster gone quiet) waits out. Only acked (done) newer segments count
// toward the guard: a noBatch neighbor proves nothing about a batch commit.
func (s *statementInfoSpool) lagSatisfiedLocked(seqs []uint64, seq uint64, now time.Time) bool {
	seg := s.sealed[seq]
	if seg == nil {
		return false
	}
	if s.ackLagDuration > 0 && now.Sub(seg.sealed) >= s.ackLagDuration {
		return true
	}
	// count newer segments whose own batch committed
	var newerAcked uint64
	for _, newer := range seqs {
		if newer <= seq {
			continue
		}
		other := s.sealed[newer]
		if other == nil || !other.done || other.failed {
			break
		}
		newerAcked++
	}
	return newerAcked >= s.ackLagSegments
}

// removeSegmentLocked deletes one segment file (and its ack marker, if any)
// and forgets about it. The segment file goes before the marker: removing
// the marker first would open a window in which a kill leaves an exported
// segment without its proof of export.
func (s *statementInfoSpool) removeSegmentLocked(seg *spoolSegment) {
	path := filepath.Join(s.dir, seg.name)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		logutil.Error("statement_info spool remove segment failed",
			zap.String("file", path), zap.Error(err))
		return
	}
	removeSpoolAckFile(s.dir, seg.name)
	s.totalSize -= seg.size
	if s.totalSize < 0 {
		s.totalSize = 0
	}
	delete(s.sealed, seg.seq)
}

// removeSpoolAckFile deletes the ack marker of one segment, if present.
func removeSpoolAckFile(dir, segName string) {
	path := filepath.Join(dir, spoolAckFileName(segName))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		logutil.Error("statement_info spool remove ack marker failed",
			zap.String("file", path), zap.Error(err))
	}
}

// runJanitor periodically removes acked segments even in quiet periods, when
// no new export arrives to trigger the inline cleanup.
func (s *statementInfoSpool) runJanitor() {
	defer close(s.janitorDone)
	ticker := time.NewTicker(s.janitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.janitorStop:
			return
		case <-ticker.C:
			s.mux.Lock()
			if !s.closed {
				s.collectRemovableLocked()
			}
			s.mux.Unlock()
		}
	}
}

// replaySegment writes one orphan segment's content through the SQL export
// path (LOAD DATA INLINE) and removes the file only after the write is
// confirmed. The content is already in the exact export (CSV) format. The
// exporter's silent CSV fallback is NOT acceptable here: it would report
// success while deferring visibility of the records to the ETL merge cron
// (hours), and would lose the retry opportunity while the destination is not
// ready yet (boot edge). A flush failure keeps the segment for the next
// retry.
//
// skipped reports the segment left the replay queue WITHOUT an export: a
// poison segment is quarantined (see parkPoisonedSegment) and replay must
// continue with later segments instead of head-of-line blocking on it. Only
// transient failures (destination not ready, read error) return an error;
// they stop the pass so the spool order is preserved for healthy segments.
func (s *statementInfoSpool) replaySegment(ctx context.Context, seg *spoolSegment) (skipped bool, err error) {
	path := filepath.Join(s.dir, seg.name)
	size := seg.size
	if !seg.exportedPendingRemove {
		raw, err := os.ReadFile(path)
		if err != nil {
			return false, moerr.ConvertGoError(ctx, err)
		}
		content, poison := recoverSpoolSegmentContent(seg.name, raw)
		if poison {
			s.parkPoisonedSegment(seg)
			return true, nil
		}
		if len(content) > 0 {
			if err := writeSpoolContent(ctx, content); err != nil {
				return false, err
			}
		}
		// the content is committed at the destination now: leave the durable
		// ack marker behind, so a kill between this commit and the file
		// removal below cannot cause a re-export on the next boot. Best
		// effort: on a marker failure the file is still removed; only a
		// kill in between can then duplicate this one batch once.
		if err := writeSpoolAckMarker(s.dir, seg.name); err != nil {
			v2.TraceMOLoggerSpoolErrorCounter.Inc()
			logutil.Error("statement_info spool replay ack marker write failed",
				zap.String("file", spoolAckFileName(seg.name)), zap.Error(err))
		}
		size = int64(len(content))
		seg.size = size
		seg.exportedPendingRemove = true
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		// the content is exported already; keep the file but never
		// re-export it (see exportedPendingRemove).
		return false, moerr.ConvertGoError(ctx, err)
	}
	removeSpoolAckFile(s.dir, seg.name)
	s.mux.Lock()
	s.totalSize -= size
	if s.totalSize < 0 {
		s.totalSize = 0
	}
	s.mux.Unlock()
	v2.TraceMOLoggerSpoolReplayCounter.Inc()
	logutil.Info("statement_info spool replayed segment",
		zap.String("file", seg.name), zap.Int64("size", size))
	return false, nil
}

// hasPendingOrphans reports whether any orphan segment still waits for replay.
func (s *statementInfoSpool) hasPendingOrphans() bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	return len(s.orphans) > 0
}

// replayOnce tries to replay every remaining orphan, oldest first. A
// transient failure (destination not ready, read error) stops the pass so the
// spool order is preserved; a poison segment is quarantined and skipped so it
// can never block the replay of later segments.
func (s *statementInfoSpool) replayOnce(ctx context.Context) {
	s.mux.Lock()
	orphans := make([]*spoolSegment, 0, len(s.orphans))
	orphans = append(orphans, s.orphans...)
	s.mux.Unlock()
	for _, seg := range orphans {
		if ctx.Err() != nil {
			return
		}
		skipped, err := s.replaySegment(ctx, seg)
		if err != nil {
			logutil.Warn("statement_info spool replay segment failed, retry later",
				zap.String("file", seg.name), zap.Error(err))
			v2.TraceMOLoggerSpoolErrorCounter.Inc()
			return
		}
		if skipped {
			logutil.Warn("statement_info spool replay skips quarantined segment",
				zap.String("file", seg.name))
		}
		s.mux.Lock()
		for idx, o := range s.orphans {
			if o.seq == seg.seq {
				s.orphans = append(s.orphans[:idx], s.orphans[idx+1:]...)
				break
			}
		}
		s.mux.Unlock()
	}
}

// startReplay replays leftover segments from the previous process run (if any)
// and retries periodically while the destination is not ready. Segments are
// only removed after a confirmed write; an unreplayed segment must never be
// deleted.
func (s *statementInfoSpool) startReplay(ctx context.Context) {
	// janitor runs regardless of replay progress.
	s.janitorStarted = true
	s.replayStarted = true
	go s.runJanitor()
	if !s.hasPendingOrphans() {
		close(s.replayDone)
		return
	}
	go func() {
		defer close(s.replayDone)
		logutil.Info("statement_info spool replay start", zap.Int("segments", len(s.orphans)))
		ticker := time.NewTicker(s.replayRetryInterval)
		defer ticker.Stop()
		// first attempt immediately
		s.replayOnce(ctx)
		for s.hasPendingOrphans() {
			select {
			case <-s.replayStop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.replayOnce(ctx)
			}
		}
		logutil.Info("statement_info spool replay complete")
	}()
}

// startJanitorOnly runs only the cleanup loop, for the configurations where
// replay cannot run (no writer factory) yet the sealed lifecycle must proceed.
func (s *statementInfoSpool) startJanitorOnly() {
	s.janitorStarted = true
	s.replayStarted = true
	go s.runJanitor()
	close(s.replayDone)
}

// close stops the background loops and closes the active file. Already
// exported content was removed by ackSegment; the un-exported remainder stays
// on disk and is replayed by the next boot, so a graceful shutdown racing an
// in-flight report does not lose the record either.
func (s *statementInfoSpool) close() {
	s.closeOnce.Do(func() {
		close(s.replayStop)
		close(s.janitorStop)
		if s.replayStarted {
			<-s.replayDone
		} else {
			close(s.replayDone)
		}
		if s.janitorStarted {
			<-s.janitorDone
		} else {
			close(s.janitorDone)
		}
		s.mux.Lock()
		defer s.mux.Unlock()
		s.closed = true
		// graceful path: sync the trailing records before closing the file.
		s.fg.mu.Lock()
		_, err := s.fg.sealFileLocked()
		s.fg.mu.Unlock()
		s.fg.close()
		if err != nil {
			logutil.Error("statement_info spool close active failed", zap.Error(err))
		}
		// verify nothing removable is left behind before exit
		s.collectRemovableLocked()
	})
}

// spoolPoisonFileSuffix extends a segment file name when the segment is
// quarantined: its content failed CSV validation mid-file, which a torn
// append can never cause (only the tail can tear). A poison segment is never
// replayed (the suffix also excludes it from orphan scans), keeps counting
// toward the spool size bound, and is left on disk for operator forensics.
const spoolPoisonFileSuffix = ".poison"

// lastCompleteCSVRecordEnd scans data under the exact quoting rules of
// encoding/csv ('"' special only at field start, '""' the escape inside a
// quoted field, '\n' the record terminator with '\r' tolerated) and returns
// the offset just past the last COMPLETE record's terminator. A record whose
// quoted field embeds newlines therefore counts as complete only at its real
// end: "the last newline byte" is not a record boundary (round-1 review
// defect). clean reports whether data ends exactly on a record boundary.
func lastCompleteCSVRecordEnd(data []byte) (end int, clean bool) {
	var inQuotes, fieldStart = false, true
	for i := 0; i < len(data); i++ {
		c := data[i]
		if inQuotes {
			if c == '"' {
				if i+1 < len(data) && data[i+1] == '"' {
					i++ // escaped quote inside a quoted field
				} else {
					inQuotes = false
				}
			}
			continue
		}
		switch {
		case c == '"' && fieldStart:
			inQuotes, fieldStart = true, false
		case c == ',':
			fieldStart = true
		case c == '\n':
			end = i + 1
			fieldStart = true
		case c == '\r':
			// tolerated before '\n' (or discarded by the CSV reader)
		default:
			fieldStart = false
		}
	}
	return end, end == len(data)
}

// validateSpoolCSVContent parses the complete-record prefix of a segment with
// the same CSV rules the LOAD DATA INLINE destination applies. It returns the
// parse error if the content is malformed.
func validateSpoolCSVContent(content []byte) error {
	r := csv.NewReader(bytes.NewReader(content))
	r.FieldsPerRecord = -1 // records render one per statement; never infer
	for {
		if _, err := r.Read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// recoverSpoolSegmentContent splits one crashed segment's raw content into
// the replayable prefix and its fate:
//   - the complete-record prefix: every record ending on a CSV boundary;
//   - a torn tail (unterminated remnant of an append that was in flight when
//     the process died — never fsync-confirmed as a complete record),
//     dropped, logged and metered;
//   - poison = the complete prefix itself does not parse. A torn append can
//     only damage the tail, so this means mid-file corruption; the caller
//     quarantines the segment instead of replaying it.
func recoverSpoolSegmentContent(segName string, raw []byte) (content []byte, poison bool) {
	if len(raw) == 0 {
		return raw, false
	}
	end, clean := lastCompleteCSVRecordEnd(raw)
	if !clean {
		v2.TraceMOLoggerSpoolDropCounter.Inc()
		logutil.Warn("statement_info spool replay drops torn tail record, it was never fsync-confirmed",
			zap.String("file", segName), zap.Int("droppedBytes", len(raw)-end))
	}
	content = raw[:end]
	if len(content) == 0 {
		return content, false
	}
	if err := validateSpoolCSVContent(content); err != nil {
		v2.TraceMOLoggerSpoolDropCounter.Inc()
		logutil.Error("statement_info spool segment content fails CSV validation, quarantine as poison",
			zap.String("file", segName), zap.Error(err))
		return nil, true
	}
	return content, false
}

// parkPoisonedSegment quarantines a segment whose content cannot parse: the
// file is renamed out of the spool scan, never retried, never removed; it
// keeps occupying the spool size bound until an operator deletes it.
func (s *statementInfoSpool) parkPoisonedSegment(seg *spoolSegment) {
	oldPath := filepath.Join(s.dir, seg.name)
	newPath := oldPath + spoolPoisonFileSuffix
	if err := os.Rename(oldPath, newPath); err != nil {
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		logutil.Error("statement_info spool quarantine poison segment failed",
			zap.String("file", oldPath), zap.Error(err))
		return
	}
	removeSpoolAckFile(s.dir, seg.name)
	logutil.Error("statement_info spool poison segment quarantined for operator forensics",
		zap.String("file", newPath), zap.Int64("size", seg.size))
}

// sqlFlushSpoolContent is the replay flush, indirection allows unit tests to
// stub the destination. It performs the very same LOAD DATA INLINE (same SQL
// text, same quoting, same connection pool) the exporter's SQL path issues
// for one CSV content block — see etl.SQLFlusher.FlushBuffer, duplicated
// here to keep this package importable from the etl package's tests (import
// cycle otherwise).
var sqlFlushSpoolContent = func(ctx context.Context, content []byte) error {
	return flushStatementInfoCSV(ctx, SingleStatementTable, content)
}

// flushStatementInfoCSV loads one CSV content block into the table via LOAD
// DATA INLINE, mirroring etl.SQLFlusher.FlushBuffer byte for byte.
func flushStatementInfoCSV(ctx context.Context, tbl *table.Table, src []byte) error {
	forceNewConn := !db_holder.DBConnErrCount.Check()
	conn, err := db_holder.GetOrInitDBConn(forceNewConn, true)
	if err != nil {
		v2.TraceMOLoggerErrorConnDBCounter.Inc()
		_ = db_holder.DBConnErrCount.Count()
		return err
	}
	if err = conn.Ping(); err != nil {
		v2.TraceMOLoggerErrorPingDBCounter.Inc()
		_ = db_holder.DBConnErrCount.Count()
		return err
	}

	extraEscapes := 0
	for _, c := range src {
		if c == '\\' || c == '\'' {
			extraEscapes++
		}
	}
	database, tableName := tbl.GetDatabase(), tbl.GetName()
	var loadSQL strings.Builder
	loadSQL.Grow(len("LOAD DATA INLINE FORMAT='csv', DATA='' INTO TABLE . FIELDS TERMINATED BY ','") +
		len(src) + extraEscapes + len(database) + len(tableName))
	loadSQL.WriteString("LOAD DATA INLINE FORMAT='csv', DATA='")
	for _, c := range src {
		// \ -> \\, ' -> '' (see etl.SQLFlusher.FlushBuffer)
		if c == '\\' || c == '\'' {
			loadSQL.WriteByte(c)
		}
		loadSQL.WriteByte(c)
	}
	loadSQL.WriteString("' INTO TABLE ")
	loadSQL.WriteString(database)
	loadSQL.WriteByte('.')
	loadSQL.WriteString(tableName)
	loadSQL.WriteString(" FIELDS TERMINATED BY ','")
	loadSQLStr := loadSQL.String()
	v2.TraceMOLoggerExportSqlHistogram.Observe(float64(len(loadSQLStr)))

	_, err = conn.Exec(loadSQLStr)
	if len(loadSQLStr) > 10*mpool.MB {
		logutil.Info("generate req sql", zap.String("type", tableName), zap.Int("csv", len(src)), zap.Int("sql", len(loadSQLStr)))
	}
	if err != nil {
		_ = db_holder.DBConnErrCount.Count()
	} else {
		db_holder.DBConnErrCount.Reset()
	}
	return err
}

// writeSpoolContent exports one raw export-format content block through the
// SQL destination (LOAD DATA INLINE), the same destination as the regular
// SQL export path, so replayed records become visible within the same latency
// as normally exported ones. Unlike the exporter's ContentWriter it does NOT
// fall back to a CSV file silently: on failure the error is returned so the
// caller keeps the segment and retries (the destination DB connection may
// simply not be up yet during boot).
func writeSpoolContent(ctx context.Context, content []byte) error {
	return sqlFlushSpoolContent(ctx, content)
}

// statementInfoSpoolCSVBufferPool pools the scratch buffer used to encode one
// record line.
var statementInfoSpoolCSVBufferPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, 1024))
}}

// encodeStatementInfoCSVLine renders the record's final export row in the
// exact CSV format used by the export pipeline (see etl.ContentFormatter using
// db_holder.NewCSVWriterWithBuffer). The returned byte slice is owned by the
// caller.
func encodeStatementInfoCSVLine(ctx context.Context, fields []string) []byte {
	buf := statementInfoSpoolCSVBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer statementInfoSpoolCSVBufferPool.Put(buf)
	w := csv.NewWriter(buf)
	// csv.Writer.Write never fails on a bytes.Buffer.
	_ = w.Write(fields)
	w.Flush()
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}

// spoolBatchRequest wraps the export request generated from one buffer flush
// so the fate of the export controls the fate of the spool segment holding
// the same records. It implements table.WriteRequest: the wrapped request is
// executed; the segment is marked exported only on success.
type spoolBatchRequest struct {
	spool *statementInfoSpool
	seg   *spoolSegment
	req   table.WriteRequest
}

var _ table.WriteRequest = (*spoolBatchRequest)(nil)

func (r *spoolBatchRequest) Handle() (n int, err error) {
	n, err = r.req.Handle()
	if err != nil {
		r.spool.markExportFailed(r.seg.seq)
	} else {
		r.spool.ackSegment(r.seg.seq)
	}
	return n, err
}

// spooledItemBuffer decorates the statement_info item buffer with the
// rotation of the spool at every export batch generation.
type spooledItemBuffer struct {
	Buffer
	spool *statementInfoSpool
}

var _ Buffer = (*spooledItemBuffer)(nil)

// GetBatch rotates the spool then delegates the batch generation. The sealed
// segment wraps the generated batch; the export commits the batch only after
// the segment was rotated away, so any kill after the rotation keeps the full
// record set of the batch durable. When the drain produces no batch for the
// fresh segment (nil, or a non-export shape) the segment is flagged noBatch:
// no ack will ever reference it, and its removal follows the ack-lag guard
// while its records ship inside a later batch (see spoolSegment.noBatch).
func (b *spooledItemBuffer) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	seg := b.spool.sealForExport()
	batch := b.Buffer.GetBatch(ctx, buf)
	if seg == nil {
		return batch
	}
	if batch == nil {
		b.spool.markNoBatch(seg.seq)
		return nil
	}
	if req, ok := batch.(table.WriteRequest); ok {
		return &spoolBatchRequest{spool: b.spool, seg: seg, req: req}
	}
	b.spool.markNoBatch(seg.seq)
	return batch
}

// spoolStatementInfo renders the StatementInfo record's final row and makes it
// durable in the spool. It runs on the caller's goroutine at statement end,
// after the report filters of ReportStatement have passed and before the
// record is handed to the collector.
//
// The row rendered here is byte-identical to the row the exporter would
// produce for the record, because it uses the same FillRow implementation and
// the same CSV encoding. Aggregation folded by the exporter later is not
// reflected here; the spool keeps one line per report, so a replay always
// preserves every statement (never less detail than the in-memory export).
//
// The function must never panic or otherwise break the statement path: any
// failure downgrades to the pre-spool (memory-only) behavior and is logged
// and metered.
func spoolStatementInfo(ctx context.Context, s *StatementInfo) {
	provider := GetTracerProvider()
	spool := provider.stmtInfoSpool
	if spool == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			v2.TraceMOLoggerSpoolErrorCounter.Inc()
			logutil.Error("statement_info spool render record failed, continue without spool",
				zap.Any("recover", r))
		}
	}()
	snapshot := s.cloneForSpool(ctx)
	row := snapshot.GetTable().GetRow(ctx)
	snapshot.FillRow(ctx, row)
	fields := row.ToStrings()
	row.Free()
	spool.appendRecord(encodeStatementInfoCSVLine(ctx, fields))
}
