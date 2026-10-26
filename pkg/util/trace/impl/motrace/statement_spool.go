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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
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
//   - The spool is a sequence of segment files rotated at every export batch
//     generation. Rotation boundaries carry no ownership meaning: the
//     exporter detaches a buffer before generating asynchronously, so a
//     sealed segment holds the detached buffer's records AND records already
//     appended for the next, still-pending batch. Retirement therefore
//     follows the exported records, never segment/batch sequence numbers:
//   - Every appended record takes a spool record sequence, stamped onto the
//     StatementInfo before the collector hand-off. Each collector buffer
//     instance (one future export batch) allocates a batch id on its first
//     Add; the Add hook parents the record to that batch (folded into the
//     owning segment's taggedCount/maxTag). On batch commit the spool
//     advances an export watermark W — the highest batch id with every id
//     below it committed (MOCollector runs multiple doExport workers:
//     completion order is not guaranteed; a later commit must not advance
//     past a pending one). A failed batch advances nothing: the stall
//     retains every segment owning uncommitted records, for restart replay.
//   - A sealed segment is removable (in order, head-of-line) once the
//     watermark covers the highest batch id owning any of its records
//     (W >= maxTag), its records are fully parented (or it aged past the
//     ack-lag horizon, which bounds stragglers to the pre-existing
//     collector-drop loss class), and the ack-lag horizon passed. A kill
//     loses the in-memory watermark, so everything still on disk replays
//     at-least-once: the duplicate window is exactly the batches committed
//     inside the horizon.
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
//   - The size of the spool directory is bounded and charged on write: a
//     record's bytes are reserved before the append and stay charged once
//     written — a failed fsync must not uncharge bytes that sit on disk —
//     and startup accounting walks every file (quarantined poison included).
//     Once the bound is exceeded newly reported records are dropped from
//     the spool (the in-memory pipeline keeps its old behavior) and the
//     loss is logged and metered.
//
// Failure semantics (explicitly documented):
//   - At-least-once: a record that reached the spool is eventually exported,
//     even across an abrupt process exit, as long as the local spool directory
//     survives (same-node restart; a replacement node without the directory
//     cannot replay it).
//   - Duplicates stay bounded: a process kill loses the in-memory watermark
//     and tag state, so on restart the replay re-exports every segment that
//     was covered but not yet removed when the process died. With the
//     ack-lag guard that is at most the export batches committed inside the
//     last ack-lag horizon (aggregation window + export interval + margin,
//     typically one or two batches). Their rows are re-inserted;
//     statement_info has no dedup key, so at-least-once is the strongest
//     guarantee achievable here. The horizon never resurrects older
//     batches: covered-and-aged segments are removed in run, so nothing
//     remains to replay.
//   - A segment is removed only when the export watermark passed every batch
//     id parented into it (contiguous completion — a pending batch blocks
//     the watermark, so its records keep their durable copy); the ack-lag
//     guard then delays removal, which also covers records that were still
//     draining through the collector queue when their segment was sealed.
//     No ack state is persisted ahead of that guard, so a kill can never
//     turn an un-exported record into a permanently lost one.
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
	// sealed holds rotated segments that still wait for export coverage,
	// keyed by seq. sealedOrder mirrors the map, sorted ascending, so
	// segmentOfLocked can binary-search a record sequence to its segment.
	sealed      map[uint64]*spoolSegment
	sealedOrder []*spoolSegment
	nextSeq     uint64
	totalSize   int64

	// nextRecordSeq is the spool record sequence assigned to the NEXT
	// successfully appended record (starts at 1; 0 means "not spooled" on
	// StatementInfo.spoolSeq).
	nextRecordSeq uint64
	// nextBatchID allocates export-batch ids; a collector buffer instance
	// takes one on its first Add (empty buffers never consume ids, so no
	// ghost id can stall the watermark).
	nextBatchID uint64
	// committed holds recently committed batch ids; watermark is the
	// highest id with every id <= it committed (contiguous completion).
	// Both are in-memory only, intentionally lost on kill (see header).
	committed map[uint64]bool
	watermark uint64

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
	// syncing marks one appender leading a sync cycle.
	syncing bool
	// writesSinceSync counts appends since the last completed cycle's
	// snapshot; the leader closes over it to size the cycle's group.
	writesSinceSync int
	// linger is the adaptive gather window for the NEXT cycle: a crowded
	// cycle (group >= 2) keeps the window open so bursts share one fsync; a
	// solo cycle closes it to zero, so a lightly loaded single client pays
	// exactly the pre-linger cost. It is never larger than
	// spoolFsyncLingerDefault (bounded linger, no indefinite waiting).
	linger time.Duration
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

// spoolFsyncLingerDefault bounds the group-commit gather window: a leader
// whose previous cycle confirmed a crowded group first holds its Sync back
// by this much, so a burst of statements shares one fsync (the classic
// group-commit trade: up to linger of added latency per statement against a
// ceiling-lifting factor of ~group size). Bounded: no cycle ever lingers
// longer.
var spoolFsyncLingerDefault = time.Millisecond

// spoolFsyncSleep lets tests replace the gather-window sleep with a
// deterministic barrier.
var spoolFsyncSleep = time.Sleep

// adoptLocked makes f the group's current file. Content already present in f
// (a re-opened segment) was confirmed by its previous writer, so the
// confirmed width starts at the file's size: only NEW bytes must be fsynced.
// Call with g.mu held, with no Sync in flight.
func (g *spoolFsyncGroup) adoptLocked(f spoolSyncFile, size int64) {
	g.f = f
	g.fileSize = size
	g.confirmedWidth = size
	g.writesSinceSync = 0
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
	g.writesSinceSync++
	end := g.fileSize
	for g.confirmedWidth < end {
		if g.terminate || g.sealGen != myGen {
			// The file I wrote to was sealed (or the group is going down
			// after sealing it): the seal's final Sync confirmed my write.
			break
		}
		if !g.syncing {
			// lead the cycle: if the previous cycle was crowded, first hold
			// the Sync back by the bounded gather window so this burst
			// shares one fsync; a solo previous cycle skips the window, so
			// an idle client pays nothing extra.
			g.syncing = true
			if linger := g.linger; linger > 0 {
				g.mu.Unlock()
				spoolFsyncSleep(linger)
				g.mu.Lock()
			}
			// it confirms everything written up to target. A write landing
			// while the Sync runs is NOT covered by it and keeps
			// confirmedWidth behind fileSize.
			target := g.fileSize
			f := g.f
			g.mu.Unlock()
			syncErr := f.Sync()
			g.mu.Lock()
			g.syncing = false
			// adapt the gather window from the cycle just closed: arrivals
			// during the window AND during the Sync itself mean a crowded
			// pipeline — keep the window open; idleness closes it.
			group := g.writesSinceSync
			g.writesSinceSync = 0
			if group >= 2 {
				g.linger = spoolFsyncLingerDefault
			} else {
				g.linger = 0
			}
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

	// recBase is the spool record sequence of this segment's first record;
	// the segment covers [recBase, recBase+recordCount). Rotation never
	// reorders records, so sealed segments sort by recBase.
	recBase uint64
	// recordCount counts the records appended into this segment.
	recordCount int64
	// taggedCount counts how many of those records have been parented to
	// an export batch at the collector buffer's Add hook. taggedCount ==
	// recordCount means the segment is fully parented.
	taggedCount int64
	// maxTag is the highest batch id parented into this segment. The
	// segment's records are all exported once watermark >= maxTag.
	maxTag uint64

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
// path is intentionally NOT persisted (see ackBatch and the file header).
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
		committed:           make(map[uint64]bool),
		nextRecordSeq:       1,
		nextBatchID:         1,
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
		info, err := e.Info()
		if err != nil {
			continue
		}
		// Quarantined poison files are deliberately RETAINED at quarantine,
		// so they must stay charged against the bound at every boot — they
		// are excluded from replay, never from accounting.
		if strings.HasSuffix(e.Name(), spoolPoisonFileSuffix) {
			s.totalSize += info.Size()
			continue
		}
		seq, ok := parseSpoolSegmentFileName(e.Name())
		if !ok {
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
		seq:     s.nextSeq,
		name:    spoolSegmentFileName(s.nextSeq),
		size:    size,
		sealed:  s.now(),
		recBase: s.nextRecordSeq,
	}
	s.nextSeq++
	return nil
}

// appendRecord makes one already-encoded record line durable and returns its
// spool record sequence (0 when the record was not spooled: spool closed,
// full, or the append failed). The sequence parents the record to its export
// batch later, at the collector buffer's Add hook.
//
// The write is a single Write call, so process termination can only leave
// whole lines behind. The fsync per call bounds the loss window of a process
// kill to "the statement whose report is in flight right now" and keeps the
// audit-durability contract independent of OS page-cache behavior.
//
// Quota is charged on write: the record's bytes are reserved up front, and
// only bytes never written release the reservation — a failed fsync must not
// uncharge bytes that sit on disk, or a persistently failing disk would let
// the directory outgrow its bound.
//
// On any failure the record falls back to the pre-spool (in-memory only)
// behavior; the failure must never break the statement path.
func (s *statementInfoSpool) appendRecord(line []byte) uint64 {
	if len(line) == 0 {
		return 0
	}
	s.mux.Lock()
	if s.closed {
		s.mux.Unlock()
		return 0
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
		return 0
	}
	if s.active == nil {
		if err := s.openActiveLocked(); err != nil {
			s.mux.Unlock()
			v2.TraceMOLoggerSpoolErrorCounter.Inc()
			s.logErrRateLimited("statement_info spool open active segment failed", err)
			return 0
		}
	}
	// reserve quota, consume a record sequence, and count the ledger record
	// atomically, so two concurrent appenders can never share a sequence
	// (tagging is keyed on it).
	s.totalSize += int64(len(line))
	seq := s.nextRecordSeq
	s.nextRecordSeq++
	s.active.recordCount++
	s.mux.Unlock()

	n, err := s.fg.append(line)
	if err != nil {
		s.mux.Lock()
		// charge-on-write: only bytes never written release the reservation;
		// a failed fsync must not uncharge bytes that sit on disk. The
		// sequence stays consumed and the ledger record stays counted even
		// though it can never be parented (no stamp reaches the collector):
		// the segment clears through the age leg of the ack-lag horizon,
		// which is the documented fate of a broken-disk window.
		s.totalSize -= int64(len(line) - n)
		if s.totalSize < 0 {
			s.totalSize = 0
		}
		s.mux.Unlock()
		v2.TraceMOLoggerSpoolErrorCounter.Inc()
		s.logErrRateLimited("statement_info spool append record failed", err)
		return 0
	}
	v2.TraceMOLoggerSpoolWriteCounter.Inc()
	return seq
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

// sealForExport rotates the active segment: the current active segment
// becomes sealed and a fresh active segment is opened. Returns the sealed
// segment; nil if the active segment holds no record. The file swap is atomic
// against appends, so a record can neither land in a sealed file after the
// rotation nor fall between two files. Rotation boundaries carry no ownership
// meaning (see the file header): which export batch owns a record is decided
// by the collector buffer it lands in, tracked via tagRecord.
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
	idx := sort.Search(len(s.sealedOrder), func(i int) bool { return s.sealedOrder[i].seq >= seg.seq })
	s.sealedOrder = append(s.sealedOrder, nil)
	copy(s.sealedOrder[idx+1:], s.sealedOrder[idx:])
	s.sealedOrder[idx] = seg
	if openErr != nil {
		// the next append reopens lazily; meanwhile the rotated segment is
		// on disk and tracked, so no record is stranded.
		s.active = nil
	} else {
		s.active = &spoolSegment{
			seq:     s.nextSeq,
			name:    spoolSegmentFileName(s.nextSeq),
			size:    nextSize,
			sealed:  s.now(),
			recBase: s.nextRecordSeq,
		}
	}
	s.nextSeq++
	logutil.Debug("statement_info spool seal segment",
		zap.Uint64("seq", seg.seq), zap.Int64("size", seg.size))
	return seg
}

// nextBatch allocates an export-batch id. Ids are the watermark space: tags
// (record -> batch ownership) and acks (batch commits) share it, so coverage
// stays exact no matter how generation and export completion reorder.
func (s *statementInfoSpool) nextBatch() uint64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	id := s.nextBatchID
	s.nextBatchID++
	return id
}

// segmentOfLocked finds the segment covering one spool record sequence:
// the active segment if the record may still be there, else the sealed
// segment whose [recBase, recBase+recordCount) range holds it. Records whose
// segment has already been removed have no owner to update (their durable
// copy is gone either way: exported, or in the documented loss class).
func (s *statementInfoSpool) segmentOfLocked(recSeq uint64) *spoolSegment {
	if s.active != nil && recSeq >= s.active.recBase {
		return s.active
	}
	idx := sort.Search(len(s.sealedOrder), func(i int) bool {
		seg := s.sealedOrder[i]
		return recSeq < seg.recBase+uint64(seg.recordCount)
	})
	if idx < len(s.sealedOrder) {
		seg := s.sealedOrder[idx]
		if recSeq >= seg.recBase {
			return seg
		}
	}
	return nil
}

// tagRecord parents one spooled record to its export batch: the record is
// exported exactly when that batch commits (see the watermark protocol in the
// file header). Called from the collector buffer's Add hook, before the item
// is rendered and freed.
func (s *statementInfoSpool) tagRecord(recSeq, batchID uint64) {
	if recSeq == 0 || batchID == 0 {
		return
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	seg := s.segmentOfLocked(recSeq)
	if seg == nil {
		return
	}
	seg.taggedCount++
	if batchID > seg.maxTag {
		seg.maxTag = batchID
	}
}

// ackBatch records one export batch's commit and advances the export
// watermark to the highest batch id with every id below it committed.
// Completion order across MOCollector's doExport workers is not guaranteed:
// a commit must never pass a pending batch, or the still-pending batch's
// records could lose their durable copy while unexported. On success the
// covered segments are re-examined for removal. A failed Handle never calls
// this: the stall itself retains everything the batch owned.
func (s *statementInfoSpool) ackBatch(batchID uint64) {
	if batchID == 0 {
		return
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.committed[batchID] {
		return
	}
	s.committed[batchID] = true
	for s.committed[s.watermark+1] {
		s.watermark++
		delete(s.committed, s.watermark)
	}
	s.collectRemovableLocked()
}

// segmentExportCoveredLocked reports whether every record parented into the
// segment is covered by a committed batch: the watermark passed the segment's
// highest owning batch id. A segment whose records are not all parented yet
// (still queued between report and collector Add) is only covered once it
// aged past the ack-lag horizon — the bound that separates real stragglers
// from the pre-existing collector-overload drop class (never Added).
func (s *statementInfoSpool) segmentExportCoveredLocked(seg *spoolSegment, now time.Time) bool {
	if seg.maxTag > s.watermark {
		return false
	}
	if seg.taggedCount < seg.recordCount && s.ackLagDuration > 0 && now.Sub(seg.sealed) < s.ackLagDuration {
		return false
	}
	return true
}

// collectRemovableLocked removes the head of the sealed list while every head
// segment (a) is export-covered (see segmentExportCoveredLocked) and (b)
// clears the ack-lag guard: followed by ackLagSegments newer covered
// segments, or older than ackLagDuration. Removal stays in sequence order;
// an under-covered head head-of-line blocks newer removals, which the spool
// size bound keeps finite.
func (s *statementInfoSpool) collectRemovableLocked() {
	now := s.now()
	seqs := make([]uint64, 0, len(s.sealed))
	for seq := range s.sealed {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		seg := s.sealed[seq]
		if seg == nil {
			return
		}
		if !s.segmentExportCoveredLocked(seg, now) {
			// still owned by an uncommitted batch (or too fresh to tell): a
			// gap stops the cleanup, later segments wait for the next round.
			return
		}
		if !s.lagSatisfiedLocked(seqs, seq, now) {
			return
		}
		s.removeSegmentLocked(seg)
	}
}

// lagSatisfiedLocked checks the ack-lag guard for one covered segment:
// ackLagDuration reached, or ackLagSegments newer export-covered segments
// exist.
func (s *statementInfoSpool) lagSatisfiedLocked(seqs []uint64, seq uint64, now time.Time) bool {
	seg := s.sealed[seq]
	if seg == nil {
		return false
	}
	if s.ackLagDuration > 0 && now.Sub(seg.sealed) >= s.ackLagDuration {
		return true
	}
	// count newer segments whose records are all covered
	var newerAcked uint64
	for _, newer := range seqs {
		if newer <= seq {
			continue
		}
		other := s.sealed[newer]
		if other == nil || !s.segmentExportCoveredLocked(other, now) {
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
	idx := sort.Search(len(s.sealedOrder), func(i int) bool { return s.sealedOrder[i].seq >= seg.seq })
	if idx < len(s.sealedOrder) && s.sealedOrder[idx].seq == seg.seq {
		s.sealedOrder = append(s.sealedOrder[:idx], s.sealedOrder[idx+1:]...)
	}
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
// exported-and-collected content was removed in run; the remainder stays
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

// spoolBatchAck tracks one export batch's commit across its possibly
// multiple load requests (table.ExportRequests splits by account). The
// batch's id is fed to the export watermark only when EVERY request of the
// batch committed; any failure stalls the watermark — which retains every
// record the batch owned, for restart replay.
type spoolBatchAck struct {
	spool *statementInfoSpool
	tag   uint64
	total int32
	done  atomic.Int32
	err   atomic.Int32
}

// spoolBatchRequest wraps one load request of an export batch. It implements
// table.WriteRequest: the wrapped request is executed; the batch id acks when
// all of the batch's requests succeeded.
type spoolBatchRequest struct {
	shared *spoolBatchAck
	req    table.WriteRequest
}

var _ table.WriteRequest = (*spoolBatchRequest)(nil)

func (r *spoolBatchRequest) Handle() (n int, err error) {
	n, err = r.req.Handle()
	if err != nil {
		r.shared.err.Add(1)
	} else if r.shared.done.Add(1) == r.shared.total && r.shared.err.Load() == 0 {
		r.shared.spool.ackBatch(r.shared.tag)
	}
	return n, err
}

// spooledItemBuffer decorates the statement_info item buffer with the spool
// lifecycle: rotation at every export batch generation, and the record->batch
// parenting at every Add.
type spooledItemBuffer struct {
	Buffer
	spool *statementInfoSpool
	// batchID is this buffer instance's (== one future batch's) id in the
	// watermark space, allocated on the first Add: an empty buffer never
	// consumes an id, so no ghost id can stall the watermark. Adds to one
	// buffer instance are serialized by the collector's holder.
	batchID uint64
}

var _ Buffer = (*spooledItemBuffer)(nil)

// Add parents the incoming record to this buffer's batch BEFORE the inner
// Add renders and frees the item: a record is exported exactly when its
// batch commits, and the owning segment's retirement needs the parenthood
// (see the watermark protocol in the file header).
func (b *spooledItemBuffer) Add(item bp.HasName) {
	if si, ok := item.(*StatementInfo); ok && si.spoolSeq != 0 {
		if b.batchID == 0 {
			b.batchID = b.spool.nextBatch()
		}
		b.spool.tagRecord(si.spoolSeq, b.batchID)
	}
	b.Buffer.Add(item)
}

// GetBatch rotates the spool then delegates the batch generation. The export
// commits the batch only after the rotation, so any kill after it keeps
// everything reported so far durable. The generated request(s) are wrapped so
// the batch id acks on commit (with table.ExportRequests, after ALL of the
// batch's requests committed); a drain producing no batch wraps nothing.
func (b *spooledItemBuffer) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	b.spool.sealForExport()
	batch := b.Buffer.GetBatch(ctx, buf)
	switch req := batch.(type) {
	case table.WriteRequest:
		if b.batchID == 0 {
			return batch
		}
		return &spoolBatchRequest{shared: &spoolBatchAck{spool: b.spool, tag: b.batchID, total: 1}, req: req}
	case table.ExportRequests:
		if b.batchID == 0 || len(req) == 0 {
			return batch
		}
		shared := &spoolBatchAck{spool: b.spool, tag: b.batchID, total: int32(len(req))}
		out := make(table.ExportRequests, len(req))
		for i, r := range req {
			out[i] = &spoolBatchRequest{shared: shared, req: r}
		}
		return out
	default:
		return batch
	}
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
	// Stamp the spool sequence onto the ORIGINAL record before it is handed
	// to the collector: the buffer's Add hook parents the record to its
	// export batch through it (see StatementInfo.spoolSeq). A 0 result means
	// the record never entered the spool ledger; it stays batch-only.
	s.spoolSeq = spool.appendRecord(encodeStatementInfoCSVLine(ctx, fields))
}
