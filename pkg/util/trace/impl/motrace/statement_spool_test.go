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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

// spoolCaptureWriter is a buffer-settable RowWriter capturing the pre-filled
// content it commits, modeling the production ContentWriter.
type spoolCaptureWriter struct {
	storage *bytes.Buffer
	pending *bytes.Buffer
	failErr error
	flushed int
	needBuf bool
}

var _ table.RowWriter = (*spoolCaptureWriter)(nil)
var _ table.BufferSettable = (*spoolCaptureWriter)(nil)

func (w *spoolCaptureWriter) WriteRow(row *table.Row) error { return nil }
func (w *spoolCaptureWriter) GetContent() string            { return w.storage.String() }
func (w *spoolCaptureWriter) GetContentLength() int         { return w.storage.Len() }
func (w *spoolCaptureWriter) NeedBuffer() bool              { return w.needBuf }
func (w *spoolCaptureWriter) SetBuffer(buf *bytes.Buffer, cb func(*bytes.Buffer)) {
	// capture the handed-over content into the pre-allocated storage so the
	// tests can simply read the storage after Handle.
	if w.storage == nil {
		w.storage = &bytes.Buffer{}
	}
	// keep the handed-over buffer pending: only a successful FlushAndClose
	// commits it, exactly like the production writer.
	w.pending = buf
}
func (w *spoolCaptureWriter) FlushAndClose() (int, error) {
	w.flushed++
	if w.failErr != nil {
		return 0, w.failErr
	}
	if w.pending == nil {
		return 0, nil
	}
	if w.storage == nil {
		w.storage = &bytes.Buffer{}
	}
	_, _ = w.storage.Write(w.pending.Bytes())
	return w.pending.Len(), nil
}

// spoolCapturableFactory feeds the export path of the pipeline-level tests
// with capture writers.
type spoolCapturableFactory struct {
	rowWriter *spoolCaptureWriter
}

var _ table.WriterFactory = (*spoolCapturableFactory)(nil)

func (f *spoolCapturableFactory) GetRowWriter(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
	return f.rowWriter
}
func (f *spoolCapturableFactory) GetWriter(ctx context.Context, fp string) io.WriteCloser {
	return f
}
func (f *spoolCapturableFactory) Write(p []byte) (int, error) { return len(p), nil }
func (f *spoolCapturableFactory) Close() error                { return nil }

// spoolFlushStub stubs the replay SQL flush destination (writeSpoolContent)
// and captures the committed content, modeling the production SQLFlusher.
type spoolFlushStub struct {
	mux     sync.Mutex
	storage *bytes.Buffer
	failErr error
	flushed int
}

func (s *spoolFlushStub) flush(ctx context.Context, content []byte) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.flushed++
	if s.failErr != nil {
		return s.failErr
	}
	if s.storage == nil {
		s.storage = &bytes.Buffer{}
	}
	_, _ = s.storage.Write(content)
	return nil
}

func (s *spoolFlushStub) calls() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.flushed
}

func (s *spoolFlushStub) bytes() []byte {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.storage == nil {
		return nil
	}
	return append([]byte(nil), s.storage.Bytes()...)
}

// stubSpoolSQLFlush installs a spoolFlushStub as the replay flush destination
// for the duration of the test (or until the next stub is installed).
func stubSpoolSQLFlush(t *testing.T) *spoolFlushStub {
	t.Helper()
	stub := &spoolFlushStub{}
	old := sqlFlushSpoolContent
	sqlFlushSpoolContent = stub.flush
	t.Cleanup(func() { sqlFlushSpoolContent = old })
	return stub
}

// spoolTestClock is a hand-driven clock
func spoolTestClock(base time.Time) (*time.Time, func() time.Time) {
	now := new(time.Time)
	*now = base
	return now, func() time.Time { return *now }
}

func openTestSpool(t *testing.T, dir string, maxSize int64, ackLagSegments uint64, ackLagDuration time.Duration) *statementInfoSpool {
	t.Helper()
	s, err := newStatementInfoSpool(dir, maxSize, ackLagSegments, ackLagDuration)
	require.NoError(t, err)
	return s
}

func readSpoolFile(t *testing.T, dir string, seq uint64) []byte {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(dir, spoolSegmentFileName(seq)))
	require.NoError(t, err)
	return content
}

func spoolDirFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestSpoolSegmentFileName(t *testing.T) {
	name := spoolSegmentFileName(42)
	seq, ok := parseSpoolSegmentFileName(name)
	require.True(t, ok)
	assert.Equal(t, uint64(42), seq)

	for _, bad := range []string{
		"statement_info_1.csv",
		"statement_info_x" + spoolFileSuffix,
		"other_0000000000000001" + spoolFileSuffix,
		"statement_info_" + spoolFileSuffix,
		"statement_info_0000000000000001" + spoolFileSuffix + ".tmp",
		"",
	} {
		_, ok := parseSpoolSegmentFileName(bad)
		assert.False(t, ok, bad)
	}
}

func TestStatementInfoSpoolAppendThenSeal(t *testing.T) {
	dir := t.TempDir()
	s := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s.close()

	assert.Empty(t, s.orphans)
	assert.Equal(t, uint64(1), s.active.seq)

	lines := [][]byte{
		encodeStatementInfoCSVLine(context.Background(), []string{"a", "b"}),
		encodeStatementInfoCSVLine(context.Background(), []string{"c", "d"}),
	}
	expect := append([]byte(nil), lines[0]...)
	expect = append(expect, lines[1]...)
	s.appendRecord(lines[0])
	s.appendRecord(lines[1])
	assert.Equal(t, int64(len(expect)), s.totalSize)

	seg := s.sealForExport()
	require.NotNil(t, seg)
	assert.Equal(t, uint64(1), seg.seq)
	assert.Equal(t, int64(len(expect)), seg.size)
	assert.Equal(t, expect, readSpoolFile(t, dir, 1))
	// after rotation a fresh active file is ready, still empty
	assert.NotEqual(t, seg.seq, s.active.seq)

	// seal over an empty active segment returns nil
	assert.Nil(t, s.sealForExport())
}

func TestStatementInfoSpoolFreshClusterNoOrphans(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-yet-existing", "statement_info_spool")
	s := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s.close()
	assert.False(t, s.hasPendingOrphans())
	// dir was created lazily
	assert.DirExists(t, dir)
	// replay with nothing to do returns immediately and closes replayDone
	stub := stubSpoolSQLFlush(t)
	s.startReplay(context.Background())
	<-s.replayDone
	assert.Equal(t, 0, stub.calls())
}

func TestStatementInfoSpoolMaxSizeBound(t *testing.T) {
	dir := t.TempDir()
	line := encodeStatementInfoCSVLine(context.Background(), []string{"0123456789"})
	s := openTestSpool(t, dir, int64(len(line)), 1, time.Minute)
	defer s.close()

	s.appendRecord(line) // fits
	size := s.totalSize
	require.Equal(t, int64(len(line)), size)

	// the second one must be dropped: the spool stays within its bound,
	// the record keeps the memory-only behavior, and the loss is counted.
	s.appendRecord(line)
	assert.Equal(t, size, s.totalSize)
	assert.Equal(t, int64(1), s.droppedRecords)
	assert.Equal(t, line, readSpoolFile(t, dir, 1))
}

// TestStatementInfoSpoolWatermarkLifecycle mirrors the pre-watermark lag
// semantics through record->batch parenthood: coverage comes from committed
// batches (contiguous watermark), and the ack-lag guard (segments or time)
// still delays removal.
func TestStatementInfoSpoolWatermarkLifecycle(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	base := time.Now()
	current, clock := spoolTestClock(base)
	s := openTestSpool(t, dir, 0, 1, 30*time.Second)
	s.now = clock
	defer s.close()

	line := encodeStatementInfoCSVLine(ctx, []string{"r"})
	mkBatch := func() (*spoolBatchRequest, uint64) {
		seq := s.appendRecord(line)
		require.NotZero(t, seq)
		buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
		buf.Add(&StatementInfo{spoolSeq: seq})
		return buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest), buf.batchID
	}
	reqA, tagA := mkBatch() // seals segment 1 (its GetBatch rotated)
	reqB, tagB := mkBatch() // seals segment 2
	require.NotNil(t, reqA)
	require.NotNil(t, reqB)
	require.Equal(t, tagA+1, tagB)
	files := spoolDirFiles(t, dir)
	require.Len(t, files, 3) // seg1, seg2, fresh active
	seg1Name, seg2Name := files[0], files[1]

	// commit batch A: segment 1 is covered (W=1) but the lag guard wants one
	// newer covered segment (segment 2 needs W>=2): it must stay.
	_, err := reqA.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	assert.Equal(t, uint64(1), s.watermark)
	s.mux.Unlock()
	assert.Contains(t, spoolDirFiles(t, dir), seg1Name)
	assert.Contains(t, spoolDirFiles(t, dir), seg2Name)

	// commit batch B: the watermark passes segment 2, the lag for seg1 is
	// satisfied, seg1 is removed; seg2 has no newer covered segment and no
	// time passed: stays.
	_, err = reqB.Handle()
	require.NoError(t, err)
	assert.NotContains(t, spoolDirFiles(t, dir), seg1Name)
	assert.Contains(t, spoolDirFiles(t, dir), seg2Name)

	// time-based lag fallback: after ackLagDuration seg2 can be removed.
	*current = base.Add(31 * time.Second)
	s.mux.Lock()
	s.collectRemovableLocked()
	s.mux.Unlock()
	assert.NotContains(t, spoolDirFiles(t, dir), seg2Name)
	assert.Equal(t, int64(0), s.totalSize)
}

func TestStatementInfoSpoolCrashRecoveryReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// first process lifetime: stage records over two segments, then "crash"
	// (no graceful close, no ack).
	lines1 := [][]byte{encodeStatementInfoCSVLine(ctx, []string{"s1-r1"}), encodeStatementInfoCSVLine(ctx, []string{"s1-r2"})}
	lines2 := [][]byte{encodeStatementInfoCSVLine(ctx, []string{"s2-r1"})}
	{
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		s.appendRecord(lines1[0])
		s.appendRecord(lines1[1])
		_ = s.sealForExport()
		s.appendRecord(lines2[0])
		// simulate kill: release the file handle without lifecycle handling
		spoolKillFile(s)
	}

	// second process lifetime: orphans are detected in order and replayed
	// through the SQL flush; files are removed after confirmation.
	stub := stubSpoolSQLFlush(t)
	s2 := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s2.close()
	require.True(t, s2.hasPendingOrphans())
	require.Len(t, s2.orphans, 2)

	s2.replayOnce(ctx)
	assert.False(t, s2.hasPendingOrphans())

	// replayed content is the byte-exact export content of both segments,
	// in segment order.
	expect := append(append(append([]byte(nil), lines1[0]...), lines1[1]...), lines2[0]...)
	assert.Equal(t, expect, stub.bytes())
	files := spoolDirFiles(t, dir)
	assert.NotContains(t, files, spoolSegmentFileName(1))
	assert.NotContains(t, files, spoolSegmentFileName(2))

	// no-duplicate after recovery: replaying again produces no new write.
	flushed := stub.calls()
	s2.replayOnce(ctx)
	assert.Equal(t, flushed, stub.calls())
}

func TestStatementInfoSpoolReplayFailureKeepsSegment(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	{
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		s.appendRecord(line)
		spoolKillFile(s)
	}

	stub := stubSpoolSQLFlush(t)
	stub.failErr = io.ErrClosedPipe
	s2 := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s2.close()
	s2.replayOnce(ctx)
	// failure keeps the file and the orphan
	assert.True(t, s2.hasPendingOrphans())
	assert.Contains(t, spoolDirFiles(t, dir), spoolSegmentFileName(1))

	// retry succeeds later
	stub.failErr = nil
	s2.replayOnce(ctx)
	assert.False(t, s2.hasPendingOrphans())
	assert.Equal(t, line, stub.bytes())
}

func TestStatementInfoSpoolExportedPendingRemove(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	{
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		s.appendRecord(line)
		spoolKillFile(s)
	}

	stub := stubSpoolSQLFlush(t)
	s2 := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s2.close()
	// mark as exported-but-not-removed, as a remove-failure leaves it
	s2.orphans[0].exportedPendingRemove = true
	s2.replayOnce(ctx)
	// no re-export happened, file removed: no duplicates after a remove-failure recovery
	assert.Equal(t, 0, stub.calls())
	assert.NotContains(t, spoolDirFiles(t, dir), spoolSegmentFileName(1))
}

func TestStatementInfoSpoolAckMarkerPreventsReplayDup(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	marker := spoolAckFileName(spoolSegmentFileName(1))
	{
		// first run leaves one orphan segment; a replay then commits its
		// content at the destination and writes the replay tombstone, but
		// the process is killed before the segment file is removed.
		s := openTestSpool(t, dir, 0, 1, time.Hour)
		s.appendRecord(line)
		spoolKillFile(s)
		segFile := filepath.Join(dir, spoolSegmentFileName(1))
		require.FileExists(t, segFile)
		// the tombstone replaySegment writes right after its commit.
		require.NoError(t, writeSpoolAckMarker(dir, spoolSegmentFileName(1)))
	}

	stub := stubSpoolSQLFlush(t)
	s2 := openTestSpool(t, dir, 0, 1, time.Hour)
	defer s2.close()
	require.True(t, s2.hasPendingOrphans())
	// the tombstone turns the orphan into remove-only: the boot replay must
	// not write a single row again, and both files are collected.
	s2.replayOnce(ctx)
	assert.Equal(t, 0, stub.calls())
	assert.False(t, s2.hasPendingOrphans())
	files := spoolDirFiles(t, dir)
	assert.NotContains(t, files, spoolSegmentFileName(1))
	assert.NotContains(t, files, marker)
}

func TestStatementInfoSpoolAckBatchLeavesNoMarker(t *testing.T) {
	dir := t.TempDir()
	// the ack of the regular export path is in-memory only and stays behind
	// the pipeline lag: no marker is written for it (see ackBatch), so a
	// kill inside the ack-lag horizon re-exports the batch instead of
	// losing records that were still draining through the collector.
	s := openTestSpool(t, dir, 0, 1, time.Hour)
	defer s.close()
	line := encodeStatementInfoCSVLine(context.Background(), []string{"s1-r1"})
	s.appendRecord(line)
	seg := s.sealForExport()
	require.NotNil(t, seg)
	s.ackBatch(42)
	for _, f := range spoolDirFiles(t, dir) {
		assert.NotEqual(t, spoolAckFileName(seg.name), f)
	}
}

func TestStatementInfoSpoolReplayDropsTornTail(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	full := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	{
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		s.appendRecord(full)
		spoolKillFile(s)
		// a kill landing mid-append leaves an unterminated tail line behind
		// (the record it belonged to never got its fsync confirmation).
		f, err := os.OpenFile(filepath.Join(dir, spoolSegmentFileName(1)), os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte("\"partially-written-row"))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	stub := stubSpoolSQLFlush(t)
	s2 := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s2.close()
	s2.replayOnce(ctx)
	// the complete prefix replays; the torn tail is dropped, not rejected.
	assert.Equal(t, full, stub.bytes())
	assert.False(t, s2.hasPendingOrphans())
	assert.NotContains(t, spoolDirFiles(t, dir), spoolSegmentFileName(1))
}

// TestLastCompleteCSVRecordEnd pins the quote-aware boundary scan: a
// newline inside a quoted field is not a record boundary, so torn-tail
// recovery must stop at the end of the last COMPLETE record.
func TestLastCompleteCSVRecordEnd(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		end   int // expected offset just past the last complete record
		clean bool
	}{
		{"empty", "", 0, true},
		{"single complete", "a,b\n", 4, true},
		{"two complete", "a,b\nc,d\n", 8, true},
		{"simple torn", "a,b\nc,d", 4, false},
		{"torn without any newline", "no-newline-torn", 0, false},
		{"quoted newline inside a complete record", "a,\"x\ny\"\n", 8, true},
		{"quoted CRLF record", "a,\"x\r\ny\"\r\n", 10, true},
		// the round-1 probe shape: the newline inside the unterminated
		// quoted field must NOT be treated as a boundary.
		{"torn mid quoted multiline field", "a,b\n\"partial\nrecord", 4, false},
		{"escaped quotes then boundary", "\"a\"\"b\",c\n", 9, true},
		{"quote mid-field is literal", "ab\"c,d\ne,f\n", 11, true},
		{"torn inside escaped quote pair", "\"a\"\"b\",c\n\"tor\"\"n", 9, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			end, clean := lastCompleteCSVRecordEnd([]byte(tc.in))
			assert.Equal(t, tc.end, end)
			assert.Equal(t, tc.clean, clean)
			if tc.end > 0 {
				assert.Equal(t, byte('\n'), tc.in[tc.end-1])
			}
		})
	}
}

// TestRecoverSpoolSegmentContent pins the crash-content split: the complete
// prefix passes, the torn tail is dropped (never fsync-confirmed), and a
// mid-file malformed record is poison, not a tail.
func TestRecoverSpoolSegmentContent(t *testing.T) {
	t.Run("clean passes unchanged", func(t *testing.T) {
		content, poison := recoverSpoolSegmentContent("x", []byte("a,b\nc,d\n"))
		assert.False(t, poison)
		assert.Equal(t, []byte("a,b\nc,d\n"), content)
	})

	t.Run("the complete prefix of a torn file replays cleanly", func(t *testing.T) {
		content, poison := recoverSpoolSegmentContent("x", []byte("a,b\nc,d"))
		assert.False(t, poison)
		assert.Equal(t, []byte("a,b\n"), content)
	})

	// the round-1 probe: a crash leaves a partial CSV record containing a
	// newline inside a quoted field. Recovery must keep the complete row,
	// and the remaining prefix must parse under the destination's CSV rules.
	t.Run("torn multiline quoted field preserves the complete prefix", func(t *testing.T) {
		row := encodeStatementInfoCSVLine(context.Background(), []string{"id-1", "insert into t values (1)"})
		raw := append(append([]byte(nil), row...), []byte("\"id-2\",\"insert\nSELECT partial")...)
		content, poison := recoverSpoolSegmentContent("x", raw)
		assert.False(t, poison)
		assert.Equal(t, row, content)
		require.NoError(t, validateSpoolCSVContent(content))
		r := csv.NewReader(bytes.NewReader(content))
		rows, err := r.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "id-1", rows[0][0])
	})

	t.Run("control: single-line torn tail", func(t *testing.T) {
		content, poison := recoverSpoolSegmentContent("x", []byte("a,b\n\"part"))
		assert.False(t, poison)
		assert.Equal(t, []byte("a,b\n"), content)
	})

	t.Run("mid-file malformed record is poison", func(t *testing.T) {
		// a bare quote inside a quoted field: encoding/csv rejects it, and
		// no torn append can produce it (tears only strip the tail).
		content, poison := recoverSpoolSegmentContent("x", []byte("a,b\n\"bad\"quote,c\nd,e\n"))
		assert.True(t, poison)
		assert.Nil(t, content)
	})

	t.Run("empty and fully torn stay harmless", func(t *testing.T) {
		content, poison := recoverSpoolSegmentContent("x", nil)
		assert.False(t, poison)
		assert.Empty(t, content)
		content, poison = recoverSpoolSegmentContent("x", []byte("\"unterminated-from-the-start"))
		assert.False(t, poison)
		assert.Empty(t, content) // whole file was one torn record
	})
}

// TestStatementInfoSpoolReplayPoisonSkippedNotBlocking proves a quarantined
// segment can never head-of-line block the replay of later segments.
func TestStatementInfoSpoolReplayPoisonSkippedNotBlocking(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	line2 := encodeStatementInfoCSVLine(ctx, []string{"healthy-2"})
	// plant two orphans by hand: segment 1 is poison mid-file, segment 2 is
	// healthy.
	require.NoError(t, os.WriteFile(filepath.Join(dir, spoolSegmentFileName(1)),
		[]byte("a,b\n\"bad\"quote,c\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, spoolSegmentFileName(2)), line2, 0644))

	stub := stubSpoolSQLFlush(t)
	s := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s.close()
	require.Len(t, s.orphans, 2)

	s.replayOnce(ctx)
	// both orphans left the queue; only the healthy segment was flushed.
	assert.False(t, s.hasPendingOrphans())
	assert.Equal(t, line2, stub.bytes())
	// the poison segment is quarantined on disk (operator forensics), not
	// replayed, not blocking, not silently deleted.
	files := spoolDirFiles(t, dir)
	assert.NotContains(t, files, spoolSegmentFileName(1))
	assert.Contains(t, files, spoolSegmentFileName(1)+spoolPoisonFileSuffix)

	// a later pass is a no-op: the quarantine never re-enters the queue.
	flushed := stub.calls()
	s.replayOnce(ctx)
	assert.Equal(t, flushed, stub.calls())
	// and a re-booted spool does not even see the quarantined file (the one
	// orphan it inherits is the first spool's own empty active segment).
	s.close()
	s2 := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s2.close()
	require.Len(t, s2.orphans, 1)
	assert.Equal(t, uint64(3), s2.orphans[0].seq)
	for _, o := range s2.orphans {
		assert.NotContains(t, o.name, spoolPoisonFileSuffix)
	}
}

func TestStatementInfoSpoolStrayAckMarkerPurged(t *testing.T) {
	dir := t.TempDir()
	// a marker whose segment file is already gone carries no information and
	// must not linger.
	stray := filepath.Join(dir, spoolAckFileName(spoolSegmentFileName(7)))
	require.NoError(t, os.WriteFile(stray, nil, 0644))
	s := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s.close()
	assert.NoFileExists(t, stray)
}

func TestStatementInfoSpoolStartReplayRetryLoop(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	line := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	{
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		s.appendRecord(line)
		spoolKillFile(s)
	}

	stub := stubSpoolSQLFlush(t)
	stub.failErr = io.ErrClosedPipe
	s2 := openTestSpool(t, dir, 0, 1, time.Minute)
	s2.replayRetryInterval = 30 * time.Millisecond
	defer s2.close()
	s2.startReplay(ctx)
	// still failing
	require.Eventually(t, func() bool { return stub.calls() >= 1 }, 2*time.Second, 10*time.Millisecond)
	assert.True(t, s2.hasPendingOrphans())
	// heal: the retry loop drains the orphan by itself
	stub.failErr = nil
	require.Eventually(t, func() bool { return !s2.hasPendingOrphans() }, 2*time.Second, 10*time.Millisecond)
	<-s2.replayDone
	assert.Equal(t, line, stub.bytes())
}

func TestWriteSpoolContent(t *testing.T) {
	ctx := context.Background()
	content := []byte("row1,1\nrow2,2\n")

	// the replay flush hands the byte-exact content to the SQL destination
	stub := stubSpoolSQLFlush(t)
	require.NoError(t, writeSpoolContent(ctx, content))
	assert.Equal(t, content, stub.bytes())
	assert.Equal(t, 1, stub.calls())

	// a flush failure must surface: the caller keeps the spool segment and
	// retries instead of dropping the records into the hours-late CSV
	// merge path (see writeSpoolContent).
	stub.failErr = io.ErrClosedPipe
	require.Error(t, writeSpoolContent(ctx, content))
}

func TestEncodeStatementInfoCSVLine(t *testing.T) {
	ctx := context.Background()
	fields := []string{
		"573dc0ed-b62a-11ee-b8e5-96e93aa98c85",
		"text with, comma and \"quote\" and \n newline",
		"",
		"2024-01-01 00:00:01.000000",
		"中文字段",
	}
	var expect bytes.Buffer
	w := db_holder.NewCSVWriterWithBuffer(ctx, &expect)
	require.NoError(t, w.WriteStrings(fields))
	_, err := w.FlushAndClose()
	require.NoError(t, err)
	assert.Equal(t, expect.Bytes(), encodeStatementInfoCSVLine(ctx, fields))
}

// TestSpooledItemBufferWire covers the statement_info buffer decoration:
// Add parents records to the instance's batch id, GetBatch wraps the
// generated request(s) with the ack tracker, and only a fully committed
// batch advances the export watermark.
func TestSpooledItemBufferWire(t *testing.T) {
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"r"})

	t.Run("single request: commit acks, watermark advances, retirement waits for lag", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Hour)
		defer s.close()
		seq := s.appendRecord(line)
		require.NotZero(t, seq)
		buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
		buf.Add(&StatementInfo{spoolSeq: seq})
		require.NotZero(t, buf.batchID)

		batch := buf.GetBatch(ctx, new(bytes.Buffer))
		req, ok := batch.(*spoolBatchRequest)
		require.True(t, ok)
		_, err := req.Handle()
		require.NoError(t, err)
		s.mux.Lock()
		assert.Equal(t, buf.batchID, s.watermark)
		require.Len(t, s.sealed, 1)
		var seg *spoolSegment
		for _, g := range s.sealed {
			seg = g
		}
		s.mux.Unlock()
		// covered (tagged 1/1, W >= maxTag) but the lag guard keeps it
		assert.True(t, s.segmentExportCoveredLocked(seg, s.now()))
		assert.Contains(t, spoolDirFiles(t, dir), seg.name)
	})

	t.Run("failure stalls the watermark, everything stays for replay", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Hour)
		defer s.close()
		seq := s.appendRecord(line)
		buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{err: io.ErrClosedPipe}}, spool: s}
		buf.Add(&StatementInfo{spoolSeq: seq})
		req := buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
		_, err := req.Handle()
		require.Error(t, err)
		s.mux.Lock()
		assert.Equal(t, uint64(0), s.watermark)
		s.mux.Unlock()
		assert.Len(t, spoolDirFiles(t, dir), 2) // sealed segment + fresh active
	})

	t.Run("export requests ack only after ALL requests of the batch commit", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Hour)
		defer s.close()
		seq := s.appendRecord(line)
		inner := &spoolFakeBuffer{batch: table.ExportRequests{&spoolFakeWriteRequest{}, &spoolFakeWriteRequest{}}}
		buf := &spooledItemBuffer{Buffer: inner, spool: s}
		buf.Add(&StatementInfo{spoolSeq: seq})
		batch := buf.GetBatch(ctx, new(bytes.Buffer))
		reqs, ok := batch.(table.ExportRequests)
		require.True(t, ok)
		require.Len(t, reqs, 2)
		_, err := reqs[0].Handle()
		require.NoError(t, err)
		s.mux.Lock()
		assert.Equal(t, uint64(0), s.watermark) // half the batch is not a commit
		s.mux.Unlock()
		_, err = reqs[1].Handle()
		require.NoError(t, err)
		s.mux.Lock()
		assert.Equal(t, buf.batchID, s.watermark)
		s.mux.Unlock()
	})

	t.Run("one failed request of a split batch poisons the commit", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Hour)
		defer s.close()
		seq := s.appendRecord(line)
		inner := &spoolFakeBuffer{batch: table.ExportRequests{&spoolFakeWriteRequest{}, &spoolFakeWriteRequest{err: io.ErrClosedPipe}}}
		buf := &spooledItemBuffer{Buffer: inner, spool: s}
		buf.Add(&StatementInfo{spoolSeq: seq})
		reqs := buf.GetBatch(ctx, new(bytes.Buffer)).(table.ExportRequests)
		_, err := reqs[0].Handle()
		require.NoError(t, err)
		_, err = reqs[1].Handle()
		require.Error(t, err)
		s.mux.Lock()
		assert.Equal(t, uint64(0), s.watermark)
		s.mux.Unlock()
	})

	t.Run("unspooled items consume no batch id, non-request batches pass through", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Hour)
		defer s.close()
		s.appendRecord(line)
		buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: "not-a-write-request"}, spool: s}
		buf.Add(&StatementInfo{}) // spoolSeq 0: never parented
		assert.Zero(t, buf.batchID)
		assert.Equal(t, "not-a-write-request", buf.GetBatch(ctx, new(bytes.Buffer)))
		s.mux.Lock()
		assert.Equal(t, uint64(0), s.watermark)
		next := s.nextBatchID
		s.mux.Unlock()
		assert.Equal(t, uint64(1), next) // no id ever allocated
	})
}

// spoolFakeBuffer is a minimal Buffer implementation for the wrapper tests.
type spoolFakeBuffer struct {
	noopReminder
	batch          any
	getBatchCalled bool
}

type noopReminder struct{}

func (noopReminder) RemindNextAfter() time.Duration { return time.Hour }
func (noopReminder) RemindBackOff()                 {}
func (noopReminder) RemindBackOffCnt() int          { return 0 }
func (noopReminder) RemindReset()                   {}

func (b *spoolFakeBuffer) Add(item bp.HasName) {}
func (b *spoolFakeBuffer) Reset()              {}
func (b *spoolFakeBuffer) IsEmpty() bool       { return false }
func (b *spoolFakeBuffer) ShouldFlush() bool   { return false }
func (b *spoolFakeBuffer) Size() int64         { return 0 }
func (b *spoolFakeBuffer) GetBatch(ctx context.Context, _ *bytes.Buffer) any {
	b.getBatchCalled = true
	return b.batch
}

// spoolFakeWriteRequest captures Handle results.
type spoolFakeWriteRequest struct {
	err error
}

func (r *spoolFakeWriteRequest) Handle() (int, error) { return 1, r.err }

// spoolFakeExecPlan counts Marshal calls to prove the plan is marshaled once.
type spoolFakeExecPlan struct {
	marshaled int
	json      []byte
}

func (p *spoolFakeExecPlan) Marshal(context.Context) []byte {
	p.marshaled++
	return p.json
}
func (p *spoolFakeExecPlan) Free() {}
func (p *spoolFakeExecPlan) Stats(ctx context.Context) (statistic.StatsArray, Statistic) {
	return statistic.StatsArray{}, Statistic{}
}

func TestCloneForSpoolSharesPlanJson(t *testing.T) {
	ctx := context.Background()
	plan := &spoolFakeExecPlan{json: []byte(`{"plan": 42}`)}
	s := &StatementInfo{
		Statement:     []byte("insert into t values (1)"),
		StatementType: "Insert",
		QueryType:     "DML",
		Status:        StatementStatusSuccess,
		ExecPlan:      plan,
	}
	snapshot := s.cloneForSpool(ctx)
	assert.Equal(t, 1, plan.marshaled)
	assert.Equal(t, s.jsonByte, snapshot.jsonByte)

	// the later export (FillRow on the source) must reuse the cached json:
	// the plan is still marshaled exactly once.
	row := s.GetTable().GetRow(ctx)
	s.FillRow(ctx, row)
	srcFields := row.ToStrings()
	row.Free()
	assert.Equal(t, 1, plan.marshaled)

	// and the snapshot renders the very same row.
	row2 := snapshot.GetTable().GetRow(ctx)
	snapshot.FillRow(ctx, row2)
	snapshotFields := row2.ToStrings()
	row2.Free()
	assert.Equal(t, srcFields, snapshotFields)
}

func TestSpoolStatementInfoDurableContent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	provider := GetTracerProvider()
	oldSpool := provider.stmtInfoSpool
	defer func() { provider.stmtInfoSpool = oldSpool }()

	s := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s.close()
	provider.stmtInfoSpool = s

	stmt := &StatementInfo{
		Statement:     []byte("insert /* ut_marker_30812 */ into t values (1)"),
		StatementType: "Insert",
		QueryType:     "DML",
		Status:        StatementStatusSuccess,
		Account:       "sys",
		User:          "dump",
		ConnType:      1,
	}
	spoolStatementInfo(ctx, stmt)

	content := readSpoolFile(t, dir, s.active.seq)
	r := csv.NewReader(bytes.NewReader(content))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	// the persisted line carries the statement text exactly like the export
	joined := strings.Join(rows[0], "|")
	assert.Contains(t, joined, "ut_marker_30812")
	assert.Contains(t, joined, "Insert")

	// a nil-spool provider is a silent no-op (disabled configuration).
	provider.stmtInfoSpool = nil
	spoolStatementInfo(ctx, stmt)
}

func TestStatementInfoSpoolDoubleClose(t *testing.T) {
	dir := t.TempDir()
	s := openTestSpool(t, dir, 0, 1, time.Minute)
	s.startJanitorOnly()
	// close must be idempotent
	s.close()
	s.close()
	assert.True(t, s.closed)
}

// BenchmarkStatementInfoSpoolRecord measures the added cost of the
// crash-durable staging on the report path: the row render plus the line
// encode plus the append with fsync. Compare against the plain encode without
// the file I/O to isolate the durability cost.
//
//	go test -run=NONE -bench=BenchmarkStatementInfoSpoolRecord -benchtime=2000x ./pkg/util/trace/impl/motrace/
func BenchmarkStatementInfoSpoolRecord(b *testing.B) {
	ctx := context.Background()
	provider := GetTracerProvider()
	oldSpool := provider.stmtInfoSpool
	defer func() { provider.stmtInfoSpool = oldSpool }()
	mkStmt := func() *StatementInfo {
		return &StatementInfo{
			Statement:     []byte("insert /* si_bench_30812 */ into bench.t values(1,1.25)"),
			StatementType: "Insert",
			QueryType:     "DML",
			Status:        StatementStatusSuccess,
			Account:       "sys",
			User:          "root",
			ConnType:      1,
			Database:      "bench",
			SqlSourceType: "external",
			RequestAt:     time.Now(),
		}
	}
	renderOnly := func() {
		stm := mkStmt()
		snapshot := stm.cloneForSpool(ctx)
		row := snapshot.GetTable().GetRow(ctx)
		snapshot.FillRow(ctx, row)
		fields := row.ToStrings()
		row.Free()
		_ = encodeStatementInfoCSVLine(ctx, fields)
	}
	b.Run("render_and_encode_without_io", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			renderOnly()
		}
	})
	b.Run("fsync_per_record", func(b *testing.B) {
		dir := b.TempDir()
		s, err := newStatementInfoSpool(dir, 0, 1, time.Minute)
		require.NoError(b, err)
		defer s.close()
		provider.stmtInfoSpool = s
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			spoolStatementInfo(ctx, mkStmt())
		}
	})
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("group_commit_%d_writers", workers), func(b *testing.B) {
			dir := b.TempDir()
			s, err := newStatementInfoSpool(dir, 0, 1, time.Minute)
			require.NoError(b, err)
			defer s.close()
			provider.stmtInfoSpool = s
			line := encodeStatementInfoCSVLine(ctx, []string{"a", "b"})
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < b.N; i++ {
						s.appendRecord(line)
					}
				}()
			}
			wg.Wait()
		})
	}
}

func TestStatementInfoSpoolErrorPaths(t *testing.T) {
	line := encodeStatementInfoCSVLine(context.Background(), []string{"r"})

	// every spool I/O failure must degrade to memory-only behavior without
	// breaking the statement path.
	t.Run("append when the spool home vanished", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		defer s.close()
		require.NoError(t, os.RemoveAll(dir))
		// the active file handles still work; a rotation then fails to open
		// the next file and the append drops the record silently.
		s.appendRecord(line)
		_ = s.sealForExport()
		s.appendRecord(line)                        // open fails: rate-limited error log + drop
		s.appendRecord(line)                        // rate-limited second failure within a minute
		assert.Equal(t, int64(0), s.droppedRecords) // errors are counted via metric, not the drop counter
	})

	t.Run("write on a stale file handle", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 1, time.Minute)
		defer s.close()
		spoolStaleFile(s)    // stale: as after an abrupt fd loss
		s.appendRecord(line) // write error: rate-limited, no panic
	})

	t.Run("remove failure keeps the segment", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 0, 0)
		defer s.close()
		s.appendRecord(line)
		seg := s.sealForExport()
		require.NotNil(t, seg)
		// block the removal: replace the segment with a non-empty directory
		// of the same name. os.Remove fails deterministically, even for root.
		path := filepath.Join(dir, seg.name)
		require.NoError(t, os.Remove(path))
		require.NoError(t, os.Mkdir(path, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "blocker"), []byte("x"), 0644))
		defer func() { _ = os.RemoveAll(path) }()
		s.mux.Lock() // removal fails loudly, segment stays known
		s.collectRemovableLocked()
		_, stillSealed := s.sealed[seg.seq]
		s.mux.Unlock()
		assert.True(t, stillSealed)
		require.NoError(t, os.RemoveAll(path))
		// next cleanup round succeeds
		s.mux.Lock()
		s.collectRemovableLocked()
		s.mux.Unlock()
		assert.NotContains(t, spoolDirFiles(t, dir), seg.name)
	})

	t.Run("janitor removes acked segments by itself", func(t *testing.T) {
		dir := t.TempDir()
		s := openTestSpool(t, dir, 0, 0, 0)
		s.janitorInterval = 20 * time.Millisecond
		s.startReplay(context.Background())
		defer s.close()
		s.appendRecord(line)
		seg := s.sealForExport()
		require.NotNil(t, seg)
		// untagged segment, zero lag duration: covered immediately (see
		// segmentExportCoveredLocked), the janitor collects it on its own.
		require.Eventually(t, func() bool {
			files := spoolDirFiles(t, dir)
			for _, name := range files {
				if name == seg.name {
					return false
				}
			}
			return true
		}, 3*time.Second, 10*time.Millisecond)
	})
}

// spoolKillFile drops the active segment file the way an abrupt process exit
// leaves it: the appends are durable on disk, the handle is gone, no
// lifecycle bookkeeping ran.
func spoolKillFile(s *statementInfoSpool) {
	s.fg.mu.Lock()
	defer s.fg.mu.Unlock()
	_, _ = s.fg.sealFileLocked()
}

// spoolStaleFile mimics a stale file descriptor for error-path tests: the raw
// handle is closed but the group still believes it owns it, so the next
// append hits the write error branch.
func spoolStaleFile(s *statementInfoSpool) {
	s.fg.mu.Lock()
	defer s.fg.mu.Unlock()
	if s.fg.f != nil {
		_ = s.fg.f.Close()
	}
}

// ---------------------------------------------------------------------------
// Group-commit fsync protocol: an append must never report success before an
// fsync of the file holding its record confirmed it (round-1 review repair).
// ---------------------------------------------------------------------------

// spoolCountingFile wraps a real segment file, counting Sync calls and
// optionally failing them.
type spoolCountingFile struct {
	*os.File
	mux   sync.Mutex
	syncs int
	fail  error
}

func (f *spoolCountingFile) Sync() error {
	f.mux.Lock()
	f.syncs++
	f.mux.Unlock()
	if f.fail != nil {
		return f.fail
	}
	return f.File.Sync()
}

func (f *spoolCountingFile) syncCount() int {
	f.mux.Lock()
	defer f.mux.Unlock()
	return f.syncs
}

// spoolFileFactoryStub wraps every segment file the spool opens with a
// spoolCountingFile, keyed by segment seq.
type spoolFileFactoryStub struct {
	mux      sync.Mutex
	files    map[uint64]*spoolCountingFile
	failSync error
}

func stubSpoolFileFactory(t *testing.T) *spoolFileFactoryStub {
	t.Helper()
	st := &spoolFileFactoryStub{files: make(map[uint64]*spoolCountingFile)}
	old := openSpoolSegmentFile
	openSpoolSegmentFile = func(dir string, seq uint64) (spoolSyncFile, int64, error) {
		f, size, err := openSegmentFile(dir, seq)
		if err != nil {
			return nil, 0, err
		}
		st.mux.Lock()
		w := &spoolCountingFile{File: f, fail: st.failSync}
		st.files[seq] = w
		st.mux.Unlock()
		return w, size, nil
	}
	t.Cleanup(func() { openSpoolSegmentFile = old })
	return st
}

func (st *spoolFileFactoryStub) file(seq uint64) *spoolCountingFile {
	st.mux.Lock()
	defer st.mux.Unlock()
	return st.files[seq]
}

// spoolGateFile is a memory-backed spoolSyncFile whose FIRST Sync blocks on a
// gate, for deterministic concurrency tests of the group-commit protocol.
type spoolGateFile struct {
	mux         sync.Mutex
	buf         []byte
	writes      int
	syncs       int
	blockFirst  bool
	gate        chan struct{} // released to unblock the first Sync
	enteredSync chan struct{} // closed when the first Sync starts
	secondWrite chan struct{} // closed when the second Write lands
	thirdWrite  chan struct{} // closed when the third Write lands
	onceEntered sync.Once
	failOn      int   // Sync calls with syncs >= failOn return failErr (0 = never)
	failErr     error // error returned by failing Syncs
}

func (f *spoolGateFile) Write(p []byte) (int, error) {
	f.mux.Lock()
	f.buf = append(f.buf, p...)
	f.writes++
	w := f.writes
	f.mux.Unlock()
	if w == 2 && f.secondWrite != nil {
		close(f.secondWrite)
	}
	if w == 3 && f.thirdWrite != nil {
		close(f.thirdWrite)
	}
	return len(p), nil
}

func (f *spoolGateFile) Sync() error {
	f.mux.Lock()
	f.syncs++
	n := f.syncs
	f.mux.Unlock()
	if n == 1 {
		f.onceEntered.Do(func() {
			if f.enteredSync != nil {
				close(f.enteredSync)
			}
		})
		if f.blockFirst {
			<-f.gate
		}
	}
	f.mux.Lock()
	failOn, failErr := f.failOn, f.failErr
	f.mux.Unlock()
	if failOn > 0 && n >= failOn {
		return failErr
	}
	return nil
}

func (f *spoolGateFile) setFail(failOn int, err error) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.failOn = failOn
	f.failErr = err
}

func (f *spoolGateFile) Close() error { return nil }

func (f *spoolGateFile) Truncate(size int64) error {
	f.mux.Lock()
	defer f.mux.Unlock()
	if int64(len(f.buf)) > size {
		f.buf = f.buf[:size]
	}
	return nil
}

func (f *spoolGateFile) counters() (writes, syncs int) {
	f.mux.Lock()
	defer f.mux.Unlock()
	return f.writes, f.syncs
}

// TestSpoolFsyncGroupAppendSurfacesSyncError is the round-1 probe shape: an
// append whose fsync fails must surface the error AT THE APPEND, before any
// seal — never report success over a record that only sits in the page cache.
func TestSpoolFsyncGroupAppendSurfacesSyncError(t *testing.T) {
	newGroupWith := func(f spoolSyncFile) *spoolFsyncGroup {
		g := &spoolFsyncGroup{}
		g.init()
		g.mu.Lock()
		g.adoptLocked(f, 0)
		g.mu.Unlock()
		return g
	}

	// Sync-failing file: the first append of a fresh file must attempt the
	// fsync (round-1 defect: the wait loop compared equal zeros and skipped
	// it entirely) and must surface its error.
	fb := &spoolMemFile{fail: io.ErrClosedPipe}
	g := newGroupWith(fb)
	_, err := g.append([]byte("x\n"))
	require.Error(t, err)
	assert.Equal(t, 1, fb.syncCount())
	// the next append retries the fsync and surfaces the error again
	_, err = g.append([]byte("y\n"))
	require.Error(t, err)
	assert.Equal(t, 2, fb.syncCount())

	// healthy control: exactly one confirmed fsync per lonely append.
	hb := &spoolMemFile{}
	g2 := newGroupWith(hb)
	_, err = g2.append([]byte("x\n"))
	require.NoError(t, err)
	assert.Equal(t, 1, hb.syncCount())
}

// spoolMemFile is a memory-backed spoolSyncFile with an optional Sync error
// and one-shot partial-write injection (partialN/partialErr) for the torn
// tail repair probe.
type spoolMemFile struct {
	mux        sync.Mutex
	buf        []byte
	syncs      int
	fail       error
	partialN   int   // next Write keeps only this many bytes and fails
	partialErr error // error paired with the partial write
	truncs     []int64
}

func (f *spoolMemFile) Write(p []byte) (int, error) {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.partialN > 0 {
		n := f.partialN
		if n > len(p) {
			n = len(p)
		}
		err := f.partialErr
		if err == nil {
			err = io.ErrShortWrite
		}
		f.partialN = 0
		f.partialErr = nil
		f.buf = append(f.buf, p[:n]...)
		return n, err
	}
	f.buf = append(f.buf, p...)
	return len(p), nil
}

func (f *spoolMemFile) Truncate(size int64) error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.truncs = append(f.truncs, size)
	if int64(len(f.buf)) > size {
		f.buf = f.buf[:size]
	}
	return nil
}

func (f *spoolMemFile) Sync() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.syncs++
	return f.fail
}

func (f *spoolMemFile) Close() error { return nil }

func (f *spoolMemFile) syncCount() int {
	f.mux.Lock()
	defer f.mux.Unlock()
	return f.syncs
}

// TestSpoolFsyncGroupAppendSyncFailsOnRealPipe keeps the real-fd fidelity of
// the probe: Sync on a pipe fd fails with EINVAL, and append surfaces it.
func TestSpoolFsyncGroupAppendSyncFailsOnRealPipe(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer func() {
		_ = r.Close()
		_ = w.Close()
	}()
	go func() { _, _ = io.Copy(io.Discard, r) }()

	g := &spoolFsyncGroup{}
	g.init()
	g.mu.Lock()
	g.adoptLocked(w, 0)
	g.mu.Unlock()
	_, err = g.append([]byte("x\n"))
	require.Error(t, err)
}

// TestSpoolFsyncGroupCommitSharesSync queues three records behind one
// in-flight Sync: exactly one more Sync confirms the whole queue — the
// group-commit amortization the protocol exists for.
func TestSpoolFsyncGroupCommitSharesSync(t *testing.T) {
	f := &spoolGateFile{
		blockFirst: true,
		gate:       make(chan struct{}),
		thirdWrite: make(chan struct{}),
	}
	// the first Sync unblocks once all three appends have landed, so the
	// leader's generation snapshot can only cover the first record; the
	// queued two must then share exactly one following Sync.
	go func() {
		<-f.thirdWrite
		close(f.gate)
	}()

	g := &spoolFsyncGroup{}
	g.init()
	g.mu.Lock()
	g.adoptLocked(f, 0)
	g.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = g.append([]byte("x\n"))
		}(i)
	}
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i])
	}
	writes, syncs := f.counters()
	assert.Equal(t, 3, writes)
	assert.Equal(t, 2, syncs) // in-flight leader + one Sync for the queue
	g.mu.Lock()
	assert.Equal(t, g.fileSize, g.confirmedWidth)
	g.mu.Unlock()
}

// TestSpoolFsyncGroupWaiterAcrossSealSucceeds: an appender whose fsync
// confirmation is delivered by the rotation's seal Sync (sealGen bump) must
// return success, not hang behind the fresh file and not double-fsync.
func TestSpoolFsyncGroupWaiterAcrossSealSucceeds(t *testing.T) {
	f := &spoolGateFile{
		blockFirst:  true,
		gate:        make(chan struct{}),
		enteredSync: make(chan struct{}),
		secondWrite: make(chan struct{}),
	}
	g := &spoolFsyncGroup{}
	g.init()
	g.mu.Lock()
	g.adoptLocked(f, 0)
	g.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(1)
	go func() { // appender A: writes, then blocks inside the first Sync
		defer wg.Done()
		_, errs[0] = g.append([]byte("a\n"))
	}()
	<-f.enteredSync
	wg.Add(1)
	go func() { // appender B: writes during A's Sync, then waits
		defer wg.Done()
		_, errs[1] = g.append([]byte("b\n"))
	}()
	<-f.secondWrite

	// the rotation's seal waits out A's in-flight Sync, then confirms the
	// whole file once; both appenders must return success through it.
	sealDone := make(chan error, 1)
	go func() {
		g.mu.Lock()
		_, err := g.sealFileLocked()
		g.mu.Unlock()
		sealDone <- err
	}()
	close(f.gate)
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NoError(t, <-sealDone)
	_, syncs := f.counters()
	// A's in-flight Sync + the seal Sync; B may additionally lead one Sync
	// if it wins the mutex between A's completion and the seal — all shapes
	// are correct, only the appenders' success and the single seal matter.
	assert.GreaterOrEqual(t, syncs, 2)
	assert.LessOrEqual(t, syncs, 3)
	g.mu.Lock()
	assert.Nil(t, g.f)
	assert.Equal(t, uint64(1), g.sealGen)
	g.mu.Unlock()
}

// TestSpoolFsyncGroupFreshFileRequiresNewSync: confirmation is per file — the
// first append after every rotation must pay a fresh fsync on the new file;
// the previous file's history can never cover it.
func TestSpoolFsyncGroupFreshFileRequiresNewSync(t *testing.T) {
	f1 := &spoolMemFile{}
	f2 := &spoolMemFile{}
	g := &spoolFsyncGroup{}
	g.init()
	g.mu.Lock()
	g.adoptLocked(f1, 0)
	g.mu.Unlock()

	_, err := g.append([]byte("a\n"))
	require.NoError(t, err)
	assert.Equal(t, 1, f1.syncCount())

	g.mu.Lock()
	_, err = g.sealFileLocked()
	require.NoError(t, err)
	g.adoptLocked(f2, 0)
	g.mu.Unlock()

	// a fresh file starts with nothing confirmed: the append fsyncs again.
	_, err = g.append([]byte("b\n"))
	require.NoError(t, err)
	assert.Equal(t, 1, f2.syncCount())
	assert.Equal(t, 2, f1.syncCount()) // append + seal; untouched afterwards
}

// TestStatementInfoSpoolAppendFsyncBeforeAck is the spool-level
// Sync-injection proof the durability contract holds end to end: the record
// is on disk (fsync completed) BEFORE the export batch is even generated —
// the export ack then only manages retirement, never durability.
func TestStatementInfoSpoolAppendFsyncBeforeAck(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st := stubSpoolFileFactory(t)
	s := openTestSpool(t, dir, 0, 1, time.Hour)
	defer s.close()

	line := encodeStatementInfoCSVLine(ctx, []string{"durable-1"})
	seq := s.appendRecord(line)
	require.NotZero(t, seq)

	// BEFORE any seal/export tick the append already paid its fsync — the
	// exact step the round-1 defect skipped (records sat un-fsynced until
	// seal or close).
	f1 := st.file(1)
	require.NotNil(t, f1)
	assert.Equal(t, 1, f1.syncCount())
	// and the bytes are really in the file, not only in the page cache
	// pipeline assumption: reading the handle back shows the full record.
	assert.Equal(t, line, readSpoolFile(t, dir, 1))

	// export: seal adds one more sync to the old file, adopts a fresh one;
	// the batch commit then advances the watermark (retirement, not
	// durability — that was proven above).
	buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	buf.Add(&StatementInfo{spoolSeq: seq})
	req := buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	_, err := req.Handle()
	require.NoError(t, err)
	assert.Equal(t, 2, f1.syncCount()) // +1 from the rotation seal
	f2 := st.file(2)
	require.NotNil(t, f2)
	assert.Equal(t, 0, f2.syncCount()) // fresh file: nothing confirmed yet
	s.mux.Lock()
	assert.Equal(t, buf.batchID, s.watermark)
	s.mux.Unlock()

	// the rotation reset is real: the next append fsyncs the fresh file.
	s.appendRecord(line)
	assert.Equal(t, 1, f2.syncCount())
}

// TestStatementInfoSpoolAppendSyncFailureDegrades: a broken spool disk must
// degrade appends to the pre-spool memory-only behavior without panicking,
// with the quota rolled back and the loss metered through the error path.
func TestStatementInfoSpoolAppendSyncFailureDegrades(t *testing.T) {
	dir := t.TempDir()
	st := stubSpoolFileFactory(t)
	st.failSync = io.ErrClosedPipe
	s := openTestSpool(t, dir, 0, 1, time.Hour)
	defer s.close()

	line := encodeStatementInfoCSVLine(context.Background(), []string{"r"})
	s.appendRecord(line)
	s.appendRecord(line)
	// charge-on-write: a failed fsync must not uncharge bytes that sit on
	// disk; both records' bytes stay charged (see TestSyncErrorMustNotUnchargeWrittenBytes).
	assert.Equal(t, 2*int64(len(line)), s.totalSize)
	assert.Equal(t, int64(0), s.droppedRecords) // I/O error, not a capacity drop
	// close over a file whose Sync failed must not panic either.
	s.close()
	assert.True(t, s.closed)
	// still counted so the failure is observable.
	f1 := st.file(1)
	require.NotNil(t, f1)
	assert.GreaterOrEqual(t, f1.syncCount(), 3) // two appends + the close seal
}

// ---------------------------------------------------------------------------
// Round-2 review probes: segment/batch misalignment, charge-on-write quota,
// poison boot accounting.
// ---------------------------------------------------------------------------

// TestBatchCommitMustCoverEverySpoolRecord is the round-2 review probe.
// Buffer A is detached before B is reported; A's asynchronous GetBatch only
// seals the spool AFTER B's append — so the sealed file contains A and B,
// but only A's batch commits, and a later batch C commits while B remains
// pending. Retirement must NOT delete the file holding B's durable copy.
func TestBatchCommitMustCoverEverySpoolRecord(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	// zero lag: removal is a pure function of coverage (the lag+horizon
	// delay is exercised by TestStatementInfoSpoolWatermarkLifecycle).
	s := openTestSpool(t, dir, 0, 0, 0)
	defer s.close()
	line := encodeStatementInfoCSVLine(ctx, []string{"r"})

	// A's records land; B's record lands BEFORE A's generation runs —
	// A's seal will capture all three into one file (the misalignment).
	seqA1 := s.appendRecord(line)
	seqA2 := s.appendRecord(line)
	seqB := s.appendRecord(line)
	require.NotZero(t, seqA1)
	require.NotZero(t, seqA2)
	require.NotZero(t, seqB)

	// batch A buffers records 1,2; its GetBatch seals segment 1 = {A1,A2,B}.
	bufA := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	bufA.Add(&StatementInfo{spoolSeq: seqA1})
	bufA.Add(&StatementInfo{spoolSeq: seqA2})
	reqA := bufA.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	require.NotNil(t, reqA)
	s.mux.Lock()
	require.Len(t, s.sealed, 1)
	segAB := s.sealedOrder[0]
	require.Equal(t, int64(3), segAB.recordCount)
	require.Equal(t, int64(2), segAB.taggedCount) // B not parented yet
	s.mux.Unlock()

	// B's buffer parents its record to batch 2 — but B's batch stays
	// pending forever (its request never even generates in this leg).
	bufB := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	bufB.Add(&StatementInfo{spoolSeq: seqB})
	s.mux.Lock()
	require.Equal(t, int64(3), segAB.taggedCount)
	require.Equal(t, bufB.batchID, segAB.maxTag)
	s.mux.Unlock()

	// C's record lands in the fresh active segment; C's GetBatch seals it.
	seqC := s.appendRecord(line)
	require.NotZero(t, seqC)
	bufC := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	bufC.Add(&StatementInfo{spoolSeq: seqC})
	reqC := bufC.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	require.NotNil(t, reqC)

	// A commits; C commits — the round-2 defect deleted the A/B file here.
	_, err := reqA.Handle()
	require.NoError(t, err)
	_, err = reqC.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	// contiguous completion: B (tag 2) pending pins the watermark at 1,
	// even though C (tag 3) committed.
	assert.Equal(t, uint64(1), s.watermark)
	s.mux.Unlock()
	files := spoolDirFiles(t, dir)
	assert.Contains(t, files, segAB.name, "the file holding B's records must survive while B is pending")
	require.Len(t, s.sealedOrder, 2)
	segC := s.sealedOrder[1]
	assert.Contains(t, files, segC.name)

	// aligned control: once B commits, contiguous coverage retires both.
	reqB := bufB.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	_, err = reqB.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	assert.Equal(t, uint64(3), s.watermark)
	s.mux.Unlock()
	files = spoolDirFiles(t, dir)
	assert.NotContains(t, files, segAB.name)
	assert.NotContains(t, files, segC.name)
}

// TestStatementInfoSpoolWatermarkOutOfOrderFuzz acks batches in an
// adversarial order; every segment must survive until the watermark truly
// covers its highest owning batch, and removal afterwards follows the
// in-order head.
func TestStatementInfoSpoolWatermarkOutOfOrderFuzz(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	// zero lag: removal is a pure function of coverage.
	s := openTestSpool(t, dir, 0, 0, 0)
	defer s.close()
	line := encodeStatementInfoCSVLine(ctx, []string{"r"})

	const batches = 6
	reqs := make([]*spoolBatchRequest, batches)
	segs := make([]*spoolSegment, 0, batches)
	for i := 0; i < batches; i++ {
		seq := s.appendRecord(line)
		require.NotZero(t, seq)
		buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
		buf.Add(&StatementInfo{spoolSeq: seq})
		reqs[i] = buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
		require.NotNil(t, reqs[i])
		s.mux.Lock()
		segs = append(segs, s.sealedOrder[len(s.sealedOrder)-1])
		s.mux.Unlock()
	}
	// commit 2,4,6 then 1,3,5; the watermark pins after the first wave at 1.
	committed := make(map[int]bool)
	watermark := func() uint64 {
		w := uint64(0)
		for committed[int(w)] {
			w++
		}
		return w
	}
	for _, idx := range []int{1, 3, 5, 0, 2, 4} {
		_, err := reqs[idx].Handle()
		require.NoError(t, err)
		committed[idx] = true
		w := watermark()
		s.mux.Lock()
		assert.Equal(t, w, s.watermark)
		s.mux.Unlock()
		files := spoolDirFiles(t, dir)
		for i, seg := range segs {
			// segment i covers batch i+1: removable only when w <= i
			// (batch ids are 1-based) AND all earlier segments gone.
			removable := uint64(i+1) <= w
			if removable {
				// in-order: only if every earlier is also removable
				for j := 0; j < i; j++ {
					if uint64(j+1) > w {
						removable = false
						break
					}
				}
			}
			if removable {
				assert.NotContains(t, files, seg.name, "after committing %d (w=%d)", idx+1, w)
			} else {
				assert.Contains(t, files, seg.name, "after committing %d (w=%d)", idx+1, w)
			}
		}
	}
}

// TestStatementInfoSpoolAckUnknownBatchSettlesNothing: a stray ack (duplicate
// completion, unknown id) must never advance the watermark.
func TestStatementInfoSpoolAckUnknownBatchSettlesNothing(t *testing.T) {
	dir := t.TempDir()
	s := openTestSpool(t, dir, 0, 0, time.Hour)
	defer s.close()
	s.appendRecord(encodeStatementInfoCSVLine(context.Background(), []string{"r"}))
	seg := s.sealForExport()
	require.NotNil(t, seg)

	s.ackBatch(999)
	s.mux.Lock()
	assert.Equal(t, uint64(0), s.watermark)
	s.mux.Unlock()
	assert.Contains(t, spoolDirFiles(t, dir), seg.name)

	// re-acking the same real id is a no-op, not a second advance; the
	// never-parented segment stays uncovered regardless (age < horizon).
	s.ackBatch(1)
	s.ackBatch(1)
	s.mux.Lock()
	assert.Equal(t, uint64(1), s.watermark)
	s.mux.Unlock()
	assert.Contains(t, spoolDirFiles(t, dir), seg.name)
}

// TestSyncErrorMustNotUnchargeWrittenBytes is the round-2 review probe:
// writes succeed but Sync fails repeatedly — the written bytes stay on disk
// and must stay charged against the spool bound. (The pre-repair accounting
// released the whole reservation on a Sync error.)
func TestSyncErrorMustNotUnchargeWrittenBytes(t *testing.T) {
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"0123456789"}) // 11 bytes

	t.Run("sync errors keep the charge and the bound still bites", func(t *testing.T) {
		dir := t.TempDir()
		st := stubSpoolFileFactory(t)
		st.failSync = io.ErrClosedPipe
		s := openTestSpool(t, dir, 32, 1, time.Minute)
		defer s.close()

		s.appendRecord(line)
		assert.Equal(t, int64(len(line)), s.totalSize)
		s.appendRecord(line)
		assert.Equal(t, 2*int64(len(line)), s.totalSize)
		// third goes over 32: dropped by quota, not by disk — exact accounting.
		s.appendRecord(line)
		assert.Equal(t, 2*int64(len(line)), s.totalSize)
		assert.Equal(t, int64(1), s.droppedRecords)
		// ten more appends: everything beyond the quota keeps dropping, the
		// accounting never drifts from the bytes on disk.
		for i := 0; i < 7; i++ {
			s.appendRecord(line)
		}
		assert.Equal(t, 2*int64(len(line)), s.totalSize)
		assert.Equal(t, 2*int64(len(line)), int64(len(readSpoolFile(t, dir, 1))))
	})

	t.Run("healthy quota control", func(t *testing.T) {
		dir := t.TempDir()
		stubSpoolFileFactory(t)
		s := openTestSpool(t, dir, 32, 1, time.Minute)
		defer s.close()
		_ = ctx
		s.appendRecord(line)
		s.appendRecord(line)
		s.appendRecord(line) // over the 32-byte bound: capacity drop
		assert.Equal(t, 2*int64(len(line)), s.totalSize)
		assert.Equal(t, int64(1), s.droppedRecords)
		assert.Equal(t, 2*int64(len(line)), int64(len(readSpoolFile(t, dir, 1))))
	})
}

// TestPoisonBytesSurviveQuotaRestart is the round-2 review probe: quarantined
// poison files are counted in the startup accounting while staying excluded
// from replay — the bound must survive restarts.
func TestPoisonBytesSurviveQuotaRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	// 90 quarantined bytes + a 10-byte orphan (ordinary accounting control).
	require.NoError(t, os.WriteFile(filepath.Join(dir, spoolSegmentFileName(1)+spoolPoisonFileSuffix),
		make([]byte, 90), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, spoolSegmentFileName(2)),
		make([]byte, 10), 0644))

	s := openTestSpool(t, dir, 100, 1, time.Minute)
	defer s.close()
	assert.Equal(t, int64(100), s.totalSize) // 90 poison + 10 orphan
	// poison is excluded from replay
	assert.Len(t, s.orphans, 1)
	// any append exceeds the bound and drops, exactly as the bound intends
	line := encodeStatementInfoCSVLine(ctx, []string{"r"})
	require.Greater(t, s.totalSize+int64(len(line)), int64(100))
	s.appendRecord(line)
	assert.Equal(t, int64(100), s.totalSize)
	assert.Equal(t, int64(1), s.droppedRecords)

	// a larger bound frees exactly the accounted headroom
	dir2 := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir2, spoolSegmentFileName(1)+spoolPoisonFileSuffix),
		make([]byte, 90), 0644))
	s2 := openTestSpool(t, dir2, 100, 1, time.Minute)
	defer s2.close()
	assert.Equal(t, int64(90), s2.totalSize)
	s2.appendRecord(line) // fits only because accounting is exact
	assert.Equal(t, int64(90)+int64(len(line)), s2.totalSize)
	assert.Equal(t, int64(0), s2.droppedRecords)
}

// ---------------------------------------------------------------------------
// Round-3 review probes: pooled-buffer batch identity, aggregation
// constituents, shared-sync failure delivery, torn-tail repair, durable name
// publication.
// ---------------------------------------------------------------------------

// TestSpooledItemBufferResetDropsCommittedBatchID (round-3, finding 1): the
// collector pool REUSES buffers (bufferHolder.putBuffer -> Reset) after the
// write request was generated but possibly before it committed. The Reset
// must drop the buffer's batch identity WITHOUT touching the id the already
// generated request captured, and a reused buffer must allocate a fresh id:
// a reused buffer that still carried the committed id would let the next
// batch parent its records under an ALREADY committed batch, retiring
// segments whose export never covered them.
func TestSpooledItemBufferResetDropsCommittedBatchID(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := openTestSpool(t, dir, 0, 0, 0)
	defer s.close()
	line := encodeStatementInfoCSVLine(ctx, []string{"r"})

	seqA := s.appendRecord(line)
	require.NotZero(t, seqA)
	buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	buf.Add(&StatementInfo{spoolSeq: seqA})
	idA := buf.batchID
	require.NotZero(t, idA)
	reqA := buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	require.NotNil(t, reqA)

	// pool return while the generated request is still in flight: the
	// buffer's identity resets, the request keeps what it captured.
	buf.Reset()
	require.Zero(t, buf.batchID)
	_, err := reqA.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	require.Equal(t, idA, s.watermark, "the in-flight request must still commit its captured id after Reset")
	s.mux.Unlock()

	// reuse: the pooled buffer serves a new batch under a FRESH id.
	seqB := s.appendRecord(line)
	require.NotZero(t, seqB)
	buf.Add(&StatementInfo{spoolSeq: seqB})
	require.NotZero(t, buf.batchID)
	require.NotEqual(t, idA, buf.batchID, "a reused buffer must never re-tag records under a committed id")
	reqB := buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	_, err = reqB.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	assert.Equal(t, buf.batchID, s.watermark)
	s.mux.Unlock()
}

// TestSpooledItemBufferAggregationParentsEveryConstituent (round-3, finding 2):
// the aggregation window clones the first statement and merges the rest; the
// spool record of every merged statement must ride the aggregated record to
// its export batch, so the batch commit is what retires their durable copies
// (positive export completion), not the ack-lag age.
func TestSpooledItemBufferAggregationParentsEveryConstituent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := openTestSpool(t, dir, 0, 0, 0)
	defer s.close()
	line := encodeStatementInfoCSVLine(ctx, []string{"agg"})
	ctx = context.WithValue(ctx, DurationKey, 5*time.Second)

	seq1 := s.appendRecord(line)
	seq2 := s.appendRecord(line)
	require.NotZero(t, seq1)
	require.NotZero(t, seq2)

	// real aggregation path: AddItem(raw1) -> StatementInfoNew clones the
	// grouped record; AddItem(raw2) -> StatementInfoUpdate merges it in.
	raw1 := &StatementInfo{Statement: []byte("insert into t values (1)"), Status: StatementStatusSuccess, Duration: time.Millisecond}
	raw1.statsArray.Init()
	raw1.spoolSeq = seq1
	grouped := StatementInfoNew(raw1, ctx).(*StatementInfo)
	require.Zero(t, grouped.spoolSeq)
	require.Equal(t, []uint64{seq1}, grouped.spoolSeqs, "the clone must carry the first constituent's sequence")

	raw2 := &StatementInfo{Statement: []byte("insert into t values (1)"), Status: StatementStatusSuccess, Duration: time.Millisecond, ResultCount: 1}
	raw2.statsArray.Init()
	raw2.spoolSeq = seq2
	StatementInfoUpdate(ctx, grouped, raw2)
	require.ElementsMatch(t, []uint64{seq1, seq2}, grouped.spoolSeqs)

	// the aggregated record enters the buffer: BOTH constituents are
	// parented to the same export batch.
	buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	buf.Add(grouped)
	require.NotZero(t, buf.batchID)
	req := buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	require.NotNil(t, req)
	s.mux.Lock()
	require.Len(t, s.sealedOrder, 1)
	seg := s.sealedOrder[0]
	require.Equal(t, int64(2), seg.recordCount)
	require.Equal(t, int64(2), seg.taggedCount, "every constituent must be parented, not just the first")
	require.Equal(t, buf.batchID, seg.maxTag)
	// positive completion required: before the batch commits, the segment
	// holding the durable copies must survive.
	s.collectRemovableLocked()
	s.mux.Unlock()
	require.Contains(t, spoolDirFiles(t, dir), seg.name)

	// the export batch commits: contiguous coverage retires the segment.
	_, err := req.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	assert.Equal(t, buf.batchID, s.watermark)
	s.mux.Unlock()
	assert.NotContains(t, spoolDirFiles(t, dir), seg.name)

	// pool hygiene: freeing a grouped record must not leak constituent
	// linkage into the next pooled StatementInfo.
	grouped.free()
	pooled := stmtPool.Get().(*StatementInfo)
	require.Zero(t, pooled.spoolSeq)
	require.Empty(t, pooled.spoolSeqs)
	stmtPool.Put(pooled)
}

// TestSpoolFsyncGroupSharedSyncFailureReachesEveryWaiter (round-3, finding 4):
// one fsync confirms (or fails) a byte window shared by many concurrent
// appenders; when that fsync FAILS, every waiter whose bytes sit inside the
// failed window must observe the error — a failure consumed by a single
// caller lets the others claim unconfirmed durability.
func TestSpoolFsyncGroupSharedSyncFailureReachesEveryWaiter(t *testing.T) {
	f := &spoolGateFile{
		blockFirst:  true,
		gate:        make(chan struct{}),
		enteredSync: make(chan struct{}),
		secondWrite: make(chan struct{}),
		thirdWrite:  make(chan struct{}),
		failOn:      2, // the first (gated) sync succeeds; everything after fails
		failErr:     io.ErrClosedPipe,
	}
	g := &spoolFsyncGroup{}
	g.init()
	g.mu.Lock()
	g.adoptLocked(f, 0)
	g.mu.Unlock()

	type outcome struct {
		n   int
		err error
	}
	res := make(chan outcome, 3)
	appendAsync := func(rec string) {
		n, err := g.append([]byte(rec))
		res <- outcome{n, err}
	}

	go appendAsync("A\n")
	<-f.enteredSync // A leads the gate-held first fsync
	go appendAsync("B\n")
	<-f.secondWrite // B's bytes land while A's fsync is in flight
	go appendAsync("C\n")
	<-f.thirdWrite // C joins; both now wait on the in-flight fsync
	close(f.gate)  // A's fsync SUCCEEDS but covers A only

	oA := <-res // the leader exits first (it completes the wait cycle)
	require.NoError(t, oA.err, "A's own successful fsync covers its record")
	// Whoever wakes next re-leads a fsync whose window covers BOTH remaining
	// waiters; that fsync FAILS and both must see the failure (pre-fix the
	// error was consumed by exactly one of them).
	oB := <-res
	oC := <-res
	require.ErrorIs(t, oB.err, io.ErrClosedPipe)
	require.ErrorIs(t, oC.err, io.ErrClosedPipe)
	w, sc := f.counters()
	assert.Equal(t, 3, w)
	assert.Equal(t, 2, sc, "one success covering A, one shared failure covering B and C")

	// healing leg: once the disk recovers, a later successful fsync restores
	// success for new appends on the same file.
	f.setFail(0, nil)
	n, err := g.append([]byte("D\n"))
	require.NoError(t, err)
	assert.Equal(t, len("D\n"), n)
}

// TestSpoolFsyncGroupPartialWriteRestoresBoundary (round-3, finding 5): a
// short write leaves a fragment without its newline terminator; before any
// next record can join onto that fragment the group must restore the last
// complete boundary, so the joined-line corruption of the X/C/D shape is
// impossible and the segment's byte accounting stays exact.
func TestSpoolFsyncGroupPartialWriteRestoresBoundary(t *testing.T) {
	fm := &spoolMemFile{}
	g := &spoolFsyncGroup{}
	g.init()
	g.mu.Lock()
	g.adoptLocked(fm, 0)
	g.mu.Unlock()

	n, err := g.append([]byte("X\n"))
	require.NoError(t, err)
	require.Equal(t, 2, n)

	fm.partialN = 3
	fm.partialErr = io.ErrClosedPipe
	_, err = g.append([]byte("CCCCC\n"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	// the fragment is gone: the file is back at the last complete boundary,
	// and the repair was a single truncate to exactly that boundary.
	require.Equal(t, []int64{2}, fm.truncs)
	require.Equal(t, []byte("X\n"), fm.buf)

	n, err = g.append([]byte("D\n"))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	// no join corruption: D starts a fresh line right after X's terminator.
	require.Equal(t, []byte("X\nD\n"), fm.buf)
}

// spoolPartialWriteFile fails exactly one armed Write with a short count,
// then heals — the real-file twin of spoolMemFile.partialN.
type spoolPartialWriteFile struct {
	*os.File
	mux    sync.Mutex
	arm    bool
	keep   int
	truncs []int64
}

func (f *spoolPartialWriteFile) Write(p []byte) (int, error) {
	f.mux.Lock()
	armed := f.arm
	if armed {
		f.arm = false
	}
	f.mux.Unlock()
	if armed {
		keep := f.keep
		if keep > len(p) {
			keep = len(p)
		}
		if _, err := f.File.Write(p[:keep]); err != nil {
			return 0, err
		}
		return keep, io.ErrShortWrite
	}
	return f.File.Write(p)
}

func (f *spoolPartialWriteFile) Truncate(size int64) error {
	f.mux.Lock()
	f.truncs = append(f.truncs, size)
	f.mux.Unlock()
	return f.File.Truncate(size)
}

// TestStatementInfoSpoolPartialWriteKeepsLedgerExact (round-3, finding 5):
// at the spool level a repaired partial write releases the entire
// reservation of the record that did not survive, and the physical file
// ends up exactly with the surviving records — quota and bytes never
// desynchronize.
func TestStatementInfoSpoolPartialWriteKeepsLedgerExact(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	stub := &spoolPartialWriteFile{}
	old := openSpoolSegmentFile
	openSpoolSegmentFile = func(dir string, seq uint64) (spoolSyncFile, int64, error) {
		f, size, err := openSegmentFile(dir, seq)
		if err != nil {
			return nil, 0, err
		}
		if stub.File == nil {
			stub.File = f
			return stub, size, nil
		}
		return f, size, nil
	}
	t.Cleanup(func() { openSpoolSegmentFile = old })

	s := openTestSpool(t, dir, 0, 0, 0)
	defer s.close()

	lineX := encodeStatementInfoCSVLine(ctx, []string{"x-survives"})
	seqX := s.appendRecord(lineX)
	require.NotZero(t, seqX)
	require.Equal(t, int64(len(lineX)), s.totalSize)

	stub.arm = true
	stub.keep = 3
	lineC := encodeStatementInfoCSVLine(ctx, []string{"cccc-victim"})
	require.Greater(t, len(lineC), 3)
	seqC := s.appendRecord(lineC)
	require.Zero(t, seqC, "the partially written record must not be acknowledged")
	require.Equal(t, []int64{int64(len(lineX))}, stub.truncs)
	require.Equal(t, int64(len(lineX)), s.totalSize, "the lost record's whole reservation is released")
	require.Equal(t, lineX, readSpoolFile(t, dir, 1))

	lineD := encodeStatementInfoCSVLine(ctx, []string{"d-next"})
	seqD := s.appendRecord(lineD)
	require.NotZero(t, seqD)
	want := append(append([]byte{}, lineX...), lineD...)
	require.Equal(t, want, readSpoolFile(t, dir, 1), "the next record appends at the restored boundary, never joined")
	require.Equal(t, int64(len(lineX)+len(lineD)), s.totalSize)
}

// TestStatementInfoSpoolDurablyPublishesNames (round-3, finding 6): an
// fsynced record under a segment name that never reached the disk is lost on
// power loss exactly like an un-fsynced record. New segment names and a
// newly created spool directory must be directory-fsynced BEFORE the first
// record they can hold is acknowledged.
func TestStatementInfoSpoolDurablyPublishesNames(t *testing.T) {
	base := t.TempDir()
	ctx := context.Background()
	var mux sync.Mutex
	var synced []string
	old := syncSpoolDirFn
	syncSpoolDirFn = func(d string) error {
		mux.Lock()
		synced = append(synced, d)
		mux.Unlock()
		return syncSpoolDir(d)
	}
	t.Cleanup(func() { syncSpoolDirFn = old })
	count := func(d string) int {
		mux.Lock()
		defer mux.Unlock()
		c := 0
		for _, s := range synced {
			if s == d {
				c++
			}
		}
		return c
	}

	// a NEW spool directory: its own name is published in the parent, and
	// the FIRST segment's name is published in the spool directory itself —
	// both before the spool acknowledges anything.
	spoolDir := filepath.Join(base, "audit-spool")
	s := openTestSpool(t, spoolDir, 0, 0, 0)
	require.Equal(t, 1, count(filepath.Dir(spoolDir)), "new spool dir name must be published once")
	require.Equal(t, 1, count(spoolDir), "the first segment name must be published before its first ack")

	// an EXISTING segment name is not re-published per record.
	line := encodeStatementInfoCSVLine(ctx, []string{"r"})
	require.NotZero(t, s.appendRecord(line))
	require.NotZero(t, s.appendRecord(line))
	require.Equal(t, 1, count(spoolDir), "appends to an existing segment must not pay a directory fsync")

	// the NEXT segment name is published when rotation creates it.
	buf := &spooledItemBuffer{Buffer: &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}, spool: s}
	buf.Add(&StatementInfo{spoolSeq: s.appendRecord(line)})
	req := buf.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	require.NotNil(t, req)
	require.Equal(t, 2, count(spoolDir), "rotation publishes the fresh segment name")
	_, err := req.Handle()
	require.NoError(t, err)
	s.close()

	// reopen over the EXISTING spool directory: the directory's own name is
	// already durable; the parent must not be synced again. (A new segment
	// name still gets its own publication.)
	require.Equal(t, 1, count(filepath.Dir(spoolDir)))
	s2 := openTestSpool(t, spoolDir, 0, 0, 0)
	defer s2.close()
	require.Equal(t, 1, count(filepath.Dir(spoolDir)), "an existing spool dir needs no parent publication")
	require.Equal(t, 3, count(spoolDir), "the reopen segment name is still published before its first ack")
}
