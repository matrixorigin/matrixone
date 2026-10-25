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

func TestStatementInfoSpoolAckLifecycle(t *testing.T) {
	dir := t.TempDir()
	base := time.Now()
	current, clock := spoolTestClock(base)
	s := openTestSpool(t, dir, 0, 1, 30*time.Second)
	s.now = clock
	defer s.close()

	line := encodeStatementInfoCSVLine(context.Background(), []string{"r"})
	s.appendRecord(line)
	seg1 := s.sealForExport()
	require.NotNil(t, seg1)
	s.appendRecord(line)
	seg2 := s.sealForExport()
	require.NotNil(t, seg2)

	// ack the first one: with ackLagSegments=1 it must wait for a newer ack.
	s.ackSegment(seg1.seq)
	assert.Contains(t, spoolDirFiles(t, dir), seg1.name)

	// ack the second one: now both lag conditions hold for seg1
	s.ackSegment(seg2.seq)
	assert.NotContains(t, spoolDirFiles(t, dir), seg1.name)
	// seg2 has no newer acked segment, and no time passed: stays
	assert.Contains(t, spoolDirFiles(t, dir), seg2.name)

	// time-based lag fallback: after ackLagDuration it can be removed.
	*current = base.Add(31 * time.Second)
	s.mux.Lock()
	s.collectRemovableLocked()
	s.mux.Unlock()
	assert.NotContains(t, spoolDirFiles(t, dir), seg2.name)
	assert.Equal(t, int64(0), s.totalSize)
}

func TestStatementInfoSpoolFailedExportKeepsSegment(t *testing.T) {
	dir := t.TempDir()
	s := openTestSpool(t, dir, 0, 1, time.Second)
	defer s.close()

	line := encodeStatementInfoCSVLine(context.Background(), []string{"r"})
	s.appendRecord(line)
	seg := s.sealForExport()
	require.NotNil(t, seg)
	s.markExportFailed(seg.seq)

	s.mux.Lock()
	s.collectRemovableLocked()
	s.mux.Unlock()
	assert.Contains(t, spoolDirFiles(t, dir), seg.name)

	// even a later ack cannot remove a segment that failed once
	s.ackSegment(seg.seq)
	assert.Contains(t, spoolDirFiles(t, dir), seg.name)
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

func TestStatementInfoSpoolAckSegmentLeavesNoMarker(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	// the ack of the regular export path is in-memory only and stays behind
	// the pipeline lag: no marker is written for it (see ackSegment), so a
	// kill inside the ack-lag horizon re-exports the batch instead of
	// losing records that were still draining through the collector.
	s := openTestSpool(t, dir, 0, 1, time.Hour)
	defer s.close()
	s.appendRecord(line)
	seg := s.sealForExport()
	require.NotNil(t, seg)
	s.ackSegment(seg.seq)
	for _, f := range spoolDirFiles(t, dir) {
		assert.NotEqual(t, spoolAckFileName(seg.name), f)
	}
}

func TestStatementInfoSpoolAckSettlesEarlierSegments(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	line := encodeStatementInfoCSVLine(ctx, []string{"s1-r1"})
	// zero ack lag: a covered segment is removed immediately.
	s := openTestSpool(t, dir, 0, 0, 0)
	defer s.close()
	s.appendRecord(line)
	seg1 := s.sealForExport() // sealed; its batch fired empty — never wrapped
	require.NotNil(t, seg1)
	s.appendRecord(line)
	seg2 := s.sealForExport()
	require.NotNil(t, seg2)
	// the successful export of seg2's batch settles the earlier segment,
	// too — the pipeline drains in order (see ackSegment).
	s.ackSegment(seg2.seq)
	files := spoolDirFiles(t, dir)
	assert.NotContains(t, files, seg1.name)
	assert.NotContains(t, files, seg2.name)
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

func TestDropTornTailRecord(t *testing.T) {
	assert.Equal(t, []byte("a\nb\n"), dropTornTailRecord("x", []byte("a\nb\n")))
	assert.Equal(t, []byte("a\n"), dropTornTailRecord("x", []byte("a\nb")))
	assert.Empty(t, dropTornTailRecord("x", []byte("no-newline-torn")))
	assert.Empty(t, dropTornTailRecord("x", nil))
	assert.Empty(t, dropTornTailRecord("x", []byte{}))
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

func TestSpooledItemBufferGetBatchLifecycle(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := openTestSpool(t, dir, 0, 1, time.Minute)
	defer s.close()

	line := encodeStatementInfoCSVLine(ctx, []string{"r1"})
	s.appendRecord(line)

	// fake inner buffer returning a capturing WriteRequest
	inner := &spoolFakeBuffer{batch: &spoolFakeWriteRequest{}}
	buf := &spooledItemBuffer{Buffer: inner, spool: s}
	batch := buf.GetBatch(ctx, new(bytes.Buffer))
	require.IsType(t, &spoolBatchRequest{}, batch)
	req := batch.(*spoolBatchRequest)
	require.True(t, inner.getBatchCalled)
	// rotation happened before the batch is executed
	require.Len(t, s.sealed, 1)

	// export success marks the segment done; with ackLagSegments=1 and no
	// newer batch it is kept on disk but flagged.
	_, err := req.Handle()
	require.NoError(t, err)
	s.mux.Lock()
	seg := s.sealed[req.seg.seq]
	s.mux.Unlock()
	require.NotNil(t, seg)
	assert.True(t, seg.done)
	assert.Contains(t, spoolDirFiles(t, dir), seg.name)

	// failure keeps the segment flagged failed for restart replay
	s.appendRecord(line)
	inner2 := &spoolFakeBuffer{batch: &spoolFakeWriteRequest{err: io.ErrClosedPipe}}
	buf2 := &spooledItemBuffer{Buffer: inner2, spool: s}
	req2 := buf2.GetBatch(ctx, new(bytes.Buffer)).(*spoolBatchRequest)
	_, err = req2.Handle()
	require.Error(t, err)
	s.mux.Lock()
	seg2 := s.sealed[req2.seg.seq]
	s.mux.Unlock()
	require.NotNil(t, seg2)
	assert.True(t, seg2.failed)
	assert.Contains(t, spoolDirFiles(t, dir), seg2.name)

	// a non-request batch shape keeps the segment pending instead of acking
	s.appendRecord(line)
	inner3 := &spoolFakeBuffer{batch: "not-a-write-request"}
	buf3 := &spooledItemBuffer{Buffer: inner3, spool: s}
	assert.Equal(t, "not-a-write-request", buf3.GetBatch(ctx, new(bytes.Buffer)))
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
		s.ackSegment(seg.seq) // removal fails loudly, segment stays known
		s.mux.Lock()
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
		s.ackSegment(seg.seq)
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
