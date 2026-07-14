// Copyright 2026 Matrix Origin
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

package spillutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestComputeXXHash(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
	hashValues := make([]uint64, 3)
	err := ComputeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NoError(t, err)
	require.NotEqual(t, uint64(0), hashValues[0])
}

func TestFlushBucketBatchAndReadRoundtrip(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_rt")
	require.NoError(t, err)
	defer f.Close()

	var buf bytes.Buffer
	w := BucketWriter{Name: "test_rt", Fd: f}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.SetRowCount(3)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())
	reader.Close()
}

func TestBucketReaderEOF(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	var buf bytes.Buffer
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_eof")
	require.NoError(t, err)
	w := BucketWriter{Name: "test_eof", Fd: f}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat.SetRowCount(2)
	FlushBucketBatch(proc, bat, &w, &buf, nil)

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, got.RowCount())
	_, err = reader.ReadBatch(proc, reuseBat)
	require.Equal(t, io.EOF, err)
	reader.Close()
}

func TestBucketReaderCorruptedMagic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_corrupt")
	require.NoError(t, err)

	// Write a valid batch via FlushBucketBatch, then corrupt the magic.
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_corrupt", Fd: f}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)

	// Overwrite last 8 bytes (magic) with zeros.
	f.Seek(-8, io.SeekEnd)
	var zeroMagic uint64
	f.Write(types.EncodeUint64(&zeroMagic))
	f.Seek(0, io.SeekStart)

	reader := BucketReader{}
	reader.ResetForFd(f)
	reuseBat := batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")
	reader.Close()
	f.Close()
}

func TestBucketWriterHandOffFd(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	spillfs, _ := proc.GetSpillFileService()
	f, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_handoff")
	w := BucketWriter{Fd: f}
	fd := w.HandOffFd()
	require.NotNil(t, fd)
	require.Nil(t, w.Fd)
	require.False(t, w.Created())
	fd.Close()
}

func TestMakeBucketWriters(t *testing.T) {
	writers := MakeBucketWriters("test")
	require.Equal(t, SpillNumBuckets, len(writers))
	for i := range writers {
		require.NotEmpty(t, writers[i].Name)
		require.Nil(t, writers[i].Fd)
	}
}

func TestShouldSpill(t *testing.T) {
	// Zero threshold disables spill.
	require.False(t, ShouldSpill(100, 1000, 0))
	// Row-count mode (threshold <= 100000): compare rowCnt.
	require.False(t, ShouldSpill(100, 900, 1000))
	require.True(t, ShouldSpill(100, 1000, 1000))
	// Byte mode (threshold > 100000): compare memUsed.
	require.False(t, ShouldSpill(200000, 1000, 200001))
	require.True(t, ShouldSpill(200001, 1000, 200000))
}

func TestScatterProbeTableRejectsRecursiveMarker(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	marker := batch.NewWithSize(0)
	marker.SetRowCount(1)
	marker.SetLast()
	engine := NewSpillEngine(SpillEngineConfig{})
	called := false
	err := engine.ScatterProbeTable(
		proc,
		func() (*batch.Batch, error) {
			if called {
				return nil, nil
			}
			called = true
			return marker, nil
		},
		nil,
		func(*batch.Batch) ([]*vector.Vector, error) {
			t.Fatal("recursive marker must not be evaluated as data")
			return nil, nil
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "recursive input")
	engine.Cleanup(proc)
}

func TestReusableBufferPool(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	pool := ReusableBufferPool{}
	bufs := pool.Acquire(SpillNumBuckets)
	require.Equal(t, SpillNumBuckets, len(bufs))
	for i := range bufs {
		require.Nil(t, bufs[i])
	}
	pool.Release(proc)
}

func TestScatterBatchBasic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_scatter")
	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
	bat.SetRowCount(5)
	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)
}

func TestBucketReaderEmptyFile(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	r := BucketReader{}
	r.ResetForFd(nil)
	reuseBat := batch.NewOffHeapWithSize(0)
	_, err := r.ReadBatch(proc, reuseBat)
	require.Equal(t, io.EOF, err)
}

func TestLazySpillFileCreation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_lazy")
	for i := range writers {
		require.Nil(t, writers[i].Fd, "all writers should start with nil Fd")
	}

	// Write a batch that will populate some buckets
	var buf bytes.Buffer
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
	bat.SetRowCount(5)
	buffers := make([]*batch.Batch, len(writers))
	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)

	// Flush remaining buffers — files are created lazily on first write
	for i, b := range buffers {
		if b != nil && b.RowCount() > 0 {
			err := FlushBucketBatch(proc, b, &writers[i], &buf, nil)
			require.NoError(t, err)
			require.True(t, writers[i].Created(), "writer should have created file on first flush")
		}
	}

	// Clean up
	for i := range writers {
		writers[i].Close()
	}
}

func TestReaderRowCountMismatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_mismatch")
	require.NoError(t, err)

	// Write valid batch then corrupt the row count in the header.
	var buf bytes.Buffer
	w := BucketWriter{Name: "test", Fd: f}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat.SetRowCount(2)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)

	// Overwrite the count (first 8 bytes) with a wrong value.
	f.Seek(0, io.SeekStart)
	wrongCnt := int64(999)
	f.Write(types.EncodeInt64(&wrongCnt))
	f.Seek(0, io.SeekStart)

	reader := BucketReader{}
	reader.ResetForFd(f)
	reuseBat := batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")
	reader.Close()
	f.Close()
}

func TestScatterBatchDistribution(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_dist")
	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer

	nRows := SpillNumBuckets * 100
	vals := make([]int32, nRows)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.SetRowCount(nRows)

	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)

	// Most buckets should have data with enough rows.
	nonEmpty := 0
	for _, b := range buffers {
		if b != nil && b.RowCount() > 0 {
			nonEmpty++
		}
	}
	require.Greater(t, nonEmpty, SpillNumBuckets/2, "at least half the buckets should have data")

	// Total rows should be preserved.
	totalRows := 0
	for i, b := range buffers {
		if b != nil {
			FlushBucketBatch(proc, b, &writers[i], &buf, nil)
			totalRows += b.RowCount()
		}
	}
	require.Equal(t, nRows, totalRows)
}

func TestBucketReaderPartialRead(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_partial")
	require.NoError(t, err)

	// Write incomplete data (only a count, no batch body).
	cnt := int64(5)
	f.Write(types.EncodeInt64(&cnt))
	f.Seek(0, io.SeekStart)

	reader := BucketReader{}
	reader.ResetForFd(f)
	reuseBat := batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuseBat)
	require.Error(t, err)
	reader.Close()
	f.Close()
}

func TestBucketReaderDoubleClose(t *testing.T) {
	r := BucketReader{}
	r.Close()
	r.Close() // should not panic
}

func TestComputeXXHashWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, []uint64{1}, mp) // null at index 1
	hashValues := make([]uint64, 3)
	err := ComputeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NoError(t, err)
	require.NotEqual(t, uint64(0), hashValues[0])
	require.NotEqual(t, uint64(0), hashValues[2])
}

func TestComputeXXHashMultipleColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	vec1 := testutil.MakeInt32Vector([]int32{1, 1, 2}, nil, mp)
	vec2 := testutil.MakeVarcharVector([]string{"a", "b", "a"}, nil, mp)
	hashValues := make([]uint64, 3)
	err := ComputeXXHash([]*vector.Vector{vec1, vec2}, hashValues, 0)
	require.NoError(t, err)
	// Same (col1, col2) pairs should hash differently.
	require.NotEqual(t, hashValues[0], hashValues[1])
	require.NotEqual(t, hashValues[0], hashValues[2])
}

func TestHandOffFdSeeksToStart(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_seek")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test_seek", Fd: f}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.SetRowCount(3)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)

	// Position should be past data.
	pos, _ := w.Fd.Seek(0, io.SeekCurrent)
	require.Greater(t, pos, int64(0))

	fd := w.HandOffFd()
	require.NotNil(t, fd)
	pos, _ = fd.Seek(0, io.SeekCurrent)
	require.Equal(t, int64(0), pos, "HandOffFd must seek to start")
	fd.Close()
}

func TestWriterHandoffReaderRoundtrip(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_roundtrip")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test_roundtrip", Fd: f}
	for i := 0; i < 3; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i * 10), int32(i*10 + 1)}, nil, proc.Mp())
		bat.SetRowCount(2)
		err := FlushBucketBatch(proc, bat, &w, &buf, nil)
		require.NoError(t, err)
	}

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)

	totalRows := 0
	for {
		got, err := reader.ReadBatch(proc, reuseBat)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += got.RowCount()
	}
	require.Equal(t, 6, totalRows) // 3 batches × 2 rows
	reader.Close()
}

func TestComputeXXHashMultipleTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	tests := []struct {
		name string
		vec  *vector.Vector
	}{
		{"int8", testutil.MakeInt8Vector([]int8{1, 2, 3}, nil, mp)},
		{"int16", testutil.MakeInt16Vector([]int16{100, 200, 300}, nil, mp)},
		{"int64", testutil.MakeInt64Vector([]int64{1000, 2000, 3000}, nil, mp)},
		{"uint32", testutil.MakeUint32Vector([]uint32{10, 20, 30}, nil, mp)},
		{"float32", testutil.MakeFloat32Vector([]float32{1.1, 2.2, 3.3}, nil, mp)},
		{"float64", testutil.MakeFloat64Vector([]float64{10.1, 20.2, 30.3}, nil, mp)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashValues := make([]uint64, 3)
			err := ComputeXXHash([]*vector.Vector{tt.vec}, hashValues, 0)
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), hashValues[0])
		})
	}
}

func TestScatterBatchLargeData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_large")
	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer

	// Large enough to trigger internal flush (>8192 rows).
	size := 10000
	vals := make([]int32, size)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.SetRowCount(size)

	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)

	// Verify total rows preserved.
	totalRows := 0
	for i, b := range buffers {
		if b != nil {
			FlushBucketBatch(proc, b, &writers[i], &buf, nil)
			totalRows += b.RowCount()
		}
	}
	require.Equal(t, size, totalRows)
}

func TestResetForFdReusesReader(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	var buf bytes.Buffer
	f1, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_reuse_1")
	w1 := BucketWriter{Name: "test1", Fd: f1}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)
	FlushBucketBatch(proc, bat, &w1, &buf, nil)
	fd1 := w1.HandOffFd()

	f2, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_reuse_2")
	w2 := BucketWriter{Name: "test2", Fd: f2}
	FlushBucketBatch(proc, bat, &w2, &buf, nil)
	fd2 := w2.HandOffFd()

	r := BucketReader{}
	r.ResetForFd(fd1)
	require.False(t, r.Empty)

	// Second ResetForFd reuses internal state.
	r.ResetForFd(fd2)
	require.False(t, r.Empty)

	r.Close()
	fd1.Close()
}

func TestFlushBucketBatchMultipleCalls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_multi_flush")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test_multi_flush", Fd: f}
	totalRows := 0
	for i := 0; i < 10; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i)}, nil, proc.Mp())
		bat.SetRowCount(1)
		err := FlushBucketBatch(proc, bat, &w, &buf, nil)
		require.NoError(t, err)
		totalRows++
	}

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	readRows := 0
	for {
		got, err := reader.ReadBatch(proc, reuseBat)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		readRows += got.RowCount()
	}
	require.Equal(t, totalRows, readRows)
	reader.Close()
}

func TestHashDistribution(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil, mp)
	hashValues := make([]uint64, 20)
	err := ComputeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NoError(t, err)

	bucketCounts := make([]int, SpillNumBuckets)
	for _, h := range hashValues {
		bucketCounts[h&(SpillNumBuckets-1)]++
	}
	nonEmpty := 0
	for _, c := range bucketCounts {
		if c > 0 {
			nonEmpty++
		}
	}
	require.Greater(t, nonEmpty, 1, "hashes must distribute across multiple buckets")
}

func TestSpillFileCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	file, err := spillfs.CreateFile(context.Background(), "test_cleanup")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test_cleanup", Fd: file}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)
	file.Close()

	// File should still exist (it was CreateFile, not CreateAndRemoveFile).
	f2, err := spillfs.OpenFile(context.Background(), "test_cleanup")
	require.NoError(t, err)
	f2.Close()

	spillfs.RemoveFile(context.Background(), "test_cleanup")
}

func TestFileWriteError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	file, err := spillfs.CreateFile(context.Background(), "test_error")
	require.NoError(t, err)
	file.Close() // close before write

	var buf bytes.Buffer
	w := BucketWriter{Name: "test_error", Fd: file}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.Error(t, err)

	spillfs.RemoveFile(context.Background(), "test_error")
}

// TestReSpillFlushError verifies that flush errors during re-spill are
// propagated (covers the error-return paths added for copilot review).
func TestReSpillFlushError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Create a large enough build batch to trigger re-spill.
	vals := make([]int32, 5000)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := makeInt32Batch(proc, vals)
	fd := writeBuildFile(proc, "test_re_flush_err", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:  makeTestKeyExpr(),
		SpillThreshold: 100,
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	// Set probeKeyEval so re-spill can scatter probe if needed.
	engine.probeKeyEval = makeTestEvalKeysFn()

	// Rebuild should trigger re-spill. The flush inside reSpillBucket
	// should succeed (valid writers). We verify no error.
	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.NotEqual(t, BucketQueueEmpty, res)
	if jm != nil {
		jm.Free()
	}

	// Drain remaining buckets.
	for engine.HasMoreBuckets() {
		jm2, _, err2 := engine.RebuildHashmap(proc, analyzer)
		require.NoError(t, err2)
		if jm2 != nil {
			jm2.Free()
		}
	}
	engine.Cleanup(proc)
}

func TestScatterBatchWithNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_null")
	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, []uint64{1}, proc.Mp()) // null at index 1
	bat.SetRowCount(4)

	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)
	// Should not panic with nulls.
}

func TestReaderBatchReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	var buf bytes.Buffer
	f, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_reuse_read")
	w := BucketWriter{Name: "test", Fd: f}

	// Write batches with different sizes.
	for _, size := range []int{5, 2} {
		vals := make([]int32, size)
		for i := range vals {
			vals[i] = int32(i)
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
		bat.SetRowCount(size)
		FlushBucketBatch(proc, bat, &w, &buf, nil)
	}

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)

	// Read with same reuseBat - vector allocations should be reused.
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 5, got.RowCount())

	got, err = reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, got.RowCount())

	reader.Close()
}

func TestHashValuesBufferGrowth(t *testing.T) {
	mp := mpool.MustNewZero()

	// Small input.
	smallVec := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
	smallHash := make([]uint64, 2)
	err := ComputeXXHash([]*vector.Vector{smallVec}, smallHash, 0)
	require.NoError(t, err)

	// Large input - hash buffers grow to accommodate.
	largeVec := testutil.MakeVarcharVector([]string{"long_string_a", "long_string_b", "long_string_c"}, nil, mp)
	largeHash := make([]uint64, 3)
	err = ComputeXXHash([]*vector.Vector{largeVec}, largeHash, 0)
	require.NoError(t, err)
	for _, h := range largeHash {
		require.NotEqual(t, uint64(0), h)
	}
}

func TestScatterWithMultiColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_multi_col")
	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"a", "b", "c", "d", "e"}, nil, proc.Mp())
	bat.SetRowCount(5)

	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)

	// All 5 rows must be distributed across buffers.
	totalRows := 0
	hasTwoCols := false
	for _, b := range buffers {
		if b != nil && b.RowCount() > 0 {
			totalRows += b.RowCount()
			if len(b.Vecs) == 2 {
				hasTwoCols = true
			}
		}
	}
	require.Equal(t, 5, totalRows, "all rows must be accounted for in buffers")
	require.True(t, hasTwoCols, "buffer batches must preserve column count")
}

func TestScatterLargeVarchar(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_large_varchar")
	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer

	size := 100
	vals := make([]string, size)
	for i := range vals {
		vals[i] = fmt.Sprintf("large_string_value_%d_with_padding", i)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeVarcharVector(vals, nil, proc.Mp())
	bat.SetRowCount(size)

	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)

	totalRows := 0
	for i, b := range buffers {
		if b != nil && b.RowCount() > 0 {
			FlushBucketBatch(proc, b, &writers[i], &buf, nil)
			totalRows += b.RowCount()
		}
	}
	require.Equal(t, size, totalRows)
}

func TestBucketBufferReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_buf_reuse")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test", Fd: f}

	// Reuse same writer across multiple batches.
	for range 2 {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
		bat.SetRowCount(2)
		err := FlushBucketBatch(proc, bat, &w, &buf, nil)
		require.NoError(t, err)
	}

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	totalRows := 0
	for {
		got, err := reader.ReadBatch(proc, reuseBat)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += got.RowCount()
	}
	require.Equal(t, 4, totalRows) // 2 batches × 2 rows
	reader.Close()
}

func TestReusableBufferPoolWithData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	pool := ReusableBufferPool{}
	bufs := pool.Acquire(3)

	// Populate buffers with data.
	for i := range bufs {
		bufs[i] = batch.NewWithSize(1)
		bufs[i].Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
		bufs[i].SetRowCount(3)
	}

	// Release should clean everything.
	pool.Release(proc)
}

func TestResetForFdReadData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_fd_read")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test", Fd: f}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.SetRowCount(3)
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())

	col := vector.MustFixedColWithTypeCheck[int32](got.Vecs[0])
	require.Equal(t, int32(10), col[0])
	require.Equal(t, int32(20), col[1])
	require.Equal(t, int32(30), col[2])

	_, err = reader.ReadBatch(proc, reuseBat)
	require.Equal(t, io.EOF, err)
	reader.Close()
}

func TestSpillFileFormatMultipleBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_format")
	require.NoError(t, err)

	var buf bytes.Buffer
	w := BucketWriter{Name: "test", Fd: f}
	for i := 0; i < 3; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i * 10), int32(i*10 + 1)}, nil, proc.Mp())
		bat.SetRowCount(2)
		err := FlushBucketBatch(proc, bat, &w, &buf, nil)
		require.NoError(t, err)
	}

	fd := w.HandOffFd()
	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	batchCount := 0
	totalRows := 0
	for {
		got, err := reader.ReadBatch(proc, reuseBat)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		batchCount++
		totalRows += got.RowCount()
	}
	require.Equal(t, 3, batchCount)
	require.Equal(t, 6, totalRows)
	reader.Close()
}

func TestScatterSkipsDisabledWriters(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers := MakeBucketWriters("test_skip")
	for i := 0; i < len(writers); i += 2 {
		writers[i].Name = "" // disable even buckets
	}

	buffers := make([]*batch.Batch, len(writers))
	var buf bytes.Buffer
	nRows := SpillNumBuckets * 100
	vals := make([]int32, nRows)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.SetRowCount(nRows)

	err := ScatterBatch(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil)
	require.NoError(t, err)

	// Disabled buckets must have no buffer and no file created.
	for i := 0; i < len(writers); i += 2 {
		require.Nil(t, buffers[i], "disabled bucket %d must have no buffer", i)
		require.False(t, writers[i].Created(), "disabled bucket %d must not have file", i)
	}

	// Enabled buckets should have received data.
	hashValues := make([]uint64, nRows)
	ComputeXXHash(bat.Vecs[:1], hashValues, 0)
	var expectedOddRows int
	for _, h := range hashValues {
		if h&uint64(SpillNumBuckets-1)&1 == 1 {
			expectedOddRows++
		}
	}
	require.Greater(t, expectedOddRows, 0)

	var oddRows int
	for i := 1; i < len(writers); i += 2 {
		if buffers[i] != nil {
			oddRows += buffers[i].RowCount()
		}
	}
	require.Equal(t, expectedOddRows, oddRows, "all odd-bucket rows must be in buffers")
}

// --- SpillEngine tests ---

func makeTestKeyExpr() []*plan.Expr {
	return []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}}
}

func makeTestEvalKeysFn() func(*batch.Batch) ([]*vector.Vector, error) {
	return func(bat *batch.Batch) ([]*vector.Vector, error) {
		return bat.Vecs[:1], nil
	}
}

func makeInt32Batch(proc *process.Process, vals []int32) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.SetRowCount(len(vals))
	return bat
}

func writeBuildFile(proc *process.Process, name string, bat *batch.Batch) *os.File {
	spillfs, _ := proc.GetSpillFileService()
	f, _ := spillfs.CreateAndRemoveFile(context.Background(), name)
	var buf bytes.Buffer
	w := BucketWriter{Name: name, Fd: f}
	FlushBucketBatch(proc, bat, &w, &buf, nil)
	return w.HandOffFd()
}

func TestNewSpillEngineAndInit(t *testing.T) {
	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:  makeTestKeyExpr(),
		SpillThreshold: 1024,
		HashOnPK:       true,
	})
	require.NotNil(t, engine)
	require.Equal(t, 0, len(engine.buckets))

	fds := make([]*os.File, 3)
	err := engine.InitFromSpilledMap(fds)
	require.NoError(t, err)
	require.Equal(t, 3, len(engine.buckets))
	for _, b := range engine.buckets {
		require.Nil(t, b.BuildFd)
		require.Equal(t, 1, b.Depth)
	}
}

func TestInitFromSpilledMapMixed(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	fd1 := writeBuildFile(proc, "test_mixed_1", bat)
	fd2 := writeBuildFile(proc, "test_mixed_2", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	err := engine.InitFromSpilledMap([]*os.File{fd1, nil, fd2})
	require.NoError(t, err)
	require.Equal(t, 3, len(engine.buckets))
	require.NotNil(t, engine.buckets[0].BuildFd)
	require.Nil(t, engine.buckets[1].BuildFd)
	require.NotNil(t, engine.buckets[2].BuildFd)
	engine.Cleanup(proc)
}

func TestRebuildHashmapBasic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	vals := make([]int32, 100)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := makeInt32Batch(proc, vals)
	fd := writeBuildFile(proc, "test_rebuild", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketReady, res)
	require.NotNil(t, jm)
	require.Equal(t, int64(100), jm.GetRowCount())

	jm2, res2, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketQueueEmpty, res2)
	require.Nil(t, jm2)

	jm.Free()
	engine.Cleanup(proc)
}

func TestRebuildHashmapRespectsNeedFlags(t *testing.T) {
	tests := []struct {
		name             string
		needAllocateSels bool
		needBatches      bool
	}{
		{name: "sels only", needAllocateSels: true},
		{name: "batches only", needBatches: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()

			bat := makeInt32Batch(proc, []int32{1, 1})
			fd := writeBuildFile(proc, "test_rebuild_flags", bat)
			bat.Clean(proc.Mp())

			engine := NewSpillEngine(SpillEngineConfig{
				BuildKeyExprs:    makeTestKeyExpr(),
				NeedAllocateSels: tt.needAllocateSels,
				NeedBatches:      tt.needBatches,
			})
			require.NoError(t, engine.InitFromSpilledMap([]*os.File{fd}))

			jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
			require.NoError(t, err)
			require.Equal(t, BucketReady, res)
			require.NotNil(t, jm)

			if tt.needAllocateSels {
				require.Equal(t, []int32{0, 1}, jm.GetSels(0))
			} else {
				require.Nil(t, jm.GetSels(0))
			}
			if tt.needBatches {
				require.Len(t, jm.GetBatches(), 1)
				require.Equal(t, 2, jm.GetBatches()[0].RowCount())
			} else {
				require.Empty(t, jm.GetBatches())
			}

			jm.Free()
			engine.Cleanup(proc)
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestRebuildHashmapEmptyBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{nil})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketSkip, res)
	require.Nil(t, jm)
	require.False(t, engine.HasMoreBuckets())
	engine.Cleanup(proc)
}

func TestRebuildHashmapEmptyBuildOuterJoin(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	var buf bytes.Buffer
	spillfs, _ := proc.GetSpillFileService()
	f, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_outer_probe")
	bat := makeInt32Batch(proc, []int32{1, 2})
	w := BucketWriter{Name: "test_outer_probe", Fd: f}
	FlushBucketBatch(proc, bat, &w, &buf, nil)
	probeFd := w.HandOffFd()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsProbeForEmptyBuild: true,
	})
	engine.InitFromSpilledMap([]*os.File{nil})
	engine.TestSetBucketProbeFd(0, probeFd)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketEmptyBuild, res)
	require.Nil(t, jm)
	require.True(t, engine.IsProbing())
	require.False(t, engine.HasMoreBuckets())

	got, err := engine.NextProbeBatch(proc)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, 2, got.RowCount())

	engine.Cleanup(proc)
}

func TestScatterProbeTable(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{10, 20, 30})
	fd := writeBuildFile(proc, "test_sp_build", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	// Use many rows to ensure distribution across buckets.
	vals := make([]int32, 1000)
	for i := range vals {
		vals[i] = int32(i)
	}
	batches := []*batch.Batch{makeInt32Batch(proc, vals)}
	idx := 0
	children := func() (*batch.Batch, error) {
		if idx >= len(batches) {
			return nil, nil
		}
		b := batches[idx]
		idx++
		return b, nil
	}

	analyzer := process.NewAnalyzer(0, false, false, "test")
	err := engine.ScatterProbeTable(proc, children, analyzer, makeTestEvalKeysFn())
	require.NoError(t, err)
	require.NotNil(t, engine.probeKeyEval)

	// At least one bucket should have probe data.
	hasProbe := false
	for _, b := range engine.buckets {
		if b.ProbeFd != nil {
			hasProbe = true
			break
		}
	}
	require.True(t, hasProbe, "at least one bucket should have probe data")

	engine.Cleanup(proc)
}

func TestScatterProbeTableSkipEmptyBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	fd1 := writeBuildFile(proc, "test_skip_build_1", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd1, nil})

	// Use many rows to ensure distribution.
	vals := make([]int32, 1000)
	for i := range vals {
		vals[i] = int32(i)
	}
	batches := []*batch.Batch{makeInt32Batch(proc, vals)}
	idx := 0
	children := func() (*batch.Batch, error) {
		if idx >= len(batches) {
			return nil, nil
		}
		b := batches[idx]
		idx++
		return b, nil
	}

	analyzer := process.NewAnalyzer(0, false, false, "test")
	err := engine.ScatterProbeTable(proc, children, analyzer, makeTestEvalKeysFn())
	require.NoError(t, err)

	// Bucket 1 (nil build, not outer join) should have no probe data.
	require.Nil(t, engine.buckets[1].ProbeFd)

	engine.Cleanup(proc)
}

func TestScatterProbeTableWithEmptyBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2})
	fd := writeBuildFile(proc, "test_empty_bat", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	batches := []*batch.Batch{
		batch.NewWithSize(0),
		makeInt32Batch(proc, []int32{5, 6}),
	}
	idx := 0
	children := func() (*batch.Batch, error) {
		if idx >= len(batches) {
			return nil, nil
		}
		b := batches[idx]
		idx++
		return b, nil
	}

	analyzer := process.NewAnalyzer(0, false, false, "test")
	err := engine.ScatterProbeTable(proc, children, analyzer, makeTestEvalKeysFn())
	require.NoError(t, err)

	engine.Cleanup(proc)
}

func TestNextProbeBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Write build file.
	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	fd := writeBuildFile(proc, "test_npb_build", bat)

	// Write probe file manually (ensures bucket 0 has probe data).
	probeFd := writeBuildFile(proc, "test_npb_probe", makeInt32Batch(proc, []int32{5, 6, 7}))

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd})
	engine.TestSetBucketProbeFd(0, probeFd)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketReady, res)
	require.True(t, engine.IsProbing())

	got, err := engine.NextProbeBatch(proc)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, 3, got.RowCount())

	got2, err2 := engine.NextProbeBatch(proc)
	require.NoError(t, err2)
	require.Nil(t, got2)

	engine.FinishBucket()
	require.False(t, engine.IsProbing())
	got3, err3 := engine.NextProbeBatch(proc)
	require.NoError(t, err3)
	require.Nil(t, got3)

	jm.Free()
	engine.Cleanup(proc)
}

func TestAdvanceToNextBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	vals := make([]int32, 50)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := makeInt32Batch(proc, vals)
	fd := writeBuildFile(proc, "test_advance_build", bat)
	probeFd := writeBuildFile(proc, "test_advance_probe", makeInt32Batch(proc, []int32{5, 6}))

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd})
	engine.TestSetBucketProbeFd(0, probeFd)

	var capturedJM *message.JoinMap
	var capturedRes BucketResult
	analyzer := process.NewAnalyzer(0, false, false, "test")

	ok, err := engine.AdvanceToNextBucket(proc, analyzer, func(jm *message.JoinMap, res BucketResult) {
		capturedJM = jm
		capturedRes = res
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, capturedJM)
	require.Equal(t, BucketReady, capturedRes)
	require.True(t, engine.IsProbing())
	require.False(t, engine.HasMoreBuckets())

	capturedJM.Free()
	engine.Cleanup(proc)
}

func TestAdvanceToNextBucketWithSkip(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	fd1 := writeBuildFile(proc, "test_skip_adv_1", bat)
	fd2 := writeBuildFile(proc, "test_skip_adv_2", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd1, fd2})

	var results []BucketResult
	analyzer := process.NewAnalyzer(0, false, false, "test")
	for engine.HasMoreBuckets() {
		_, err := engine.AdvanceToNextBucket(proc, analyzer, func(jm *message.JoinMap, res BucketResult) {
			results = append(results, res)
			if jm != nil {
				jm.Free()
			}
		})
		require.NoError(t, err)
		engine.FinishBucket()
	}
	require.Equal(t, 2, len(results))
	engine.Cleanup(proc)
}

func TestReSpillBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	vals := make([]int32, 5000)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := makeInt32Batch(proc, vals)
	fd := writeBuildFile(proc, "test_respill_build", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:  makeTestKeyExpr(),
		SpillThreshold: 500000, // byte mode (> 100000), 5000 rows trigger re-spill
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.NotEqual(t, BucketQueueEmpty, res)
	if res == BucketReSpilled {
		require.Nil(t, jm)
	}

	// Drain all remaining buckets.
	for engine.HasMoreBuckets() {
		jm2, _, err2 := engine.RebuildHashmap(proc, analyzer)
		require.NoError(t, err2)
		if jm2 != nil {
			jm2.Free()
		}
	}

	engine.Cleanup(proc)
}

func TestReSpillDepthLimit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2, 3, 4, 5})
	fd := writeBuildFile(proc, "test_depth_build", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:  makeTestKeyExpr(),
		SpillThreshold: 1,
	})
	engine.InitFromSpilledMap([]*os.File{fd})
	engine.TestSetBucketDepth(0, SpillMaxPass)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketReady, res, "must return BucketReady at max depth")
	require.NotNil(t, jm)

	jm.Free()
	engine.Cleanup(proc)
}

func TestReSpillWithProbe(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	vals := make([]int32, 5000)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := makeInt32Batch(proc, vals)
	fd := writeBuildFile(proc, "test_rsp_probe_build", bat)

	// Manually write probe file.
	probeFd := writeBuildFile(proc, "test_rsp_probe", makeInt32Batch(proc, []int32{100, 200, 300}))

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:  makeTestKeyExpr(),
		SpillThreshold: 10000,
	})
	engine.InitFromSpilledMap([]*os.File{fd})
	engine.TestSetBucketProbeFd(0, probeFd)

	// Set probeKeyEval so scatterProbe works during re-spill.
	engine.probeKeyEval = makeTestEvalKeysFn()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.NotEqual(t, BucketQueueEmpty, res)

	if jm != nil {
		jm.Free()
	}

	for engine.HasMoreBuckets() {
		jm2, _, err := engine.RebuildHashmap(proc, analyzer)
		require.NoError(t, err)
		if jm2 != nil {
			jm2.Free()
		}
	}

	engine.Cleanup(proc)
}

func TestAdvanceToNextBucketReSpilled(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	vals := make([]int32, 5000)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := makeInt32Batch(proc, vals)
	fd := writeBuildFile(proc, "test_adv_re_build", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:  makeTestKeyExpr(),
		SpillThreshold: 10000,
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	analyzer := process.NewAnalyzer(0, false, false, "test")

	for engine.HasMoreBuckets() {
		ok, err := engine.AdvanceToNextBucket(proc, analyzer, func(jm *message.JoinMap, _ BucketResult) {
			if jm != nil {
				jm.Free()
			}
		})
		require.NoError(t, err)
		if ok {
			engine.FinishBucket()
		}
	}
	// Test passes if the loop terminates without errors.
	engine.Cleanup(proc)
}

func TestBuilderMemSize(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	builder := &hashbuild.HashmapBuilder{}
	err := builder.Prepare(makeTestKeyExpr(), -1, -1, nil, proc)
	require.NoError(t, err)

	sz := builderMemSize(builder)
	require.Equal(t, int64(0), sz)

	bat := makeInt32Batch(proc, []int32{1, 2, 3, 4, 5})
	err = builder.Batches.CopyIntoBatches(bat, proc)
	require.NoError(t, err)
	builder.InputBatchRowCount += bat.RowCount()

	sz2 := builderMemSize(builder)
	require.Greater(t, sz2, int64(0), "size should grow after adding batches")

	builder.FreeHashMapAndBatches(proc)
	builder.Free(proc)
}

func TestFinishBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{nil})

	engine.probingActive = true
	require.True(t, engine.IsProbing())

	engine.FinishBucket()
	require.False(t, engine.IsProbing())

	engine.FinishBucket()
	require.False(t, engine.IsProbing())

	engine.Cleanup(proc)
}

func TestIsProbingAndHasMoreBuckets(t *testing.T) {
	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	require.False(t, engine.IsProbing())
	require.False(t, engine.HasMoreBuckets())

	engine.probingActive = true
	require.True(t, engine.IsProbing())

	engine.buckets = []SpillBucket{{}}
	require.True(t, engine.HasMoreBuckets())
}

func TestCleanupSpillEngine(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	fd1 := writeBuildFile(proc, "test_cl_build", bat)

	var buf bytes.Buffer
	spillfs, _ := proc.GetSpillFileService()
	probeFile, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_cl_probe")
	pw := BucketWriter{Name: "test_cl_probe", Fd: probeFile}
	FlushBucketBatch(proc, bat, &pw, &buf, nil)
	fd2 := pw.HandOffFd()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{fd1})
	engine.TestSetBucketProbeFd(0, fd2)

	engine.buildReadBatch = batch.NewOffHeapWithSize(0)
	engine.probeReadBatch = batch.NewOffHeapWithSize(0)
	engine.probingActive = true

	engine.keyExecs = make([]colexec.ExpressionExecutor, 1)
	exec, _ := colexec.NewExpressionExecutor(proc, makeTestKeyExpr()[0])
	engine.keyExecs[0] = exec

	engine.Cleanup(proc)

	require.False(t, engine.IsProbing())
	require.Nil(t, engine.buckets)
	require.Nil(t, engine.buildReadBatch)
	require.Nil(t, engine.probeReadBatch)
	require.Nil(t, engine.keyExecs)

	b := make([]byte, 1)
	_, err := fd1.Read(b)
	require.Error(t, err, "fd1 should be closed")
	_, err = fd2.Read(b)
	require.Error(t, err, "fd2 should be closed")
}

func TestScatterProbeFunctionUsesStoredEval(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})

	evalCalled := false
	engine.probeKeyEval = func(bat *batch.Batch) ([]*vector.Vector, error) {
		evalCalled = true
		return []*vector.Vector{bat.Vecs[0]}, nil
	}

	writers := MakeBucketWriters("test_scatter_func")
	buffers := make([]*batch.Batch, len(writers))
	bat := makeInt32Batch(proc, []int32{5, 15, 25})

	err := scatterProbe(proc, engine, bat, writers, buffers, 1, nil)
	require.NoError(t, err)
	require.True(t, evalCalled, "probeKeyEval must be used for scatterProbe")

	for i := range writers {
		writers[i].Close()
	}
}

func TestGetSpillFS(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	fs, err := GetSpillFS(proc)
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Second call returns the same FS.
	fs2, err := GetSpillFS(proc)
	require.NoError(t, err)
	require.NotNil(t, fs2)
}

func TestScatterProbeTableOuterJoinKeepsProbe(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{10, 20, 30})
	fd1 := writeBuildFile(proc, "test_outer_build", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsProbeForEmptyBuild: true,
	})
	engine.InitFromSpilledMap([]*os.File{fd1, nil})

	vals := make([]int32, 1000)
	for i := range vals {
		vals[i] = int32(i)
	}
	batches := []*batch.Batch{makeInt32Batch(proc, vals)}
	idx := 0
	children := func() (*batch.Batch, error) {
		if idx >= len(batches) {
			return nil, nil
		}
		b := batches[idx]
		idx++
		return b, nil
	}

	analyzer := process.NewAnalyzer(0, false, false, "test")
	err := engine.ScatterProbeTable(proc, children, analyzer, makeTestEvalKeysFn())
	require.NoError(t, err)

	require.NotNil(t, engine.buckets[0].ProbeFd)
	require.NotNil(t, engine.buckets[1].ProbeFd, "outer join must keep probe for empty build")

	engine.Cleanup(proc)
}

func TestCleanupDoubleSafe(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{nil, nil, nil})

	engine.Cleanup(proc)
	engine.Cleanup(proc)

	require.Nil(t, engine.buckets)
}

// TestRebuildHashmapPrepareError covers the builder.Free(proc) path
// when HashmapBuilder.Prepare fails (e.g., with an invalid key expression).
func TestRebuildHashmapPrepareError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	fd := writeBuildFile(proc, "test_prep_err", bat)

	// Use an expression that will fail in Prepare (nil Expr field).
	badExpr := []*plan.Expr{{}}

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: badExpr,
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	_, res, err := engine.RebuildHashmap(proc, analyzer)
	require.Error(t, err)
	require.Equal(t, BucketSkip, res)

	engine.Cleanup(proc)
}

// TestFlushBucketBatchShortWriteCheck verifies the short-write guard.
func TestFlushBucketBatchShortWriteCheck(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_short")
	require.NoError(t, err)

	// Write a batch — this should succeed without short write.
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_short", Fd: f}
	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	err = FlushBucketBatch(proc, bat, &w, &buf, nil)
	require.NoError(t, err)

	f.Close()
}
