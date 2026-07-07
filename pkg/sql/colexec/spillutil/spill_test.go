package spillutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
	require.False(t, ShouldSpill(1, 0))
	require.True(t, ShouldSpill(1000, 1000))
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
