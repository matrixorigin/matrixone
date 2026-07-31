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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type boundaryCancelReader struct {
	reader   *bytes.Reader
	boundary int64
	read     int64
	cancel   func()
	canceled bool
}

func (r *boundaryCancelReader) Read(p []byte) (int, error) {
	if !r.canceled && r.read >= r.boundary {
		r.canceled = true
		r.cancel()
	}
	if !r.canceled {
		remaining := r.boundary - r.read
		if int64(len(p)) > remaining {
			p = p[:remaining]
		}
	}
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

func TestTakeSpillBuildPayloadRejectsWrongBudgetRef(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	_, _, err := TakeSpillBuildPayload(proc, nil)
	require.ErrorContains(t, err, message.ErrSpillBuildPayloadEmpty.Error())

	fd, err := os.CreateTemp(t.TempDir(), "wrong-budget-ref")
	require.NoError(t, err)
	t.Cleanup(func() { _ = fd.Close() })
	releases := 0
	file := message.NewSpillFile(fd, 1, 1, func() { releases++ })
	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
	jm.IncRef(1)
	jmFreed := false
	t.Cleanup(func() {
		if !jmFreed {
			jm.Free()
		}
	})
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		Files:     []*message.SpillFile{file},
		BudgetRef: struct{}{},
	}))

	_, _, err = TakeSpillBuildPayload(proc, jm)
	require.ErrorContains(t, err, "missing its producer budget generation")
	require.Equal(t, 1, releases)
	_, err = fd.Stat()
	require.Error(t, err)

	jm.Free()
	jmFreed = true
	require.Equal(t, 1, releases)
}

func TestTakeSpillBuildPayloadLegacyResolvesConsumerBudget(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	fd, err := os.CreateTemp(t.TempDir(), "legacy-build-payload")
	require.NoError(t, err)
	t.Cleanup(func() { _ = fd.Close() })
	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
	jm.IncRef(1)
	jmFreed := false
	t.Cleanup(func() {
		if !jmFreed {
			jm.Free()
		}
	})
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		LegacyFds: []*os.File{fd},
	}))

	wantBudget, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	payload, budget, err := TakeSpillBuildPayload(proc, jm)
	require.NoError(t, err)
	require.Same(t, fd, payload.LegacyFds[0])
	require.Same(t, wantBudget, budget)
	t.Cleanup(func() { _ = payload.Close() })
	require.NoError(t, payload.Close())
	_, err = fd.Stat()
	require.Error(t, err)

	jm.Free()
	jmFreed = true
}

func TestComputeXXHash(t *testing.T) {
	mp := mpool.MustNewZero()
	ComputeXXHash(nil, nil, 0)

	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
	defer vec.Free(mp)
	hashValues := make([]uint64, 3)
	ComputeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NotEqual(t, uint64(0), hashValues[0])

	constVec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(7), 3, mp)
	require.NoError(t, err)
	defer constVec.Free(mp)
	ComputeXXHash([]*vector.Vector{constVec}, hashValues, 1)
	require.Equal(t, hashValues[0], hashValues[2])

	shortVec := testutil.MakeInt32Vector([]int32{9}, nil, mp)
	defer shortVec.Free(mp)
	ComputeXXHash([]*vector.Vector{shortVec}, hashValues, 2)

	constNull := vector.NewConstNull(types.T_int32.ToType(), 3, mp)
	defer constNull.Free(mp)
	nullHashes := make([]uint64, 3)
	ComputeXXHash([]*vector.Vector{constNull}, nullHashes, 7)
	require.Equal(t, hashCombine(uint64(7), uint64(0)), nullHashes[0])
	require.Equal(t, nullHashes[0], nullHashes[2])
}

func TestClassifyRowsConservesRows(t *testing.T) {
	hashes := make([]uint64, 257)
	for i := range hashes {
		// Include skew, an empty bucket, and rows that differ only at the
		// re-spill bit offset.
		hashes[i] = uint64(i%7) | (uint64(i&3) << 5)
	}
	rowIDs := make([]int32, len(hashes))
	counts := make([]int32, SpillNumBuckets)
	offsets := make([]int32, SpillNumBuckets+1)
	require.NoError(t, classifyRows(hashes, SpillNumBuckets, 0, rowIDs, counts, offsets))
	require.Equal(t, int32(len(hashes)), offsets[SpillNumBuckets])
	seen := make([]bool, len(hashes))
	for bucket := 0; bucket < SpillNumBuckets; bucket++ {
		for _, rowID := range rowIDs[offsets[bucket]:offsets[bucket+1]] {
			row := int(rowID)
			require.GreaterOrEqual(t, row, 0)
			require.Less(t, row, len(hashes))
			require.False(t, seen[row])
			seen[row] = true
			require.Equal(t, bucket, int(hashes[row]&(SpillNumBuckets-1)))
		}
	}
	for _, ok := range seen {
		require.True(t, ok)
	}

	// Re-spill consumes the next five hash bits without changing the row
	// conservation invariant.
	require.NoError(t, classifyRows(hashes, SpillNumBuckets, 5, rowIDs, counts, offsets))
	require.Equal(t, int32(len(hashes)), offsets[SpillNumBuckets])
}

func legacyClassifyRows(hashes []uint64, rowIDs []int32) {
	pos := 0
	for bucket := uint64(0); bucket < SpillNumBuckets; bucket++ {
		for row, hash := range hashes {
			if hash&(SpillNumBuckets-1) == bucket {
				rowIDs[pos] = int32(row)
				pos++
			}
		}
	}
}

func BenchmarkClassifyRows(b *testing.B) {
	hashes := make([]uint64, 8192)
	for i := range hashes {
		hashes[i] = uint64(i*2654435761) ^ uint64(i>>3)
	}
	rowIDs := make([]int32, len(hashes))
	counts := make([]int32, SpillNumBuckets)
	offsets := make([]int32, SpillNumBuckets+1)
	b.Run("counts_prefix_rowids", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := classifyRows(hashes, SpillNumBuckets, 0, rowIDs, counts, offsets); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("legacy_bucket_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			legacyClassifyRows(hashes, rowIDs)
		}
	})
}

func TestBucketWriterAccountedHandOffSeekFailureRetainsOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(1<<20, 1<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	fdToken, err := generation.ReserveSpillFD(1)
	require.NoError(t, err)

	fd, err := os.CreateTemp(t.TempDir(), "closed-spill")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	w := BucketWriter{Fd: fd, fdReservation: fdToken}
	file, err := w.handOffSpillFile()
	require.Error(t, err)
	require.Nil(t, file)
	require.Same(t, fd, w.Fd, "failed rewind must retain file ownership")
	require.Equal(t, uint64(1), generation.SpillFDUsed())
	w.Close()
	require.Zero(t, generation.SpillFDUsed())
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
	require.NoError(t, FlushBucketBatch(proc, nil, &w, &buf, nil))
	err = FlushBucketBatch(proc, bat, &w, &buf, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)

	fd := w.HandOffFd()
	reader := BucketReader{fd: fd}
	reuseBat := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())
	reader.Close()
}

func TestBucketReaderAccountedLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(8<<20, 8<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	bat := makeInt32Batch(proc, []int32{1, 2, 3, 4})
	defer bat.Clean(proc.Mp())
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_accounted_reader", Budget: generation}
	require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
	file, err := w.handOffSpillFile()
	require.NoError(t, err)
	require.NotNil(t, file)
	require.Positive(t, generation.SpillDiskUsed())
	require.Equal(t, uint64(1), generation.SpillFDUsed())

	reader := BucketReader{}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForSpillFile(file)
	reuseBat := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 4, got.RowCount())
	require.Positive(t, generation.Used())

	reuseBat.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
}

func TestReconcileReadReservation(t *testing.T) {
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	t.Run("shrink", func(t *testing.T) {
		token, err := generation.Reserve(1024)
		require.NoError(t, err)
		require.NoError(t, reconcileReadReservation(token, 256))
		require.Equal(t, uint64(256), generation.Used())
		require.True(t, token.Release())
		require.Zero(t, generation.Used())
	})

	t.Run("underestimated-retained-bytes", func(t *testing.T) {
		token, err := generation.Reserve(256)
		require.NoError(t, err)
		require.ErrorIs(t, reconcileReadReservation(token, 257), process.ErrHashBuildBudgetInvalid)
		// Failed upward reconciliation keeps the original token live so both
		// reader cleanup paths can release the complete reservation exactly once.
		require.Equal(t, uint64(256), generation.Used())
		require.True(t, token.Release())
		require.Zero(t, generation.Used())
	})
}

func TestPredictMergedRetainedBytesMatchesUnionBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	tests := []struct {
		name string
		dst  func() (*batch.Batch, *batch.Batch)
	}{
		{
			name: "fixed-and-varlen-multi-column",
			dst: func() (*batch.Batch, *batch.Batch) {
				dst := batch.NewWithSize(2)
				dst.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
				dst.Vecs[1] = testutil.MakeVarcharVector([]string{"left", "side"}, nil, proc.Mp())
				dst.SetRowCount(2)
				src := batch.NewWithSize(2)
				src.Vecs[0] = testutil.MakeInt32Vector([]int32{3, 4, 5}, nil, proc.Mp())
				src.Vecs[1] = testutil.MakeVarcharVector([]string{"right", "hand", "rows"}, nil, proc.Mp())
				src.SetRowCount(3)
				return dst, src
			},
		},
		{
			name: "const-fixed",
			dst: func() (*batch.Batch, *batch.Batch) {
				dst := makeInt32Batch(proc, []int32{1, 2})
				src := batch.NewWithSize(1)
				var err error
				src.Vecs[0], err = vector.NewConstFixed(types.T_int32.ToType(), int32(9), 3, proc.Mp())
				require.NoError(t, err)
				src.SetRowCount(3)
				return dst, src
			},
		},
		{
			name: "const-inline-varlen",
			dst: func() (*batch.Batch, *batch.Batch) {
				dst := batch.NewWithSize(1)
				dst.Vecs[0] = testutil.MakeVarcharVector([]string{"left", "side"}, nil, proc.Mp())
				dst.SetRowCount(2)
				src := batch.NewWithSize(1)
				var err error
				src.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("inline"), 3, proc.Mp())
				require.NoError(t, err)
				src.SetRowCount(3)
				return dst, src
			},
		},
		{
			name: "const-non-inline",
			dst: func() (*batch.Batch, *batch.Batch) {
				dst := batch.NewWithSize(1)
				dst.Vecs[0] = testutil.MakeVarcharVector([]string{"left", "side"}, nil, proc.Mp())
				dst.SetRowCount(2)
				src := batch.NewWithSize(1)
				var err error
				src.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("a sufficiently long constant value"), 3, proc.Mp())
				require.NoError(t, err)
				src.SetRowCount(3)
				return dst, src
			},
		},
		{
			name: "const-null",
			dst: func() (*batch.Batch, *batch.Batch) {
				dst := batch.NewWithSize(1)
				dst.Vecs[0] = testutil.MakeVarcharVector([]string{"left", "side"}, nil, proc.Mp())
				dst.SetRowCount(2)
				src := batch.NewWithSize(1)
				src.Vecs[0] = vector.NewConstNull(types.T_varchar.ToType(), 3, proc.Mp())
				src.SetRowCount(3)
				return dst, src
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dst, src := tc.dst()
			defer dst.Clean(proc.Mp())
			defer src.Clean(proc.Mp())
			predicted, ok := predictMergedRetainedBytes(dst, src)
			require.True(t, ok)
			require.NoError(t, dst.UnionWindow(src, 0, src.RowCount(), proc.Mp()))
			actual, ok := batchRetainedBytes(dst)
			require.True(t, ok)
			require.LessOrEqual(t, actual, predicted)
			require.Equal(t, src.RowCount()+2, dst.RowCount())
		})
	}
}

func TestPredictMergedRetainedBytesAdmissionBudget(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	dstVals := make([]int32, 100)
	srcVals := make([]int32, 100)
	for i := range dstVals {
		dstVals[i] = int32(i)
		srcVals[i] = int32(i + len(dstVals))
	}
	dst := makeInt32Batch(proc, dstVals)
	src := makeInt32Batch(proc, srcVals)
	defer dst.Clean(proc.Mp())
	defer src.Clean(proc.Mp())
	old, ok := batchRetainedBytes(dst)
	require.True(t, ok)
	next, ok := batchRetainedBytes(src)
	require.True(t, ok)
	predicted, ok := predictMergedRetainedBytes(dst, src)
	require.True(t, ok)
	require.Greater(t, predicted, old+next, "rounded destination growth must be admitted independently")

	reserveAll := func(t *testing.T, cap uint64) {
		budget := process.MustNewHashBuildBudget(cap, cap)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		o, err := generation.Reserve(old)
		require.NoError(t, err)
		n, err := generation.Reserve(next)
		require.NoError(t, err)
		d, err := generation.Reserve(predicted)
		require.NoError(t, err)
		o.Release()
		n.Release()
		d.Release()
		require.Zero(t, generation.Used())
	}
	reserveAll(t, old+next+predicted)

	budget := process.MustNewHashBuildBudget(old+next+predicted-1, old+next+predicted-1)
	generation, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	o, err := generation.Reserve(old)
	require.NoError(t, err)
	n, err := generation.Reserve(next)
	require.NoError(t, err)
	_, err = generation.Reserve(predicted)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	// Admission happens before UnionBatch, so the destination remains intact.
	require.Equal(t, len(dstVals), dst.RowCount())
	require.Equal(t, dstVals[0], vector.GetFixedAtNoTypeCheck[int32](dst.Vecs[0], 0))
	o.Release()
	n.Release()
	require.Zero(t, generation.Used())
}

func TestBucketWriterAggregatesDiskAccountingPerFile(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(8<<20, 8<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	defer bat.Clean(proc.Mp())

	w := BucketWriter{Name: "aggregate_disk_token", Budget: generation}
	var buf bytes.Buffer
	require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
	first := w.diskReservation
	require.NotNil(t, first)
	firstSize := first.Size()
	require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
	require.Same(t, first, w.diskReservation)
	require.Greater(t, w.diskReservation.Size(), firstSize)
	require.Equal(t, w.diskReservation.Size(), generation.SpillDiskUsed())
	w.Close()
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
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
	budget := process.MustNewHashBuildBudget(8<<20, 8<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

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
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(f)
	reuseBat := batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")
	require.Equal(t, uint64(64<<10), generation.Used(), "failed read must release its decoded-batch lease")
	reader.Close()
	require.Zero(t, generation.Used())
	f.Close()
}

func TestBucketReaderTruncatedMagic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{1})
	fd := writeBuildFile(proc, "test_truncated_magic", bat)
	bat.Clean(proc.Mp())
	info, err := fd.Stat()
	require.NoError(t, err)
	require.NoError(t, fd.Truncate(info.Size()-4))
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)

	reader := BucketReader{}
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuseBat)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	reader.Close()
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

	budget, err := process.NewHashBuildBudget(8<<20, 8<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	accounted := BucketWriter{Name: "test_accounted_raw_handoff", Budget: generation}
	bat := makeInt32Batch(proc, []int32{1})
	var buf bytes.Buffer
	require.NoError(t, FlushBucketBatch(proc, bat, &accounted, &buf, nil))
	bat.Clean(proc.Mp())
	require.Nil(t, accounted.HandOffFd(), "raw handoff must not orphan accounting tokens")
	accounted.Close()
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
}

func TestMakeBucketWriters(t *testing.T) {
	writers := MakeBucketWriters("test")
	require.Equal(t, SpillNumBuckets, len(writers))
	for i := range writers {
		require.NotEmpty(t, writers[i].Name)
		require.Nil(t, writers[i].Fd)
	}
}

type countingMutableFileService struct {
	fileservice.MutableFileService
	ensureCalls int
	closeCalls  int
}

func (s *countingMutableFileService) EnsureDir(ctx context.Context, path string) error {
	s.ensureCalls++
	return s.MutableFileService.EnsureDir(ctx, path)
}

func (s *countingMutableFileService) Close(ctx context.Context) {
	s.closeCalls++
	s.MutableFileService.Close(ctx)
}

func TestSpillEngineSharesFileServiceAcrossWriters(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	local, err := fileservice.Get[fileservice.MutableFileService](
		proc.GetFileService(),
		defines.LocalFileServiceName,
	)
	require.NoError(t, err)
	countingLocal := &countingMutableFileService{MutableFileService: local}
	services, err := fileservice.NewFileServices("", countingLocal)
	require.NoError(t, err)
	proc.SetFileService(services)

	engine := NewSpillEngine(SpillEngineConfig{})
	first := engine.makeBucketWriters("cached_first")
	second := engine.makeBucketWriters("cached_second")
	require.Same(t, first[0].spillFS, second[0].spillFS)
	require.Same(t, &engine.spillFS, first[0].spillFS)
	require.Zero(t, countingLocal.ensureCalls, "resolution remains lazy")

	require.NoError(t, writeBucketPayload(proc, []byte("first"), 1, &first[0], nil))
	require.Equal(t, 1, countingLocal.ensureCalls)
	cached := engine.spillFS.fs
	require.NotNil(t, cached)

	require.NoError(t, writeBucketPayload(proc, []byte("second"), 1, &second[1], nil))
	require.Equal(t, 1, countingLocal.ensureCalls, "all engine writers reuse one resolved service")
	require.Equal(t, cached, engine.spillFS.fs)

	first[0].Close()
	second[1].Close()

	// The service is borrowed. Cleanup releases engine-owned files and memory,
	// but must neither close nor invalidate the process-owned service.
	borrowed := &countingMutableFileService{MutableFileService: cached}
	engine.spillFS.fs = borrowed
	engine.Cleanup(proc)
	require.Zero(t, borrowed.closeCalls)
	file, err := borrowed.CreateAndRemoveFile(proc.Ctx, "after_engine_cleanup")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	// A writer constructed outside SpillEngine has no shared cache and keeps
	// the historical process lookup fallback.
	direct := BucketWriter{Name: "direct_writer_fallback"}
	require.NoError(t, writeBucketPayload(proc, []byte("direct"), 1, &direct, nil))
	require.Equal(t, 2, countingLocal.ensureCalls)
	direct.Close()
}

func TestScatterProbeTableRejectsRecursiveMarker(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	marker := batch.NewWithSize(0)
	marker.SetRowCount(1)
	marker.SetLast()
	engine := NewSpillEngine(SpillEngineConfig{})
	engine.InitFromSpilledMap([]*os.File{nil})
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

func TestScatterProbeTableErrors(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	wantErr := errors.New("scatter probe failure")

	engine := NewSpillEngine(SpillEngineConfig{})
	engine.InitFromSpilledMap([]*os.File{nil})
	err := engine.ScatterProbeTable(proc,
		func() (*batch.Batch, error) { return nil, wantErr }, nil,
		func(*batch.Batch) ([]*vector.Vector, error) { return nil, nil })
	require.ErrorIs(t, err, wantErr)
	engine.Cleanup(proc)

	bat := makeInt32Batch(proc, []int32{1})
	engine = NewSpillEngine(SpillEngineConfig{})
	engine.InitFromSpilledMap([]*os.File{nil})
	err = engine.ScatterProbeTable(proc,
		func() (*batch.Batch, error) { return bat, nil }, nil,
		func(*batch.Batch) ([]*vector.Vector, error) { return nil, wantErr })
	require.ErrorIs(t, err, wantErr)
	engine.Cleanup(proc)
	bat.Clean(proc.Mp())
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

func TestBucketReaderCancellationStopsBeforeMergingNextRecord(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)

	first := makeInt32Batch(proc, []int32{1, 2})
	second := makeInt32Batch(proc, []int32{3, 4})
	var encoded bytes.Buffer
	require.NoError(t, marshalSpillRecord(first, &encoded))
	firstRecord := bytes.Clone(encoded.Bytes())
	require.NoError(t, marshalSpillRecord(second, &encoded))
	stream := append(firstRecord, encoded.Bytes()...)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())

	source := &boundaryCancelReader{
		reader:   bytes.NewReader(stream),
		boundary: int64(len(firstRecord)),
		cancel:   func() { proc.Cancel(context.Canceled) },
	}
	fd, err := os.CreateTemp(t.TempDir(), "bucket-reader-cancel")
	require.NoError(t, err)
	reader := BucketReader{
		fd:           fd,
		reader:       bufio.NewReaderSize(source, 16),
		mergeRecords: true,
	}
	reuseBat := batch.NewOffHeapWithSize(0)

	got, err := reader.ReadBatch(proc, reuseBat)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, got)
	require.True(t, source.canceled)
	require.Equal(t, int64(len(firstRecord)+16), source.read,
		"reader may inspect the next header but must not decode its payload after cancellation")
	require.Zero(t, reuseBat.RowCount())

	reader.Close()
	reuseBat.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
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
	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
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
	require.NoError(t, scatterImpl(proc, batch.NewWithSize(0), nil, nil, nil, 0, nil, nil, nil, nil))

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

	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
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
	ComputeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NotEqual(t, uint64(0), hashValues[0])
	require.NotEqual(t, uint64(0), hashValues[2])
}

func TestComputeXXHashMultipleColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	vec1 := testutil.MakeInt32Vector([]int32{1, 1, 2}, nil, mp)
	vec2 := testutil.MakeVarcharVector([]string{"a", "b", "a"}, nil, mp)
	hashValues := make([]uint64, 3)
	ComputeXXHash([]*vector.Vector{vec1, vec2}, hashValues, 0)
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
			ComputeXXHash([]*vector.Vector{tt.vec}, hashValues, 0)
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

	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
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
	require.NotNil(t, r.fd)

	// Second ResetForFd reuses internal state.
	r.ResetForFd(fd2)
	require.NotNil(t, r.fd)

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
	ComputeXXHash([]*vector.Vector{vec}, hashValues, 0)

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

	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
	require.NoError(t, err)
	// Should not panic with nulls.
}

func TestReaderBatchReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewHashBuildBudget(8<<20, 8<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

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
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	before := generation.Snapshot()

	// Read with the same reuseBat. The second record admits the one-time
	// old-plus-new transition and then keeps the bounded high-water lease.
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 5, got.RowCount())
	afterFirst := generation.Snapshot()
	require.Equal(t, before.ReserveCount+1, afterFirst.ReserveCount)
	require.Equal(t, before.ReconcileCount, afterFirst.ReconcileCount)

	got, err = reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, got.RowCount())
	afterSecond := generation.Snapshot()
	require.Equal(t, afterFirst.ReserveCount+1, afterSecond.ReserveCount)
	require.Equal(t, afterFirst.ReconcileCount, afterSecond.ReconcileCount)
	require.Equal(t, afterFirst.ReleaseCount, afterSecond.ReleaseCount)

	reuseBat.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
}

func TestReaderBatchLeaseGrowsForLargerRecord(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewHashBuildBudget(8<<20, 8<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_read_lease_grow")
	require.NoError(t, err)
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_read_lease_grow", Fd: f}
	for _, size := range []int{2, 1_000} {
		vals := make([]int32, size)
		bat := makeInt32Batch(proc, vals)
		require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
		bat.Clean(proc.Mp())
	}

	reader := BucketReader{}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(w.HandOffFd())
	reuseBat := batch.NewOffHeapWithSize(0)
	before := generation.Snapshot()

	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, got.RowCount())
	afterFirst := generation.Snapshot()
	require.Equal(t, before.ReserveCount+1, afterFirst.ReserveCount)

	got, err = reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 1_000, got.RowCount())
	afterSecond := generation.Snapshot()
	require.Equal(t, afterFirst.ReserveCount+1, afterSecond.ReserveCount, "larger record should grow the existing lease once")
	require.Greater(t, afterSecond.Used, afterFirst.Used)
	require.Equal(t, afterFirst.ReconcileCount, afterSecond.ReconcileCount)

	reuseBat.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
}

func TestDecodedBatchReusePeakCoversMpoolGrowth(t *testing.T) {
	const oldCapacity = int64(4 << 20)
	required := oldCapacity + 1
	newCapacity, ok := mpool.GrowCapacity(oldCapacity, required)
	require.True(t, ok)
	projected, ok := decodedBatchProjectedBytes(uint64(required), 1, 1)
	require.True(t, ok)
	peak, ok := decodedBatchReusePeakBytes(uint64(oldCapacity), projected, uint64(required))
	require.True(t, ok)
	require.GreaterOrEqual(t, peak, uint64(oldCapacity+newCapacity))
}

func TestReaderBatchReuseFallsBackBeforeTransientGrowth(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_read_reuse_transient")
	require.NoError(t, err)
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_read_reuse_transient", Fd: f}
	for _, width := range []int{1_024, 1_025} {
		values := make([]string, 4_096)
		for i := range values {
			values[i] = strings.Repeat("x", width)
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
		bat.SetRowCount(len(values))
		require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
		bat.Clean(proc.Mp())
	}

	fd := w.HandOffFd()
	var projected [2]uint64
	for i := range projected {
		var header [16]byte
		_, err = io.ReadFull(fd, header[:])
		require.NoError(t, err)
		rows := types.DecodeInt64(header[:8])
		payload := types.DecodeInt64(header[8:])
		require.Positive(t, rows)
		require.Positive(t, payload)
		var ok bool
		projected[i], ok = decodedBatchProjectedBytes(uint64(payload), rows, 1)
		require.True(t, ok)
		_, err = fd.Seek(payload+8, io.SeekCurrent)
		require.NoError(t, err)
	}
	require.Greater(t, projected[1], projected[0])
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// The cap fits the retained record plus one logical new payload, but not the
	// allocator's 1.25x replacement capacity. The reader must release the old
	// lease and decode the second record fresh.
	cap := uint64(64<<10) + projected[0] + projected[1]
	budget := process.MustNewHashBuildBudget(cap, cap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	reader := BucketReader{}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	baseline := uint64(proc.Mp().CurrNB())
	epoch := proc.Mp().StartResourcePeakEpoch()
	require.NotNil(t, epoch)

	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 4_096, got.RowCount())
	got, err = reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 4_096, got.RowCount())

	peak, ok := proc.Mp().EndResourcePeakEpoch(epoch)
	require.True(t, ok)
	require.LessOrEqual(t, peak, baseline+projected[1])
	require.LessOrEqual(t, generation.Peak(), cap)

	reuseBat.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
}

func TestReaderBatchLeaseUsesSinglePayloadEstimate(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewHashBuildBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_read_lease_trim")
	require.NoError(t, err)
	values := make([]string, 4_096)
	for i := range values {
		values[i] = strings.Repeat("x", 1_024)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	bat.SetRowCount(len(values))
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_read_lease_trim", Fd: f}
	require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
	bat.Clean(proc.Mp())

	reader := BucketReader{}
	require.NoError(t, reader.EnsureBuffer(generation))
	fd := w.HandOffFd()
	var header [16]byte
	_, err = io.ReadFull(fd, header[:])
	require.NoError(t, err)
	payload := types.DecodeInt64(header[8:])
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)
	before := generation.Snapshot()
	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	actual, ok := batchRetainedBytes(got)
	require.True(t, ok)
	after := generation.Snapshot()
	require.Equal(t, before.ReconcileCount, after.ReconcileCount)
	require.GreaterOrEqual(t, after.Used, before.Used+actual)
	projected, ok := decodedBatchProjectedBytes(uint64(payload), int64(got.RowCount()), int32(len(got.Vecs)))
	require.True(t, ok)
	require.Equal(t, before.Used+projected, after.Used)

	reuseBat.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
}

func TestMarshalSpillRecordPreallocatesExactPayload(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	bat := batch.NewWithSize(1)
	var err error
	bat.Vecs[0], err = vector.NewConstBytes(
		types.T_varchar.ToType(), make([]byte, 4<<20), 64, proc.Mp(),
	)
	require.NoError(t, err)
	bat.SetRowCount(64)
	defer bat.Clean(proc.Mp())

	buf := bytes.NewBuffer(make([]byte, 0, 1<<20))
	require.NoError(t, marshalSpillRecord(bat, buf))
	size, err := bat.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, size+24, buf.Cap())

	small := batch.NewWithSize(1)
	small.Vecs[0], err = vector.NewConstBytes(
		types.T_varchar.ToType(), make([]byte, 1024), 1, proc.Mp(),
	)
	require.NoError(t, err)
	small.SetRowCount(1)
	defer small.Clean(proc.Mp())
	require.NoError(t, marshalSpillRecord(small, buf),
		"a retained large serialization buffer must be reusable for a smaller batch")
}

func TestReaderBatchLeaseGrowRejectionReleasesToken(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_read_lease_reject")
	require.NoError(t, err)
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_read_lease_reject", Fd: f}
	for _, size := range []int{2, 1_000} {
		bat := makeInt32Batch(proc, make([]int32, size))
		require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
		bat.Clean(proc.Mp())
	}
	fd := w.HandOffFd()

	var header [16]byte
	_, err = io.ReadFull(fd, header[:])
	require.NoError(t, err)
	firstPayload := types.DecodeInt64(header[8:])
	_, err = fd.Seek(firstPayload+8, io.SeekCurrent)
	require.NoError(t, err)
	_, err = io.ReadFull(fd, header[:])
	require.NoError(t, err)
	secondPayload := types.DecodeInt64(header[8:])
	require.Greater(t, secondPayload, firstPayload)
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)

	firstProjected, ok := decodedBatchProjectedBytes(uint64(firstPayload), 2, 1)
	require.True(t, ok)
	secondProjected, ok := decodedBatchProjectedBytes(uint64(secondPayload), 1_000, 1)
	require.True(t, ok)
	cap := uint64(64<<10) + firstProjected + (secondProjected-firstProjected)/2
	budget := process.MustNewHashBuildBudget(cap, cap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	reader := BucketReader{}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(fd)
	reuseBat := batch.NewOffHeapWithSize(0)

	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, got.RowCount())
	_, err = reader.ReadBatch(proc, reuseBat)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Equal(t, uint64(64<<10), generation.Used(), "grow rejection must release the existing decoded-batch lease")

	reader.Close()
	require.Zero(t, generation.Used())
}

func TestReaderBatchClosedLeaseDoesNotRetryAdmission(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	f, err := spillfs.CreateAndRemoveFile(context.Background(), "test_read_closed_lease")
	require.NoError(t, err)
	var buf bytes.Buffer
	w := BucketWriter{Name: "test_read_closed_lease", Fd: f}
	for _, size := range []int{2, 1_000} {
		bat := makeInt32Batch(proc, make([]int32, size))
		require.NoError(t, FlushBucketBatch(proc, bat, &w, &buf, nil))
		bat.Clean(proc.Mp())
	}

	budget := process.MustNewHashBuildBudget(8<<20, 8<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	reader := BucketReader{}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(w.HandOffFd())
	reuseBat := batch.NewOffHeapWithSize(0)

	got, err := reader.ReadBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, got.RowCount())
	before := generation.Snapshot()
	generation.Close()

	_, err = reader.ReadBatch(proc, reuseBat)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetClosed)
	require.NotErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	after := generation.Snapshot()
	require.Equal(t, before.RejectCount+1, after.RejectCount,
		"a closed lease must not fall through to a second Reserve attempt")
	require.Nil(t, reader.batchToken)
	require.Zero(t, reader.batchCharge)
	require.Zero(t, reuseBat.RowCount())

	reader.Close()
	require.Zero(t, generation.Used())
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

	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
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

	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
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

func TestBucketReaderMergesAdjacentAccountedRecords(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(16<<20, 16<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var buf bytes.Buffer
	writer := BucketWriter{Name: "merge_records", Budget: generation}
	for _, values := range [][]int32{{1, 2}, {3, 4, 5}} {
		bat := makeInt32Batch(proc, values)
		require.NoError(t, FlushBucketBatch(proc, bat, &writer, &buf, nil))
		bat.Clean(proc.Mp())
	}
	file, err := writer.handOffSpillFile()
	require.NoError(t, err)

	reader := BucketReader{mergeRecords: true}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForSpillFile(file)
	reuse := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Equal(t, 5, got.RowCount())
	require.Positive(t, generation.Used())
	_, err = reader.ReadBatch(proc, reuse)
	require.ErrorIs(t, err, io.EOF)
	reuse.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
}

func TestBucketReaderMergeRejectsTruncatedTrailingHeader(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(16<<20, 16<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	bat := makeInt32Batch(proc, []int32{1, 2, 3})
	var payload bytes.Buffer
	require.NoError(t, marshalSpillRecord(bat, &payload))
	bat.Clean(proc.Mp())

	fd, err := os.CreateTemp(t.TempDir(), "truncated-spill")
	require.NoError(t, err)
	t.Cleanup(func() { _ = fd.Close() })
	_, err = fd.Write(payload.Bytes())
	require.NoError(t, err)
	// A clean file boundary has zero bytes left. Any non-empty fragment of the
	// next 16-byte frame header is corruption and must not be accepted as EOF.
	_, err = fd.Write(types.EncodeInt64(new(int64)))
	require.NoError(t, err)
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)

	reader := BucketReader{mergeRecords: true}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(fd)
	reuse := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuse)
	require.Nil(t, got)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Zero(t, reuse.RowCount())
	require.Nil(t, reader.batchToken)
	require.Zero(t, reader.batchCharge)

	reuse.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, generation.Used())
}

func TestBucketReaderMergeRecordsRespectsBatchBoundary(t *testing.T) {
	tests := []struct {
		name       string
		recordRows []int
		wantRows   []int
	}{
		{
			name:       "two medium records stay separate",
			recordRows: []int{5000, 5000},
			wantRows:   []int{5000, 5000},
		},
		{
			name:       "records exactly fill the boundary",
			recordRows: []int{8191, 1},
			wantRows:   []int{8192},
		},
		{
			name:       "record crossing the boundary stays separate",
			recordRows: []int{8191, 2},
			wantRows:   []int{8191, 2},
		},
		{
			name:       "one oversized source record remains indivisible",
			recordRows: []int{9000},
			wantRows:   []int{9000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget, err := process.NewHashBuildBudget(16<<20, 16<<20)
			require.NoError(t, err)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)

			var buf bytes.Buffer
			writer := BucketWriter{Name: "merge_boundary", Budget: generation}
			for record, rows := range tt.recordRows {
				values := make([]int32, rows)
				for row := range values {
					values[row] = int32(record*10000 + row)
				}
				bat := makeInt32Batch(proc, values)
				require.NoError(t, FlushBucketBatch(proc, bat, &writer, &buf, nil))
				bat.Clean(proc.Mp())
			}
			file, err := writer.handOffSpillFile()
			require.NoError(t, err)

			reader := BucketReader{mergeRecords: true}
			require.NoError(t, reader.EnsureBuffer(generation))
			reader.ResetForSpillFile(file)
			reuse := batch.NewOffHeapWithSize(0)
			var gotRows []int
			for {
				got, err := reader.ReadBatch(proc, reuse)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				gotRows = append(gotRows, got.RowCount())
			}
			require.Equal(t, tt.wantRows, gotRows)

			reuse.Clean(proc.Mp())
			reader.Close()
			require.Zero(t, generation.Used())
			require.Zero(t, generation.SpillDiskUsed())
			require.Zero(t, generation.SpillFDUsed())
		})
	}
}

func TestBucketReaderMergeErrorReleasesAllOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(1<<20, 1<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	readerToken, err := generation.Reserve(1)
	require.NoError(t, err)
	sourceToken, err := generation.Reserve(1)
	require.NoError(t, err)
	extraToken, err := generation.Reserve(1)
	require.NoError(t, err)
	reader := BucketReader{batchToken: readerToken, batchCharge: 1}
	dst := makeInt32Batch(proc, []int32{1})
	src := makeInt32Batch(proc, []int32{2})
	want := errors.New("merge failed")
	require.ErrorIs(t, reader.mergeReadError(proc, dst, src, sourceToken, want, extraToken, nil), want)
	require.Nil(t, reader.batchToken)
	require.Zero(t, reader.batchCharge)
	require.Zero(t, generation.Used())
	require.True(t, readerToken.Released())
	require.True(t, sourceToken.Released())
	require.True(t, extraToken.Released())

	require.ErrorIs(t, reader.mergeReadError(proc, nil, nil, nil, want, nil), want)
}

func TestSpillEngineInitFromOwnedFilesAndErrorClassification(t *testing.T) {
	first, err := os.CreateTemp(t.TempDir(), "owned-build")
	require.NoError(t, err)
	owned := message.NewSpillFile(first, 7, 11, nil)
	engine := NewSpillEngine(SpillEngineConfig{})
	engine.InitFromSpilledFiles([]*message.SpillFile{owned, nil})
	require.Len(t, engine.buckets, 2)
	require.Same(t, owned, engine.buckets[0].BuildFd)
	require.Equal(t, int64(7), engine.buckets[0].BuildRows)
	require.Equal(t, 1, engine.buckets[0].Depth)
	require.Nil(t, engine.buckets[1].BuildFd)
	require.Zero(t, engine.buckets[1].BuildRows)

	require.False(t, isBudgetAdmission(nil))
	require.False(t, isBudgetAdmission(io.EOF))
	require.True(t, isBudgetAdmission(process.ErrHashBuildBudgetAdmission))
	require.False(t, isBudgetAdmission(process.ErrHashBuildBudgetClosed))
	require.False(t, isBudgetAdmission(&process.HashBuildBudgetError{
		Kind:      process.HashBuildBudgetErrorAdmission,
		Component: process.HashBuildBudgetComponentSpillDisk,
	}))
	require.False(t, isBudgetAdmission(&process.HashBuildBudgetError{
		Kind:      process.HashBuildBudgetErrorAdmission,
		Component: process.HashBuildBudgetComponentSpillFD,
	}))
	require.Equal(t,
		hashbuild.MemoryPressureMinimumUnit,
		hashbuild.MemoryPressureReasonOf(noProgressError(nil, 3)))
	require.NoError(t, owned.Close())
}

func TestSpillSizeHelpersRejectInvalidAndOverflowInputs(t *testing.T) {
	require.ErrorIs(t, writeBucketPayload(nil, nil, 0, nil, nil), process.ErrHashBuildBudgetInvalid)
	require.NoError(t, marshalSpillRecord(nil, &bytes.Buffer{}))

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	var header bytes.Buffer
	negativeRows := int64(-1)
	zero := int64(0)
	header.Write(types.EncodeInt64(&negativeRows))
	header.Write(types.EncodeInt64(&zero))
	reader := BucketReader{reader: bufio.NewReader(&header)}
	_, _, _, err := reader.readBatchRecord(proc, batch.NewOffHeapWithSize(0), nil, 0, false)
	require.Error(t, err)

	makeHeader := func(batchSize int64) *bufio.Reader {
		var data bytes.Buffer
		rows := int64(0)
		data.Write(types.EncodeInt64(&rows))
		data.Write(types.EncodeInt64(&batchSize))
		data.Write(make([]byte, 12))
		return bufio.NewReader(&data)
	}
	budgetForHeader, err := process.NewHashBuildBudget(1, 1)
	require.NoError(t, err)
	headerGeneration, err := budgetForHeader.OpenGeneration(1)
	require.NoError(t, err)
	reader = BucketReader{reader: makeHeader(math.MaxInt64), budget: headerGeneration}
	_, _, _, err = reader.readBatchRecord(proc, batch.NewOffHeapWithSize(0), nil, 0, false)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	var truncatedPayload bytes.Buffer
	truncatedPayload.Write(types.EncodeInt64(&zero))
	one := int64(1)
	truncatedPayload.Write(types.EncodeInt64(&one))
	truncatedPayload.Write(make([]byte, 4))
	reader = BucketReader{reader: bufio.NewReader(&truncatedPayload), budget: headerGeneration}
	_, _, _, err = reader.readBatchRecord(proc, batch.NewOffHeapWithSize(0), nil, 0, false)
	require.Error(t, err)

	var mismatchedRows bytes.Buffer
	twelve := int64(12)
	mismatchedRows.Write(types.EncodeInt64(&one))
	mismatchedRows.Write(types.EncodeInt64(&twelve))
	mismatchedRows.Write(types.EncodeInt64(&zero))
	mismatchedRows.Write(make([]byte, 4))
	reader = BucketReader{reader: bufio.NewReader(&mismatchedRows), budget: headerGeneration}
	_, _, _, err = reader.readBatchRecord(proc, batch.NewOffHeapWithSize(0), nil, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "row count mismatch")

	reader = BucketReader{reader: makeHeader(1), budget: headerGeneration}
	_, _, _, err = reader.readBatchRecord(proc, batch.NewOffHeapWithSize(0), nil, 0, false)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	headerToken, err := headerGeneration.Reserve(1)
	require.NoError(t, err)
	reader = BucketReader{reader: makeHeader(1), budget: headerGeneration}
	_, returnedToken, _, err := reader.readBatchRecord(proc, batch.NewOffHeapWithSize(0), headerToken, 1, false)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, returnedToken)
	require.True(t, headerToken.Released())

	_, ok := addUint64(math.MaxUint64, 1)
	require.False(t, ok)
	_, ok = mulUint64(math.MaxUint64, 2)
	require.False(t, ok)
	_, ok = decodedBatchReusePeakBytes(math.MaxUint64, 1, 1)
	require.False(t, ok)
	_, ok = decodedBatchReusePeakBytes(0, math.MaxUint64, 1)
	require.False(t, ok)
	_, ok = decodedBatchProjectedBytes(0, -1, 0)
	require.False(t, ok)
	_, ok = decodedBatchProjectedBytes(0, 0, -1)
	require.False(t, ok)
	_, ok = decodedBatchProjectedBytes(math.MaxUint64, 0, 0)
	require.False(t, ok)
	_, ok = decodedBatchProjectedBytes(0, math.MaxInt64, math.MaxInt32)
	require.False(t, ok)
	_, ok = batchRetainedMetadataBytes(1, math.MaxUint64)
	require.False(t, ok)
	_, ok = batchRetainedMetadataBytes(math.MaxUint64, 1)
	require.False(t, ok)
	_, ok = batchPayloadWithAllocationSlack(0, math.MaxUint64)
	require.False(t, ok)
	_, ok = batchPayloadWithAllocationSlack(math.MaxUint64, 0)
	require.False(t, ok)
	_, ok = intToUint64(-1)
	require.False(t, ok)
	_, ok = predictedCapacity(-1, 1)
	require.False(t, ok)
	_, ok = predictedCapacity(1, math.MaxUint64)
	require.False(t, ok)

	_, ok = batchRetainedBytes(nil)
	require.False(t, ok)
	invalidRows := batch.NewOffHeapWithSize(0)
	invalidRows.SetRowCount(-1)
	_, ok = batchRetainedBytes(invalidRows)
	require.False(t, ok)
	_, err = scatterTransientBudgetBytes(nil, false)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, ok = (&SpillEngine{}).scatterCapacityGrowthBytes(-1, 0)
	require.False(t, ok)
	_, ok = (&SpillEngine{}).scatterCapacityGrowthBytes(math.MaxInt, 0)
	require.False(t, ok)

	valid := batch.NewOffHeapWithSize(0)
	valid.SetRowCount(0)
	_, ok = predictMergedRetainedBytes(nil, valid)
	require.False(t, ok)
	invalidRows = batch.NewOffHeapWithSize(0)
	invalidRows.SetRowCount(-1)
	_, ok = predictMergedRetainedBytes(invalidRows, valid)
	require.False(t, ok)
	mismatched := batch.NewOffHeapWithSize(1)
	mismatched.SetRowCount(0)
	_, ok = predictMergedRetainedBytes(valid, mismatched)
	require.False(t, ok)
	nilVectorDst := batch.NewOffHeapWithSize(1)
	nilVectorDst.SetRowCount(0)
	nilVectorSrc := batch.NewOffHeapWithSize(1)
	nilVectorSrc.SetRowCount(0)
	_, ok = predictMergedRetainedBytes(nilVectorDst, nilVectorSrc)
	require.False(t, ok)
	hugeRowsDst := batch.NewOffHeapWithSize(0)
	hugeRowsDst.SetRowCount(maxIntValue())
	hugeRowsSrc := batch.NewOffHeapWithSize(0)
	hugeRowsSrc.SetRowCount(1)
	_, ok = predictMergedRetainedBytes(hugeRowsDst, hugeRowsSrc)
	require.False(t, ok)

	mp := mpool.MustNewZero()
	fixed := testutil.MakeInt32Vector([]int32{1}, nil, mp)
	defer fixed.Free(mp)
	_, ok = mergedVarlenAreaAdd(nil, 1)
	require.False(t, ok)
	_, ok = mergedVarlenAreaAdd(fixed, 1)
	require.False(t, ok)
	constNull := vector.NewConstNull(types.T_varchar.ToType(), 1, mp)
	defer constNull.Free(mp)
	bytes, ok := mergedVarlenAreaAdd(constNull, 0)
	require.True(t, ok)
	require.Zero(t, bytes)
	bytes, ok = mergedVarlenAreaAdd(constNull, 1)
	require.True(t, ok)
	require.Zero(t, bytes)

	require.NoError(t, reconcileReadReservation(nil, 0))
	budget, err := process.NewHashBuildBudget(10, 10)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	token, err := generation.Reserve(1)
	require.NoError(t, err)
	require.ErrorIs(t, reconcileReadReservation(token, 2), process.ErrHashBuildBudgetInvalid)
	require.True(t, token.Release())
	require.ErrorIs(t, reconcileReadReservation(token, 0), process.ErrHashBuildReservationInactive)
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

	err := scatterImpl(proc, bat, bat.Vecs[:1], writers, buffers, 0, &buf, nil, nil, nil)
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

func TestScatterProbeAdmitsExpressionBeforeEvaluation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	col := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	modulo, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"%",
		[]*plan.Expr{col, plan2.MakePlan2Int32ConstExprWithType(2)},
	)
	require.NoError(t, err)
	execs, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{modulo})
	require.NoError(t, err)
	budget, err := process.NewHashBuildBudget(8<<20, 8<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	probeLease, err := hashbuild.NewExpressionMemoryLease(
		generation, []*plan.Expr{modulo}, execs, false)
	require.NoError(t, err)
	engine := NewSpillEngine(SpillEngineConfig{
		ProbeKeyExprs:        []*plan.Expr{modulo},
		Budget:               generation,
		ProbeExpressionLease: probeLease,
	})
	engine.InitFromSpilledMap(make([]*os.File, SpillNumBuckets))
	input := makeInt32Batch(proc, []int32{1, 2, 3, 4})
	defer input.Clean(proc.Mp())
	childrenCalls := 0
	fallbackCalled := false
	err = engine.ScatterProbeTable(
		proc,
		func() (*batch.Batch, error) {
			childrenCalls++
			if childrenCalls == 1 {
				return input, nil
			}
			return nil, nil
		},
		process.NewAnalyzer(0, false, false, "test"),
		func(*batch.Batch) ([]*vector.Vector, error) {
			fallbackCalled = true
			return nil, errors.New("budgeted probe must evaluate its leased executors")
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, childrenCalls)
	require.False(t, fallbackCalled)
	require.Positive(t, probeLease.Reserved())
	require.Positive(t, generation.Used())
	engine.Cleanup(proc)
	require.Positive(t, generation.Used(), "SpillEngine only borrows the probe lease")
	for _, exec := range execs {
		exec.Free()
	}
	probeLease.Release()
	require.Zero(t, generation.Used())
}

func TestScatterProbeExpressionAdmissionRejectsBeforeEval(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	col := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	modulo, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"%",
		[]*plan.Expr{col, plan2.MakePlan2Int32ConstExprWithType(2)},
	)
	require.NoError(t, err)
	execs, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{modulo})
	require.NoError(t, err)
	retained, ok := colexec.ExpressionExecutorsRetainedBytes(execs)
	require.True(t, ok)
	peak, err := hashbuild.ExpressionVectorPeak(proc, modulo, 4, false)
	require.NoError(t, err)
	budgetCap := retained + peak - 1
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	probeLease, err := hashbuild.NewExpressionMemoryLease(
		generation, []*plan.Expr{modulo}, execs, false)
	require.NoError(t, err)
	engine := NewSpillEngine(SpillEngineConfig{
		ProbeKeyExprs:        []*plan.Expr{modulo},
		Budget:               generation,
		ProbeExpressionLease: probeLease,
	})
	engine.InitFromSpilledMap(make([]*os.File, SpillNumBuckets))
	input := makeInt32Batch(proc, []int32{1, 2, 3, 4})
	defer input.Clean(proc.Mp())
	childrenCalls := 0
	evalCalled := false
	err = engine.ScatterProbeTable(
		proc,
		func() (*batch.Batch, error) {
			childrenCalls++
			if childrenCalls == 1 {
				return input, nil
			}
			return nil, nil
		},
		process.NewAnalyzer(0, false, false, "test"),
		func(*batch.Batch) ([]*vector.Vector, error) {
			evalCalled = true
			return nil, nil
		},
	)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Equal(t, 1, childrenCalls)
	require.False(t, evalCalled)
	engine.Cleanup(proc)
	for _, exec := range execs {
		exec.Free()
	}
	probeLease.Release()
	require.Zero(t, generation.Used())
}

func makeInt32Batch(proc *process.Process, vals []int32) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.SetRowCount(len(vals))
	return bat
}

func makeInt32PayloadBatch(t *testing.T, proc *process.Process, start, rows, payloadBytes int) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(2)
	vals := make([]int32, rows)
	for i := range vals {
		vals[i] = int32(start + i)
	}
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	payload := bytes.Repeat([]byte{'x'}, payloadBytes)
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], payload, false, proc.Mp()))
	}
	bat.SetRowCount(rows)
	return bat
}

func makeDedupKeepLastSpillBatch(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeInt32Vector(
		[]int32{10, 20, 30}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeInt32Vector(
		[]int32{100, 0, 0}, []uint64{1, 2}, proc.Mp())
	bat.SetRowCount(3)
	return bat
}

func runtimeStackHasFunctionSuffix(suffix string) bool {
	var callers [32]uintptr
	n := runtime.Callers(2, callers[:])
	frames := runtime.CallersFrames(callers[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, suffix) {
			return true
		}
		if !more {
			return false
		}
	}
}

func writeBuildFile(proc *process.Process, name string, bat *batch.Batch) *os.File {
	return writeBuildRecords(proc, name, bat)
}

func writeBuildRecords(proc *process.Process, name string, batches ...*batch.Batch) *os.File {
	spillfs, _ := proc.GetSpillFileService()
	f, _ := spillfs.CreateAndRemoveFile(context.Background(), name)
	var buf bytes.Buffer
	w := BucketWriter{Name: name, Fd: f}
	for _, bat := range batches {
		FlushBucketBatch(proc, bat, &w, &buf, nil)
	}
	return w.HandOffFd()
}

func makeCorruptBatchFile(t *testing.T) *os.File {
	f, err := os.CreateTemp(t.TempDir(), "corrupt-spill")
	require.NoError(t, err)
	rowCount, batchSize := int64(1), int64(1)
	var buf bytes.Buffer
	buf.Write(types.EncodeInt64(&rowCount))
	buf.Write(types.EncodeInt64(&batchSize))
	buf.WriteByte(0xff)
	_, err = f.Write(buf.Bytes())
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return f
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
	engine.InitFromSpilledMap([]*os.File{fd1, nil, fd2})
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
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
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

func TestRebuildHashmapCancellationKeepsFileOwnedUntilCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)
	budget, err := process.NewHashBuildBudget(16<<20, 16<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	build := makeInt32Batch(proc, []int32{1, 2, 3, 4})
	var serialized bytes.Buffer
	writer := BucketWriter{Name: "rebuild_cancel", Budget: generation}
	defer writer.Close()
	require.NoError(t, FlushBucketBatch(proc, build, &writer, &serialized, nil))
	build.Clean(proc.Mp())
	file, err := writer.handOffSpillFile()
	require.NoError(t, err)
	require.Positive(t, generation.SpillDiskUsed())
	require.Positive(t, generation.SpillFDUsed())

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		Budget:                  generation,
	})
	engine.InitFromSpilledFiles([]*message.SpillFile{file})
	defer engine.Cleanup(proc)

	proc.Cancel(context.Canceled)
	jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, jm)
	require.Equal(t, BucketSkip, res)
	require.True(t, engine.HasMoreBuckets(), "cancellation must leave the queued file with the engine cleanup owner")
	require.Positive(t, generation.SpillDiskUsed())
	require.Positive(t, generation.SpillFDUsed())

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
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
				BuildKeyExprs:           makeTestKeyExpr(),
				NeedsBuildForEmptyProbe: true,
				NeedAllocateSels:        tt.needAllocateSels,
				NeedBatches:             tt.needBatches,
			})
			engine.InitFromSpilledMap([]*os.File{fd})

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

func TestRebuildHashmapWithoutBatchesDropsBatchBudgetBeforeProbe(t *testing.T) {
	run := func(t *testing.T, needBatches bool) uint64 {
		t.Helper()
		const budgetCap = uint64(64 << 20)
		budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
		require.NoError(t, err)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)

		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		defer proc.Free()
		values := make([]int32, colexec.DefaultBatchSize/2)
		for i := range values {
			values[i] = int32(i)
		}
		buildBat := makeInt32Batch(proc, values)
		buildFd := writeBuildFile(proc, "test_rebuild_batch_budget", buildBat)
		buildBat.Clean(proc.Mp())
		probeBat := makeInt32Batch(proc, []int32{1})
		probeFd := writeBuildFile(proc, "test_rebuild_batch_budget_probe", probeBat)
		probeBat.Clean(proc.Mp())

		engine := NewSpillEngine(SpillEngineConfig{
			BuildKeyExprs: makeTestKeyExpr(),
			NeedBatches:   needBatches,
			Budget:        generation,
		})
		engine.InitFromSpilledMap([]*os.File{buildFd})
		engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, 1, 0, nil)

		jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
		require.NoError(t, err)
		require.Equal(t, BucketReady, res)
		require.NotNil(t, jm)
		if needBatches {
			require.NotEmpty(t, jm.GetBatches())
		} else {
			require.Empty(t, jm.GetBatches())
		}
		used := generation.Used()
		require.Positive(t, used, "hash map and reader ownership remain live during probe")

		jm.Free()
		engine.Cleanup(proc)
		require.Zero(t, generation.Used())
		return used
	}

	withoutBatches := run(t, false)
	withBatches := run(t, true)
	require.Greater(t, withBatches, withoutBatches,
		"NeedBatches=false must not transfer destroyed batch reservations into the JoinMap")
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
	engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, 0, 0, nil)

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

func TestRebuildHashmapEmptyFile(t *testing.T) {
	for _, keepProbe := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep_probe_%t", keepProbe), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			budget, err := process.NewHashBuildBudget(16<<20, 16<<20)
			require.NoError(t, err)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)

			spillfs, err := proc.GetSpillFileService()
			require.NoError(t, err)
			buildFd, err := spillfs.CreateAndRemoveFile(proc.Ctx, "test_empty_build_file")
			require.NoError(t, err)
			probeBat := makeInt32Batch(proc, []int32{1})
			probeFd := writeBuildFile(proc, "test_empty_build_probe", probeBat)
			probeBat.Clean(proc.Mp())

			engine := NewSpillEngine(SpillEngineConfig{
				// A literal executor owns an mpool vector as soon as Prepare
				// succeeds. This makes the empty-build branch's builder.Free
				// observable instead of relying on a zero-allocation column
				// executor.
				BuildKeyExprs: []*plan.Expr{
					plan2.MakePlan2Int32ConstExprWithType(1),
				},
				NeedsProbeForEmptyBuild: keepProbe,
				Budget:                  generation,
			})
			engine.InitFromSpilledMap([]*os.File{buildFd})
			engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, 0, 0, nil)

			jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
			require.NoError(t, err)
			require.Nil(t, jm)
			if keepProbe {
				require.Equal(t, BucketEmptyBuild, res)
				require.True(t, engine.IsProbing())
			} else {
				require.Equal(t, BucketSkip, res)
			}
			engine.Cleanup(proc)
			require.Equal(t, baseline, proc.Mp().CurrNB())
			require.Zero(t, generation.Used())
			require.Zero(t, generation.SpillDiskUsed())
			require.Zero(t, generation.SpillFDUsed())
		})
	}
}

func TestScatterProbeTable(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := makeInt32Batch(proc, []int32{10, 20, 30})
	fd := writeBuildFile(proc, "test_sp_build", bat)
	bat.Clean(proc.Mp())

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
	defer batches[0].Clean(proc.Mp())
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
	require.Len(t, engine.buckets, 1)
	require.NotNil(t, engine.buckets[0].ProbeFd)
	require.Equal(t, int64(len(vals)), engine.buckets[0].ProbeRows,
		"probe partitioning must conserve every row at the build payload's fanout")

	engine.Cleanup(proc)
}

func TestScatterProbeTableRejectsInvalidBuildFanoutBeforeInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	for _, bucketCount := range []int{0, 3, SpillNumBuckets + 1} {
		t.Run(fmt.Sprintf("buckets_%d", bucketCount), func(t *testing.T) {
			engine := NewSpillEngine(SpillEngineConfig{})
			engine.InitFromSpilledMap(make([]*os.File, bucketCount))
			inputCalled := false
			err := engine.ScatterProbeTable(
				proc,
				func() (*batch.Batch, error) {
					inputCalled = true
					return nil, nil
				},
				process.NewAnalyzer(0, false, false, "test"),
				makeTestEvalKeysFn(),
			)
			require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
			require.False(t, inputCalled, "invalid fanout must fail before consuming probe input")
			engine.Cleanup(proc)
		})
	}
}

func TestScatterProbeCancellationReleasesPhysicalAndMemoryBudget(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)
	budget, err := process.NewHashBuildBudget(64<<20, 64<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	// Identical keys force one large selected payload through a real spill
	// writer before the second upstream call cancels the pipeline.
	const rows = 8192
	keys := make([]int32, rows)
	payload := make([]string, rows)
	for i := range keys {
		keys[i] = 7
		payload[i] = strings.Repeat("x", 128)
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeVarcharVector(payload, nil, proc.Mp())
	input.SetRowCount(rows)
	defer func() {
		if input != nil {
			input.Clean(proc.Mp())
		}
	}()

	engine := NewSpillEngine(SpillEngineConfig{
		ProbeKeyExprs:           makeTestKeyExpr(),
		NeedsProbeForEmptyBuild: true,
		Budget:                  generation,
	})
	engine.InitFromSpilledMap(make([]*os.File, SpillNumBuckets))
	analyzer := process.NewAnalyzer(0, false, false, "test")
	childrenCalls := 0
	var peakDisk, peakFD uint64
	err = engine.ScatterProbeTable(
		proc,
		func() (*batch.Batch, error) {
			childrenCalls++
			if childrenCalls == 1 {
				return input, nil
			}
			peakDisk = generation.SpillDiskUsed()
			peakFD = generation.SpillFDUsed()
			proc.Cancel(context.Canceled)
			return input, nil
		},
		analyzer,
		makeTestEvalKeysFn(),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, childrenCalls)
	require.Positive(t, peakDisk, "first batch must reach a physical spill file")
	require.Positive(t, peakFD, "first batch must own an admitted spill descriptor")

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	input.Clean(proc.Mp())
	input = nil
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSpillEntryPointsRejectPreCanceledProcessWithoutOwnershipTransfer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)
	proc.Cancel(context.Canceled)

	reuseBat := batch.NewOffHeapWithSize(0)
	reuseCleaned := false
	cleanReuse := func() {
		if !reuseCleaned {
			reuseBat.Clean(proc.Mp())
			reuseCleaned = true
		}
	}
	t.Cleanup(cleanReuse)
	reader := BucketReader{}
	got, err := reader.ReadBatch(proc, reuseBat)
	require.Nil(t, got)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, reuseBat.RowCount())

	writer := BucketWriter{Name: "must_not_be_created"}
	t.Cleanup(writer.Close)
	err = writeBucketPayload(
		proc,
		[]byte{1},
		1,
		&writer,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, writer.Created())
	require.Nil(t, writer.diskReservation)
	require.Nil(t, writer.fdReservation)

	input := makeInt32Batch(proc, []int32{1})
	inputCleaned := false
	cleanInput := func() {
		if !inputCleaned {
			input.Clean(proc.Mp())
			inputCleaned = true
		}
	}
	t.Cleanup(cleanInput)
	engine := NewSpillEngine(SpillEngineConfig{})
	engineCleaned := false
	cleanEngine := func() {
		if !engineCleaned {
			engine.Cleanup(proc)
			engineCleaned = true
		}
	}
	t.Cleanup(cleanEngine)
	scatterWriters := []BucketWriter{{Name: "must_not_be_created"}}
	t.Cleanup(scatterWriters[0].Close)
	err = engine.scatterBatchWithPressure(
		proc,
		input,
		[]*vector.Vector{input.Vecs[0]},
		scatterWriters,
		0,
		false,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, scatterWriters[0].Created())
	require.Nil(t, scatterWriters[0].diskReservation)
	require.Nil(t, scatterWriters[0].fdReservation)

	engine.InitFromSpilledMap([]*os.File{nil})
	childrenCalled := false
	err = engine.ScatterProbeTable(
		proc,
		func() (*batch.Batch, error) {
			childrenCalled = true
			return input, nil
		},
		process.NewAnalyzer(0, false, false, "test"),
		makeTestEvalKeysFn(),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, childrenCalled)

	probeFd, err := os.CreateTemp(t.TempDir(), "pre-canceled-probe")
	require.NoError(t, err)
	t.Cleanup(func() { _ = probeFd.Close() })
	probeReleases := 0
	probeFile := message.NewSpillFile(probeFd, 1, 1, func() { probeReleases++ })
	engine.probeReader.ResetForSpillFile(probeFile)
	got, err = engine.NextProbeBatch(proc)
	require.Nil(t, got)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, probeReleases)
	_, err = probeFd.Stat()
	require.NoError(t, err)

	reSpillFd, err := os.CreateTemp(t.TempDir(), "pre-canceled-respill")
	require.NoError(t, err)
	t.Cleanup(func() { _ = reSpillFd.Close() })
	reSpillReleases := 0
	reSpillFile := message.NewSpillFile(reSpillFd, 1, 1, func() { reSpillReleases++ })
	t.Cleanup(func() { _ = reSpillFile.Close() })
	subBuckets, err := engine.reSpillBucket(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
		SpillBucket{BuildFd: reSpillFile},
		nil,
		nil,
		nil,
	)
	require.Nil(t, subBuckets)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, reSpillReleases)
	_, err = reSpillFd.Stat()
	require.NoError(t, err)
	require.NoError(t, reSpillFile.Close())
	require.Equal(t, 1, reSpillReleases)
	_, err = reSpillFd.Stat()
	require.Error(t, err)

	cleanEngine()
	require.Equal(t, 1, probeReleases)
	_, err = probeFd.Stat()
	require.Error(t, err)
	cleanInput()
	cleanReuse()
	require.Zero(t, proc.Mp().CurrNB())
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
	engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, 0, 0, nil)

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

func TestCorruptSpillBatchErrors(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := NewSpillEngine(SpillEngineConfig{})
	engine.probeReader.ResetForFd(makeCorruptBatchFile(t))
	_, err := engine.NextProbeBatch(proc)
	require.Error(t, err)
	engine.Cleanup(proc)

	engine = NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
	})
	engine.InitFromSpilledMap([]*os.File{makeCorruptBatchFile(t)})
	jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
	require.Error(t, err)
	require.Nil(t, jm)
	require.Equal(t, BucketSkip, res)
	engine.Cleanup(proc)
}

func TestRebuildSkipsBuildOnlyBucketWhenJoinCannotUseIt(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	fd := makeCorruptBatchFile(t)
	releases := 0
	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledFiles([]*message.SpillFile{
		message.NewSpillFile(fd, 1, 17, func() { releases++ }),
	})

	jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err, "an irrelevant build-only file must not be read")
	require.Nil(t, jm)
	require.Equal(t, BucketSkip, res)
	require.False(t, engine.HasMoreBuckets())
	require.Equal(t, 1, releases)
	_, err = fd.Stat()
	require.Error(t, err)

	engine.Cleanup(proc)
	require.Equal(t, 1, releases)
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
	engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, 0, 0, nil)

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
	engine.FinishBucket()
	ok, err = engine.AdvanceToNextBucket(proc, analyzer, func(*message.JoinMap, BucketResult) {
		t.Fatal("queue exhaustion must not invoke callback")
	})
	require.NoError(t, err)
	require.False(t, ok)
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
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		SpillThreshold:          100,
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketReSpilled, res)
	require.Nil(t, jm)

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

func TestRebuildHashmapKeepsScratchHeadroomForCopyAdmissionReSpill(t *testing.T) {
	const (
		budgetCap   = uint64(12 << 20)
		recordRows  = colexec.DefaultBatchSize
		recordCount = 3
		payloadSize = 256
	)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	records := make([]*batch.Batch, recordCount)
	for i := range records {
		records[i] = makeInt32PayloadBatch(t, proc, i*recordRows, recordRows, payloadSize)
	}
	buildFd := writeBuildRecords(proc, "rebuild_scratch_headroom", records...)
	for i := range records {
		records[i].Clean(proc.Mp())
	}

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		SpillThreshold:          1 << 30,
		Budget:                  generation,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})
	analyzer := process.NewAnalyzer(0, false, false, "test")

	jm, result, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Nil(t, jm)
	require.Equal(t, BucketReSpilled, result)
	var childRows int64
	for _, child := range engine.buckets {
		childRows += child.BuildRows
	}
	require.Equal(t, int64(recordRows*recordCount), childRows)
	extra := analyzer.GetOpStats().ExtraStats
	require.Positive(t, extra["JoinSpillRebuildScratchFloorBytes"])
	require.Equal(t, int64(1), extra["JoinSpillRebuildCopyAdmissionReSpillAttempts"])
	require.Zero(t, extra["JoinSpillRebuildPreCopyReSpillAttempts"])

	// The rejected record is reader-owned pending state: retained, pending, and
	// unread rows must form an exact partition of the original stream.
	seen := make([]uint8, recordRows*recordCount)
	reuse := batch.NewOffHeapWithSize(0)
	reader := BucketReader{}
	for i := range engine.buckets {
		file := engine.buckets[i].BuildFd
		engine.buckets[i].BuildFd = nil
		reader.ResetForSpillFile(file)
		for {
			bat, readErr := reader.ReadBatch(proc, reuse)
			if readErr == io.EOF {
				break
			}
			require.NoError(t, readErr)
			for _, key := range vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0]) {
				require.GreaterOrEqual(t, key, int32(0))
				require.Less(t, key, int32(len(seen)))
				seen[key]++
			}
		}
		reader.closeCurrentFile()
	}
	reader.Close()
	reuse.Clean(proc.Mp())
	for key, count := range seen {
		require.Equalf(t, uint8(1), count, "key %d must be emitted exactly once", key)
	}

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
	generation.Close()
	proc.Free()
}

func TestRebuildScratchAdmissionIsBestEffortForResidentBucket(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	build := makeInt32PayloadBatch(t, proc, 0, 1024, 1024)
	buildFd := writeBuildFile(proc, "rebuild_scratch_best_effort", build)
	build.Clean(proc.Mp())

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		NeedBatches:             true,
		SpillThreshold:          1 << 30,
		Budget:                  generation,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})
	floorRejected := false
	budget.SetAggregateCapProvider(func() (uint64, error) {
		if !floorRejected && runtimeStackHasFunctionSuffix(
			"spillutil.(*SpillEngine).reserveRebuildScatterScratch",
		) {
			floorRejected = true
			return generation.Used(), nil
		}
		return budgetCap, nil
	})
	analyzer := process.NewAnalyzer(0, false, false, "test")

	jm, result, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.True(t, floorRejected)
	require.Equal(t, BucketReady, result)
	require.NotNil(t, jm)
	require.Equal(t, int64(1024), jm.GetRowCount())
	require.Equal(t, int64(1),
		analyzer.GetOpStats().ExtraStats["JoinSpillRebuildScratchReserveRejects"])
	jm.Free()

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
	generation.Close()
	proc.Free()
}

func TestRebuildScratchLifecycleFailureIsNotRecoveredAsAdmission(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	build := makeInt32Batch(proc, []int32{1, 2, 3})
	buildFd := writeBuildFile(proc, "rebuild_scratch_closed", build)
	build.Clean(proc.Mp())
	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		SpillThreshold:          1 << 30,
		Budget:                  generation,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})
	closedErr := &process.HashBuildBudgetError{
		Kind:    process.HashBuildBudgetErrorClosed,
		Message: "forced closed rebuild scratch budget",
	}
	budget.SetAggregateCapProvider(func() (uint64, error) {
		if runtimeStackHasFunctionSuffix(
			"spillutil.(*SpillEngine).reserveRebuildScatterScratch",
		) {
			return 0, closedErr
		}
		return budgetCap, nil
	})
	analyzer := process.NewAnalyzer(0, false, false, "test")

	jm, result, err := engine.RebuildHashmap(proc, analyzer)
	require.Same(t, closedErr, err)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetClosed)
	require.NotErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, jm)
	require.Equal(t, BucketSkip, result)
	require.Zero(t, analyzer.GetOpStats().ExtraStats["JoinSpillRebuildCopyAdmissionReSpillAttempts"])
	require.Zero(t, analyzer.GetOpStats().ExtraStats["JoinSpillRebuildPreCopyReSpillAttempts"])

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
	generation.Close()
	proc.Free()
}

func TestRebuildHashmapRejectsReSpillAfterDedupRewrite(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	build := makeDedupKeepLastSpillBatch(proc)
	buildFd := writeBuildFile(proc, "dedup_unsafe_respill", build)
	build.Clean(proc.Mp())

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:             makeTestKeyExpr(),
		NeedsBuildForEmptyProbe:   true,
		NeedBatches:               true,
		IsDedup:                   true,
		OnDuplicateAction:         plan.Node_FAIL,
		DedupBuildKeepLast:        true,
		DedupColName:              "id",
		DedupColTypes:             []plan.Type{{Id: int32(types.T_int32)}},
		DelColIdx:                 -1,
		DedupDeleteMarkerColIdx:   2,
		DedupDeleteKeepColIdxList: []int32{2},
		SpillThreshold:            1 << 30,
		Budget:                    generation,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})

	forcedUnsafeReject := false
	budget.SetAggregateCapProvider(func() (uint64, error) {
		// keepDiscardedRowsForDelete has already compacted the retained input
		// when it asks copyBuildBatch to admit the delete-only rows. Reject that
		// exact transition without depending on a fragile global call ordinal.
		if runtimeStackHasFunctionSuffix(
			"hashbuild.(*HashmapBuilder).keepDiscardedRowsForDelete",
		) {
			forcedUnsafeReject = true
			return max(uint64(1), generation.Used()), nil
		}
		return budgetCap, nil
	})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, result, err := engine.RebuildHashmap(proc, analyzer)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.True(t, forcedUnsafeReject)
	require.Nil(t, jm)
	require.Equal(t, BucketSkip, result)
	require.Len(t, engine.buckets, 1,
		"unsafe recovery must not replace the parent with child buckets")
	require.Equal(t, 1, engine.buckets[0].Depth)
	require.Nil(t, engine.buckets[0].BuildFd,
		"the consumed parent file must not be republished as a JoinMap or child")

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
	generation.Close()
	proc.Free()
}

func TestRebuildHashmapReSpillsAdmissionBeforeDedupRewrite(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	build := makeDedupKeepLastSpillBatch(proc)
	buildFd := writeBuildFile(proc, "dedup_safe_respill", build)
	build.Clean(proc.Mp())

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:             makeTestKeyExpr(),
		NeedsBuildForEmptyProbe:   true,
		NeedBatches:               true,
		IsDedup:                   true,
		OnDuplicateAction:         plan.Node_FAIL,
		DedupBuildKeepLast:        true,
		DedupColName:              "id",
		DedupColTypes:             []plan.Type{{Id: int32(types.T_int32)}},
		DelColIdx:                 -1,
		DedupDeleteMarkerColIdx:   2,
		DedupDeleteKeepColIdxList: []int32{2},
		SpillThreshold:            1 << 30,
		Budget:                    generation,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})

	forcedSafeReject := false
	budget.SetAggregateCapProvider(func() (uint64, error) {
		// The first budget request made from buildHashmap is reserveBuildAux,
		// before any Dedup batch rewrite. Reject once; re-spill itself does not
		// call buildHashmap and therefore retains the normal cap.
		if !forcedSafeReject &&
			runtimeStackHasFunctionSuffix(
				"hashbuild.(*HashmapBuilder).buildHashmap",
			) &&
			!runtimeStackHasFunctionSuffix(
				"hashbuild.(*HashmapBuilder).keepDiscardedRowsForDelete",
			) {
			forcedSafeReject = true
			return max(uint64(1), generation.Used()), nil
		}
		return budgetCap, nil
	})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, result, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.True(t, forcedSafeReject)
	require.Nil(t, jm)
	require.Equal(t, BucketReSpilled, result)
	require.NotEmpty(t, engine.buckets)
	var childRows int64
	for _, child := range engine.buckets {
		require.Equal(t, 2, child.Depth)
		childRows += child.BuildRows
	}
	require.Equal(t, int64(3), childRows,
		"safe recovery must conserve the original retained rows")
	require.Equal(t, int64(1),
		analyzer.GetOpStats().ExtraStats["JoinSpillRebuildMapAdmissionReSpillAttempts"])

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
	generation.Close()
	proc.Free()
}

func TestRebuildHashmapClosedBudgetDoesNotReSpill(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	build := makeInt32Batch(proc, []int32{1, 2, 3})
	buildFd := writeBuildFile(proc, "closed_budget_no_respill", build)
	build.Clean(proc.Mp())

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		SpillThreshold:          1 << 30,
		Budget:                  generation,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})

	closedErr := &process.HashBuildBudgetError{
		Kind:    process.HashBuildBudgetErrorClosed,
		Message: "forced closed hash-build budget",
	}
	forcedClosed := false
	budget.SetAggregateCapProvider(func() (uint64, error) {
		if !forcedClosed && runtimeStackHasFunctionSuffix(
			"hashbuild.(*HashmapBuilder).buildHashmap",
		) {
			forcedClosed = true
			return 0, closedErr
		}
		return budgetCap, nil
	})

	jm, result, err := engine.RebuildHashmap(
		proc, process.NewAnalyzer(0, false, false, "test"))
	require.True(t, forcedClosed)
	require.Same(t, closedErr, err,
		"a lifecycle failure must be returned unchanged")
	require.ErrorIs(t, err, process.ErrHashBuildBudgetClosed)
	require.NotErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, jm)
	require.Equal(t, BucketSkip, result)
	require.Len(t, engine.buckets, 1,
		"a lifecycle failure must not replace the parent with child buckets")
	require.Equal(t, 1, engine.buckets[0].Depth)
	require.Nil(t, engine.buckets[0].BuildFd)

	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
	generation.Close()
	proc.Free()
}

func TestReSpillReleasesBuilderExecutorsBeforeReplacementAdmission(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	col := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	modulo, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"%",
		[]*plan.Expr{col, plan2.MakePlan2Int32ConstExprWithType(2)},
	)
	require.NoError(t, err)
	exprs := []*plan.Expr{modulo}

	probeExecs, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, exprs)
	require.NoError(t, err)
	retained, ok := colexec.ExpressionExecutorsRetainedBytes(probeExecs)
	require.True(t, ok)
	require.Positive(t, retained)
	for _, executor := range probeExecs {
		executor.Free()
	}

	// The cap intentionally fits exactly one executor set. reSpillBucket must
	// release the failed builder's equivalent set before constructing its own.
	budget := process.MustNewHashBuildBudget(retained, retained)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	builder := &hashbuild.HashmapBuilder{}
	builder.SetBudget(generation)
	require.NoError(t, builder.Prepare(exprs, -1, -1, nil, proc))
	require.Equal(t, retained, generation.Used())

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: exprs,
		Budget:        generation,
	})
	subBuckets, err := engine.reSpillBucket(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
		SpillBucket{},
		builder,
		&BucketReader{},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, subBuckets)
	require.Equal(t, retained, generation.Used())
	require.NotNil(t, engine.buildExprLease)

	builder.Free(proc)
	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
}

func TestReSpillBucketReleasesDrainedBatchBudget(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	builder := &hashbuild.HashmapBuilder{}
	builder.SetBudget(generation)
	require.NoError(t, builder.Prepare(makeTestKeyExpr(), -1, -1, nil, proc))
	defer builder.Free(proc)

	values := make([]int32, colexec.DefaultBatchSize/2)
	for i := range values {
		values[i] = int32(i)
	}
	input := makeInt32Batch(proc, values)
	require.NoError(t, builder.CopyBuildBatch(input, proc))
	builder.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())
	batchCharge := generation.Used()
	require.Positive(t, batchCharge)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
		Budget:        generation,
	})
	subBuckets, err := engine.reSpillBucket(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
		SpillBucket{Depth: 1, BuildRows: int64(len(values))},
		builder,
		&BucketReader{},
		nil,
	)
	require.NoError(t, err)
	for i := range subBuckets {
		if subBuckets[i].BuildFd != nil {
			require.NoError(t, subBuckets[i].BuildFd.Close())
		}
		if subBuckets[i].ProbeFd != nil {
			require.NoError(t, subBuckets[i].ProbeFd.Close())
		}
	}
	engine.Cleanup(proc)

	require.Empty(t, builder.Batches.Buf)
	require.Less(t, generation.Used(), batchCharge,
		"re-spill must not retain the destroyed build-batch reservation")
	require.Zero(t, generation.Used())

	// Model the scratch/read admission that follows the drain. The full cap is
	// available only when re-spill released the stale batch ownership itself,
	// instead of relying on its caller to free the builder later.
	next, err := generation.Reserve(budgetCap)
	require.NoError(t, err)
	require.True(t, next.Release())
}

func TestReSpillConservesBuildAndProbeRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	vals := make([]int32, 5000)
	for i := range vals {
		vals[i] = int32(i)
	}
	build := makeInt32Batch(proc, vals)
	probe := makeInt32Batch(proc, vals)
	buildFd := writeBuildFile(proc, "test_conserve_build", build)
	probeFd := writeBuildFile(proc, "test_conserve_probe", probe)
	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		SpillThreshold:          500,
		NeedsBuildForEmptyProbe: true,
		NeedsProbeForEmptyBuild: true,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})
	engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, int64(len(vals)), 0, nil)
	engine.probeKeyEval = makeTestEvalKeysFn()

	jm, res, err := engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)
	require.Equal(t, BucketReSpilled, res)
	require.Nil(t, jm)
	var buildRows, probeRows, largest int64
	for _, child := range engine.buckets {
		buildRows += child.BuildRows
		probeRows += child.ProbeRows
		if child.BuildRows > largest {
			largest = child.BuildRows
		}
	}
	require.Equal(t, int64(len(vals)), buildRows)
	require.Equal(t, int64(len(vals)), probeRows)
	require.Less(t, largest, int64(len(vals)))

	for engine.HasMoreBuckets() {
		jm, _, err = engine.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
		require.NoError(t, err)
		if jm != nil {
			jm.Free()
		}
	}
	engine.Cleanup(proc)
}

func TestReSpillDepthLimit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(64<<10, 64<<10)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()

	bat := makeInt32Batch(proc, []int32{1, 2, 3, 4, 5})
	fd := writeBuildFile(proc, "test_depth_build", bat)

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		SpillThreshold:          1,
		Budget:                  generation,
	})
	engine.InitFromSpilledMap([]*os.File{fd})
	engine.buckets[0].Depth = SpillMaxPass

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.Error(t, err, "depth limit must not force an over-budget hashmap build")
	require.Equal(t, BucketSkip, res)
	require.Nil(t, jm)
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
		SpillThreshold: 100,
	})
	engine.InitFromSpilledMap([]*os.File{fd})
	engine.buckets[0].ProbeFd = message.NewSpillFile(probeFd, 0, 0, nil)

	// Set probeKeyEval so scatterProbe works during re-spill.
	engine.probeKeyEval = makeTestEvalKeysFn()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketReSpilled, res)
	require.Nil(t, jm)

	for engine.HasMoreBuckets() {
		jm2, _, err := engine.RebuildHashmap(proc, analyzer)
		if jm2 != nil {
			jm2.Free()
		}
		if err != nil {
			require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
			break
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
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
		SpillThreshold:          100,
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	analyzer := process.NewAnalyzer(0, false, false, "test")

	callbackCalled := false
	ok, err := engine.AdvanceToNextBucket(proc, analyzer, func(jm *message.JoinMap, _ BucketResult) {
		callbackCalled = true
		if jm != nil {
			jm.Free()
		}
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, callbackCalled, "re-spill is consumed before the callback")

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

func TestBuilderMemSizeIncludesCompletedBatchesAndPartialTail(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	builder := &hashbuild.HashmapBuilder{}
	fullValues := make([]int32, colexec.DefaultBatchSize)
	full := makeInt32Batch(proc, fullValues)
	partial := makeInt32Batch(proc, []int32{1, 2, 3})
	require.NoError(t, builder.Batches.CopyIntoBatches(full, proc))
	require.NoError(t, builder.Batches.CopyIntoBatches(partial, proc))
	require.Len(t, builder.Batches.Buf, 2)
	require.Equal(t, colexec.DefaultBatchSize, builder.Batches.Buf[0].RowCount())
	require.Equal(t, 3, builder.Batches.Buf[1].RowCount())

	want := builder.Batches.MemSize + int64(builder.Batches.Buf[1].Size())
	require.Equal(t, want, builderMemSize(builder))

	full.Clean(proc.Mp())
	partial.Clean(proc.Mp())
	builder.FreeHashMapAndBatches(proc)
	builder.Free(proc)
}

func TestShouldReSpillBeforeRetainUsesPredictedBytesAndRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	builder := &hashbuild.HashmapBuilder{}
	retained := makeInt32Batch(proc, []int32{1, 2, 3})
	next := makeInt32Batch(proc, []int32{4, 5})
	require.NoError(t, builder.Batches.CopyIntoBatches(retained, proc))
	builder.InputBatchRowCount = retained.RowCount()
	builder.Batches.MemSize = 200_000

	predictedBytes := builderMemSize(builder) + int64(next.Size())
	require.False(t, shouldReSpillBeforeRetain(builder, next, predictedBytes))
	require.True(t, shouldReSpillBeforeRetain(builder, next, predictedBytes-1))
	require.False(t, shouldReSpillBeforeRetain(builder, next, 6))
	require.True(t, shouldReSpillBeforeRetain(builder, next, 5))
	require.False(t, shouldReSpillBeforeRetain(builder, next, 0))

	retained.Clean(proc.Mp())
	next.Clean(proc.Mp())
	builder.FreeHashMapAndBatches(proc)
	builder.Free(proc)
}

func TestRebuildScratchFloorBoundsCoalescedPhysicalBatch(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	builder := &hashbuild.HashmapBuilder{}
	builder.SetBudget(generation)
	require.NoError(t, builder.Prepare(makeTestKeyExpr(), -1, -1, nil, proc))
	firstRows := make([]int32, colexec.DefaultBatchSize/2)
	first := makeInt32Batch(proc, firstRows)
	second := makeInt32Batch(proc, firstRows)
	require.NoError(t, builder.CopyBuildBatch(first, proc))
	builder.InputBatchRowCount = first.RowCount()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
		Budget:        generation,
	})
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, engine.reserveRebuildScatterScratch(builder, second, analyzer))
	require.Positive(t, engine.scatterScratchFloor)
	require.Equal(t, engine.scatterScratchFloor, engine.scatterScratchReservation.Size())
	require.NoError(t, builder.CopyBuildBatch(second, proc))
	builder.InputBatchRowCount += second.RowCount()
	require.Len(t, builder.Batches.Buf, 1, "two half records must coalesce")
	physical := builder.Batches.Buf[0]
	transient, err := scatterTransientBudgetBytes(physical, true)
	require.NoError(t, err)
	growth, ok := engine.scatterCapacityGrowthBytes(physical.RowCount(), 1)
	require.True(t, ok)
	require.GreaterOrEqual(t, engine.scatterScratchFloor, transient+growth)

	require.NoError(t, engine.reconcileScatterScratch())
	require.Equal(t, engine.scatterScratchFloor, engine.scatterScratchReservation.Size(),
		"repartition headroom must survive per-batch reconciliation")
	extra := analyzer.GetOpStats().ExtraStats
	require.Equal(t, int64(1), extra["JoinSpillRebuildScratchReserveCount"])
	require.Equal(t, spillStatInt64(engine.scatterScratchFloor), extra["JoinSpillRebuildScratchFloorBytes"])

	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	builder.FreeHashMapAndBatches(proc)
	builder.Free(proc)
	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
}

func TestFinishBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := NewSpillEngine(SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{nil})

	fd, err := os.CreateTemp(t.TempDir(), "probe")
	require.NoError(t, err)
	engine.probeReader.ResetForFd(fd)
	require.True(t, engine.IsProbing())

	engine.FinishBucket()
	require.False(t, engine.IsProbing())

	engine.FinishBucket()
	require.False(t, engine.IsProbing())

	engine.Cleanup(proc)
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
	engine.probeReader.ResetForFd(fd2)

	engine.buildReadBatch = batch.NewOffHeapWithSize(0)
	engine.probeReadBatch = batch.NewOffHeapWithSize(0)

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
	bat := makeInt32Batch(proc, []int32{5, 15, 25})

	err := scatterProbe(proc, engine, bat, writers, 1, nil)
	require.NoError(t, err)
	require.True(t, evalCalled, "probeKeyEval must be used for scatterProbe")

	wantErr := errors.New("probe key evaluation failed")
	engine.probeKeyEval = func(*batch.Batch) ([]*vector.Vector, error) { return nil, wantErr }
	require.ErrorIs(t, scatterProbe(proc, engine, bat, writers, 1, nil), wantErr)

	for i := range writers {
		writers[i].Close()
	}
}

func TestScatterPeakDoesNotDoubleChargeReservedSource(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]int32, 8192)
	for i := range values {
		values[i] = int32(i)
	}
	bat := makeInt32Batch(proc, values)
	defer bat.Clean(proc.Mp())

	charged, err := scatterTransientBudgetBytes(bat, true)
	require.NoError(t, err)
	uncharged, err := scatterTransientBudgetBytes(bat, false)
	require.NoError(t, err)
	source := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > source {
		source = size
	}
	require.Equal(t, source, uncharged-charged)

	emptyEngine := NewSpillEngine(SpillEngineConfig{})
	retained, ok := emptyEngine.scatterRetainedBytes()
	require.True(t, ok)
	growth, ok := emptyEngine.scatterCapacityGrowthBytes(bat.RowCount(), 1)
	require.True(t, ok)
	marshalSize, err := bat.MarshalBinarySize()
	require.NoError(t, err)
	capacity := source + retained + growth + charged + uint64(marshalSize+24)
	budget, err := process.NewHashBuildBudget(capacity, capacity)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(capacity)
	require.NoError(t, err)
	defer generation.Close()
	sourceReservation, err := generation.Reserve(source)
	require.NoError(t, err)
	defer sourceReservation.Release()

	engine := NewSpillEngine(SpillEngineConfig{Budget: generation})
	writers := MakeBucketWriters("test_scatter_charged_source")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
		engine.Cleanup(proc)
	}()
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, engine.scatterBatchBounded(
		proc, bat, []*vector.Vector{bat.Vecs[0]}, writers, 0, true, analyzer,
	))
}

func TestScatterCapacityGrowthChargesCompleteReplacement(t *testing.T) {
	engine := NewSpillEngine(SpillEngineConfig{})
	engine.scatterHashValues = make([]uint64, 8)
	engine.scatterBucketRowIds = make([]int32, 8)
	engine.keyVecs = make([]*vector.Vector, 1)
	growth, ok := engine.scatterCapacityGrowthBytes(9, 2)
	require.True(t, ok)
	require.Equal(t, uint64(9*8+9*4+2*8), growth)
}

func TestScatterScratchLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(64<<20, 64<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(64 << 20)
	require.NoError(t, err)
	defer generation.Close()

	engine := NewSpillEngine(SpillEngineConfig{Budget: generation})
	writers := MakeBucketWriters("test_scatter_scratch")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	values := make([]int32, 8192)
	for i := range values {
		values[i] = int32(i)
	}
	bat := makeInt32Batch(proc, values)
	defer bat.Clean(proc.Mp())
	analyzer := process.NewAnalyzer(0, false, false, "test")
	keys := []*vector.Vector{bat.Vecs[0]}
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	require.NotNil(t, engine.scatterScratchReservation)
	firstHashCap := cap(engine.scatterHashValues)
	firstRowIDCap := cap(engine.scatterBucketRowIds)
	require.Equal(t, len(values), firstHashCap)
	require.Equal(t, len(values), firstRowIDCap)
	firstHash := &engine.scatterHashValues[0]
	firstRowID := &engine.scatterBucketRowIds[0]
	retained := generation.Used()
	firstReserveCount := generation.ReserveCount()
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	require.Greater(t, generation.ReserveCount(), firstReserveCount, "each batch peak must be admitted above retained scratch")
	require.Equal(t, retained, generation.Used(), "batch peak must reconcile to retained scratch")
	require.Equal(t, firstHashCap, cap(engine.scatterHashValues))
	require.Equal(t, firstRowIDCap, cap(engine.scatterBucketRowIds))
	require.Equal(t, firstHash, &engine.scatterHashValues[0])
	require.Equal(t, firstRowID, &engine.scatterBucketRowIds[0])

	for i := range writers {
		writers[i].Close()
	}
	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	for i := range engine.scatterWriteBuffers {
		require.Zero(t, engine.scatterWriteBuffers[i].Cap())
	}
	// Cleanup is an idempotent terminal release point.
	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
}

func TestScatterScratchRejectsPeakAboveRetainedBudget(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(1<<20, 1<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1 << 20)
	require.NoError(t, err)
	defer generation.Close()
	engine := NewSpillEngine(SpillEngineConfig{Budget: generation})
	writers := MakeBucketWriters("test_scatter_peak_reject")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	values := make([]int32, 8192)
	for i := range values {
		values[i] = int32(i)
	}
	bat := makeInt32Batch(proc, values)
	defer bat.Clean(proc.Mp())
	keys := []*vector.Vector{bat.Vecs[0]}
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	rowCap := cap(engine.scatterBucketRowIds)
	used := generation.Used()
	require.Positive(t, used)
	largerValues := make([]int32, len(values)*2)
	for i := range largerValues {
		largerValues[i] = int32(i)
	}
	larger := makeInt32Batch(proc, largerValues)
	defer larger.Clean(proc.Mp())
	err = engine.scatterBatchBounded(
		proc, larger, []*vector.Vector{larger.Vecs[0]}, writers, 0, false, analyzer,
	)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Equal(t, rowCap, cap(engine.scatterBucketRowIds), "rejection must precede new scratch allocation")
	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
}

func TestScatterPhaseReleasesScratchKeepsSpillOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(64<<20, 64<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(64 << 20)
	require.NoError(t, err)
	defer generation.Close()
	buildBat := makeInt32Batch(proc, []int32{1})
	buildFd := writeBuildFile(proc, "test_phase_build", buildBat)
	defer buildBat.Clean(proc.Mp())
	engine := NewSpillEngine(SpillEngineConfig{
		ProbeKeyExprs:           makeTestKeyExpr(),
		NeedsProbeForEmptyBuild: true,
		Budget:                  generation,
	})
	buildFds := make([]*os.File, SpillNumBuckets)
	buildFds[0] = buildFd
	engine.InitFromSpilledMap(buildFds)
	probeBat := makeInt32Batch(proc, []int32{2, 2, 2, 2})
	defer probeBat.Clean(proc.Mp())
	childrenDone := false
	analyzer := process.NewAnalyzer(0, false, false, "test")
	err = engine.ScatterProbeTable(proc, func() (*batch.Batch, error) {
		if childrenDone {
			return nil, nil
		}
		childrenDone = true
		return probeBat, nil
	}, analyzer, func(bat *batch.Batch) ([]*vector.Vector, error) {
		return []*vector.Vector{bat.Vecs[0]}, nil
	})
	require.NoError(t, err)
	require.Zero(t, generation.Used(), "scatter memory scratch must end with the phase")
	require.Nil(t, engine.scatterScratchReservation)
	for i := range engine.scatterWriteBuffers {
		require.Zero(t, engine.scatterWriteBuffers[i].Cap())
	}
	require.Positive(t, generation.SpillDiskUsed(), "handed-off probe file keeps disk accounting")
	require.Positive(t, generation.SpillFDUsed(), "handed-off probe file keeps FD accounting")
	engine.Cleanup(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
}

func TestScatterCoalescesAcrossBatchesUntilFlush(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	engine := NewSpillEngine(SpillEngineConfig{})
	writers := MakeBucketWriters("test_scatter_coalesce")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	bat := makeInt32Batch(proc, []int32{1, 1, 1})
	defer bat.Clean(proc.Mp())
	keys := []*vector.Vector{bat.Vecs[0]}
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	var pending int
	for i := range engine.scatterWriteBuffers {
		pending += engine.scatterWriteBuffers[i].Len()
	}
	require.Positive(t, pending)
	for i := range writers {
		require.Zero(t, writers[i].Rows)
	}
	require.NoError(t, engine.flushScatterBuffers(proc, writers, analyzer))
	for i := range engine.scatterWriteBuffers {
		require.Zero(t, engine.scatterWriteBuffers[i].Len())
	}
	var rows int64
	for i := range writers {
		rows += writers[i].Rows
	}
	require.Equal(t, int64(6), rows)
}

func TestScatterCoalescedRecordRoundTrip(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	engine := NewSpillEngine(SpillEngineConfig{})
	writers := MakeBucketWriters("test_scatter_coalesce_roundtrip")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	bat := makeInt32Batch(proc, []int32{7, 7, 7})
	defer bat.Clean(proc.Mp())
	keys := []*vector.Vector{bat.Vecs[0]}
	analyzer := process.NewAnalyzer(0, false, false, "test")
	for i := 0; i < 3; i++ {
		require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	}
	require.NoError(t, engine.flushScatterBuffers(proc, writers, analyzer))
	var target *BucketWriter
	for i := range writers {
		if writers[i].Rows > 0 {
			target = &writers[i]
			break
		}
	}
	require.NotNil(t, target)
	require.Equal(t, int64(9), target.Rows)
	_, err := target.Fd.Seek(0, io.SeekStart)
	require.NoError(t, err)
	reader := BucketReader{fd: target.Fd}
	reuse := batch.NewOffHeapWithSize(0)
	got, err := reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())
	got, err = reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())
	got, err = reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())
	_, err = reader.ReadBatch(proc, reuse)
	require.ErrorIs(t, err, io.EOF)
	reader.Close()
}

func TestScatterCoalesceFlushErrorClearsPending(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	engine := NewSpillEngine(SpillEngineConfig{})
	writers := MakeBucketWriters("test_scatter_coalesce_error")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	bat := makeInt32Batch(proc, []int32{11, 11, 11})
	defer bat.Clean(proc.Mp())
	keys := []*vector.Vector{bat.Vecs[0]}
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	require.NoError(t, engine.flushScatterBuffers(proc, writers, analyzer))
	var target *BucketWriter
	for i := range writers {
		if writers[i].Rows > 0 {
			target = &writers[i]
			break
		}
	}
	require.NotNil(t, target)
	require.NoError(t, engine.scatterBatchBounded(proc, bat, keys, writers, 0, false, analyzer))
	require.Positive(t, engine.scatterWriteBuffers[targetIndex(writers, target)].Len())
	require.NoError(t, target.Fd.Close())
	require.Error(t, engine.flushScatterBuffers(proc, writers, analyzer))
	for i := range engine.scatterWriteBuffers {
		require.Zero(t, engine.scatterWriteBuffers[i].Len())
	}
}

func targetIndex(writers []BucketWriter, target *BucketWriter) int {
	for i := range writers {
		if &writers[i] == target {
			return i
		}
	}
	return -1
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
		BuildKeyExprs:           badExpr,
		NeedsBuildForEmptyProbe: true,
	})
	engine.InitFromSpilledMap([]*os.File{fd})

	callbackCalled := false
	ok, err := engine.AdvanceToNextBucket(proc, process.NewAnalyzer(0, false, false, "test"),
		func(*message.JoinMap, BucketResult) { callbackCalled = true })
	require.Error(t, err)
	require.False(t, ok)
	require.False(t, callbackCalled)

	engine.Cleanup(proc)
}
