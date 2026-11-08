// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package materialized

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type testBudgetKind int

const (
	testBudgetMemory testBudgetKind = iota
	testBudgetDisk
	testBudgetFD
)

type testSpillBudget struct {
	mu sync.Mutex

	cap  [3]uint64
	used [3]uint64
}

type testSpillReservation struct {
	budget   *testSpillBudget
	kind     testBudgetKind
	size     uint64
	released bool
}

func newTestSpillBudget(memoryCap, diskCap, fdCap uint64) *testSpillBudget {
	return &testSpillBudget{cap: [3]uint64{memoryCap, diskCap, fdCap}}
}

func (b *testSpillBudget) config(factory SpillFileFactory) SpillConfig {
	return SpillConfig{FileFactory: factory, Budget: SpillBudget{
		ReserveMemory: func(size uint64) (Reservation, error) {
			return b.reserve(testBudgetMemory, size)
		},
		ReserveDisk: func(size uint64) (GrowingReservation, error) {
			return b.reserve(testBudgetDisk, size)
		},
		ReserveFD: func(size uint64) (Reservation, error) {
			return b.reserve(testBudgetFD, size)
		},
	}}
}

func (b *testSpillBudget) reserve(kind testBudgetKind, size uint64) (*testSpillReservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used[kind] > b.cap[kind] || size > b.cap[kind]-b.used[kind] {
		return nil, errors.New("test spill budget exceeded")
	}
	b.used[kind] += size
	return &testSpillReservation{budget: b, kind: kind, size: size}, nil
}

func (r *testSpillReservation) Grow(size uint64) error {
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if r.released {
		return errors.New("test spill reservation released")
	}
	if r.budget.used[r.kind] > r.budget.cap[r.kind] || size > r.budget.cap[r.kind]-r.budget.used[r.kind] {
		return errors.New("test spill budget exceeded")
	}
	r.budget.used[r.kind] += size
	r.size += size
	return nil
}

func (r *testSpillReservation) Release() bool {
	if r == nil || r.budget == nil {
		return false
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	if r.released {
		return false
	}
	r.released = true
	r.budget.used[r.kind] -= r.size
	return true
}

func (b *testSpillBudget) usage() (memory, disk, fd uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used[testBudgetMemory], b.used[testBudgetDisk], b.used[testBudgetFD]
}

func (b *testSpillBudget) setCap(kind testBudgetKind, value uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cap[kind] = value
}

func testSpillFactory(dir string) SpillFileFactory {
	return func(_ context.Context, name string) (*os.File, error) {
		file, err := os.CreateTemp(dir, name)
		if err == nil {
			err = os.Remove(file.Name())
		}
		return file, err
	}
}

func testInt64Batch(t *testing.T, mp *mpool.MPool, value int64) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
	bat.SetRowCount(1)
	return bat
}

func TestSharedMaterializedSourceAllowsDependentReaders(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(2)
	require.NoError(t, source.Begin(mp))

	for i := int64(0); i < 4; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], i, false, mp))
		bat.SetRowCount(1)
		require.NoError(t, source.Append(t.Context(), bat))
		bat.Clean(mp)
	}
	source.Finish(nil)

	// Reader 1 can consume the complete producer before reader 0 starts.
	for i := 0; i < 4; i++ {
		bat, reservation, end, err := source.Next(context.Background(), 1, i)
		require.Nil(t, reservation)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
		bat.Clean(mp)
	}
	_, _, end, err := source.Next(context.Background(), 1, 4)
	require.NoError(t, err)
	require.True(t, end)

	for i := 0; i < 4; i++ {
		bat, reservation, end, err := source.Next(context.Background(), 0, i)
		require.Nil(t, reservation)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
		bat.Clean(mp)
	}

	source.ReleaseReader(1)
	require.Len(t, source.batches, 4)
	source.ReleaseReader(0)
	require.Empty(t, source.batches)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSharedMaterializedSourceCancellationAndReuse(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(1)
	require.NoError(t, source.Begin(mp))

	ctx, cancel := context.WithCancelCause(context.Background())
	want := context.DeadlineExceeded
	cancel(want)
	_, _, end, err := source.Next(ctx, 0, 0)
	require.True(t, end)
	require.ErrorIs(t, err, want)

	source.ReleaseReader(0)
	source.Finish(want)
	require.Equal(t, int64(0), mp.CurrNB())
	require.NoError(t, source.Begin(mp))
	source.Finish(nil)
	source.ReleaseReader(0)
}

func TestSharedMaterializedSourceCancellationWhileWaiting(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(1)
	require.NoError(t, source.Begin(mp))

	ctx, cancel := context.WithCancelCause(context.Background())
	want := moerr.NewInternalErrorNoCtx("reader canceled")
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, _, end, err := source.Next(ctx, 0, 0)
		if !end || !errors.Is(err, want) {
			result <- moerr.NewInternalErrorNoCtxf("unexpected wait result: end=%t err=%v", end, err)
			return
		}
		result <- nil
	}()
	<-started
	cancel(want)
	require.NoError(t, <-result)

	source.Finish(nil)
	source.ReleaseReader(0)
	require.Zero(t, mp.CurrNB())
}

func TestSharedMaterializedSourceCompletedStateWinsOverCanceledContext(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(1)
	require.NoError(t, source.Begin(mp))
	source.Finish(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, end, err := source.Next(ctx, 0, 0)
	require.True(t, end)
	require.NoError(t, err)
	source.ReleaseReader(0)
	require.Zero(t, mp.CurrNB())
}

func TestSharedMaterializedSourcePublishesProducerErrorAfterBufferedData(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(1)
	require.NoError(t, source.Begin(mp))

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	require.NoError(t, source.Append(t.Context(), bat))
	bat.Clean(mp)
	want := moerr.NewInternalErrorNoCtx("producer failed")
	source.Finish(want)

	got, reservation, end, err := source.Next(context.Background(), 0, 0)
	require.Nil(t, reservation)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	got.Clean(mp)
	_, _, end, err = source.Next(context.Background(), 0, 1)
	require.True(t, end)
	require.ErrorIs(t, err, want)
	source.ReleaseReader(0)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSharedMaterializedSourceWaitsForAllPreparedInputs(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("producer_error_%t", failed), func(t *testing.T) {
			mp := mpool.MustNewZero()
			sources := []*Source{NewSource(1), NewSource(1)}
			for _, source := range sources {
				defer source.Close()
				source.RequireCompletionOf(sources)
				require.NoError(t, source.Begin(mp))
			}
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			defer input.Clean(mp)
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, mp))
			input.SetRowCount(1)
			for _, source := range sources {
				require.NoError(t, source.Append(t.Context(), input))
			}
			sources[0].Finish(nil)
			ctx, cancel := context.WithCancelCause(context.Background())
			stopped := moerr.NewInternalErrorNoCtx("stop before other input finished")
			cancel(stopped)
			out, reservation, end, err := sources[0].Next(ctx, 0, 0)
			require.Nil(t, reservation)
			require.ErrorIs(t, err, stopped)
			require.Nil(t, out)
			require.True(t, end)
			if failed {
				cause := moerr.NewInternalErrorNoCtx("old index read failed")
				sources[1].Finish(cause)
				out, reservation, end, err = sources[0].Next(context.Background(), 0, 0)
				require.Nil(t, reservation)
				require.ErrorIs(t, err, cause)
				require.Nil(t, out)
				require.True(t, end)
				return
			}
			sources[1].Finish(nil)
			for _, source := range sources {
				out, reservation, end, err = source.Next(context.Background(), 0, 0)
				require.Nil(t, reservation)
				require.NoError(t, err)
				require.False(t, end)
				require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0))
				out.Clean(mp)
				// Finishing one reader frees its source's storage; that successful
				// producer result must remain valid for the remaining writer.
				source.ReleaseReader(0)
			}
		})
	}
}

func TestSharedMaterializedSourceUnderestimatedFixedWidthProducerSpills(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.SetRowCount(1)
	inputMemory := mp.CurrNB()

	reserved := int64(max(bat.Size(), bat.Allocated()))
	// A stale estimate selected materialization for one row, but the fixed-width
	// producer actually emits two. Crossing the runtime memory budget must spill
	// instead of turning the optimizer choice into a new query error.
	source := newSource(2, reserved)
	defer source.Close()
	defer bat.Clean(mp)
	spillDir := t.TempDir()
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(spillDir))))
	require.NoError(t, source.Append(t.Context(), bat), "the exact in-memory boundary is allowed")
	require.Equal(t, reserved, source.CurrentBytes())
	require.NoError(t, source.Append(t.Context(), bat), "actual rows above the estimate must spill")
	require.Positive(t, source.spillBytes)
	source.Finish(nil)

	for readerID := 0; readerID < 2; readerID++ {
		for position := 0; position < 2; position++ {
			readCtx := context.Background()
			if readerID == 0 && position == 1 {
				// Published spill data has the same data-before-cancellation
				// ordering as an in-memory batch.
				canceled, cancel := context.WithCancel(context.Background())
				cancel()
				readCtx = canceled
			}
			got, reservation, end, readerErr := source.Next(readCtx, readerID, position)
			require.NoError(t, readerErr)
			require.False(t, end)
			require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
			live, _, _ := budget.usage()
			got.Clean(mp)
			if reservation != nil {
				reservation.Release()
			}
			if position == 1 {
				require.Positive(t, live, "a returned spill frame remains charged while its vectors borrow it")
			}
		}
		_, _, end, readerErr := source.Next(context.Background(), readerID, 2)
		require.NoError(t, readerErr)
		require.True(t, end)
		source.ReleaseReader(readerID)
	}
	require.Equal(t, inputMemory, mp.CurrNB())
	require.Nil(t, source.spillFile)
	require.Equal(t, [3]uint64{}, func() [3]uint64 {
		memory, disk, fd := budget.usage()
		return [3]uint64{memory, disk, fd}
	}())
	bat.Clean(mp)
}

func TestSharedMaterializedSourceBoundsTinyBatchMetadataWithSpill(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.SetRowCount(1)
	inputMemory := mp.CurrNB()

	source := newSource(1, sharedMaterializedSourceMaxBytes)
	source.memoryBatchLimit = 1
	spillDir := t.TempDir()
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(spillDir))))
	require.NoError(t, source.Append(t.Context(), bat))
	require.Len(t, source.batches, 1)
	require.NoError(t, source.Append(t.Context(), bat), "Go batch metadata must stay bounded by spilling later tiny batches")
	require.Len(t, source.batches, 1)
	require.Equal(t, 1, source.spillBatchCount)
	source.Finish(nil)

	for position := 0; position < 2; position++ {
		got, reservation, end, err := source.Next(context.Background(), 0, position)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
		got.Clean(mp)
		if reservation != nil {
			reservation.Release()
		}
	}
	_, _, end, err := source.Next(context.Background(), 0, 2)
	require.NoError(t, err)
	require.True(t, end)
	source.ReleaseReader(0)
	require.Equal(t, inputMemory, mp.CurrNB())
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
	bat.Clean(mp)
}

func TestSharedMaterializedSourceSpillBudgetExactBoundaryAndCleanup(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 7)
	t.Cleanup(func() { bat.Clean(mp) })
	encodedSize, err := bat.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	readBytes := testSpillReadBytes(t, bat)
	recordBytes := uint64(spillBatchHeaderSize) + serialized

	budget := newTestSpillBudget(readBytes, recordBytes, 1)
	source := newSource(1, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(t.Context(), bat))
	memory, disk, fd := budget.usage()
	require.Zero(t, memory, "marshal serialized must be released after Append")
	require.Equal(t, recordBytes, disk)
	require.Equal(t, uint64(1), fd)

	source.Finish(nil)
	got, reservation, end, err := source.Next(context.Background(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	got.Clean(mp)
	if reservation != nil {
		reservation.Release()
	}
	source.ReleaseReader(0)
	memory, disk, fd = budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)

	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(t.Context(), bat), "a new generation must not inherit released spill charges")
	source.Finish(nil)
	source.ReleaseReader(0)
	memory, disk, fd = budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
}

func TestSharedMaterializedSourceRejectsSpillBeforeFileMutation(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 9)
	t.Cleanup(func() { bat.Clean(mp) })
	encodedSize, err := bat.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized

	tests := []struct {
		name    string
		diskCap uint64
		fdCap   uint64
	}{
		{name: "disk", diskCap: recordBytes - 1, fdCap: 1},
		{name: "file descriptor", diskCap: recordBytes, fdCap: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			budget := newTestSpillBudget(serialized, tc.diskCap, tc.fdCap)
			created := 0
			source := newSource(1, 0)
			require.NoError(t, source.Begin(mp, budget.config(func(context.Context, string) (*os.File, error) {
				created++
				return nil, errors.New("file factory must not run")
			})))
			appendErr := source.Append(t.Context(), bat)
			require.Error(t, appendErr)
			require.Zero(t, created)
			memory, disk, fd := budget.usage()
			require.Zero(t, memory)
			require.Zero(t, disk)
			require.Zero(t, fd)
			source.Finish(appendErr)
			source.ReleaseReader(0)
		})
	}
}

func TestSharedMaterializedSourcesShareOneSpillBudget(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 11)
	t.Cleanup(func() { bat.Clean(mp) })
	encodedSize, err := bat.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized
	budget := newTestSpillBudget(serialized, recordBytes, 2)
	config := budget.config(testSpillFactory(t.TempDir()))

	first := newSource(1, 0)
	require.NoError(t, first.Begin(mp, config))
	require.NoError(t, first.Append(t.Context(), bat))
	second := newSource(1, 0)
	require.NoError(t, second.Begin(mp, config))
	secondErr := second.Append(t.Context(), bat)
	require.Error(t, secondErr, "the first source must retain the statement spill charge")
	second.Finish(secondErr)
	second.ReleaseReader(0)

	first.Finish(nil)
	first.ReleaseReader(0)
	_, disk, _ := budget.usage()
	require.Zero(t, disk)

	third := newSource(1, 0)
	require.NoError(t, third.Begin(mp, config))
	require.NoError(t, third.Append(t.Context(), bat), "released spill capacity must be reusable")
	third.Finish(nil)
	third.ReleaseReader(0)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
}

func TestSharedMaterializedSourceBoundsTotalSpillGrowth(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 12)
	t.Cleanup(func() { bat.Clean(mp) })
	encodedSize, err := bat.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized
	budget := newTestSpillBudget(serialized, 2*recordBytes, 1)
	source := newSource(1, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(t.Context(), bat))
	require.NoError(t, source.Append(t.Context(), bat))
	require.Equal(t, int64(2*recordBytes), source.spillBytes)
	info, err := source.spillFile.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(2*recordBytes), info.Size())

	appendErr := source.Append(t.Context(), bat)
	require.Error(t, appendErr)
	info, err = source.spillFile.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(2*recordBytes), info.Size(), "rejected spill must not mutate the file")
	_, disk, _ := budget.usage()
	require.Equal(t, 2*recordBytes, disk)

	source.Finish(appendErr)
	source.ReleaseReader(0)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
}

func TestSharedMaterializedSourceSpillReadUsesMemoryBudget(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 13)
	t.Cleanup(func() { bat.Clean(mp) })
	bat.Attrs, bat.ExtraBuf = []string{strings.Repeat("attribute", 32)}, []byte(strings.Repeat("metadata", 64))
	encodedSize, err := bat.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	readBytes := testSpillReadBytes(t, bat)
	recordBytes := uint64(spillBatchHeaderSize) + serialized
	budget := newTestSpillBudget(serialized, recordBytes, 1)
	source := newSource(1, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(t.Context(), bat))
	source.Finish(nil)

	budget.setCap(testBudgetMemory, serialized-1)
	_, _, end, readErr := source.Next(context.Background(), 0, 0)
	require.True(t, end)
	require.Error(t, readErr)
	memory, _, _ := budget.usage()
	require.Zero(t, memory)

	budget.setCap(testBudgetMemory, readBytes-1)
	out, charge, _, readErr := source.Next(context.Background(), 0, 0)
	require.Error(t, readErr, "metadata must be admitted before it is decoded")
	require.Nil(t, out)
	require.Nil(t, charge)
	memory, _, _ = budget.usage()
	require.Zero(t, memory, "metadata denial releases the frame too")
	budget.setCap(testBudgetMemory, readBytes)
	got, reservation, end, err := source.Next(context.Background(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, bat.Attrs, got.Attrs)
	require.Equal(t, bat.ExtraBuf, got.ExtraBuf)
	got.Clean(mp)
	if reservation != nil {
		reservation.Release()
	}
	source.ReleaseReader(0)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
}

func TestSharedMaterializedSourceSpillFactoryFailureReleasesBudget(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 15)
	t.Cleanup(func() { bat.Clean(mp) })
	encodedSize, err := bat.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	budget := newTestSpillBudget(serialized, uint64(spillBatchHeaderSize)+serialized, 1)
	source := newSource(1, 0)
	want := errors.New("spill file unavailable")
	require.NoError(t, source.Begin(mp, budget.config(func(context.Context, string) (*os.File, error) {
		return nil, want
	})))
	appendErr := source.Append(t.Context(), bat)
	require.ErrorIs(t, appendErr, want)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
	source.Finish(appendErr)
	source.ReleaseReader(0)
}

func TestReadSpilledBatchValidatesExtentAndAdmissionBeforeAllocation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "wide-spill")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	const frameSize = uint64(64*mpool.MB + 1)
	var header [spillBatchHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], frameSize)
	_, err = file.Write(header[:])
	require.NoError(t, err)
	reserveCalls := 0
	budget := SpillBudget{ReserveMemory: func(size uint64) (Reservation, error) {
		reserveCalls++
		require.Equal(t, frameSize, size)
		return nil, mpool.ErrAllocationAccountCapacity
	}}
	_, _, _, err = readSpilledBatch(file, 0, int64(frameSize)+spillBatchHeaderSize, nil, budget)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Zero(t, reserveCalls, "the header cannot attest the actual file extent")
	// A sparse file proves a real wide frame reaches admission, without
	// allocating or writing a giant routine CI payload.
	require.NoError(t, file.Truncate(int64(frameSize)+spillBatchHeaderSize))
	_, _, _, err = readSpilledBatch(file, 0, int64(frameSize)+spillBatchHeaderSize, nil, budget)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, 1, reserveCalls)
	binary.LittleEndian.PutUint64(header[:], math.MaxUint64)
	_, err = file.WriteAt(header[:], 0)
	require.NoError(t, err)
	_, _, _, err = readSpilledBatch(file, 0, math.MaxInt64, nil, budget)
	require.Error(t, err)
	require.Equal(t, 1, reserveCalls, "native integer overflow never reaches allocation")
}

func TestSharedMaterializedSourceCopyFailureRollsBackReservation(t *testing.T) {
	mp, err := mpool.NewMPool("materialized-copy-failure", mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(1)
	require.NoError(t, source.Begin(mp))

	inputMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(inputMP) })
	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	values := make([]int64, mpool.MB/4+1)
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, inputMP))
	bat.SetRowCount(len(values))

	err = source.Append(t.Context(), bat)
	require.Error(t, err)
	require.Zero(t, source.bytes)
	_, _, end, readerErr := source.Next(context.Background(), 0, 0)
	require.True(t, end)
	require.Same(t, err, readerErr)
	source.ReleaseReader(0)
	source.Finish(err)
	require.Zero(t, mp.CurrNB())
	bat.Clean(inputMP)
}

func TestSharedMaterializedSpillFramesOwnChargesAcrossSourceReuse(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	input := testInt64Batch(t, mp, 13)
	defer input.Clean(mp)
	encodedSize, err := input.MarshalBinarySize()
	serialized := uint64(encodedSize)
	require.NoError(t, err)
	readBytes := testSpillReadBytes(t, input)
	budget := newTestSpillBudget(serialized, math.MaxUint64, 1)
	source := newSource(2, 0)
	defer source.Close()
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(t.Context(), input))
	source.Finish(nil)
	budget.setCap(testBudgetMemory, 2*readBytes)
	first, firstCharge, end, err := source.Next(t.Context(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.NotNil(t, firstCharge)
	defer func() { first.Clean(mp); firstCharge.Release() }()
	second, secondCharge, end, err := source.Next(t.Context(), 1, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.NotNil(t, secondCharge)
	defer func() { second.Clean(mp); secondCharge.Release() }()
	memory, _, _ := budget.usage()
	require.Equal(t, 2*readBytes, memory)
	denied, err := budget.reserve(testBudgetMemory, 1)
	require.Error(t, err)
	require.Nil(t, denied)

	// Closing/reusing the source owns its file and retained input, never
	// a returned batch still held by either independent consumer.
	source.Close()
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	memory, disk, fd := budget.usage()
	require.Equal(t, 2*readBytes, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
	first.Clean(mp)
	require.True(t, firstCharge.Release())
	admitted, err := budget.reserve(testBudgetMemory, readBytes)
	require.NoError(t, err)
	admitted.Release()
	require.Equal(t, int64(13), vector.GetFixedAtNoTypeCheck[int64](second.Vecs[0], 0))
	second.Clean(mp)
	require.True(t, secondCharge.Release())
	memory, _, _ = budget.usage()
	require.Zero(t, memory)
}

func TestSharedMaterializedSpillReadFailureReleasesFrame(t *testing.T) {
	for _, failure := range []string{"truncated", "malformed", "generation ended"} {
		t.Run(failure, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			input := testInt64Batch(t, mp, 13)
			defer input.Clean(mp)
			budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
			source := newSource(1, 0)
			defer source.Close()
			config := budget.config(testSpillFactory(t.TempDir()))
			require.NoError(t, source.Begin(mp, config))
			require.NoError(t, source.Append(t.Context(), input))
			source.Finish(nil)
			switch failure {
			case "truncated":
				require.NoError(t, source.spillFile.Truncate(source.spillBytes-1))
			case "malformed":
				_, err := source.spillFile.WriteAt([]byte{255, 255, 255, 255, 255, 255, 255, 255}, spillBatchHeaderSize)
				require.NoError(t, err)
			case "generation ended":
				original := source.spillConfig.Budget.ReserveMemory
				source.spillConfig.Budget.ReserveMemory = func(size uint64) (Reservation, error) {
					charge, err := original(size)
					// The read is outside source.mu; closing/reusing during its
					// admission must neither deadlock nor publish an old result.
					source.Close()
					require.NoError(t, source.Begin(mp, config))
					return charge, err
				}
			}
			out, charge, end, err := source.Next(t.Context(), 0, 0)
			require.Error(t, err)
			require.True(t, end)
			require.Nil(t, out)
			require.Nil(t, charge)
			memory, _, _ := budget.usage()
			require.Zero(t, memory)
		})
	}
}

func testSpillReadBytes(t *testing.T, b *batch.Batch) uint64 {
	t.Helper()
	encoded, err := b.MarshalBinary()
	require.NoError(t, err)
	metadata, err := batch.UnmarshalBinaryMetadataSize(encoded)
	require.NoError(t, err)
	return uint64(len(encoded)) + metadata
}

func TestSharedMaterializedSpillAdmitsWideMarshalPlans(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	input := batch.NewWithSize(65)
	defer input.Clean(mp)
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(input.Vecs[i], int64(i), false, mp))
	}
	input.SetRowCount(1)
	size, err := input.MarshalBinarySize()
	require.NoError(t, err)
	scratch, err := input.MarshalBinaryScratchSize()
	require.NoError(t, err)
	require.Positive(t, scratch)
	for _, capacity := range []uint64{uint64(size + scratch - 1), uint64(size + scratch)} {
		budget := newTestSpillBudget(capacity, math.MaxUint64, 1)
		source := newSource(1, 0)
		require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
		err := source.Append(t.Context(), input)
		if capacity == uint64(size+scratch) {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Nil(t, source.spillFile)
		}
		source.Close()
		memory, disk, fd := budget.usage()
		require.Zero(t, memory)
		require.Zero(t, disk)
		require.Zero(t, fd)
	}
}
