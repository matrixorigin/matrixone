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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

	cap     [3]uint64
	used    [3]uint64
	account *mpool.AllocationAccount
}

type testSpillReservation struct {
	budget   *testSpillBudget
	kind     testBudgetKind
	size     uint64
	released bool
}

func newTestSpillBudget(memoryCap, diskCap, fdCap uint64) *testSpillBudget {
	registry, err := mpool.NewAllocationAccountRegistry(1, math.MaxUint64)
	if err != nil {
		panic(err)
	}
	account, err := registry.Open(math.MaxInt64)
	if err != nil {
		panic(err)
	}
	return &testSpillBudget{
		cap:     [3]uint64{memoryCap, diskCap, fdCap},
		account: account,
	}
}

func (b *testSpillBudget) config(factory SpillFileFactory) SpillConfig {
	return SpillConfig{FileFactory: factory, AllocationAccount: b.account, Budget: SpillBudget{
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
	return func(name string) (*os.File, error) {
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
		stats, err := source.AppendWithStats(bat)
		require.NoError(t, err)
		require.Positive(t, stats.RetainedBytes)
		require.Zero(t, stats.SpilledBytes)
		require.Zero(t, stats.SpilledRows)
		bat.Clean(mp)
	}
	source.Finish(nil)

	// Reader 1 can consume the complete producer before reader 0 starts.
	for i := 0; i < 4; i++ {
		bat, end, err := source.Next(context.Background(), 1, i)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
		bat.Clean(mp)
	}
	_, end, err := source.Next(context.Background(), 1, 4)
	require.NoError(t, err)
	require.True(t, end)

	for i := 0; i < 4; i++ {
		bat, end, err := source.Next(context.Background(), 0, i)
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
	_, end, err := source.Next(ctx, 0, 0)
	require.True(t, end)
	require.ErrorIs(t, err, want)

	source.ReleaseReader(0)
	source.Finish(want)
	require.Equal(t, int64(0), mp.CurrNB())
	require.NoError(t, source.Begin(mp))
	source.Finish(nil)
	source.ReleaseReader(0)
}

func TestSharedMaterializedSourceReuseErrorIdentifiesOwners(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(2)
	require.NoError(t, source.Begin(mp))

	err := source.Begin(mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "generation=1")
	require.Contains(t, err.Error(), "producer_released=false")
	require.Contains(t, err.Error(), "unreleased_readers=[0 1]")

	source.Finish(nil)
	source.ReleaseReader(0)
	err = source.Begin(mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "producer_released=true")
	require.Contains(t, err.Error(), "unreleased_readers=[1]")

	source.Close()
	require.NoError(t, source.Begin(mp))
	source.Finish(nil)
	source.ReleaseReader(0)
	source.ReleaseReader(1)
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
		_, end, err := source.Next(ctx, 0, 0)
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
	_, end, err := source.Next(ctx, 0, 0)
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
	require.NoError(t, source.Append(bat))
	bat.Clean(mp)
	want := moerr.NewInternalErrorNoCtx("producer failed")
	source.Finish(want)

	got, end, err := source.Next(context.Background(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	got.Clean(mp)
	_, end, err = source.Next(context.Background(), 0, 1)
	require.True(t, end)
	require.ErrorIs(t, err, want)
	source.ReleaseReader(0)
	require.Equal(t, int64(0), mp.CurrNB())
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
	spillDir := t.TempDir()
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(spillDir))))
	require.NoError(t, source.Append(bat), "the exact in-memory boundary is allowed")
	require.Equal(t, reserved, source.CurrentBytes())
	require.NoError(t, source.Append(bat), "actual rows above the estimate must spill")
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
			got, end, readerErr := source.Next(readCtx, readerID, position)
			require.NoError(t, readerErr)
			require.False(t, end)
			require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
			got.Clean(mp)
		}
		_, end, readerErr := source.Next(context.Background(), readerID, 2)
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
	require.NoError(t, source.Append(bat))
	require.Len(t, source.batches, 1)
	require.NoError(t, source.Append(bat), "Go batch metadata must stay bounded by spilling later tiny batches")
	require.Len(t, source.batches, 1)
	require.Equal(t, 1, source.spillBatchCount)
	source.Finish(nil)

	for position := 0; position < 2; position++ {
		got, end, err := source.Next(context.Background(), 0, position)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
		got.Clean(mp)
	}
	_, end, err := source.Next(context.Background(), 0, 2)
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

func TestSharedMaterializedSourceSpillPreservesBinaryStringRows(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("raw"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("text"), false, mp))
	require.NoError(t, bat.Vecs[0].SetIsBinaryStringAt(0, true))
	bat.SetRowCount(2)

	source := newSource(1, 0)
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(bat))
	require.Equal(t, 1, source.spillBatchCount)
	bat.Clean(mp)
	source.Finish(nil)

	got, end, err := source.Next(context.Background(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.True(t, got.Vecs[0].GetIsBinaryStringAt(0))
	require.False(t, got.Vecs[0].GetIsBinaryStringAt(1))
	got.Clean(mp)
	_, end, err = source.Next(context.Background(), 0, 1)
	require.NoError(t, err)
	require.True(t, end)
	source.ReleaseReader(0)
	require.Zero(t, mp.CurrNB())
}

func TestSharedMaterializedSourceSplitsOversizedSpillBatch(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	payload := bytes.Repeat([]byte("x"), 256)
	for range 100 {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], payload, false, mp))
	}
	for row := 0; row < 100; row += 7 {
		bat.Vecs[0].GetGrouping().Add(uint64(row))
	}
	bat.Vecs[0].SetSorted(true)
	bat.SetRowCount(100)
	t.Cleanup(func() { bat.Clean(mp) })

	source := newSource(1, 0)
	source.spillBatchLimit = 4 * 1024
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	stats, err := source.AppendWithStats(bat)
	require.NoError(t, err)
	require.Equal(t, int64(100), stats.SpilledRows)
	require.Greater(t, source.spillBatchCount, 1)
	source.Finish(nil)

	rows := 0
	for position := 0; ; position++ {
		got, end, err := source.Next(context.Background(), 0, position)
		require.NoError(t, err)
		if end {
			break
		}
		for row := 0; row < got.RowCount(); row++ {
			require.Equal(t, payload, got.Vecs[0].GetBytesAt(row))
			require.Equal(t, (rows+row)%7 == 0,
				got.Vecs[0].GetGrouping().Contains(uint64(row)))
		}
		require.True(t, got.Vecs[0].GetSorted())
		require.Positive(t, budget.account.Snapshot().Used)
		rows += got.RowCount()
		got.Clean(mp)
		require.Zero(t, budget.account.Snapshot().Used)
	}
	require.Equal(t, 100, rows)
	source.ReleaseReader(0)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
}

func TestOversizedSpillSplitDoesNotCompactWholeInput(t *testing.T) {
	inputMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(inputMP) })
	spillMP, err := mpool.NewMPool(t.Name(), mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(spillMP) })

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	payload := bytes.Repeat([]byte("x"), 1024)
	for range 4096 {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], payload, false, inputMP))
	}
	bat.SetRowCount(4096)
	t.Cleanup(func() { bat.Clean(inputMP) })

	source := newSource(1, 0)
	source.spillBatchLimit = 32 * 1024
	budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
	require.NoError(t, source.Begin(spillMP, budget.config(testSpillFactory(t.TempDir()))))
	stats, err := source.AppendWithStats(bat)
	require.NoError(t, err)
	require.Equal(t, int64(4096), stats.SpilledRows)
	require.Greater(t, source.spillBatchCount, 1)
	require.Zero(t, spillMP.CurrNB())
	source.Close()
}

func TestSharedMaterializedSourceSpillBudgetExactBoundaryAndCleanup(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 7)
	t.Cleanup(func() { bat.Clean(mp) })
	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized

	budget := newTestSpillBudget(scratch, recordBytes, 1)
	source := newSource(1, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	stats, err := source.AppendWithStats(bat)
	require.NoError(t, err)
	require.Zero(t, stats.RetainedBytes)
	require.Equal(t, int64(recordBytes), stats.SpilledBytes)
	require.Equal(t, int64(bat.RowCount()), stats.SpilledRows)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory, "marshal scratch must be released after Append")
	require.Equal(t, recordBytes, disk)
	require.Equal(t, uint64(1), fd)

	source.Finish(nil)
	got, end, err := source.Next(context.Background(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	got.Clean(mp)
	source.ReleaseReader(0)
	memory, disk, fd = budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)

	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(bat), "a new generation must not inherit released spill charges")
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
	serialized, scratch, err := spillBatchSize(bat)
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
			budget := newTestSpillBudget(scratch, tc.diskCap, tc.fdCap)
			created := 0
			source := newSource(1, 0)
			require.NoError(t, source.Begin(mp, budget.config(func(string) (*os.File, error) {
				created++
				return nil, errors.New("file factory must not run")
			})))
			appendErr := source.Append(bat)
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
	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized
	budget := newTestSpillBudget(scratch, recordBytes, 2)
	config := budget.config(testSpillFactory(t.TempDir()))

	first := newSource(1, 0)
	require.NoError(t, first.Begin(mp, config))
	require.NoError(t, first.Append(bat))
	second := newSource(1, 0)
	require.NoError(t, second.Begin(mp, config))
	secondErr := second.Append(bat)
	require.Error(t, secondErr, "the first source must retain the statement spill charge")
	second.Finish(secondErr)
	second.ReleaseReader(0)

	first.Finish(nil)
	first.ReleaseReader(0)
	_, disk, _ := budget.usage()
	require.Zero(t, disk)

	third := newSource(1, 0)
	require.NoError(t, third.Begin(mp, config))
	require.NoError(t, third.Append(bat), "released spill capacity must be reusable")
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
	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized
	budget := newTestSpillBudget(scratch, 2*recordBytes, 1)
	source := newSource(1, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(bat))
	require.NoError(t, source.Append(bat))
	require.Equal(t, int64(2*recordBytes), source.spillBytes)
	info, err := source.spillFile.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(2*recordBytes), info.Size())

	appendErr := source.Append(bat)
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
	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	recordBytes := uint64(spillBatchHeaderSize) + serialized
	budget := newTestSpillBudget(scratch, recordBytes, 1)
	source := newSource(1, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(bat))
	source.Finish(nil)

	budget.setCap(testBudgetMemory, serialized-1)
	_, end, readErr := source.Next(context.Background(), 0, 0)
	require.True(t, end)
	require.Error(t, readErr)
	memory, _, _ := budget.usage()
	require.Zero(t, memory)

	budget.setCap(testBudgetMemory, serialized)
	got, end, err := source.Next(context.Background(), 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	got.Clean(mp)
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
	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	budget := newTestSpillBudget(scratch, uint64(spillBatchHeaderSize)+serialized, 1)
	source := newSource(1, 0)
	want := errors.New("spill file unavailable")
	require.NoError(t, source.Begin(mp, budget.config(func(string) (*os.File, error) {
		return nil, want
	})))
	appendErr := source.Append(bat)
	require.ErrorIs(t, appendErr, want)
	memory, disk, fd := budget.usage()
	require.Zero(t, memory)
	require.Zero(t, disk)
	require.Zero(t, fd)
	source.Finish(appendErr)
	source.ReleaseReader(0)
}

func TestSpillBatchSizeMatchesEncoding(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{1, 2}, []bool{false, true}, mp))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("materialized"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("spill"), false, mp))
	require.NoError(t, bat.Vecs[1].SetIsBinaryStringAt(0, true))
	require.NoError(t, bat.Vecs[1].SetPrepareParamKindsWithMP([]vector.PrepareParamKind{
		vector.PrepareParamInteger, vector.PrepareParamNone,
	}, mp))
	bat.Vecs[0].GetGrouping().Add(1)
	bat.SetRowCount(2)
	t.Cleanup(func() { bat.Clean(mp) })

	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, encoded.WriteByte(spillPayloadGrouping))
	require.NoError(t, bat.MarshalBinaryWithGroupingTo(&encoded))
	require.Equal(t, uint64(encoded.Len()), serialized)
	require.GreaterOrEqual(t, scratch, serialized)
}

func TestSharedMaterializedSourceSpillPreservesGroupingProvenance(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{0, 7}, []bool{true, false}, mp))
	bat.Vecs[0].GetGrouping().Add(0)
	bat.SetRowCount(2)
	t.Cleanup(func() { bat.Clean(mp) })

	serialized, scratch, err := spillBatchSize(bat)
	require.NoError(t, err)
	budget := newTestSpillBudget(
		scratch,
		uint64(spillBatchHeaderSize)+serialized,
		1,
	)
	source := newSource(2, 0)
	require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
	require.NoError(t, source.Append(bat))
	source.Finish(nil)

	for readerID := range 2 {
		got, end, err := source.Next(context.Background(), readerID, 0)
		require.NoError(t, err)
		require.False(t, end)
		require.Positive(t, budget.account.Snapshot().Used)
		require.True(t, got.Vecs[0].GetNulls().Contains(0))
		require.True(t, got.Vecs[0].GetGrouping().Contains(0))
		require.False(t, got.Vecs[0].GetGrouping().Contains(1))
		got.Clean(mp)
		require.Zero(t, budget.account.Snapshot().Used)
		source.ReleaseReader(readerID)
	}
}

func TestSharedMaterializedSourceStoresOnlyPositionalBatches(t *testing.T) {
	for _, memoryLimit := range []int64{0, sharedMaterializedSourceMaxBytes} {
		t.Run(fmt.Sprintf("memory-limit-%d", memoryLimit), func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			bat := testInt64Batch(t, mp, 7)
			bat.Attrs = []string{strings.Repeat("attribute", 64)}
			t.Cleanup(func() { bat.Clean(mp) })

			source := newSource(1, memoryLimit)
			budget := newTestSpillBudget(math.MaxUint64, math.MaxUint64, 1)
			require.NoError(t, source.Begin(mp, budget.config(testSpillFactory(t.TempDir()))))
			require.NoError(t, source.Append(bat))
			source.Finish(nil)
			got, end, err := source.Next(context.Background(), 0, 0)
			require.NoError(t, err)
			require.False(t, end)
			for _, attr := range got.Attrs {
				require.Empty(t, attr)
			}
			require.Equal(t, []int64{7}, vector.MustFixedColWithTypeCheck[int64](got.Vecs[0]))
			got.Clean(mp)
			source.ReleaseReader(0)
		})
	}
}

func TestSharedMaterializedSourceRejectsUnfinalizedBatchMetadata(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := testInt64Batch(t, mp, 1)
	bat.ExtraBuf = []byte("partial aggregate state")
	t.Cleanup(func() { bat.Clean(mp) })

	source := newSource(1, sharedMaterializedSourceMaxBytes)
	require.NoError(t, source.Begin(mp))
	require.ErrorContains(t, source.Append(bat), "requires finalized positional batches")
	source.Close()
}

func TestAddSpillBatchTailPreservesFirstOverflow(t *testing.T) {
	_, err := addSpillBatchTail(math.MaxUint64, 1, 0)
	require.ErrorContains(t, err, "spill batch size overflow")
}

func TestReadSpilledBatchRejectsRuntimeOversizeBeforeAllocation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "oversize-spill")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	var header [spillBatchHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], maxSpillBatchBytes+1)
	_, err = file.Write(header[:])
	require.NoError(t, err)

	reserveCalls := 0
	budget := SpillBudget{ReserveMemory: func(uint64) (Reservation, error) {
		reserveCalls++
		return nil, errors.New("must not reserve")
	}}
	_, _, err = readSpilledBatch(
		file, 0, int64(maxSpillBatchBytes)+spillBatchHeaderSize+1,
		nil, budget, nil,
	)
	require.Error(t, err)
	require.Zero(t, reserveCalls)
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

	err = source.Append(bat)
	require.Error(t, err)
	require.Zero(t, source.bytes)
	_, end, readerErr := source.Next(context.Background(), 0, 0)
	require.True(t, end)
	require.Same(t, err, readerErr)
	source.ReleaseReader(0)
	source.Finish(err)
	require.Zero(t, mp.CurrNB())
	bat.Clean(inputMP)
}
