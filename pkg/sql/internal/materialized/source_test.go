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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

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
		require.NoError(t, source.Append(bat))
		bat.Clean(mp)
	}
	source.Finish(nil)

	// Reader 1 can consume the complete producer before reader 0 starts.
	for i := 0; i < 4; i++ {
		bat, end, err := source.Next(context.Background(), i)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
	}
	_, end, err := source.Next(context.Background(), 4)
	require.NoError(t, err)
	require.True(t, end)

	for i := 0; i < 4; i++ {
		bat, end, err := source.Next(context.Background(), i)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
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
	_, end, err := source.Next(ctx, 0)
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
		_, end, err := source.Next(ctx, 0)
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
	_, end, err := source.Next(ctx, 0)
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

	got, end, err := source.Next(context.Background(), 0)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	_, end, err = source.Next(context.Background(), 1)
	require.True(t, end)
	require.ErrorIs(t, err, want)
	source.ReleaseReader(0)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSharedMaterializedSourceRuntimeLimit(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	source := NewSource(2)
	require.NoError(t, source.Begin(mp))

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.SetRowCount(1)
	inputMemory := mp.CurrNB()

	reserved := int64(max(bat.Size(), bat.Allocated()))
	source.bytes = sharedMaterializedSourceMaxBytes - reserved
	require.NoError(t, source.Append(bat), "the exact 64 MiB boundary is allowed")
	require.Equal(t, sharedMaterializedSourceMaxBytes, source.bytes)

	err := source.Append(bat)
	require.ErrorContains(t, err, "exceeds 64 MiB runtime limit")
	for readerID := 0; readerID < 2; readerID++ {
		_, end, readerErr := source.Next(context.Background(), 1)
		require.True(t, end)
		require.Same(t, err, readerErr)
		source.ReleaseReader(readerID)
	}
	source.Finish(err)
	require.Equal(t, inputMemory, mp.CurrNB())
	bat.Clean(mp)
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
	_, end, readerErr := source.Next(context.Background(), 0)
	require.True(t, end)
	require.Same(t, err, readerErr)
	source.ReleaseReader(0)
	source.Finish(err)
	require.Zero(t, mp.CurrNB())
	bat.Clean(inputMP)
}
