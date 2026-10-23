// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package pSpool

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func spoolOwnerInput(t *testing.T, mp *mpool.MPool, columns int) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(columns)
	t.Cleanup(func() { b.Clean(mp) })
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(b.Vecs[i], []int64{11, 12}, nil, mp))
	}
	b.SetRowCount(2)
	return b
}

func TestSpoolAttemptOwnerReclaimsMissingEndAfterReadersStop(t *testing.T) {
	for _, broadcast := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "broadcast"}[broadcast], func(t *testing.T) {
			sourceMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
			source := spoolOwnerInput(t, sourceMP, 1)
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			count, target := uint32(1), 0
			if broadcast {
				count, target = 2, SendToAllLocal
			}
			sp := InitMyPipelineSpool(mp, count)
			t.Cleanup(sp.CloseAfterReadersStopped)
			done, err := sp.SendBatch(context.Background(), target, source, nil)
			require.NoError(t, err)
			require.False(t, done)
			borrowed, err := sp.ReceiveBatch(0)
			require.NoError(t, err)
			sp.ForceCleanup()
			require.Positive(t, mp.AllocatedBytes(), "normal cleanup must preserve borrowed/queued frames")
			require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
			// The test now stops all readers without consuming End, matching the
			// query root after its pipeline goroutines and cleanup have joined.
			sp.CloseAfterReadersStopped()
			require.Zero(t, mp.AllocatedBytes())
			sp.CloseAfterReadersStopped()
			require.Zero(t, mp.AllocatedBytes(), "terminal teardown is idempotent")
		})
	}
}

func TestSpoolCopyFailureReturnsMessageAndCacheSlots(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	source := spoolOwnerInput(t, sourceMP, 2)
	mp, err := mpool.NewMPool("spool-copy-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	// MPool's supported minimum cap is 1MiB. Leave exactly enough room for
	// one 16-byte column, making the second column fail deterministically.
	filler, err := mp.Alloc((1<<20)-24, true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(filler) })
	baseline := mp.AllocatedBytes()
	sp := InitMyPipelineSpool(mp, 1)
	t.Cleanup(sp.CloseAfterReadersStopped)
	for range 3 {
		_, err = sp.SendBatch(context.Background(), 0, source, nil)
		require.Error(t, err, "second column must fail after the first allocation")
		require.Len(t, sp.freeShardPool, 2)
		require.Len(t, sp.cache.buffer.readyToUse, 2)
		require.Equal(t, baseline, mp.AllocatedBytes())
	}
	// A subsequent valid frame uses the same spool without losing a slot or
	// inheriting the failed frame's partially initialized columns.
	small := source.SelectColumns([]int{0}, nil)
	_, err = sp.SendBatch(context.Background(), 0, small, nil)
	require.NoError(t, err)
	got, err := sp.ReceiveBatch(0)
	require.NoError(t, err)
	require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](got.Vecs[0]))
	sp.ReleaseCurrent(0)
	_, err = sp.SendBatch(context.Background(), 0, nil, nil)
	require.NoError(t, err)
	got, err = sp.ReceiveBatch(0)
	require.NoError(t, err)
	require.Nil(t, got)
	sp.Close()
	require.Equal(t, baseline, mp.AllocatedBytes())
	sp.CloseAfterReadersStopped()
	require.Equal(t, baseline, mp.AllocatedBytes())
}
