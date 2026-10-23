// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package pSpool

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestControlledSpoolEvictsOnlyReturnedBackingAndReusesSmallCache(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "controlled"}[controlled], func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			srcMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { require.Zero(t, srcMP.CurrNB()); mpool.DeleteMPool(srcMP) })
			src := batch.NewWithSize(1)
			t.Cleanup(func() { src.Clean(srcMP) })
			src.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(src.Vecs[0], []byte(strings.Repeat("w", 8192)), false, srcMP))
			src.SetRowCount(1)
			sp := InitMyPipelineSpool(mp, 1)
			t.Cleanup(sp.CloseAfterReadersStopped)
			if controlled {
				sp.SetCacheMemoryLimit(1024)
			}
			_, err := sp.SendBatch(context.Background(), 0, src, nil)
			require.NoError(t, err)
			got, info := sp.ReceiveBatch(0)
			require.NoError(t, info)
			borrowed := mp.CurrNB()
			sp.ForceCleanup()
			require.Equal(t, borrowed, mp.CurrNB(), "unconfirmed borrowed backing must remain live")
			require.Len(t, got.Vecs[0].GetBytesAt(0), 8192)
			sp.ReleaseCurrent(0)
			if controlled {
				require.LessOrEqual(t, mp.CurrNB(), int64(1024))
			} else {
				require.Greater(t, mp.CurrNB(), int64(1024))
			}
			src.Vecs[0].CleanOnlyData()
			require.NoError(t, vector.AppendBytes(src.Vecs[0], []byte("small"), false, srcMP))
			for generation := 0; generation < 2; generation++ {
				_, err = sp.SendBatch(context.Background(), 0, src, nil)
				require.NoError(t, err)
				got, info = sp.ReceiveBatch(0)
				require.NoError(t, info)
				require.Equal(t, "small", string(got.Vecs[0].GetBytesAt(0)))
				require.False(t, got.Vecs[0].IsNull(0))
				sp.ReleaseCurrent(0)
				if controlled {
					require.LessOrEqual(t, mp.CurrNB(), int64(1024))
				}
			}
			_, err = sp.SendBatch(context.Background(), 0, nil, nil)
			require.NoError(t, err)
			got, info = sp.ReceiveBatch(0)
			require.NoError(t, info)
			require.Nil(t, got)
			sp.Close()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestSpoolCopyFailureReturnsActualCacheID(t *testing.T) {
	mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	filler, err := mp.Alloc((1<<20)-64, true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(filler) })
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { require.Zero(t, sourceMP.CurrNB()); mpool.DeleteMPool(sourceMP) })
	src := batch.NewWithSize(1)
	t.Cleanup(func() { src.Clean(sourceMP) })
	src.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(src.Vecs[0], []byte(strings.Repeat("x", 8192)), false, sourceMP))
	src.SetRowCount(1)
	cache := initCachedBatch(mp, 2)
	t.Cleanup(cache.free)
	for attempt := 0; attempt < 3; attempt++ {
		got, owned, id, err := cache.GetCopiedBatch(src)
		if err == nil {
			cache.CacheBatch(owned, id, got)
		}
		require.Error(t, err)
		require.Len(t, cache.buffer.readyToUse, 2, "each failed allocation must return its own cache ID")
		require.Equal(t, int64(len(filler)), mp.CurrNB())
	}
}
