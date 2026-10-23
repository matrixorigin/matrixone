// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package colexec_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJoinBatchesTrackedLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	var copied colexec.Batches
	t.Cleanup(func() { copied.Clean(proc.Mp()) })
	input := batch.NewOffHeapWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7, 8, 9}, nil, proc.Mp())
	input.SetRowCount(3)
	// Tracking can begin after a retained unbudgeted prefix.
	require.NoError(t, copied.CopyIntoBatches(input, proc))
	copied.TrackAllocation()
	require.Equal(t, copied.Allocated(), copied.RetainedBytes())
	require.NoError(t, copied.CopyIntoBatches(input, proc))
	require.Equal(t, copied.Allocated(), copied.RetainedBytes())
	var ignored bitmap.Bitmap
	ignored.InitWithSize(6)
	ignored.AddRange(1, 4)
	require.NoError(t, copied.Shrink(&ignored, proc))
	require.Equal(t, 3, copied.RowCount())
	require.Equal(t, []int32{7, 8, 9}, vector.MustFixedColNoTypeCheck[int32](copied.Buf[0].Vecs[0]))
	require.Equal(t, copied.Allocated(), copied.RetainedBytes())
	// Ownership transfer detaches both rows and retained allocation.
	transferred := copied.Buf
	copied.Reset()
	t.Cleanup(func() {
		for _, bat := range transferred {
			bat.Clean(proc.Mp())
		}
	})
	copied.TrackAllocation()
	require.Zero(t, copied.RetainedBytes())
	require.NoError(t, copied.CopyIntoBatches(input, proc))
	require.Equal(t, copied.Allocated(), copied.RetainedBytes())
	copied.Clean(proc.Mp())
	require.Zero(t, copied.RetainedBytes())
	require.Zero(t, copied.Allocated())
}

func TestJoinBatchesTrackedCopyFailure(t *testing.T) {
	mp, err := mpool.NewMPool("tracked-copy-failure", mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)
	t.Cleanup(proc.Free)
	var copied colexec.Batches
	copied.TrackAllocation()
	t.Cleanup(func() { copied.Clean(mp) })
	input := batch.NewOffHeapWithSize(2)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7, 8}, nil, mp)
	input.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	for range 2 {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 2048)), false, mp))
	}
	input.SetRowCount(2)
	require.NoError(t, copied.CopyIntoBatches(input, proc))
	require.Equal(t, copied.Allocated(), copied.RetainedBytes())
	reservation, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()-1024), true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(reservation) })
	var ignored bitmap.Bitmap
	ignored.InitWithSize(2)
	ignored.Add(0)
	before := mp.CurrNB()
	err = copied.Shrink(&ignored, proc)
	require.ErrorContains(t, err, "mpool out of space")
	require.Equal(t, before, mp.CurrNB(), "rejected replacement frees its partial columns")
	require.Equal(t, 2, copied.RowCount(), "failed replacement leaves the old owner intact")
	require.Equal(t, copied.Allocated(), copied.RetainedBytes())
	err = copied.CopyIntoBatches(input, proc)
	require.ErrorContains(t, err, "mpool out of space")
	require.Empty(t, copied.Buf, "partial copy failure retires all retained batches")
	require.Zero(t, copied.RetainedBytes())
	require.Zero(t, copied.Allocated())
}

var benchmarkJoinRetainedBytes int64

// Compare the same retained state through incremental metadata and the complete
// independent oracle. The benchmark carries no wall-clock pass threshold.
func BenchmarkJoinBatchesRetainedAccounting(b *testing.B) {
	for _, history := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("history=%d", history), func(b *testing.B) {
			proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
			b.Cleanup(proc.Free)
			input := batch.NewOffHeapWithSize(1)
			b.Cleanup(func() { input.Clean(proc.Mp()) })
			var err error
			input.Vecs[0], err = vector.NewConstFixed(types.T_int32.ToType(), int32(7), colexec.DefaultBatchSize, proc.Mp())
			require.NoError(b, err)
			input.SetRowCount(colexec.DefaultBatchSize)
			var copied colexec.Batches
			copied.TrackAllocation()
			b.Cleanup(func() { copied.Clean(proc.Mp()) })
			for range history {
				require.NoError(b, copied.CopyIntoBatches(input, proc))
			}
			for _, recount := range []bool{false, true} {
				b.Run(fmt.Sprintf("recount=%t", recount), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					var bytes int64
					for range b.N {
						if recount {
							bytes = copied.Allocated()
						} else {
							bytes = copied.RetainedBytes()
						}
					}
					benchmarkJoinRetainedBytes = bytes
				})
			}
		})
	}
}
