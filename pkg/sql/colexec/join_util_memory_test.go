// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package colexec_test

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJoinBatchesFullCopyAllocation(t *testing.T) {
	for _, mode := range []string{"flat-control", "constant-int64", "constant-null", "two-varlen-rounding", "shared-varlen"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer proc.Free()
			columns := 1
			if mode == "two-varlen-rounding" {
				columns = 2
			}
			src := batch.NewOffHeapWithSize(columns)
			defer src.Clean(mp)
			switch mode {
			case "constant-int64":
				var err error
				src.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), int64(7), colexec.DefaultBatchSize, mp)
				require.NoError(t, err)
			case "constant-null":
				src.Vecs[0] = vector.NewRollupConst(types.T_int64.ToType(), colexec.DefaultBatchSize, mp)
			case "shared-varlen":
				constant, err := vector.NewConstBytes(types.T_varchar.ToType(), bytes.Repeat([]byte{'x'}, 1<<20), colexec.DefaultBatchSize, mp)
				require.NoError(t, err)
				defer constant.Free(mp)
				src.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
				require.NoError(t, src.Vecs[0].UnionBatch(constant, 0, colexec.DefaultBatchSize, nil, mp))
			case "flat-control":
				src.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
				for range colexec.DefaultBatchSize {
					require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(7), false, mp))
				}
			default:
				for col := range src.Vecs {
					src.Vecs[col] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
					for row := range colexec.DefaultBatchSize {
						width := 25
						if row == 0 {
							width++
						}
						require.NoError(t, vector.AppendBytes(src.Vecs[col], bytes.Repeat([]byte{'x'}, width), false, mp))
					}
				}
			}
			src.SetRowCount(colexec.DefaultBatchSize)
			var copied colexec.Batches
			defer copied.Clean(mp)
			// Repeated copies also cross the list's initial sixteen-entry
			// capacity. Compare the forecast with independent owned storage.
			for step := range 18 {
				beforeOwned, beforePool := copied.Allocated(), mp.AllocatedBytes()
				promise := copied.AppendAllocation(src)
				require.NoError(t, copied.CopyIntoBatches(src, proc))
				require.LessOrEqual(t, copied.Allocated()-beforeOwned, promise, "owned growth: %s copy %d", mode, step)
				require.LessOrEqual(t, mp.AllocatedBytes()-beforePool, promise, "pool growth: %s copy %d", mode, step)
			}
			require.Equal(t, 18*colexec.DefaultBatchSize, copied.RowCount())
			if mode == "shared-varlen" {
				require.Equal(t, 1<<20, len(copied.Buf[17].Vecs[0].GetArea()))
			}
		})
	}
}

func TestJoinBatchesPartialCopyAllocation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	var copied colexec.Batches
	defer copied.Clean(proc.Mp())
	for _, rows := range []int{8191, 8193, 63, 65} {
		src := batch.NewOffHeapWithSize(2)
		constant, err := vector.NewConstBytes(types.T_varchar.ToType(), bytes.Repeat([]byte{'s'}, 256), rows, proc.Mp())
		require.NoError(t, err)
		src.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
		require.NoError(t, src.Vecs[0].UnionBatch(constant, 0, rows, nil, proc.Mp()))
		constant.Free(proc.Mp())
		src.Vecs[0].GetNulls().Add(0, uint64(rows-1))
		src.Vecs[0].GetGrouping().Add(uint64(rows - 2))
		src.Vecs[1] = vector.NewConstNull(types.T_int64.ToType(), rows, proc.Mp())
		src.SetRowCount(rows)
		before := copied.Allocated()
		promise := copied.AppendAllocation(src)
		require.NoError(t, copied.CopyIntoBatches(src, proc))
		require.LessOrEqual(t, copied.Allocated()-before, promise, "partial tail plus new windows: %d", rows)
		src.Clean(proc.Mp())
	}
	require.Equal(t, 8191+8193+63+65, copied.RowCount())
	for _, bat := range copied.Buf {
		require.LessOrEqual(t, bat.RowCount(), colexec.DefaultBatchSize)
		require.LessOrEqual(t, len(bat.Vecs[0].GetArea()), 4*256, "partial appends must not expand repeated aliases")
	}
}

func TestJoinBatchesGrowthCoversBitmapCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	src := batch.NewOffHeapWithSize(2)
	defer src.Clean(proc.Mp())
	src.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, []uint64{0}, proc.Mp())
	src.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(src.Vecs[1], []byte("a referenced payload beyond inline varlena size"), true, proc.Mp()))
	src.SetRowCount(1)
	var copied colexec.Batches
	defer copied.Clean(proc.Mp())
	for row := 0; row < 130; row++ {
		before := copied.Allocated()
		growth := copied.AppendAllocation(src)
		require.NoError(t, copied.CopyIntoBatches(src, proc))
		require.LessOrEqual(t, copied.Allocated(), before+growth, "full new allocation bound must include bitmap word expansion at row %d", row)
	}
	var bitmapBytes int
	for _, vec := range copied.Buf[0].Vecs {
		bitmapBytes += nulls.Allocated(vec.GetNulls()) + nulls.Allocated(vec.GetGrouping())
	}
	require.GreaterOrEqual(t, bitmapBytes, 48, "130 nullable rows require at least three bitmap words per vector")
	require.Less(t, copied.Buf[0].Vecs[1].Capacity(), 8192, "row-ID layout does not require full fixed allocation for small batches")
}
