// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package colexec_test

import (
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
