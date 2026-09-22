// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package minusall

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestMinusAllMultiplicityNullsAndReset(t *testing.T) {
	proc := testutil.NewProcess(t)
	arg := NewArgument()
	defer arg.Release()

	for range 2 {
		setChildren(proc, arg)
		require.NoError(t, arg.Prepare(proc))

		var values []int64
		nulls := 0
		for {
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			if result.Batch == nil {
				break
			}
			rows := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])
			for row, value := range rows {
				if result.Batch.Vecs[0].IsNull(uint64(row)) {
					nulls++
				} else {
					values = append(values, value)
				}
			}
		}
		require.Equal(t, []int64{1, 2, 2}, values)
		require.Equal(t, 1, nulls)

		for _, child := range arg.Children {
			child.Reset(proc, false, nil)
		}
		arg.Reset(proc, false, nil)
	}

	for _, child := range arg.Children {
		child.Free(proc, false, nil)
	}
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMinusAllCountsAcrossBatchesAndUnitLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	arg := NewArgument()
	defer arg.Release()

	keyCount := hashmap.UnitLimit + 3
	rightValues := make([]int64, keyCount)
	leftValues := make([]int64, 0, keyCount*2)
	for i := range rightValues {
		rightValues[i] = int64(i)
		leftValues = append(leftValues, int64(i), int64(i))
	}
	left := []*batch.Batch{testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(len(leftValues), types.T_int64.ToType(), proc.Mp(), false, leftValues),
	}, nil)}
	right := []*batch.Batch{
		testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(hashmap.UnitLimit, types.T_int64.ToType(), proc.Mp(), false, rightValues[:hashmap.UnitLimit]),
		}, nil),
		testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, rightValues[hashmap.UnitLimit:]),
		}, nil),
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(left))
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(right))
	require.NoError(t, arg.Prepare(proc))

	actual := make(map[int64]int, keyCount)
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		for _, value := range vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]) {
			actual[value]++
		}
	}
	require.Len(t, actual, keyCount)
	for _, value := range rightValues {
		require.Equal(t, 1, actual[value])
	}

	for _, child := range arg.Children {
		child.Free(proc, false, nil)
	}
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func setChildren(proc *process.Process, arg *MinusAll) {
	for _, child := range arg.Children {
		child.Free(proc, false, nil)
	}
	arg.Children = nil

	left := []*batch.Batch{
		testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 1}),
		}, nil),
		testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVectorWithNulls(5, types.T_int64.ToType(), proc.Mp(), false,
				[]bool{false, false, false, true, true}, []int64{1, 2, 2, 0, 0}),
		}, nil),
	}
	right := []*batch.Batch{
		testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVectorWithNulls(4, types.T_int64.ToType(), proc.Mp(), false,
				[]bool{false, false, false, true}, []int64{1, 1, 3, 0}),
		}, nil),
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(left))
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(right))
}
