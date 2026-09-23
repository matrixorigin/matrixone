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

package minusall

import (
	"errors"
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

type failingMinusAllInput struct {
	*colexec.MockOperator
	err   error
	calls int
}

func (input *failingMinusAllInput) Call(proc *process.Process) (vm.CallResult, error) {
	input.calls++
	if input.calls == 2 {
		return vm.CancelResult, input.err
	}
	return input.MockOperator.Call(proc)
}

func TestMinusAllResetAfterInputFailure(t *testing.T) {
	for _, side := range []int{0, 1} {
		t.Run([]string{"probe", "build"}[side], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			arg := NewArgument()
			t.Cleanup(func() {
				for _, child := range arg.Children {
					child.Free(proc, false, nil)
				}
				arg.Free(proc, false, nil)
				arg.Release()
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			setChildren(proc, arg)
			failure := errors.New("injected input failure")
			arg.Children[side] = &failingMinusAllInput{MockOperator: arg.Children[side].(*colexec.MockOperator), err: failure}
			require.NoError(t, arg.Prepare(proc))
			_, err := vm.Exec(arg, proc)
			require.ErrorIs(t, err, failure)
			arg.Reset(proc, true, err)
			require.Nil(t, arg.ctr.hashTable)
			require.Nil(t, arg.ctr.remaining)
			setChildren(proc, arg)
			require.NoError(t, arg.Prepare(proc))
			rows := 0
			for {
				result, err := vm.Exec(arg, proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				rows += result.Batch.RowCount()
			}
			require.Equal(t, 4, rows)
		})
	}
}

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
