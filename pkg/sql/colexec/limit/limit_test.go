// Copyright 2021 Matrix Origin
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

package limit

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10      // default rows
	BenchmarkRows = 1000000 // default rows for benchmark
)

// add unit tests for cases
type limitTestCase struct {
	arg         *Limit
	proc        *process.Process
	getRowCount int
}

func makeTestCases(t *testing.T) []limitTestCase {
	return []limitTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Limit{
				LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(0),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 0,
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Limit{
				LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(1),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 1,
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Limit{
				ctr: container{
					seen: 0,
				},
				LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(5),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 2, //if colexec.MakeMockBatchs return more rows, you need to change it
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)
	}
}

func TestLimit(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		res, _ := vm.Exec(tc.arg, tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, res.Batch.RowCount(), tc.getRowCount)
		} else {
			require.Equal(t, res.Batch == nil, true)
		}
		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		res, _ = vm.Exec(tc.arg, tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, res.Batch.RowCount(), tc.getRowCount)
		} else {
			require.Equal(t, res.Batch == nil, true)
		}

		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestLimitDoesNotMutateInputBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := colexec.MakeMockBatchs(proc.Mp())
	input.ShuffleIDX = 3
	inputRows := input.RowCount()
	inputLengths := make([]int, len(input.Vecs))
	for i := range input.Vecs {
		inputLengths[i] = input.Vecs[i].Length()
	}

	arg := NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1)).WithFoundRows(true)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int32(3), result.Batch.ShuffleIDX)
	require.Equal(t, inputRows, input.RowCount())
	for i := range input.Vecs {
		require.Equal(t, inputLengths[i], input.Vecs[i].Length())
	}

	arg.Reset(proc, false, nil)
	secondInput := colexec.MakeMockBatchs(proc.Mp())
	secondInput.ShuffleIDX = 7
	secondChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{secondInput})
	arg.Children = nil
	arg.AppendChild(secondChild)
	require.NoError(t, arg.Prepare(proc))

	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int32(7), result.Batch.ShuffleIDX)
	require.Equal(t, secondInput.RowCount(), inputRows)
	for i := range secondInput.Vecs {
		require.Equal(t, inputLengths[i], secondInput.Vecs[i].Length())
	}

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	secondChild.Free(proc, false, nil)
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSQLCalcFoundRowsDrainsInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.BeginFoundRowsStatement(true)

	first := colexec.MakeMockBatchs(proc.Mp())
	second := colexec.MakeMockBatchs(proc.Mp())
	arg := NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1)).WithFoundRows(true)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, vm.ExecNext, result.Status)

	for result.Batch != nil {
		result, err = arg.Call(proc)
		require.NoError(t, err)
	}
	require.Equal(t, vm.ExecStop, result.Status)
	require.Equal(t, uint64(first.RowCount()+second.RowCount()), proc.GetFoundRows())
	require.True(t, proc.FoundRowsRecorded())
	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
}

func TestNestedLimitDoesNotPublishFoundRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.BeginFoundRowsStatement(true)

	first := colexec.MakeMockBatchs(proc.Mp())
	second := colexec.MakeMockBatchs(proc.Mp())
	arg := NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1))
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.False(t, proc.FoundRowsRecorded())

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
}

func TestFoundRowsDrainOnlyConsumesInputWithoutPublishing(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.BeginFoundRowsStatement(true)

	first := colexec.MakeMockBatchs(proc.Mp())
	second := colexec.MakeMockBatchs(proc.Mp())
	arg := NewArgument().
		WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1)).
		WithFoundRowsDrain(true)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, vm.ExecNext, result.Status)

	for result.Batch != nil {
		result, err = arg.Call(proc)
		require.NoError(t, err)
	}
	require.Equal(t, vm.ExecStop, result.Status)
	require.False(t, proc.FoundRowsRecorded())
	require.False(t, arg.IsFoundRowsOwner())
	require.True(t, arg.DrainsForFoundRows())

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
}

func TestSQLCalcFoundRowsZeroLimitStillDrainsInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.BeginFoundRowsStatement(true)

	first := colexec.MakeMockBatchs(proc.Mp())
	second := colexec.MakeMockBatchs(proc.Mp())
	arg := NewArgument().
		WithLimit(plan2.MakePlan2Uint64ConstExprWithType(0)).
		WithFoundRows(true)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Equal(t, uint64(first.RowCount()+second.RowCount()), proc.GetFoundRows())
	require.True(t, proc.FoundRowsRecorded())

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
}

func TestSQLCalcFoundRowsDrainsEmptyAndLastBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.BeginFoundRowsStatement(true)

	first := colexec.MakeMockBatchs(proc.Mp())
	last := batch.NewWithSize(0)
	last.SetLast()
	arg := NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1)).WithFoundRows(true)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, batch.EmptyBatch, last})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 1, result.Batch.RowCount())

	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())
	require.Equal(t, uint64(first.RowCount()), proc.GetFoundRows())
	require.True(t, proc.FoundRowsRecorded())
	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
}

func TestLimitResetReleasesCopiedAllocationAccountData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	input := batch.NewOffHeapWithSize(1)
	input.SetVector(0, vector.NewOffHeapVecWithType(types.T_int64.ToType()))
	require.NoError(t, input.SetAllocationAccount(selection))
	for i := range 32 {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(i), false, proc.Mp()))
	}
	input.SetRowCount(32)

	arg := NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1))
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())

	// Pipeline cleanup resets children before parents. Simulate HashJoin
	// releasing its result batch, then verify Limit releases its accounted copy.
	input.Clean(proc.Mp())
	require.Positive(t, account.Snapshot().Used)
	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.buf)
	require.Zero(t, account.Snapshot().Used)

	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkLimit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []limitTestCase{
			{
				proc: testutil.NewProcessWithMPool(b, "", mpool.MustNewZero()),
				arg: &Limit{
					LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(8),
				},
			},
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			resetChildren(tc.arg, tc.proc.Mp())
			_, _ = vm.Exec(tc.arg, tc.proc)
			tc.arg.Free(tc.proc, false, nil)
		}
	}
}

func resetChildren(arg *Limit, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
