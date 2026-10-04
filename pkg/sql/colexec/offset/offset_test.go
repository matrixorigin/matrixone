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

package offset

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
type offsetTestCase struct {
	arg   *Offset
	types []types.Type
	proc  *process.Process
}

func makeTestCases(t *testing.T) []offsetTestCase {
	return []offsetTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Offset{
				OffsetExpr: plan2.MakePlan2Uint64ConstExprWithType(8),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Offset{
				OffsetExpr: plan2.MakePlan2Uint64ConstExprWithType(10),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Offset{
				OffsetExpr: plan2.MakePlan2Uint64ConstExprWithType(12),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
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

func TestOffset(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestOffsetResetReleasesCopiedAllocationAccountData(t *testing.T) {
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

	arg := NewArgument().WithOffset(plan2.MakePlan2Uint64ConstExprWithType(1))
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 31, result.Batch.RowCount())

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

func TestSQLCalcFoundRowsRecordsInputAtEnd(t *testing.T) {
	tests := []struct {
		name    string
		batches func(proc *process.Process, input *batch.Batch) []*batch.Batch
	}{
		{
			name: "nil batch",
			batches: func(_ *process.Process, input *batch.Batch) []*batch.Batch {
				return []*batch.Batch{input}
			},
		},
		{
			name: "last batch after empty batch",
			batches: func(_ *process.Process, input *batch.Batch) []*batch.Batch {
				last := batch.NewWithSize(0)
				last.SetLast()
				return []*batch.Batch{batch.EmptyBatch, input, last}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.BeginFoundRowsStatement(true)

			input := colexec.MakeMockBatchs(proc.Mp())
			arg := NewArgument().WithOffset(plan2.MakePlan2Uint64ConstExprWithType(0)).WithFoundRows(true)
			child := colexec.NewMockOperator().WithBatchs(tt.batches(proc, input))
			arg.AppendChild(child)
			require.NoError(t, arg.Prepare(proc))

			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			require.Equal(t, input.RowCount(), result.Batch.RowCount())

			result, err = arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, uint64(input.RowCount()), proc.GetFoundRows())
			require.True(t, proc.FoundRowsRecorded())
			arg.Free(proc, false, nil)
			child.Free(proc, false, nil)
			arg.Release()
		})
	}
}

func TestNestedOffsetDoesNotPublishFoundRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.BeginFoundRowsStatement(true)

	input := colexec.MakeMockBatchs(proc.Mp())
	arg := NewArgument().WithOffset(plan2.MakePlan2Uint64ConstExprWithType(0))
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.False(t, proc.FoundRowsRecorded())

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
}

func BenchmarkOffset(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []offsetTestCase{
			{
				proc: testutil.NewProcessWithMPool(b, "", mpool.MustNewZero()),
				types: []types.Type{
					types.T_int8.ToType(),
				},
				arg: &Offset{
					ctr: container{
						seen: 0,
					},
					OffsetExpr: plan2.MakePlan2Uint64ConstExprWithType(8),
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				},
			},
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			bats := []*batch.Batch{
				newBatch(tc.types, tc.proc, BenchmarkRows),
				batch.EmptyBatch,
			}
			resetChildren(tc.arg, bats)
			_, _ = vm.Exec(tc.arg, tc.proc)
			tc.arg.Free(tc.proc, false, nil)
			tc.proc.Free()
		}
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Offset, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
