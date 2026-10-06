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

package order

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type orderTestCase struct {
	arg   *Order
	types []types.Type
	proc  *process.Process
}

func makeTestCases(t *testing.T) []orderTestCase {
	return []orderTestCase{
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}, {Expr: newExpression(1), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 2}}),
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
	}
}

func TestOrder(t *testing.T) {
	expected := [][]int8{
		{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9},
		{9, 9, 8, 8, 7, 7, 6, 6, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1, 0, 0},
		{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9},
		{9, 9, 8, 8, 7, 7, 6, 6, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1, 0, 0},
	}
	for i, tc := range makeTestCases(t) {
		t.Cleanup(func() {
			tc.arg.Free(tc.proc, false, nil)
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
		for range 2 {
			func() {
				bats := []*batch.Batch{
					newBatch(tc.types, tc.proc, Rows),
					newBatch(tc.types, tc.proc, Rows),
					batch.EmptyBatch,
				}
				child := resetChildren(tc.arg, bats)
				defer child.Free(tc.proc, false, nil)
				defer tc.arg.Reset(tc.proc, false, nil)
				require.NoError(t, tc.arg.Prepare(tc.proc))
				result, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				require.NotNil(t, result.Batch)
				require.Equal(t, len(expected[i]), result.Batch.RowCount())
				require.Len(t, result.Batch.Vecs, len(tc.types))
				require.Equal(t, types.T_int8, result.Batch.Vecs[0].GetType().Oid)
				require.Equal(t, expected[i], vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[0]))
				if len(tc.types) == 2 {
					expectedSecond := make([]int64, len(expected[i]))
					for j, value := range expected[i] {
						expectedSecond[j] = int64(value)
					}
					require.Equal(t, types.T_int64, result.Batch.Vecs[1].GetType().Oid)
					require.Equal(t, expectedSecond, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
				}
			}()
		}
	}
}

func TestOrderResetReleasesPartiallyAccumulatedAccountedBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	input := batch.NewOffHeapWithSize(2)
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	}
	require.NoError(t, input.SetAllocationAccount(selection))
	for i := range 64 {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(i), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(i), false, proc.Mp()))
	}
	input.SetRowCount(64)

	arg := &Order{}
	_, err = arg.ctr.appendBatch(proc, input)
	require.NoError(t, err)
	input.Clean(proc.Mp())
	require.Positive(t, account.Snapshot().Used)

	arg.Reset(proc, true, nil)
	require.Nil(t, arg.ctr.batWaitForSort)
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)

	arg.Free(proc, true, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkOrder(b *testing.B) {
	tcs := []orderTestCase{
		newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
	}
	for _, tc := range tcs {
		b.Cleanup(func() {
			tc.arg.Free(tc.proc, false, nil)
			require.Zero(b, tc.proc.Mp().CurrNB())
			require.Zero(b, tc.proc.Mp().OnHeapCurrNB())
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range tcs {
			func() {
				bats := []*batch.Batch{
					newBatch(tc.types, tc.proc, BenchmarkRows),
					newBatch(tc.types, tc.proc, BenchmarkRows),
					batch.EmptyBatch,
				}
				resetChildren(tc.arg, bats)
				child := tc.arg.GetChildren(0)
				defer func() {
					tc.arg.Reset(tc.proc, false, nil)
					child.Free(tc.proc, false, nil)
				}()
				require.NoError(b, tc.arg.Prepare(tc.proc))
				rows := 0
				for {
					result, err := vm.Exec(tc.arg, tc.proc)
					require.NoError(b, err)
					if result.Batch != nil {
						rows += result.Batch.RowCount()
					}
					if result.Status == vm.ExecStop {
						break
					}
				}
				require.Equal(b, 2*BenchmarkRows, rows)
			}()
		}
	}
}

func newTestCase(t testing.TB, ts []types.Type, fs []*plan.OrderBySpec) orderTestCase {
	return orderTestCase{
		types: ts,
		proc:  testutil.NewProcess(t),
		arg: &Order{
			OrderBySpec: fs,
		},
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Order, bats []*batch.Batch) *colexec.MockOperator {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
	return op
}
