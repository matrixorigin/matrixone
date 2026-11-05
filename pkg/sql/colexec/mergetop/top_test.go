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

package mergetop

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type testCase struct {
	ds     []bool // Directions, ds[i] == true: the attrs[i] are in descending order
	arg    *MergeTop
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

func genTestCases(t *testing.T) []testCase {
	return []testCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase(t, []bool{false, false}, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase(t, []bool{true, false}, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase(t, []bool{true, false}, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range genTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range genTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)
	}
}

func TestTop(t *testing.T) {
	for _, tc := range genTestCases(t) {
		tc.proc.Mp().EnableDetailRecording()

		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
			newBatch(tc.types, tc.proc, Rows),
		}
		resetChildren(tc.arg, bats)

		for {
			ok, err := vm.Exec(tc.arg, tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}

		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
			newBatch(tc.types, tc.proc, Rows),
		}
		resetChildren(tc.arg, bats)

		for {
			ok, err := vm.Exec(tc.arg, tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}

		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)

		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestMergeTopMaxUint64LimitReturnsAllRows(t *testing.T) {
	batchRows := 7
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int64.ToType()}, 1,
		[]*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}})
	tc.arg.Limit = plan2.MakePlan2Uint64ConstExprWithType(^uint64(0))

	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)

	bats := []*batch.Batch{
		newBatch(tc.types, tc.proc, int64(batchRows)),
	}
	resetChildren(tc.arg, bats)

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, batchRows, result.Batch.RowCount())
	require.Equal(t, vm.ExecStop, result.Status)

	tc.arg.Free(tc.proc, false, nil)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestMergeTopRepeatedOrderKey(t *testing.T) {
	for _, mode := range []struct {
		name  string
		limit int64
	}{
		{"heap_replacement", 1},
		{"all_rows", 3},
	} {
		for _, tt := range []struct {
			name          string
			first, second plan.OrderBySpec_OrderByFlag
			input, want   []any
		}{
			{"ascending", plan.OrderBySpec_ASC, plan.OrderBySpec_DESC,
				[]any{int64(2), int64(1)}, []any{int64(1), int64(2)}},
			{"descending", plan.OrderBySpec_DESC, plan.OrderBySpec_ASC,
				[]any{int64(1), int64(2)}, []any{int64(2), int64(1)}},
			{"ascending_null", plan.OrderBySpec_ASC, plan.OrderBySpec_DESC,
				[]any{int64(2), nil, int64(1)}, []any{nil, int64(1), int64(2)}},
			{"descending_null", plan.OrderBySpec_DESC, plan.OrderBySpec_ASC,
				[]any{nil, int64(1), int64(2)}, []any{int64(2), int64(1), nil}},
			{"nulls_first", plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST, plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST,
				[]any{int64(1), nil, int64(2)}, []any{nil, int64(2), int64(1)}},
			{"nulls_last", plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_LAST, plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_FIRST,
				[]any{nil, int64(2), int64(1)}, []any{int64(1), int64(2), nil}},
		} {
			t.Run(mode.name+"/"+tt.name, func(t *testing.T) {
				typ := types.T_int64.ToType()
				tc := newTestCase(t, nil, []types.Type{typ}, mode.limit, []*plan.OrderBySpec{
					{Expr: newExpression(0), Flag: tt.first},
					{Expr: newExpression(0), Flag: tt.second},
				})
				child := colexec.NewMockOperator()
				tc.arg.AppendChild(child)
				t.Cleanup(func() {
					tc.arg.Free(tc.proc, false, nil)
					child.Free(tc.proc, false, nil)
					tc.cancel()
					tc.proc.Free()
					require.Zero(t, tc.proc.Mp().CurrNB())
				})
				for _, value := range tt.input {
					bat := batch.NewWithSize(1)
					child.WithBatchs([]*batch.Batch{bat})
					if value == nil {
						bat.Vecs[0] = vector.NewConstNull(typ, 1, tc.proc.Mp())
					} else {
						bat.Vecs[0] = vector.NewVec(typ)
						require.NoError(t, vector.AppendFixed(bat.Vecs[0], value.(int64), false, tc.proc.Mp()))
					}
					bat.SetRowCount(1)
				}
				require.NoError(t, tc.arg.Prepare(tc.proc))
				result, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecStop, result.Status)
				require.NotNil(t, result.Batch)
				require.Len(t, result.Batch.Vecs, 1)
				var got []any
				vec := result.Batch.Vecs[0]
				for row := 0; row < result.Batch.RowCount(); row++ {
					if vec.IsNull(uint64(row)) {
						got = append(got, nil)
					} else {
						got = append(got, vector.GetFixedAtWithTypeCheck[int64](vec, row))
					}
				}
				want := tt.want
				if mode.limit == 1 {
					want = want[:1]
				}
				require.Equal(t, want, got)
			})
		}
	}
}

func BenchmarkTop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []testCase{
			newTestCase(b, []bool{false}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
			newTestCase(b, []bool{true}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		}

		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(b, err)

			bats := []*batch.Batch{
				newBatch(tc.types, tc.proc, Rows),
				batch.EmptyBatch,
				newBatch(tc.types, tc.proc, Rows),
			}
			resetChildren(tc.arg, bats)

			for {
				ok, err := vm.Exec(tc.arg, tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
					break
				}
			}

			tc.arg.Free(tc.proc, false, nil)
			tc.arg.GetChildren(0).Free(tc.proc, false, nil)

			tc.proc.Free()
		}
	}
}

func newTestCase(t testing.TB, ds []bool, ts []types.Type, limit int64, fs []*plan.OrderBySpec) testCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	_, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 3),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 3),
	}
	return testCase{
		ds:    ds,
		types: ts,
		proc:  proc,
		arg: &MergeTop{
			Fs:    fs,
			Limit: plan2.MakePlan2Uint64ConstExprWithType(uint64(limit)),
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		cancel: cancel,
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

// create a new block based on the type information, ds[i] == true: in descending order
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *MergeTop, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
