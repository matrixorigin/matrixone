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

package group

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type groupTestCase struct {
	arg  *Argument
	flgs []bool // flgs[i] == true: nullable
	proc *process.Process
}

var (
	tcs []groupTestCase
)

func init() {
	tcs = []groupTestCase{
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{newExpression(0)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int8.ToType(),
			types.T_int16.ToType(),
		}, []*plan.Expr{newExpression(0), newExpression(1)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int8.ToType(),
			types.T_int16.ToType(),
			types.T_int32.ToType(),
			types.T_int64.ToType(),
		}, []*plan.Expr{newExpression(0), newExpression(3)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(3)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.New(types.T_varchar, 2, 0),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestGroup(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		bats := []*batch.Batch{
			newBatch(t, tc.flgs, tc.arg.Types, tc.proc, Rows),
			newBatch(t, tc.flgs, tc.arg.Types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)

		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkGroup(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []groupTestCase{
			newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
			newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{newExpression(0)}, []agg.Aggregate{{Op: function.AggSumOverloadID, E: newExpression(0)}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			bats := []*batch.Batch{
				newBatch(t, tc.flgs, tc.arg.Types, tc.proc, BenchmarkRows),
				newBatch(t, tc.flgs, tc.arg.Types, tc.proc, BenchmarkRows),
				batch.EmptyBatch,
			}
			resetChildren(tc.arg, bats)
			_, err = tc.arg.Call(tc.proc)
			require.NoError(t, err)

			tc.arg.Free(tc.proc, false, nil)
			tc.arg.GetChildren(0).Free(tc.proc, false, nil)
			tc.proc.FreeVectors()
		}
	}
}

func newTestCase(flgs []bool, ts []types.Type, exprs []*plan.Expr, aggs []agg.Aggregate) groupTestCase {
	for _, expr := range exprs {
		if col, ok := expr.Expr.(*plan.Expr_Col); ok {
			idx := col.Col.ColPos
			expr.Typ = plan.Type{
				Id:    int32(ts[idx].Oid),
				Width: ts[idx].Width,
				Scale: ts[idx].Scale,
			}
		}
	}
	return groupTestCase{
		flgs: flgs,
		proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
		arg: &Argument{
			Exprs: exprs,
			Types: ts,
			Aggs:  aggs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     1,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Argument, bats []*batch.Batch) {
	if arg.NumChildren() == 0 {
		arg.AppendChild(&value_scan.Argument{
			Batchs: bats,
		})

	} else {
		arg.SetChildren(
			[]vm.Operator{
				&value_scan.Argument{
					Batchs: bats,
				},
			})
	}
	arg.ctr.state = vm.Build
}
