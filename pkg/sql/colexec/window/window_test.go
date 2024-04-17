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

package window

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// add unit tests for cases
type winTestCase struct {
	arg  *Argument
	flgs []bool // flgs[i] == true: nullable
	proc *process.Process
}

var (
	tcs []winTestCase
)

func init() {
	tcs = []winTestCase{
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{newExpression(0)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int8.ToType(),
			types.T_int16.ToType(),
		}, []*plan.Expr{newExpression(0), newExpression(1)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int8.ToType(),
			types.T_int16.ToType(),
			types.T_int32.ToType(),
			types.T_int64.ToType(),
		}, []*plan.Expr{newExpression(0), newExpression(3)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(3)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.New(types.T_varchar, 2, 0),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newExpression(0)}, nil),
		}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func newTestCase(flgs []bool, ts []types.Type, exprs []*plan.Expr, aggs []aggexec.AggFuncExecExpression) winTestCase {
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
	return winTestCase{
		flgs: flgs,
		proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
		arg: &Argument{
			WinSpecList: exprs,
			Types:       ts,
			Aggs:        aggs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
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
// func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
// 	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
// }

// func cleanResult(result *vm.CallResult, proc *process.Process) {
// 	if result.Batch != nil {
// 		result.Batch.Clean(proc.Mp())
// 		result.Batch = nil
// 	}
// }
