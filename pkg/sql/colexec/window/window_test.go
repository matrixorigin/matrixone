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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const Rows = 100

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
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []*plan.Expr{newExpression(0)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int8.ToType(),
			types.T_int16.ToType(),
		}, []*plan.Expr{newExpression(0), newExpression(1)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int8.ToType(),
			types.T_int16.ToType(),
			types.T_int32.ToType(),
			types.T_int64.ToType(),
		}, []*plan.Expr{newExpression(0), newExpression(3)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(3)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.New(types.T_varchar, 2, 0),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase([]bool{false, true, false, true}, []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
			types.T_decimal128.ToType(),
		}, []*plan.Expr{newExpression(1), newExpression(2), newExpression(3)}, []agg.Aggregate{{Op: 0, E: newExpression(0)}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestWindow(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)

		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)

		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)

		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)

		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)
	}
}

func newTestCase(flgs []bool, ts []types.Type, exprs []*plan.Expr, aggs []agg.Aggregate) winTestCase {
	for _, expr := range exprs {
		if col, ok := expr.Expr.(*plan.Expr_Col); ok {
			idx := col.Col.ColPos
			expr.Typ = &plan.Type{
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
			info: &vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: new(plan.Type),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
// func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
// 	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
// }

// func cleanResult(result *vm.CallResult, proc *process.Process) {
// 	if result.Batch != nil {
// 		result.Batch.Clean(proc.Mp())
// 		result.Batch = nil
// 	}
// }
