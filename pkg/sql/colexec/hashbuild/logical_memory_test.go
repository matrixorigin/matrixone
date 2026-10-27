// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashbuild

import (
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLogicalKeyResultDoesNotContainAllocationRounding(t *testing.T) {
	for _, rows := range []int{1, 7, 8192} {
		fixed, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{newExpr(0, types.T_int8.ToType())}, int64(rows), rows, []int64{int64(rows)})
		require.True(t, ok)
		require.Equal(t, int64(rows), fixed.LogicalResultBytes)
		require.Greater(t, fixed.ResultBytes, fixed.LogicalResultBytes, "bitmap and store rounding belong to backing capacity only")
		literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "a\x00b"}}}}
		value, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{literal}, 0, rows, []int64{})
		require.True(t, ok)
		require.Equal(t, int64(rows)*(types.VarlenaSize+3), value.LogicalResultBytes)
		null := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
		value, ok = SpillKeyMemoryBoundForColumns([]*plan.Expr{null}, 0, rows, []int64{})
		require.True(t, ok)
		require.Equal(t, int64(rows)*types.VarlenaSize, value.LogicalResultBytes)
	}
}

func TestColumnKeyContractIgnoresUnreferencedJSONAndMatchesSerialEvaluation(t *testing.T) {
	const rows = 3
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	sourceMP := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	input := batch.NewWithSize(3)
	t.Cleanup(func() { input.Clean(sourceMP) })
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	input.Vecs[1] = vector.NewVec(types.T_json.ToType())
	input.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	// The unreferenced JSON is deliberately malformed and large. Column and
	// serial executors must neither parse it nor charge it as a serial key.
	unrelated := []byte(strings.Repeat("\xff{invalid-json", 1<<14))
	for row, text := range []string{strings.Repeat("\x00x", 32), "ab", ""} {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(text), false, sourceMP))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], unrelated, false, sourceMP))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], int64(row+1), false, sourceMP))
	}
	input.SetRowCount(rows)
	columnBytes := make([]int64, len(input.Vecs))
	var inputBytes int64
	for i, vec := range input.Vecs {
		columnBytes[i] = vec.WindowSize(0, rows)
		inputBytes += columnBytes[i]
	}
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "T\x00"}}}}
	fn, err := function.GetFunctionByName(proc.Ctx, "serial_full", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	expr := contractCall("serial_full", fn.GetReturnType().Oid, newExpr(0, types.T_varchar.ToType()), newExpr(2, types.T_int64.ToType()), literal)
	expr.GetF().Func.Obj = fn.GetEncodedOverloadID()
	bound, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{expr}, inputBytes, rows, columnBytes)
	require.True(t, ok)
	largerColumns := append([]int64(nil), columnBytes...)
	largerColumns[1] = 1 << 30
	larger, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{expr}, inputBytes-columnBytes[1]+largerColumns[1], rows, largerColumns)
	require.True(t, ok)
	require.Equal(t, bound, larger, "unreferenced payload cannot change any key/result/scratch bound")
	aggregate, ok := SpillKeyMemoryBound([]*plan.Expr{expr}, inputBytes, rows)
	require.True(t, ok)
	require.Less(t, bound.ExpressionBytes, aggregate.ExpressionBytes)
	require.Less(t, bound.LogicalResultBytes, aggregate.LogicalResultBytes)

	baseline := proc.Mp().AllocatedBytes()
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(func() {
		if exec != nil {
			exec.Free()
		}
	})
	result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err, "only the referenced VARCHAR and scalar columns are evaluated")
	require.Equal(t, rows, result.Length())
	require.LessOrEqual(t, result.WindowSize(0, rows), bound.LogicalResultBytes)
	require.LessOrEqual(t, int64(result.Allocated()), bound.ResultBytes)
	var encoded int64
	for row := range rows {
		encoded += int64(len(result.GetBytesAt(row))) + 3
	}
	require.LessOrEqual(t, encoded, bound.EncodedUnit)
	require.LessOrEqual(t, proc.Mp().Stats().HighWaterMark.Load()-baseline, bound.ExpressionBytes)
	exec.Free()
	exec = nil
	require.Equal(t, baseline, proc.Mp().AllocatedBytes())
}

func TestColumnKeyContractKeepsCorrelationMultiplicityAndFiniteProofs(t *testing.T) {
	left := newExpr(0, types.T_varchar.ToType())
	right := newExpr(1, types.T_varchar.ToType())
	distinct := contractCall("serial_full", types.T_varchar, left, right)
	repeated := contractCall("serial_full", types.T_varchar, left, left)
	// Two individually wide fields still share the aggregate record limit.
	columns := []int64{100, 100}
	d, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{distinct}, 100, 1, columns)
	require.True(t, ok)
	r, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{repeated}, 100, 1, columns)
	require.True(t, ok)
	require.Greater(t, r.LogicalResultBytes, d.LogicalResultBytes, "repeated source values really can amplify the output")
	require.Greater(t, r.EncodedUnit, d.EncodedUnit)
	legacy, ok := SpillKeyMemoryBound([]*plan.Expr{distinct}, 100, 1)
	require.True(t, ok)
	require.Equal(t, legacy, d, "aggregate correlation is preserved alongside per-column information")

	// Aggregate multiplication can overflow while the independent selected
	// column proof remains finite. Conversely, an overflow in selected sums
	// can still be bounded by a small aggregate record.
	finite, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{repeated}, math.MaxInt64, 1, []int64{100})
	require.True(t, ok)
	require.Equal(t, r, finite)
	correlated, ok := SpillKeyMemoryBoundForColumns([]*plan.Expr{distinct}, 100, 1, []int64{math.MaxInt64, math.MaxInt64})
	require.True(t, ok)
	require.Equal(t, d, correlated)
	_, ok = SpillKeyMemoryBoundForColumns([]*plan.Expr{distinct}, 100, 1, []int64{100})
	require.False(t, ok, "an invalid referenced index is not a proof")
	_, ok = SpillKeyMemoryBoundForColumns([]*plan.Expr{left}, 100, 1, []int64{-1})
	require.False(t, ok)
	_, ok = SpillKeyMemoryBoundForColumns([]*plan.Expr{contractCall("json_length", types.T_int32, left)}, 100, 1, columns)
	require.False(t, ok, "column precision must not waive an unknown function allocation contract")
}
