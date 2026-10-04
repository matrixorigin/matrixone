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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestReplaceFoldExprPreservesCastTargetMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		source   types.Type
		value    uint64
		wantHex  string
		ordinary bool
	}{
		{name: "hex BIT8 unsigned", source: types.New(types.T_bit, 8, 0), value: 170, wantHex: "AA"},
		{name: "hex BIT64 upper boundary", source: types.New(types.T_bit, 64, 0), value: ^uint64(0), wantHex: "FFFFFFFFFFFFFFFF"},
		{name: "hex BOOL signed", source: types.T_bool.ToType(), wantHex: "1"},
		{name: "ordinary CAST column", source: types.New(types.T_bit, 8, 0), value: 170, ordinary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			column := &planpb.Expr{
				Typ: makePlan2Type(&tc.source),
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					Name: "v", ColPos: 0,
				}},
			}
			var left, cast, right *planpb.Expr
			var err error
			if tc.ordinary {
				target := types.T_uint64.ToType()
				left, err = BindFuncExprImplByPlanExpr(t.Context(), "cast", []*planpb.Expr{
					column,
					{Typ: makePlan2Type(&target), Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
				})
				require.NoError(t, err)
				cast = left
				right = makePlan2Uint64ConstExprWithType(tc.value)
			} else {
				left, err = BindFuncExprImplByPlanExpr(t.Context(), "hex", []*planpb.Expr{column})
				require.NoError(t, err)
				cast = left.GetF().Args[0]
				_, overload := function.DecodeOverloadID(cast.GetF().Func.Obj)
				require.Equal(t, function.IntegerArgumentCastOverload, overload,
					"the real binder must reach the private integer CAST")
				right = makePlan2StringConstExprWithType(tc.wantHex)
			}
			require.Equal(t, "cast", cast.GetF().Func.ObjName)
			targetMarker := cast.GetF().Args[1]
			require.NotNil(t, targetMarker.GetT())
			filter, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*planpb.Expr{left, right})
			require.NoError(t, err)
			var executors []colexec.ExpressionExecutor
			t.Cleanup(func() {
				for _, executor := range executors {
					executor.Free()
				}
			})
			canFold, err := ReplaceFoldExpr(proc, filter, &executors)
			require.NoError(t, err)
			require.False(t, canFold, "a column-dependent predicate is not a constant")
			require.Same(t, targetMarker, cast.GetF().Args[1],
				"a CAST type marker is metadata, not an execution-time scalar")
			require.NotNil(t, cast.GetF().Args[1].GetT())
			require.NotNil(t, filter.GetF().Args[1].GetFold(),
				"preserving metadata must not disable ordinary constant folding")
			require.Len(t, executors, 1, "only the comparison value needs a fold executor")
			require.NoError(t, EvalFoldExpr(proc, filter, &executors))
			foldedValue := filter.GetF().Args[1].GetFold()
			require.True(t, foldedValue.IsConst)
			if tc.ordinary {
				require.Len(t, foldedValue.Data, 8)
				require.Equal(t, tc.value, types.DecodeUint64(foldedValue.Data))
			} else {
				require.Equal(t, tc.wantHex, string(foldedValue.Data))
			}

			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(tc.source)
			if tc.source.Oid == types.T_bool {
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{true, false, false}, []bool{false, false, true}, proc.Mp()))
			} else {
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []uint64{tc.value, 0, 0}, []bool{false, false, true}, proc.Mp()))
			}
			input.SetRowCount(3)
			// Fold nodes belong to the storage-filter consumer, not the generic
			// expression executor. Check their bytes above and independently run
			// the preserved row-dependent CAST/HEX with ordinary batch evaluation.
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, left, []*batch.Batch{input})
			require.NoError(t, err)
			t.Cleanup(free)
			if tc.ordinary {
				require.Equal(t, []uint64{tc.value, 0}, vector.MustFixedColWithTypeCheck[uint64](result)[:2])
			} else {
				require.Equal(t, tc.wantHex, result.GetStringAt(0))
				require.Equal(t, "0", result.GetStringAt(1))
			}
			require.False(t, result.IsNull(0))
			require.False(t, result.IsNull(1))
			require.True(t, result.IsNull(2), "a NULL source must remain NULL through CAST/HEX")
		})
	}
}

func TestReplaceFoldExprStillFoldsWholeConstantCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	target := types.T_uint64.ToType()
	cast, err := BindFuncExprImplByPlanExpr(t.Context(), "cast", []*planpb.Expr{
		makePlan2StringConstExprWithType("7"),
		{Typ: makePlan2Type(&target), Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
	})
	require.NoError(t, err)
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	canFold, err := ReplaceFoldExpr(proc, cast, &executors)
	require.NoError(t, err)
	require.True(t, canFold, "the type marker must not make a constant CAST non-foldable")
	require.Empty(t, executors)

	column := &planpb.Expr{
		Typ: makePlan2Type(&target),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			Name: "v", ColPos: 0,
		}},
	}
	filter, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*planpb.Expr{column, cast})
	require.NoError(t, err)
	canFold, err = ReplaceFoldExpr(proc, filter, &executors)
	require.NoError(t, err)
	require.False(t, canFold)
	require.NotNil(t, filter.GetF().Args[1].GetFold())
	require.Len(t, executors, 1, "the whole constant CAST still has one cached value")
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	foldedValue := filter.GetF().Args[1].GetFold()
	require.True(t, foldedValue.IsConst)
	require.Len(t, foldedValue.Data, 8)
	require.Equal(t, uint64(7), types.DecodeUint64(foldedValue.Data))
	result, err := executors[0].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsConstNull())
	require.Equal(t, uint64(7), vector.GetFixedAtWithTypeCheck[uint64](result, 0))
}
