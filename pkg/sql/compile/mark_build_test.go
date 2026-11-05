// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestBroadcastMarkRejectsErrorProneKey(t *testing.T) {
	for side := 0; side < 2; side++ {
		typ := types.T_int64.ToType()
		eq, err := function.GetFunctionByName(context.Background(), "=", []types.Type{typ, typ})
		require.NoError(t, err)
		plus, err := function.GetFunctionByName(context.Background(), "+", []types.Type{typ, typ})
		require.NoError(t, err)
		args := make([]*plan.Expr, 2)
		for i := range args {
			args[i] = &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: int32(i)}}}
		}
		args[side] = &plan.Expr{Typ: args[side].Typ, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: plus.GetEncodedOverloadID(), ObjName: "+"}, Args: []*plan.Expr{
				args[side], {Typ: args[side].Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
			},
		}}}
		cond := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: eq.GetEncodedOverloadID(), ObjName: "="}, Args: args,
		}}}
		join := &loopjoin.LoopJoin{NonEqCond: cond, JoinType: plan.Node_MARK, JoinMapTag: 1}
		dup := dupOperator(join, 0, 4).(*loopjoin.LoopJoin)
		t.Cleanup(dup.Release)
		for _, op := range []*loopjoin.LoopJoin{join, dup} {
			build := constructBroadcastHashBuild(op, nil, 4)
			t.Cleanup(build.Release)
			require.False(t, build.NeedHashMap, "deterministic arithmetic may error before the pairwise evaluator reaches it")
			require.False(t, build.OptionalIndex)
			require.True(t, build.NeedBatches)
		}
	}
}

func TestBroadcastMarkBuildIndex(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      types.T
		op       string
		joinType plan.Node_JoinType
		indexed  bool
	}{
		{"integer equality", types.T_int32, "=", plan.Node_MARK, true},
		{"string equality", types.T_varchar, "=", plan.Node_MARK, true},
		{"float equality", types.T_float64, "=", plan.Node_MARK, true},
		{"float array fallback", types.T_array_float64, "=", plan.Node_MARK, false},
		{"comparison fallback", types.T_int32, "<", plan.Node_MARK, false},
		{"other join fallback", types.T_int32, "=", plan.Node_LEFT, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fr, err := function.GetFunctionByName(context.Background(), tc.op, []types.Type{tc.typ.ToType(), tc.typ.ToType()})
			require.NoError(t, err)
			probe := &plan.Expr{Typ: plan.Type{Id: int32(tc.typ)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
			build := &plan.Expr{Typ: probe.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}}
			cond := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: fr.GetEncodedOverloadID(), ObjName: tc.op}, Args: []*plan.Expr{probe, build},
			}}}
			join := &loopjoin.LoopJoin{NonEqCond: cond, JoinType: tc.joinType, JoinMapTag: 1}
			dup := dupOperator(join, 0, 4).(*loopjoin.LoopJoin)
			t.Cleanup(dup.Release)
			for _, op := range []*loopjoin.LoopJoin{join, dup} {
				b := constructBroadcastHashBuild(op, nil, 4)
				require.Equal(t, tc.indexed, b.NeedHashMap)
				require.Equal(t, tc.indexed, b.OptionalIndex)
				require.True(t, b.NeedBatches, "NULL summary needs the original rows")
				require.False(t, b.CanSpill, "LoopJoin cannot consume spilled hash maps")
				require.Equal(t, int32(4), b.JoinMapRefCnt)
				if tc.indexed {
					require.False(t, b.NeedAllocateSels, "membership needs no row lists")
					require.Len(t, b.Conditions, 1)
					require.Equal(t, int32(0), b.Conditions[0].GetCol().RelPos)
					require.NotSame(t, build, b.Conditions[0])
				}
				require.Equal(t, int32(1), build.GetCol().RelPos, "build remap must not mutate MARK predicate")
				b.Release()
			}
		})
	}
}
