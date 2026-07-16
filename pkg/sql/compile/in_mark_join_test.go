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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestCanUseHashMarkJoin(t *testing.T) {
	tests := []struct {
		name       string
		conditions []*plan.Expr
		want       bool
	}{
		{
			name:       "single nullable equality",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, false)},
			want:       true,
		},
		{
			name: "composite non-null equality",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "=", 1, true),
			},
			want: true,
		},
		{
			name: "composite nullable equality needs row-aware null evaluation",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "=", 1, false),
			},
			want: false,
		},
		{
			name:       "non-equality condition",
			conditions: []*plan.Expr{makeMarkJoinTestCondition(t, "<", 0, true)},
			want:       false,
		},
		{
			name: "hash key plus residual non-equality retains loop join",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestCondition(t, "<", 1, true),
			},
			want: false,
		},
		{
			name:       "same-side equality is not a hash join key",
			conditions: []*plan.Expr{makeMarkJoinTestSameSideCondition(t)},
			want:       false,
		},
		{
			name: "hash key plus mixed-side correlated equality retains loop join",
			conditions: []*plan.Expr{
				makeMarkJoinTestCondition(t, "=", 0, true),
				makeMarkJoinTestMixedSideCondition(t),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_MARK,
				OnList:   tt.conditions,
			}
			require.Equal(t, tt.want, canUseHashMarkJoin(node))
		})
	}
}

func makeMarkJoinTestSameSideCondition(t *testing.T) *plan.Expr {
	t.Helper()

	condition := makeMarkJoinTestCondition(t, "=", 0, true)
	condition.GetF().Args[1].GetCol().RelPos = 0
	return condition
}

// makeMarkJoinTestMixedSideCondition models a pulled-up correlated predicate
// such as t2.z + t1.c = t1.d. The left operand cannot be a hash key because it
// references both the build and probe relations.
func makeMarkJoinTestMixedSideCondition(t *testing.T) *plan.Expr {
	t.Helper()

	typ := types.T_int64.ToType()
	plus, err := function.GetFunctionByName(context.Background(), "+", []types.Type{typ, typ})
	require.NoError(t, err)
	equal, err := function.GetFunctionByName(context.Background(), "=", []types.Type{typ, typ})
	require.NoError(t, err)

	mixedOperand := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: plus.GetEncodedOverloadID(), ObjName: "+"},
			Args: []*plan.Expr{
				makeMarkJoinTestColumn(1, 1, true),
				makeMarkJoinTestColumn(0, 1, true),
			},
		}},
	}

	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: equal.GetEncodedOverloadID(), ObjName: "="},
			Args: []*plan.Expr{
				mixedOperand,
				makeMarkJoinTestColumn(0, 2, true),
			},
		}},
	}
}

func TestConstructBroadcastHashBuildForMark(t *testing.T) {
	op := hashjoin.NewArgument()
	op.JoinType = plan.Node_MARK
	op.JoinMapTag = 1
	op.EqConds = [][]*plan.Expr{
		{makeMarkJoinTestColumn(0, 0, false)},
		{makeMarkJoinTestColumn(1, 0, false)},
	}
	defer op.Release()

	build := constructBroadcastHashBuild(op, nil, 1)
	defer build.Release()

	require.True(t, build.NeedHashMap)
	require.False(t, build.NeedAllocateSels)
	require.False(t, build.NeedBatches)
	require.True(t, build.TrackNullKeys)
}

func makeMarkJoinTestCondition(t *testing.T, name string, colPos int32, notNullable bool) *plan.Expr {
	t.Helper()

	typ := types.T_int64.ToType()
	overload, err := function.GetFunctionByName(context.Background(), name, []types.Type{typ, typ})
	require.NoError(t, err)

	args := make([]*plan.Expr, 2)
	for i := range args {
		args[i] = makeMarkJoinTestColumn(int32(i), colPos, notNullable)
	}

	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: notNullable,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     overload.GetEncodedOverloadID(),
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func makeMarkJoinTestColumn(relPos, colPos int32, notNullable bool) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: notNullable,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
			},
		},
	}
}
