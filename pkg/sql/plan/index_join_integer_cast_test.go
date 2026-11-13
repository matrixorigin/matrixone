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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIndexJoinWidenedIntegerKey(t *testing.T) {
	for _, tc := range []struct {
		name           string
		source, target types.T
		want           bool
	}{
		{name: "signed_widen", source: types.T_int32, target: types.T_int64, want: true},
		{name: "unsigned_widen", source: types.T_uint16, target: types.T_uint64, want: true},
		{name: "narrowing", source: types.T_int64, target: types.T_int32},
		{name: "different_signedness", source: types.T_int32, target: types.T_uint64},
		{name: "float_conversion", source: types.T_int32, target: types.T_float64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, joinID, leftID, leftDef := makeIndexHintJoinBuilder(t)
			leftDef.Indexes = []*planpb.IndexDef{{IndexName: "idx_a", IndexTableName: "idx_join_a_table",
				Parts: []string{"a", "b", catalog.CreateAlias("id")}, TableExist: true}}
			require.NoError(t, builder.recordIndexHints(leftID, leftDef, []*tree.IndexHint{{
				HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"},
			}}))
			join := builder.qry.Nodes[joinID]
			left := builder.qry.Nodes[leftID]
			right := builder.qry.Nodes[join.Children[1]]
			leftDef.Cols[1].Typ = planpb.Type{Id: int32(tc.source)}
			right.TableDef.Cols[1].Typ = planpb.Type{Id: int32(tc.target)}
			leftKey := ftjColExpr(leftDef, left.BindingTags[0], 1)
			castKey, err := appendCastBeforeExpr(builder.GetContext(), leftKey, right.TableDef.Cols[1].Typ)
			require.NoError(t, err)
			rightKey := ftjColExpr(right.TableDef, right.BindingTags[0], 1)
			join.OnList[0] = ftjMakeEqExpr(t, castKey, rightKey)
			original := DeepCopyExpr(join.OnList[0])
			_, err = builder.applyIndicesForJoins(joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			require.Equal(t, original, join.OnList[0], "the original equality remains authoritative")
			if !tc.want {
				require.Equal(t, leftID, join.Children[0])
				require.Empty(t, join.RuntimeFilterBuildList)
				return
			}
			require.NotEqual(t, leftID, join.Children[0])
			require.Len(t, join.RuntimeFilterBuildList, 1)
			build := join.RuntimeFilterBuildList[0]
			require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1, build.KeyEncoding)
			require.Len(t, build.KeyComponentProbeTypes, 1)
			require.Equal(t, int32(tc.source), build.KeyComponentProbeTypes[0].Id)
			require.Equal(t, int32(tc.source), build.BuildExpr.GetF().Args[0].Typ.Id)
			require.Equal(t, "cast", build.BuildExpr.GetF().Args[0].GetF().Func.ObjName)
			require.Equal(t, int32(0), build.BuildExpr.GetF().Args[0].GetF().Args[0].GetCol().ColPos)
		})
	}
}

func TestIndexJoinWidenedIntegerKeyRetainsHashSlotOrder(t *testing.T) {
	builder, joinID, leftID, leftDef := makeIndexHintJoinBuilder(t)
	leftDef.Indexes = []*planpb.IndexDef{{IndexName: "idx_a", IndexTableName: "idx_join_a_table",
		Parts: []string{"a", "b", catalog.CreateAlias("id")}, TableExist: true}}
	require.NoError(t, builder.recordIndexHints(leftID, leftDef, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"},
	}}))
	join := builder.qry.Nodes[joinID]
	left, right := builder.qry.Nodes[leftID], builder.qry.Nodes[join.Children[1]]
	right.TableDef.Cols[1].Typ = planpb.Type{Id: int32(types.T_int64)}
	widened, err := appendCastBeforeExpr(builder.GetContext(), ftjColExpr(leftDef, left.BindingTags[0], 1), right.TableDef.Cols[1].Typ)
	require.NoError(t, err)
	// The JOIN's hash slots are b,a, while the physical index key is a,b.
	join.OnList = []*planpb.Expr{
		ftjMakeEqExpr(t, ftjColExpr(leftDef, left.BindingTags[0], 2), ftjColExpr(right.TableDef, right.BindingTags[0], 0)),
		ftjMakeEqExpr(t, widened, ftjColExpr(right.TableDef, right.BindingTags[0], 1)),
	}
	original := DeepCopyExprList(join.OnList)
	_, err = builder.applyIndicesForJoins(joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, original, join.OnList)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	args := join.RuntimeFilterBuildList[0].BuildExpr.GetF().Args
	require.Len(t, args, 2)
	require.Equal(t, "cast", args[0].GetF().Func.ObjName)
	require.Equal(t, int32(1), args[0].GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(0), args[1].GetCol().ColPos)
}
