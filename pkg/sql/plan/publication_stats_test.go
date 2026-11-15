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
	"github.com/stretchr/testify/require"
)

func TestIndexJoinPublicationUsesCompleteFilters(t *testing.T) {
	for _, rows := range []float64{100000000, 0} {
		name := "known"
		if rows == 0 {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			ctx := &backfillTopStatsContext{MockCompilerContext: NewMockCompilerContext(true), rows: rows}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			typ := planpb.Type{Id: int32(types.T_int32)}
			table := &planpb.TableDef{Name: "t", TblId: 100001, Cols: []*planpb.ColDef{{Name: "id", Typ: typ}, {Name: "a", Typ: typ}, {Name: "b", Typ: typ}},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2}, Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
			eq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{GetColExpr(typ, 10, 1), makePlan2Int32ConstExprWithType(7)})
			require.NoError(t, err)
			bounds, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "between", []*planpb.Expr{GetColExpr(typ, 10, 2), makePlan2Int32ConstExprWithType(1), makePlan2Int32ConstExprWithType(3)})
			require.NoError(t, err)
			base := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: &planpb.ObjectRef{Obj: 100001, SchemaName: "db", ObjName: "t"}, BindingTags: []int32{10}, FilterList: []*planpb.Expr{eq, bounds}}
			builder.appendNode(base, nil)
			idx := &planpb.IndexDef{IndexName: "ix_ab", Parts: []string{"a", "b", catalog.CreateAlias("id")}, TableExist: true, IndexTableName: "idx"}
			idxTable := makeTestIndexTableDef()
			idxTable.Name = "idx"
			idxTable.TblId = 100002
			idxTable.Cols[1].Typ = typ
			leading := builder.replaceEqualCondition(idx, base.FilterList, []int32{0}, 20, idxTable)
			prepared := &preparedIndexJoin{idxDef: idx, idxTag: 20, idxObjRef: &planpb.ObjectRef{Obj: 100002, SchemaName: "db", ObjName: "idx"}, idxTableDef: idxTable, idxFilter: leading}
			joinID, indexID := builder.publishIndexJoin(prepared, base, EqualIndexCondition, []int32{0})
			scan := builder.qry.Nodes[indexID]
			join := builder.qry.Nodes[joinID]
			require.Greater(t, len(scan.FilterList), 1)
			require.Equal(t, "prefix_between", scan.FilterList[0].GetF().Func.ObjName)
			if rows == 0 {
				require.True(t, IsDefaultStats(scan.Stats))
				return
			}
			// The fully filtered estimate must shrink the leading-only candidates,
			// and remain above the unrelated historical1000-row scheduling cap.
			require.Greater(t, scan.Stats.Outcnt, float64(1000))
			require.Less(t, scan.Stats.Outcnt, rows/1000)
			require.Equal(t, base.Stats.Cost+scan.Stats.Cost, join.Stats.Cost)
			require.Equal(t, indexID, join.Children[1])
		})
	}
}

func TestScanStatsRefreshPreservesExecutionContracts(t *testing.T) {
	for _, tc := range []struct {
		name              string
		rows              float64
		reset, initialize bool
	}{
		{name: "retain", rows: 1000000},
		{name: "reset", rows: 1000000, reset: true},
		{name: "initialize", rows: 1000000, initialize: true},
		{name: "unknown stats"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &backfillTopStatsContext{MockCompilerContext: NewMockCompilerContext(true), rows: tc.rows}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			typ := planpb.Type{Id: int32(types.T_int32)}
			filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
				GetColExpr(typ, 10, 0), makePlan2Int32ConstExprWithType(7),
			})
			require.NoError(t, err)
			node := &planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10},
				ObjRef: &planpb.ObjectRef{Obj: 100001, SchemaName: "db", ObjName: "idx"},
				TableDef: &planpb.TableDef{Name: "idx", TblId: 100001,
					Cols: []*planpb.ColDef{{Name: "a", Typ: typ}}, Name2ColIndex: map[string]int32{"a": 0}},
				FilterList: []*planpb.Expr{filter},
			}
			builder.appendNode(node, nil)
			want := *node.Stats
			wantBlocks := len(node.BlockFilterList)
			hashmap := &planpb.HashMapStats{HashmapSize: 7, HashOnPK: true, Shuffle: true,
				ShuffleMethod: planpb.ShuffleMethod_Reuse, Ranges: []float64{2, 5}}
			previous := &planpb.Stats{BlockNum: -1, Cost: -1, Outcnt: -1,
				ForceOneCN: true, Dop: 3, Sql: "scan source", HashmapStats: hashmap}
			node.Stats = previous
			if tc.initialize {
				node.Stats = nil
			}
			ReCalcNodeStats(node.NodeId, builder, false, true, tc.reset)
			if !tc.reset && !tc.initialize {
				want.ForceOneCN, want.Dop, want.Sql, want.HashmapStats = true, 3, "scan source", hashmap
				require.Same(t, previous, node.Stats)
				require.Same(t, hashmap, node.Stats.HashmapStats)
			} else {
				require.NotSame(t, previous, node.Stats)
				require.NotNil(t, node.Stats.HashmapStats)
			}
			require.Equal(t, &want, node.Stats)
			require.Len(t, node.BlockFilterList, wantBlocks)
		})
	}
}
