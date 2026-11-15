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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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
			eq.Selectivity, bounds.Selectivity = 0.01, 0.25
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
			// The prefix and decoded range each contribute once to the scan
			// estimate; the range must reduce the prefix-only candidates.
			require.InDelta(t, 986232.7044933592, scan.Stats.Outcnt, 0.000001)
			require.Less(t, scan.Stats.Outcnt, rows*eq.Selectivity)
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

type backfillCachedStatsContext struct {
	*backfillTopStatsContext
	cache *StatsCache
}

func (c *backfillCachedStatsContext) GetStatsCache() *StatsCache { return c.cache }

// The encoded access condition and its decoded SQL recheck describe the same
// rows. Replanning must not independently apply both selectivities.
func TestIndexBackfillStatsCountEachConditionOnce(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
		want            float64
	}{
		{"prefix_range", "a=1 and b between 1 and 30", 30303.030303030304},
		{"prefix_and_residual", "a=1 and mod(b,2)=0", 50000},
		{"in_and_residual", "a in (1,2) and mod(b,2)=0", 100000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Cols = append(table.Cols, &planpb.ColDef{ColId: 4, Name: "payload", OriginName: "payload",
				Typ: planpb.Type{Id: int32(types.T_int32)}, Default: &planpb.Default{NullAbility: true}})
			table.Name2ColIndex["payload"] = 4
			stats := NewStatsInfo()
			stats.TableCnt, stats.BlockNumber = 1000000, 128
			stats.NdvMap["a"], stats.NdvMap["b"], stats.NdvMap["id"] = 10, 100, 1000000
			stats.NdvMap[catalog.IndexTableIndexColName], stats.NdvMap[catalog.IndexTablePrimaryColName] = 1000000, 1000000
			stats.DataTypeMap["a"], stats.DataTypeMap["b"] = uint64(types.T_int32), uint64(types.T_int32)
			stats.MinValMap["a"], stats.MaxValMap["a"] = 0, 9
			stats.MinValMap["b"], stats.MaxValMap["b"] = 0, 99
			cache := NewStatsCache()
			for _, tableID := range []uint64{25356, 25357, 25358, 25359, 25360} {
				cache.Set(tableID, stats)
			}
			ctx := &backfillCachedStatsContext{
				backfillTopStatsContext: &backfillTopStatsContext{MockCompilerContext: &mock.ctxt, rows: 1000000},
				cache:                   cache,
			}
			query := "select id,a,b,payload from index_hint_t force index(idx_ab) where " +
				tc.predicate + " order by b,id limit 2"
			stmts, err := mysql.Parse(ctx.GetContext(), query, 1)
			require.NoError(t, err)
			planned, err := BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)

			q := planned.GetQuery()
			seen := make(map[int32]bool)
			var index, base *planpb.Node
			var top bool
			var visit func(int32, bool)
			visit = func(id int32, inIndex bool) {
				if seen[id] {
					return
				}
				seen[id] = true
				node := q.Nodes[id]
				if node.IndexScanInfo.IsIndexScan {
					index = node
				} else if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "index_hint_t" {
					base = node
				}
				if inIndex && node.NodeType == planpb.Node_SORT {
					top = true
				}
				for i, child := range node.Children {
					visit(child, inIndex || node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_INDEX && i == 1)
				}
			}
			for _, id := range q.Steps {
				visit(id, false)
			}
			require.NotNil(t, index)
			require.NotNil(t, base)
			require.True(t, top, "the query must exercise the index-side Top")
			require.InDelta(t, tc.want, index.Stats.Outcnt, 0.000001)

			if tc.name != "prefix_and_residual" {
				var def *planpb.IndexDef
				for _, candidate := range base.TableDef.Indexes {
					if candidate.IndexName == "idx_ab" {
						def = candidate
					}
				}
				require.NotNil(t, def)
				var leading *planpb.Expr
				for _, filter := range base.FilterList {
					if fn := filter.GetF(); fn != nil && len(fn.Args) == 2 && fn.Args[0].GetCol() != nil &&
						fn.Args[0].GetCol().ColPos == base.TableDef.Name2ColIndex["a"] &&
						(fn.Func.ObjName == "=" || fn.Func.ObjName == "in") {
						leading = filter
					}
				}
				require.NotNil(t, leading)
				// Final plan lowering removes BindingTags; reconstruct the two
				// input bindings to exercise the pre-lowering proof.
				proofBase, proofIndex := DeepCopyNode(base), DeepCopyNode(index)
				proofBase.BindingTags = []int32{leading.GetF().Args[0].GetCol().RelPos}
				proofIndex.BindingTags = []int32{index.FilterList[0].GetF().Args[0].GetCol().RelPos}
				require.True(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, leading))
				otherTag := DeepCopyExpr(leading)
				otherTag.GetF().Args[0].GetCol().RelPos++
				require.False(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, otherTag))
				otherColumn := DeepCopyExpr(leading)
				otherColumn.GetF().Args[0].GetCol().ColPos = base.TableDef.Name2ColIndex["b"]
				require.False(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, otherColumn))
				otherSelectivity := DeepCopyExpr(leading)
				otherSelectivity.Selectivity = 0.7
				require.False(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, otherSelectivity))
				truncated := *def
				truncated.IndexAlgoParams = `{"prefix_lengths":"a:4"}`
				require.False(t, indexAccessAccountsForSelectivity(&truncated, proofBase, proofIndex, leading))

				if tc.name == "prefix_range" {
					sourced := DeepCopyExpr(leading)
					sourced.GetF().Args[1].GetLit().Src = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
					require.False(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, sourced))
					null := DeepCopyExpr(leading)
					null.GetF().Args[1].GetLit().Isnull = true
					require.False(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, null))
					physicalSource := DeepCopyNode(proofIndex)
					physicalSource.FilterList[0].GetF().Args[1].GetF().Args[0].GetLit().Src =
						&planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
					require.False(t, indexAccessAccountsForSelectivity(def, proofBase, physicalSource, leading))
				} else {
					malformed := DeepCopyExpr(leading)
					malformed.GetF().Args[1].GetVec().Data = []byte{0}
					require.False(t, indexAccessAccountsForSelectivity(def, proofBase, proofIndex, malformed))
					mismatch := DeepCopyNode(proofIndex)
					mismatch.FilterList[0].GetF().Args[1].GetF().Args[0].GetVec().Data = []byte{1}
					require.False(t, indexAccessAccountsForSelectivity(def, proofBase, mismatch, leading))
					withNull := DeepCopyExpr(leading)
					var values vector.Vector
					require.NoError(t, values.UnmarshalBinary(withNull.GetF().Args[1].GetVec().Data))
					values.GetNulls().Add(0)
					encoded, err := values.MarshalBinary()
					require.NoError(t, err)
					withNull.GetF().Args[1].GetVec().Data = encoded
					nullBound := DeepCopyNode(proofIndex)
					nullBound.FilterList[0].GetF().Args[1].GetF().Args[0].GetVec().Data = encoded
					require.False(t, indexAccessAccountsForSelectivity(def, proofBase, nullBound, withNull))
				}
			}

			// Explicit leaf refreshes must retain the corrected estimate and
			// the same block filters used by the execution plan.
			fresh := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			copied := DeepCopyNode(index)
			id := fresh.appendNode(copied, nil)
			require.InDelta(t, tc.want, fresh.qry.Nodes[id].Stats.Outcnt, 0.000001)
			blockFilters := len(fresh.qry.Nodes[id].BlockFilterList)
			ReCalcNodeStats(id, fresh, false, true, false)
			require.InDelta(t, tc.want, fresh.qry.Nodes[id].Stats.Outcnt, 0.000001)
			require.Len(t, fresh.qry.Nodes[id].BlockFilterList, blockFilters)
		})
	}
}
