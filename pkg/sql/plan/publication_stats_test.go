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
		name, predicate, order string
		ndvA, ndvB, want       float64
	}{
		{"prefix_range", "a=1 and b between 1 and 30", "b,id", 10, 100, 30303.030303030304},
		{"prefix_and_residual", "a=1 and mod(b,2)=0", "b,id", 10, 100, 50000},
		{"in_and_residual", "a in (1,2) and mod(b,2)=0", "b,id", 10, 100, 100000},
		{"multi_prefix_range", "a=1 and b=1 and c between 1 and 30", "c,id", 2, 2, 75757.57575757576},
		{"multi_prefix_lower", "a=1 and b=1 and c>10", "c,id", 2, 2, 227272.72727272726},
		{"multi_prefix_upper", "a=1 and b=1 and c<=30", "c,id", 2, 2, 78282.82828282828},
		{"multi_prefix_residual", "a=1 and b=1 and c between 1 and 30 and mod(c,2)=0", "c,id", 2, 2, 37878.78787878788},
		{"multi_prefix_low_selectivity", "a=1 and b=1 and c between 1 and 30", "c,id", 100, 100, 99.988061488},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			for _, name := range []string{"c", "payload"} {
				pos := int32(len(table.Cols))
				table.Cols = append(table.Cols, &planpb.ColDef{ColId: uint64(pos), Name: name, OriginName: name,
					Typ: planpb.Type{Id: int32(types.T_int32)}, Default: &planpb.Default{NullAbility: true}})
				table.Name2ColIndex[name] = pos
			}
			for _, idx := range table.Indexes {
				if idx.IndexName == "idx_ab" {
					idx.Parts = []string{"a", "b", "c", catalog.CreateAlias("id")}
				}
			}
			stats := NewStatsInfo()
			stats.TableCnt, stats.BlockNumber = 1000000, 128
			stats.NdvMap["a"], stats.NdvMap["b"], stats.NdvMap["c"], stats.NdvMap["id"] = tc.ndvA, tc.ndvB, 100, 1000000
			stats.NdvMap[catalog.IndexTableIndexColName], stats.NdvMap[catalog.IndexTablePrimaryColName] = 1000000, 1000000
			for _, name := range []string{"a", "b", "c"} {
				stats.DataTypeMap[name] = uint64(types.T_int32)
				stats.MinValMap[name], stats.MaxValMap[name] = 0, 99
			}
			cache := NewStatsCache()
			for _, tableID := range []uint64{25356, 25357, 25358, 25359, 25360} {
				cache.Set(tableID, stats)
			}
			ctx := &backfillCachedStatsContext{
				backfillTopStatsContext: &backfillTopStatsContext{MockCompilerContext: &mock.ctxt, rows: 1000000},
				cache:                   cache,
			}
			query := "select id,a,b,c,payload from index_hint_t force index(idx_ab) where " +
				tc.predicate + " order by " + tc.order + " limit 2"
			stmts, err := mysql.Parse(ctx.GetContext(), query, 1)
			require.NoError(t, err)
			planned, err := BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)

			q := planned.GetQuery()
			seen := make(map[int32]bool)
			var index, base, top *planpb.Node
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
				if inIndex && node.NodeType == planpb.Node_SORT && node.Limit != nil {
					top = node
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
			require.NotNil(t, top, "the query must exercise the index-side Top")
			require.InDelta(t, tc.want, index.Stats.Outcnt, 0.000001)
			require.InDelta(t, tc.want, top.Stats.Cost, 0.000001)

			// The rechecks remain executable and retain their independent
			// selectivity. Only the scan's estimation view neutralizes predicates
			// already implied by the encoded access.
			if tc.name == "prefix_range" || tc.name == "in_and_residual" || tc.name == "multi_prefix_range" {
				view := indexScanSelectivityFilters(index)
				rechecks := make([]int, 0, 2)
				for i, filter := range index.FilterList {
					fn := filter.GetF()
					if fn == nil || fn.Func == nil || len(fn.Args) != 2 ||
						(fn.Func.ObjName != "=" && fn.Func.ObjName != "in") {
						continue
					}
					extract := fn.Args[0].GetF()
					if extract != nil && extract.Func != nil && extract.Func.ObjName == "serial_extract" {
						rechecks = append(rechecks, i)
						require.NotEqual(t, float64(1), filter.Selectivity)
						require.Equal(t, float64(1), view[i].Selectivity)
					}
				}
				wantRechecks := 1
				if tc.name == "multi_prefix_range" {
					wantRechecks = 2
				}
				require.Len(t, rechecks, wantRechecks)

				recheck := rechecks[0]
				unchanged := func(n *planpb.Node) {
					t.Helper()
					estimates := indexScanSelectivityFilters(n)
					require.Equal(t, n.FilterList[recheck].Selectivity, estimates[recheck].Selectivity)
				}
				otherKey := DeepCopyNode(index)
				otherKey.FilterList[recheck].GetF().Args[0].GetF().Args[0].GetCol().RelPos++
				unchanged(otherKey)
				otherComponent := DeepCopyNode(index)
				otherComponent.FilterList[recheck].GetF().Args[0].GetF().Args[1].GetLit().Value =
					&planpb.Literal_I64Val{I64Val: 10}
				unchanged(otherComponent)
				malformedPosition := DeepCopyNode(index)
				malformedPosition.FilterList[recheck].GetF().Args[0].GetF().Args[1].GetLit().Value =
					(*planpb.Literal_I64Val)(nil)
				unchanged(malformedPosition)

				// A semantic proof must not depend on coincidentally equal
				// floating-point selectivities.
				otherSelectivity := DeepCopyNode(index)
				otherSelectivity.FilterList[recheck].Selectivity = 0.7
				require.Equal(t, float64(1), indexScanSelectivityFilters(otherSelectivity)[recheck].Selectivity)
				require.Equal(t, 0.7, otherSelectivity.FilterList[recheck].Selectivity)

				if tc.name == "multi_prefix_range" {
					// A full equality prefix supplies the same proof without a range.
					equalityAccess := DeepCopyNode(index)
					bound := equalityAccess.FilterList[0].GetF()
					prefix := DeepCopyExpr(bound.Args[1])
					prefix.GetF().Args = prefix.GetF().Args[:2]
					equalityAccess.FilterList[0], err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "prefix_eq",
						[]*planpb.Expr{bound.Args[0], prefix})
					require.NoError(t, err)
					equalityView := indexScanSelectivityFilters(equalityAccess)
					for _, pos := range rechecks {
						require.Equal(t, float64(1), equalityView[pos].Selectivity)
					}

					// Reordering executable filters must not lose the proof.
					reordered := DeepCopyNode(index)
					reordered.FilterList[0], reordered.FilterList[rechecks[0]] =
						reordered.FilterList[rechecks[0]], reordered.FilterList[0]
					reorderedView := indexScanSelectivityFilters(reordered)
					require.Equal(t, float64(1), reorderedView[0].Selectivity)
					require.Equal(t, float64(1), reorderedView[rechecks[1]].Selectivity)

					// Equal later components do not form a fixed prefix if an earlier
					// component differs between the two physical bounds.
					mismatchFirst := DeepCopyNode(index)
					mismatchFirst.FilterList[0].GetF().Args[1].GetF().Args[0] = makePlan2Int32ConstExprWithType(2)
					firstView := indexScanSelectivityFilters(mismatchFirst)
					for _, pos := range rechecks {
						require.Equal(t, mismatchFirst.FilterList[pos].Selectivity, firstView[pos].Selectivity)
					}

					mismatchSecond := DeepCopyNode(index)
					mismatchSecond.FilterList[0].GetF().Args[1].GetF().Args[1] = makePlan2Int32ConstExprWithType(2)
					secondView := indexScanSelectivityFilters(mismatchSecond)
					for _, pos := range rechecks {
						extract := mismatchSecond.FilterList[pos].GetF().Args[0].GetF()
						part := extract.Args[1].GetLit().GetI64Val()
						if part == 0 {
							require.Equal(t, float64(1), secondView[pos].Selectivity)
						} else {
							require.Equal(t, mismatchSecond.FilterList[pos].Selectivity, secondView[pos].Selectivity)
						}
					}
				}

				if tc.name != "in_and_residual" {
					sourced := DeepCopyNode(index)
					sourced.FilterList[recheck].GetF().Args[1].GetLit().Src =
						&planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
					unchanged(sourced)
					physicalSource := DeepCopyNode(index)
					physicalSource.FilterList[0].GetF().Args[1].GetF().Args[0].GetLit().Src =
						&planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
					unchanged(physicalSource)
					null := DeepCopyNode(index)
					null.FilterList[recheck].GetF().Args[1].GetLit().Isnull = true
					unchanged(null)
					mismatch := DeepCopyNode(index)
					mismatch.FilterList[recheck].GetF().Args[1] = makePlan2Int32ConstExprWithType(2)
					unchanged(mismatch)
				} else {
					malformed := DeepCopyNode(index)
					malformed.FilterList[recheck].GetF().Args[1].GetVec().Data = []byte{0}
					malformed.FilterList[0].GetF().Args[1].GetF().Args[0].GetVec().Data = []byte{0}
					unchanged(malformed)
					mismatch := DeepCopyNode(index)
					mismatch.FilterList[recheck].GetF().Args[1].GetVec().Data = []byte{1}
					unchanged(mismatch)
					withNull := DeepCopyNode(index)
					var values vector.Vector
					require.NoError(t, values.UnmarshalBinary(withNull.FilterList[recheck].GetF().Args[1].GetVec().Data))
					values.GetNulls().Add(0)
					encoded, err := values.MarshalBinary()
					require.NoError(t, err)
					withNull.FilterList[recheck].GetF().Args[1].GetVec().Data = encoded
					access := withNull.FilterList[0].GetF()
					access.Args[1].GetF().Args[0].GetVec().Data = encoded
					unchanged(withNull)
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
