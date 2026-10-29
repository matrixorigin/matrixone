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
	"fmt"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Measures only the extra late pass, including pair construction and resetting
// its output. The SQL performance experiment separately measures full planning.
func BenchmarkBoundedDeleteIndexRuntimeFilters(b *testing.B) {
	for _, count := range []int{1, 8, 32, 96} {
		for _, limited := range []bool{false, true} {
			b.Run(fmt.Sprintf("indexes=%d/limit=%v", count, limited), func(b *testing.B) {
				builder := newBoundedDeleteFilterFixture(b)
				joins := []int32{4}
				previous := int32(4)
				for i := 1; i < count; i++ {
					indexID := int32(len(builder.qry.Nodes))
					tag := indexID + 100
					table := CloneTableDefForPlan(builder.qry.Nodes[3].TableDef, true)
					table.TblId = uint64(tag)
					builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
						NodeId: indexID, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: table,
						Stats: &planpb.Stats{Outcnt: 1_000_000, TableCnt: 1_000_000, Selectivity: 1, BlockNum: 123},
					})
					joinID := int32(len(builder.qry.Nodes))
					builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
						NodeId: joinID, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{indexID, previous},
						OnList: []*planpb.Expr{makeRuntimeFilterTestEq(table.Cols[0].Typ, tag, 2, 0, 0)},
						Stats:  &planpb.Stats{Outcnt: 1_000_000, HashmapStats: &planpb.HashMapStats{HashmapSize: 1_000_000}},
					})
					builder.deleteIndexScanTables[tag] = deleteIndexScan{tableID: table.TblId}
					builder.tag2Table[tag] = table
					stats := NewStatsInfo()
					stats.TableCnt, stats.NdvMap["id"] = 1_000_000, 1_000_000
					builder.compCtx.(*statsCacheCompilerContext).statsCache.Set(table.TblId, stats)
					joins, previous = append(joins, joinID), joinID
				}
				builder.qry.Nodes[5].Children[0] = previous
				if !limited {
					builder.qry.Nodes[1].Limit = nil
				}
				builder.generateRuntimeFilters(6)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					builder.addBoundedDeleteIndexRuntimeFilters(6)
					for _, id := range joins {
						n := builder.qry.Nodes[id]
						n.RuntimeFilterBuildList = nil
						builder.qry.Nodes[n.Children[0]].RuntimeFilterProbeList = nil
					}
				}
			})
		}
	}
}
