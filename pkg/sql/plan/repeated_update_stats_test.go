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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRepeatedUpdateStatsBoundJoinFanout(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		matches, first, second, total float64
		roleRows, physicalRows        float64
	}{
		{"large target", 160000, 160000, 160000, 160000, 320000, 160000},
		{"fanout on small target", 1000000, 1, 1, 100, 2, 2},
		{"different role filters", 100, 5, 50, 1000, 55, 55},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, false)
			filter := makePlan2BoolConstExprWithType(true)
			stats := func(rows, total float64) *planpb.Stats {
				return &planpb.Stats{Outcnt: rows, TableCnt: total, Cost: rows, Selectivity: 1, HashmapStats: &planpb.HashMapStats{}}
			}
			builder.qry.Nodes = []*planpb.Node{
				{NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{TblId: 7}, Stats: stats(tc.first, tc.total)},
				{NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{TblId: 7}, Stats: stats(tc.second, tc.total)},
				{NodeType: planpb.Node_PROJECT, Stats: stats(tc.matches, tc.matches)},
				{NodeType: planpb.Node_APPLY, Children: []int32{2, 4}, Stats: DefaultStats()},
				{NodeType: planpb.Node_FUNCTION_SCAN, Stats: DefaultStats()},
				{NodeType: planpb.Node_AGG, Children: []int32{3}, GroupBy: []*planpb.Expr{makePlan2Int32ConstExprWithType(0)}, Stats: DefaultStats()},
				{NodeType: planpb.Node_FILTER, Children: []int32{5}, FilterList: []*planpb.Expr{filter}, Stats: DefaultStats()},
				{NodeType: planpb.Node_UNION_ALL, Stats: stats(tc.roleRows, tc.total)},
				{NodeType: planpb.Node_AGG, Children: []int32{7}, GroupBy: []*planpb.Expr{makePlan2Int32ConstExprWithType(0)}, Stats: DefaultStats()},
			}
			source := &repeatedUpdateStatsSource{apply: 3, scans: []int32{0, 1}, tables: []uint64{7, 7}}
			builder.setRepeatedUpdateStats(3, repeatedUpdateStatsHint{kind: repeatedStatsExpand, source: source})
			builder.setRepeatedUpdateStats(5, repeatedUpdateStatsHint{kind: repeatedStatsRoles, source: source})
			builder.setRepeatedUpdateStats(6, repeatedUpdateStatsHint{kind: repeatedStatsRole, source: source, role: 0, filters: []*planpb.Expr{filter}})
			builder.setRepeatedUpdateStats(8, repeatedUpdateStatsHint{kind: repeatedStatsParticipants, source: source, roles: []int{0, 1}})
			for _, id := range []int32{3, 5, 6, 8} {
				ReCalcNodeStats(id, builder, false, false, true)
			}
			require.Equal(t, tc.matches*2, builder.qry.Nodes[3].Stats.Outcnt)
			require.Equal(t, tc.roleRows, builder.qry.Nodes[5].Stats.Outcnt)
			require.Equal(t, tc.first, builder.qry.Nodes[6].Stats.Outcnt)
			require.Equal(t, tc.physicalRows, builder.qry.Nodes[8].Stats.Outcnt)
			require.Equal(t, tc.physicalRows, builder.qry.Nodes[8].Stats.HashmapStats.HashmapSize)
		})
	}
}
