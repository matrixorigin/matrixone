// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestShouldUseWindowHashPartition(t *testing.T) {
	const largeBudget = int64(1 << 30)
	for _, test := range []struct {
		name                 string
		rows, groups         float64
		width, keys          int
		configured, resolved int64
		want                 bool
	}{
		{name: "large narrow input", rows: 1 << 16, groups: 64, width: 8, keys: 1, resolved: largeBudget, want: true},
		{name: "near unique output cost", rows: 1 << 16, groups: 1 << 16, width: 8, keys: 1, resolved: largeBudget},
		{name: "near unique composite output cost", rows: 1 << 16, groups: 1 << 16, width: 24, keys: 3, resolved: largeBudget},
		{name: "small input", rows: 1024, groups: 64, width: 8, keys: 1, resolved: largeBudget},
		{name: "memory unsafe", rows: 1 << 16, groups: 1 << 16, width: 256, keys: 1, resolved: 1 << 20},
		{name: "row threshold", rows: 1 << 16, groups: 64, width: 8, keys: 1, configured: 1000, resolved: 1000},
		{name: "invalid ndv", rows: 1 << 16, groups: math.NaN(), width: 8, keys: 1, resolved: largeBudget},
		{name: "ndv exceeds rows", rows: 1 << 16, groups: 1 << 17, width: 8, keys: 1, resolved: largeBudget},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := shouldUseWindowHashPartition(
				test.rows, test.groups, test.width, test.keys,
				test.configured, test.resolved,
			)
			require.Equal(t, test.want, got)
		})
	}
}

func TestDetermineWindowPartitionAlgorithms(t *testing.T) {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt = 1 << 16
	stats.NdvMap["k"] = 64
	statsCache.Set(1, stats)
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	key := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32), NotNullable: true},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0, Name: "k"}},
	}
	builder.qry = &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN, Stats: &planpb.Stats{Outcnt: 1 << 16}},
		{NodeType: planpb.Node_PARTITION, Children: []int32{0}, OrderBy: []*planpb.OrderBySpec{{Expr: key}}},
		{NodeType: planpb.Node_WINDOW, Children: []int32{1}},
	}}
	builder.tag2Table[1] = &planpb.TableDef{
		TblId: 1,
		Cols:  []*planpb.ColDef{{Name: "k", Typ: key.Typ}},
	}
	builder.aggSpillMem = 1 << 30

	builder.determineWindowPartitionAlgorithms(2)
	require.Equal(t, planpb.Node_PARTITION_ALGORITHM_SORT, builder.qry.Nodes[1].PartitionAlgorithm,
		"the blocking HASH implementation must not be selected before its end-to-end acceptance gate passes")

	stats.NdvMap["k"] = 1 << 16
	builder.qry.Nodes[1].PartitionAlgorithm = planpb.Node_PARTITION_ALGORITHM_SORT
	builder.determineWindowPartitionAlgorithms(2)
	require.Equal(t, planpb.Node_PARTITION_ALGORITHM_SORT, builder.qry.Nodes[1].PartitionAlgorithm)
}
