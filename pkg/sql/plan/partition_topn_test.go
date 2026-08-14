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
	"fmt"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

const partitionTopNSelect = `
select *
from (
    select o_custkey, o_orderkey,
           row_number() over (partition by o_custkey order by o_orderkey, o_totalprice desc) as rn
    from orders
) t
where %s`

func TestPartitionTopNRecognizesLiteralRankBounds(t *testing.T) {
	tests := []struct {
		predicate string
		want      uint64
	}{
		{"rn = 1", 1},
		{"rn <= 2", 2},
		{"rn < 3", 2},
		{"2 >= rn", 2},
		{"3 > rn", 2},
		{"rn = 0", 0},
		{"rn <= 1024", 1024},
		{"rn <= 10 and rn < 3", 2},
		{"rn <= 2 and rn >= 2", 2},
		{"rn <= 2 and rn >= -1", 2},
		{"rn between 1 and 2", 2},
		{"rn <= 1 + 1", 2},
	}

	for _, test := range tests {
		t.Run(test.predicate, func(t *testing.T) {
			query := buildPartitionTopNPlan(t, test.predicate, false)
			partition := findBoundedPartition(query)
			require.NotNil(t, partition)
			require.Equal(t, int32(1), partition.PartitionByCount)
			require.Len(t, partition.OrderBy, 3)
			require.Equal(t, test.want, partition.Limit.GetLit().GetU64Val())
			window := findFilteredWindow(query)
			require.NotNil(t, window)
			require.Len(t, window.WinSpecList[0].GetW().OrderBy, 2)
		})
	}
}

func TestPartitionTopNFallsBackForUnsupportedShapes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"too large", formatPartitionTopNSQL("rn <= 1025")},
		{"or", formatPartitionTopNSQL("rn <= 2 or o_orderkey > 0")},
		{"wrapped rank", formatPartitionTopNSQL("rn + 0 <= 2")},
		{"negative", formatPartitionTopNSQL("rn <= -1")},
		{"in", formatPartitionTopNSQL("rn in (1, 2)")},
		{"volatile residual", formatPartitionTopNSQL("rn <= 2 and rand() >= rn")},
		{"volatile order", `select * from (select o_orderkey, row_number() over (partition by o_custkey order by rand(), o_orderkey) rn from orders) t where rn <= 2`},
		{"float partition key", `select * from (select o_orderkey, row_number() over (partition by cast(o_totalprice as double) order by o_orderkey) rn from orders) t where rn <= 2`},
		{"rank", `select * from (select o_orderkey, rank() over (partition by o_custkey order by o_orderkey) rn from orders) t where rn <= 2`},
		{"dense rank", `select * from (select o_orderkey, dense_rank() over (partition by o_custkey order by o_orderkey) rn from orders) t where rn <= 2`},
		{"no partition", `select * from (select o_orderkey, row_number() over (order by o_orderkey) rn from orders) t where rn <= 2`},
		{"no order", `select * from (select o_orderkey, row_number() over (partition by o_custkey) rn from orders) t where rn <= 2`},
		{"prior stacked window", `select * from (select o_orderkey, row_number() over (partition by o_custkey order by o_orderkey) rn, sum(o_totalprice) over (partition by o_custkey order by o_orderkey) total from orders) t where rn <= 2`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			require.Nil(t, findBoundedPartition(logicPlan.GetQuery()))
		})
	}
}

func TestPartitionTopNPreparedPlanFallsBack(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL, formatPartitionTopNSQL("rn <= 2"), 1)
	require.NoError(t, err)
	logicPlan, err := BuildPlan(ctx, stmt, true)
	require.NoError(t, err)
	require.Nil(t, findBoundedPartition(logicPlan.GetQuery()))
}

func buildPartitionTopNPlan(t *testing.T, predicate string, prepared bool) *planpb.Query {
	t.Helper()
	sql := formatPartitionTopNSQL(predicate)
	if !prepared {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err)
		return logicPlan.GetQuery()
	}
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	logicPlan, err := BuildPlan(ctx, stmt, true)
	require.NoError(t, err)
	return logicPlan.GetQuery()
}

func formatPartitionTopNSQL(predicate string) string {
	return fmt.Sprintf(partitionTopNSelect, predicate)
}

func findBoundedPartition(query *planpb.Query) *planpb.Node {
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_PARTITION && node.Limit != nil {
			return node
		}
	}
	return nil
}

func findFilteredWindow(query *planpb.Query) *planpb.Node {
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_WINDOW && len(node.FilterList) > 0 {
			return node
		}
	}
	return nil
}
