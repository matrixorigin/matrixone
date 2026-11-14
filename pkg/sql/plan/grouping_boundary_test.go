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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestGroupingOutputFilterBoundary(t *testing.T) {
	for _, query := range []string{
		"select k from (select n_nationkey k from nation group by n_nationkey with rollup) t where k=1",
		"select k from (select n_nationkey k from nation group by n_nationkey with rollup) t where k is null",
	} {
		p, err := buildSingleStmt(NewMockOptimizer(false), t, query)
		require.NoError(t, err)
		protected := 0
		for _, n := range p.GetQuery().Nodes {
			if n.NodeType == plan.Node_AGG && len(n.GroupingFlag) == 1 && !n.GroupingFlag[0] {
				require.NotEmpty(t, n.FilterList, "output predicates cannot be substituted with the omitted input key")
				protected++
			}
		}
		require.Equal(t, 1, protected)
	}
}

func TestGroupingCommonKeyFilterStillPushesDown(t *testing.T) {
	p, err := buildSingleStmt(NewMockOptimizer(false), t,
		"select k,r from (select n_nationkey k,n_regionkey r from nation group by grouping sets ((n_nationkey,n_regionkey),(n_nationkey))) t where k=1")
	require.NoError(t, err)
	found, scans := 0, 0
	for _, n := range p.GetQuery().Nodes {
		if n.NodeType == plan.Node_AGG && len(n.GroupingFlag) == 2 && n.GroupingFlag[0] && !n.GroupingFlag[1] {
			require.Empty(t, n.FilterList, "a predicate on a present key can still move to the input")
			found++
		}
		if n.NodeType == plan.Node_TABLE_SCAN && len(n.FilterList) > 0 {
			scans++
		}
	}
	require.Equal(t, 1, found)
	require.Positive(t, scans)
}

func TestGlobalGroupingOutputFilterBoundary(t *testing.T) {
	for _, query := range []string{
		"select * from (select 1 k from nation group by grouping sets (())) t where k=0",
		"select * from (select count(*) k from nation) t where false",
	} {
		p, err := buildSingleStmt(NewMockOptimizer(false), t, query)
		require.NoError(t, err)
		protected := 0
		for _, n := range p.GetQuery().Nodes {
			if n.NodeType == plan.Node_AGG && len(n.GroupBy) == 0 {
				require.NotEmpty(t, n.FilterList, "a global aggregate still emits a row after an input filter removes all input")
				protected++
			}
		}
		require.Positive(t, protected)
	}
}

func TestGroupingWindowRepeatedPlanning(t *testing.T) {
	mock := NewMockOptimizer(false)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table grouped_window as select nation.n_nationkey k,count(*) over() n from nation group by nation.n_nationkey with rollup order by nation.n_nationkey limit 2", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	var original string
	for generation := 0; generation < 3; generation++ {
		p, err := BuildPlan(mock.CurrentContext(), stmt, false)
		require.NoError(t, err)
		current := p.GetDdl().GetCreateTable().CreateAsSelectSql
		if generation == 0 {
			original = current
		} else {
			require.Equal(t, original, current, "grouping-stage state cannot escape into the reused AST")
		}
	}
}

func TestGroupingProjectionProvenance(t *testing.T) {
	p, err := buildSingleStmt(NewMockOptimizer(false), t,
		"select n_nationkey,n_regionkey,null product,sum(cast(n_nationkey as decimal(10,2))) s from nation group by n_nationkey,n_regionkey with rollup")
	require.NoError(t, err)
	q := p.GetQuery()
	out := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList
	require.Equal(t, int32(types.T_decimal128), out[3].Typ.Id, "a user NULL must not erase the transport aggregate's numeric type")
	p, err = buildSingleStmt(NewMockOptimizer(false), t,
		"select null x from nation group by n_regionkey with rollup union all select 42")
	require.NoError(t, err)
	q = p.GetQuery()
	out = q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList
	require.Equal(t, int32(types.T_int64), out[0].Typ.Id, "bare-NULL provenance must be restored for the enclosing UNION")
}

func TestGroupingDistinctHiddenOrderBoundary(t *testing.T) {
	_, err := buildSingleStmt(NewMockOptimizer(false), t,
		"select distinct count(*) over() n from nation group by n_nationkey with rollup order by n_nationkey")
	require.Error(t, err, "an unselected source key is not available after DISTINCT")
}

func TestGroupingWindowAfterUnion(t *testing.T) {
	for _, query := range []string{
		"select n_nationkey,count(*) over() from nation group by n_nationkey with rollup",
		"select n_nationkey,row_number() over(order by n_nationkey) from nation group by n_nationkey with rollup",
		"select n_nationkey,sum(count(*)) over() from nation group by n_nationkey with rollup",
		"select n_nationkey from nation group by n_nationkey with rollup order by count(*) over(),n_nationkey",
		"select n_nationkey,count(*) over(partition by n_nationkey) from nation group by grouping sets ((n_nationkey),(n_nationkey),())",
	} {
		t.Run(query, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false), t, query)
			require.NoError(t, err)
			nodes := p.GetQuery().Nodes
			var hasUnion func(int32) bool
			hasUnion = func(id int32) bool {
				if nodes[id].NodeType == plan.Node_UNION_ALL {
					return true
				}
				for _, child := range nodes[id].Children {
					if hasUnion(child) {
						return true
					}
				}
				return false
			}
			windows := 0
			for id, node := range nodes {
				if node.NodeType == plan.Node_WINDOW {
					windows++
					require.True(t, hasUnion(int32(id)), "the window must consume the entire grouped result")
				}
			}
			require.Equal(t, 1, windows)
		})
	}
}
