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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupingDistinctAfterUnion(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			prefix := "select "
			if distinct {
				prefix += "distinct "
			}
			mock := NewMockOptimizer(false)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				prefix+"n_regionkey from nation group by grouping sets ((n_regionkey),(n_regionkey),()) order by n_regionkey+1 limit 2", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			builder := NewQueryBuilder(plan.Query_SELECT, mock.CurrentContext(), false, false)
			ctx := NewBindContext(builder, nil)
			root, err := builder.bindSelect(stmt.(*tree.Select), ctx, true)
			require.NoError(t, err)
			var hasUnion func(int32) bool
			hasUnion = func(id int32) bool {
				n := builder.qry.Nodes[id]
				if n.NodeType == plan.Node_UNION_ALL {
					return true
				}
				for _, child := range n.Children {
					if hasUnion(child) {
						return true
					}
				}
				return false
			}
			outerDistinct := 0
			var visit func(int32)
			visit = func(id int32) {
				n := builder.qry.Nodes[id]
				if n.NodeType == plan.Node_DISTINCT && hasUnion(id) {
					outerDistinct++
					require.Nil(t, n.Limit, "LIMIT belongs above global DISTINCT")
					require.Len(t, builder.qry.Nodes[n.Children[0]].ProjectList, 1, "hidden ORDER BY expressions cannot become DISTINCT keys")
				}
				for _, child := range n.Children {
					visit(child)
				}
			}
			visit(root)
			if distinct {
				require.Equal(t, 1, outerDistinct, "DISTINCT must consume the combined grouping-set result")
			} else {
				require.Zero(t, outerDistinct, "ordinary grouping sets preserve duplicates")
			}
		})
	}
}

func TestGroupingExpressionFlags(t *testing.T) {
	for _, by := range []string{
		"cube(n_nationkey,n_regionkey+1)",
		"grouping sets ((),(n_nationkey),(n_regionkey+1),(n_nationkey,n_regionkey+1))",
		"cube(n_nationkey,abs(n_regionkey))",
	} {
		t.Run(by, func(t *testing.T) {
			key := "n_regionkey+1"
			if by == "cube(n_nationkey,abs(n_regionkey))" {
				key = "abs(n_regionkey)"
			}
			p, err := buildSingleStmt(NewMockOptimizer(false), t,
				"select n_nationkey,"+key+",count(*) from nation group by "+by)
			require.NoError(t, err)
			flags := make(map[string]int)
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == plan.Node_AGG && len(n.GroupBy) == 2 {
					flags[fmt.Sprint(n.GroupingFlag)]++
				}
			}
			require.Equal(t, map[string]int{
				"[false false]": 1, "[true false]": 1, "[false true]": 1, "[true true]": 1,
			}, flags)
		})
	}
}

func TestGroupingPrimaryKeySubtotal(t *testing.T) {
	for _, by := range []string{"n_nationkey", "n_nationkey with rollup", "cube(n_nationkey)"} {
		t.Run(by, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false), t, "select n_nationkey from nation group by "+by)
			require.NoError(t, err)
			missing, aggs := 0, 0
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == plan.Node_AGG {
					aggs++
				}
				if n.NodeType == plan.Node_AGG && len(n.GroupingFlag) == 1 && !n.GroupingFlag[0] {
					missing++
				}
			}
			if by == "n_nationkey" {
				require.Zero(t, missing)
				require.Zero(t, aggs, "ordinary PK grouping keeps the effectless-aggregate optimization")
			} else {
				require.Equal(t, 1, missing, "input PK uniqueness cannot remove a NULL-synthesizing subtotal")
			}
		})
	}
}

func TestGroupingEmptySetNoAggregate(t *testing.T) {
	for _, tc := range []struct {
		by   string
		aggs int
	}{
		{"", 0},
		{" group by grouping sets (())", 1},
		{" group by grouping sets ((),())", 2},
		{" group by grouping sets (()) having 1=0", 1},
	} {
		t.Run(tc.by, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false), t, "select 1 from nation"+tc.by)
			require.NoError(t, err)
			aggs, protectedHaving := 0, 0
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == plan.Node_FILTER && n.RollupFilter {
					protectedHaving++
				}
				if n.NodeType == plan.Node_AGG {
					aggs++
					require.Empty(t, n.GroupBy)
				}
			}
			require.Equal(t, tc.aggs, aggs)
			if tc.by == " group by grouping sets (()) having 1=0" {
				require.Equal(t, 1, protectedHaving, "HAVING must remain above the global group")
			}
		})
	}
}
