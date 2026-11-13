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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupingBranchConsumerProof(t *testing.T) {
	for _, output := range []string{"sample(n_nationkey,100 percent)", "count(*)"} {
		for _, groups := range []string{"n_regionkey with rollup", "grouping sets ((n_regionkey),(n_regionkey))"} {
			for _, operand := range []string{"n_regionkey+0", "cast(n_regionkey as signed)", "coalesce(n_regionkey,0)"} {
				query := "select n_regionkey," + output + " from nation group by " + groups +
					" having not(" + operand + " in(select r_regionkey from region))"
				t.Run(query, func(t *testing.T) {
					stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
					require.NoError(t, err)
					t.Cleanup(stmt.Free)
					builder := NewQueryBuilder(plan.Query_SELECT, NewMockOptimizer(false).CurrentContext(), false, true)
					_, err = builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
					require.NoError(t, err)
					joins := 0
					for _, node := range builder.qry.Nodes {
						if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_MARK {
							continue
						}
						ctx := builder.ctxByNode[node.NodeId]
						require.Len(t, ctx.groupingFlag, 1)
						present := ctx.groupingFlag[0]
						require.True(t, ctx.groups[0].Typ.NotNullable, "AGG input encoding must retain its source proof")
						for _, expr := range ctx.sampleFunc.columns {
							require.True(t, expr.Typ.NotNullable, "raw SAMPLE values are not omitted grouping outputs")
						}
						require.Len(t, node.OnList, 1)
						require.Equal(t, present || operand == "coalesce(n_regionkey,0)", node.OnList[0].Typ.NotNullable,
							"the first MARK consumer must already see the branch operand proof")
						refs := 0
						walkGroupingExpr(node.OnList[0], func(e *plan.Expr) {
							if col := e.GetCol(); col != nil && col.RelPos == ctx.groupTag {
								refs++
								require.Equal(t, present, e.Typ.NotNullable)
							}
						})
						require.Equal(t, 1, refs)
						if ctx.sampleFunc.hasSampleFunc {
							require.Equal(t, present, ctx.groupingStage.sampleProjects[0].Typ.NotNullable,
								"the branch projection publishes output, not source, nullability")
						}
						joins++
					}
					require.Equal(t, 2, joins, "both grouping branches must reach the checked consumer")
				})
			}
		}
	}
}

func TestGroupingDistinctLineageAdmission(t *testing.T) {
	for _, sample := range []string{"", ",sample(n_nationkey,100 percent) chosen"} {
		for _, groups := range []string{"n_regionkey with rollup", "cube(n_regionkey)", "grouping sets ((n_regionkey),(n_regionkey))"} {
			for _, tc := range []struct {
				selected string
				order    string
				valid    bool
			}{
				{"n_regionkey", "grouping(n_regionkey) desc,n_regionkey is null,n_regionkey", false},
				{"n_regionkey k", "grouping(k)+1", false},
				{"n_regionkey", "case when grouping(n_regionkey)=1 then 0 else 1 end", false},
				{"n_regionkey", "(n_regionkey,grouping(n_regionkey))", false},
				{"n_regionkey,grouping(n_regionkey) flag", "flag+1,n_regionkey", true},
				{"n_regionkey k,grouping(n_regionkey) flag", "grouping(k) desc,k", true},
				{"n_regionkey,grouping(n_regionkey)+1 marker", "grouping(n_regionkey)+1", true},
				{"n_regionkey,grouping(n_regionkey)+1 marker", "grouping(n_regionkey)", false},
				{"n_regionkey", "n_regionkey+1", true},
			} {
				query := "select distinct " + tc.selected + sample + " from nation group by " + groups + " order by " + tc.order + " limit 1"
				for _, prefix := range []string{"", "create table lineage_result as "} {
					t.Run(prefix+query, func(t *testing.T) {
						_, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+query)
						if tc.valid {
							require.NoError(t, err)
						} else {
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrSyntaxError), "%v", err)
							require.ErrorContains(t, err, "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
						}
					})
				}
			}
		}
	}
}
