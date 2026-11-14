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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupingRepeatedPlanning(t *testing.T) {
	for _, tc := range []struct {
		name, by string
		sets     int
	}{
		{"ordinary", "nation.n_nationkey,nation.n_regionkey", 1},
		{"rollup", "nation.n_nationkey,nation.n_regionkey with rollup", 3},
		{"functional rollup", "rollup(nation.n_nationkey,nation.n_regionkey)", 3},
		{"cube", "cube(nation.n_nationkey,nation.n_regionkey)", 4},
		{"sets", "grouping sets ((nation.n_nationkey,nation.n_regionkey),(nation.n_nationkey),())", 3},
		{"duplicate sets", "grouping sets ((nation.n_nationkey,nation.n_regionkey),(nation.n_nationkey,nation.n_regionkey),())", 3},
	} {
		for _, ctas := range []bool{false, true} {
			name := tc.name + "/select"
			prefix := ""
			if ctas {
				name = tc.name + "/ctas"
				prefix = "create table regroup as "
			}
			t.Run(name, func(t *testing.T) {
				mock := NewMockOptimizer(false)
				sql := prefix + "select nation.n_nationkey,nation.n_regionkey,count(*) c from nation group by " + tc.by + " having count(*) > 0"
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				var source *tree.Select
				if ctas {
					source = stmt.(*tree.CreateTable).AsSource
				} else {
					source = stmt.(*tree.Select)
				}
				original := tree.String(source, dialect.MYSQL)
				clause := source.Select.(*tree.SelectClause)
				originalSets := len(clause.GroupBy.GroupByExprsList)
				var insert string
				for generation := 0; generation < 3; generation++ {
					p, err := BuildPlan(mock.CurrentContext(), stmt, false)
					require.NoError(t, err, "generation %d", generation)
					require.Equal(t, original, tree.String(source, dialect.MYSQL), "planning must preserve the source grouping syntax")
					require.Len(t, clause.GroupBy.GroupByExprsList, originalSets)
					require.False(t, clause.Having.RollupHaving, "the derived HAVING marker must not escape into the source AST")
					if ctas {
						current := p.GetDdl().GetCreateTable().CreateAsSelectSql
						reparsed, err := parsers.ParseOne(context.Background(), dialect.MYSQL, current, 1)
						require.NoError(t, err)
						reparsed.Free()
						if generation == 0 {
							insert = current
						} else {
							require.Equal(t, insert, current)
						}
					} else {
						groups := 0
						for _, node := range p.GetQuery().Nodes {
							if node.NodeType == plan.Node_AGG {
								groups++
							}
						}
						require.Equal(t, tc.sets, groups, "grouping multiplicity at generation %d", generation)
					}
				}
			})
		}
	}
}

func TestGroupingOutputPreservesInputType(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := buildSingleStmt(mock, t, "select n_nationkey,count(*) from nation group by n_nationkey with rollup")
	require.NoError(t, err)
	missing := 0
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		for i, key := range node.GroupBy {
			require.True(t, key.Typ.NotNullable, "the evaluated input must retain its original type")
			if i < len(node.GroupingFlag) && !node.GroupingFlag[i] {
				for _, out := range node.ProjectList {
					if col := out.GetCol(); col != nil && col.RelPos == -1 && col.ColPos == int32(i) {
						missing++
						require.False(t, out.Typ.NotNullable, "a missing grouping key produces NULL")
					}
				}
			}
		}
	}
	require.Positive(t, missing)
}
