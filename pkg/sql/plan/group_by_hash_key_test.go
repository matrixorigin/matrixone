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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func groupHashKeyTestCol(tag, pos int32) *pbplan.Expr {
	return &pbplan.Expr{Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: tag, ColPos: pos}}}
}

func groupHashKeyTestTable(pkName string, pkNames ...string) *pbplan.TableDef {
	return &pbplan.TableDef{
		Cols: []*pbplan.ColDef{{Name: "id"}, {Name: "tenant"}, {Name: "payload"}},
		Name2ColIndex: map[string]int32{
			"id": 0, "tenant": 1, "payload": 2,
		},
		Pkey: &pbplan.PrimaryKeyDef{PkeyColName: pkName, Names: pkNames},
	}
}

func TestDetermineGroupByHashKeys(t *testing.T) {
	complexPayload := &pbplan.Expr{Expr: &pbplan.Expr_F{F: &pbplan.Function{
		Args: []*pbplan.Expr{groupHashKeyTestCol(1, 2)},
	}}}

	tests := []struct {
		name          string
		groupBy       []*pbplan.Expr
		groupingFlags []bool
		tables        map[int32]*pbplan.TableDef
		want          []int32
	}{
		{
			name:    "single primary key determines payload",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{0},
		},
		{
			name:    "group order is preserved",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 2), groupHashKeyTestCol(1, 0)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{1},
		},
		{
			name: "complete composite primary key",
			groupBy: []*pbplan.Expr{
				groupHashKeyTestCol(1, 2), groupHashKeyTestCol(1, 1), groupHashKeyTestCol(1, 0),
			},
			tables: map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.CPrimaryKeyColName, "id", "tenant")},
			want:   []int32{1, 2},
		},
		{
			name:    "incomplete composite primary key",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.CPrimaryKeyColName, "id", "tenant")},
		},
		{
			name:          "grouping set is not reduced",
			groupBy:       []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			groupingFlags: []bool{true, false},
			tables:        map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
		},
		{
			name: "only columns from the determined table are removed",
			groupBy: []*pbplan.Expr{
				groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2), groupHashKeyTestCol(2, 2),
			},
			tables: map[int32]*pbplan.TableDef{
				1: groupHashKeyTestTable("id", "id"),
				2: {Cols: []*pbplan.ColDef{{Name: "id"}, {Name: "tenant"}, {Name: "payload"}}},
			},
			want: []int32{0, 2},
		},
		{
			name:    "derived expression remains a physical key",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2), complexPayload},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{0, 2},
		},
		{
			name:    "synthetic column remains a physical key",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2), groupHashKeyTestCol(1, -1)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable("id", "id")},
			want:    []int32{0, 2},
		},
		{
			name:    "fake primary key is not a proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.FakePrimaryKeyColName, catalog.FakePrimaryKeyColName)},
		},
		{
			name:    "hidden composite key without components is not a proof",
			groupBy: []*pbplan.Expr{groupHashKeyTestCol(1, 0), groupHashKeyTestCol(1, 2)},
			tables:  map[int32]*pbplan.TableDef{1: groupHashKeyTestTable(catalog.CPrimaryKeyColName)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agg := &pbplan.Node{
				NodeId: 1, NodeType: pbplan.Node_AGG, Children: []int32{0},
				GroupBy: test.groupBy, GroupingFlag: test.groupingFlags,
			}
			builder := &QueryBuilder{
				qry:       &pbplan.Query{Nodes: []*pbplan.Node{{NodeId: 0, NodeType: pbplan.Node_TABLE_SCAN}, agg}},
				tag2Table: test.tables,
			}
			builder.determineGroupByHashKeys(1)
			require.Equal(t, test.want, agg.GroupByHashKey)
		})
	}
}

func TestBuildPlanUsesPrimaryKeyAsPhysicalGroupKey(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select empno, ename, sum(sal) from constraint_test.emp group by empno, ename",
	)
	require.NoError(t, err)

	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 2 {
			found = true
			require.Equal(t, []int32{0}, node.GroupByHashKey)
			require.Len(t, node.GroupBy, 2, "logical group output must remain unchanged")
		}
	}
	require.True(t, found)
}

func TestBuildPlanAnnotatesDistinctRewriteAggregate(t *testing.T) {
	for _, aggregate := range []string{"count", "sum"} {
		t.Run(aggregate, func(t *testing.T) {
			logicPlan, err := runOneStmt(
				NewMockOptimizer(false),
				t,
				fmt.Sprintf(
					"select empno, ename, %s(distinct deptno) from constraint_test.emp group by empno, ename",
					aggregate,
				),
			)
			require.NoError(t, err)

			var outer, inner *pbplan.Node
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != pbplan.Node_AGG {
					continue
				}
				switch len(node.GroupBy) {
				case 2:
					outer = node
				case 3:
					inner = node
				}
			}

			require.NotNil(t, outer, "distinct rewrite must retain the original aggregate")
			require.NotNil(t, inner, "distinct rewrite must create a deduplication aggregate")
			require.Equal(t, []int32{0}, outer.GroupByHashKey,
				"rewriting the outer expressions must preserve its proven physical key")
			require.Equal(t, []int32{0}, inner.GroupByHashKey,
				"the primary key determines both the payload and distinct argument")
		})
	}
}

func TestBuildPlanAnnotatesJoinDistinctRewriteAggregate(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select e.empno, e.ename, count(distinct d.loc) "+
			"from constraint_test.emp e left join constraint_test.dept d on e.deptno = d.deptno "+
			"group by e.empno, e.ename",
	)
	require.NoError(t, err)

	var inner *pbplan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 3 {
			inner = node
			break
		}
	}
	require.NotNil(t, inner)
	require.Equal(t, []int32{0, 2}, inner.GroupByHashKey)
}

func TestBuildPlanAnnotatesJoinPromotedCharDistinctRewriteAggregate(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select e.empno, e.ename, count(distinct "+
			"coalesce(cast(d.dname as char(8)), cast(d.loc as varchar(8)))) "+
			"from constraint_test.emp e left join constraint_test.dept d on e.deptno = d.deptno "+
			"group by e.empno, e.ename",
	)
	require.NoError(t, err)

	var inner *pbplan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_AGG && len(node.GroupBy) == 4 {
			inner = node
			break
		}
	}
	require.NotNil(t, inner)
	require.Equal(t, []int32{0, 3}, inner.GroupByHashKey)
	require.True(t, isCastOverload(inner.GroupBy[3], 3))
}
