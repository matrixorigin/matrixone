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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestValuesWithSubqueriesTypesBareNullBranches(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		column    int
	}{
		{"insert-null-literal-row", "insert into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,null,'y')", 1},
		{"insert-null-first", "insert into dept(deptno,dname,loc) values (2,null,'y'),(1,(select 'one'),'x')", 1},
		{"insert-null-after-literal", "insert into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,'two','y'),(3,null,'z')", 1},
		{"insert-null-subquery-row", "insert into dept(deptno,dname,loc) values (1,(select 'one'),null),(2,'two','y')", 2},
		{"insert-null-subquery-row-last", "insert into dept(deptno,dname,loc) values (2,'two','y'),(1,(select 'one'),null)", 2},
		{"replace-null-literal-row", "replace into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,null,'y')", 1},
		{"replace-null-first", "replace into dept(deptno,dname,loc) values (2,null,'y'),(1,(select 'one'),'x')", 1},
		{"replace-null-subquery-row", "replace into dept(deptno,dname,loc) values (1,(select 'one'),null),(2,'two','y')", 2},
		{"replace-null-subquery-row-last", "replace into dept(deptno,dname,loc) values (2,'two','y'),(1,(select 'one'),null)", 2},
		{"typed-null-control", "insert into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,cast(null as char),'y')", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			var valuesUnion *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType != plan.Node_SORT || len(node.Children) != 1 || len(node.OrderBy) != 1 {
					continue
				}
				child := query.Nodes[node.Children[0]]
				if child.NodeType == plan.Node_UNION_ALL && len(child.ProjectList) == 4 {
					valuesUnion = child
					break
				}
			}
			require.NotNil(t, valuesUnion, "the ordered VALUES branches must use UNION ALL")
			require.Equal(t, int32(types.T_varchar), valuesUnion.ProjectList[tc.column].Typ.Id)
			for _, childID := range valuesUnion.Children {
				child := query.Nodes[childID]
				require.Greater(t, len(child.ProjectList), tc.column)
				require.Equal(t, int32(types.T_varchar), child.ProjectList[tc.column].Typ.Id,
					"every VALUES branch must supply a typed vector")
			}
		})
	}
}
