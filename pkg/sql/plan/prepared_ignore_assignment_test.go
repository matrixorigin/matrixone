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

func TestPreparedIgnoreAssignmentKeepsRuntimeCast(t *testing.T) {
	tests := []string{
		"prepare stmt from 'insert ignore into constraint_test.emp (empno) values (?)'",
		"prepare stmt from 'update ignore constraint_test.emp set sal = ? where empno = ?'",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.NoError(t, err)
			casts := queryFunctionsNamed(resolveQueryPlan(prepared).GetQuery(), "cast_ignore")
			require.NotEmpty(t, casts, "prepared IGNORE assignment must use cast_ignore")
			foundDirectParam := false
			for _, cast := range casts {
				args := cast.GetF().GetArgs()
				if len(args) == 2 && args[0].GetP() != nil {
					foundDirectParam = true
					break
				}
			}
			require.True(t, foundDirectParam,
				"prepared IGNORE assignment must keep a direct parameter as cast_ignore input")
		})
	}
}

func queryFunctionsNamed(query *planpb.Query, want string) []*planpb.Expr {
	if query == nil {
		return nil
	}
	var result []*planpb.Expr
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		for _, expr := range node.ProjectList {
			result = appendFunctionExpressions(result, expr, want)
		}
		for _, expr := range node.FilterList {
			result = appendFunctionExpressions(result, expr, want)
		}
		for _, expr := range node.OnUpdateExprs {
			result = appendFunctionExpressions(result, expr, want)
		}
		if node.RowsetData != nil {
			for _, col := range node.RowsetData.Cols {
				for _, row := range col.Data {
					if row != nil {
						result = appendFunctionExpressions(result, row.Expr, want)
					}
				}
			}
		}
	}
	return result
}

func appendFunctionExpressions(result []*planpb.Expr, expr *planpb.Expr, want string) []*planpb.Expr {
	if expr == nil {
		return result
	}
	if function := expr.GetF(); function != nil {
		if function.Func != nil && function.Func.GetObjName() == want {
			result = append(result, expr)
		}
		for _, arg := range function.Args {
			result = appendFunctionExpressions(result, arg, want)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			result = appendFunctionExpressions(result, item, want)
		}
	}
	return result
}
