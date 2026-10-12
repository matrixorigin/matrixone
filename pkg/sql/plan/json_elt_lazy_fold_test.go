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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planrule "github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONStringEltLazyPlannerFolding(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		sql, want  string
		null, fail bool
	}{
		{sql: "elt(1,'ok',cast('bad' as json))", want: "ok"},
		{sql: "elt(null,cast('bad' as json),'ok')", null: true},
		{sql: "elt(0,cast('bad' as json),'ok')", null: true},
		{sql: "elt(3,cast('bad' as json),'ok')", null: true},
		{sql: "elt(1,cast('bad' as json),'ok')", fail: true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select "+tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(context.Background(), nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			for _, mode := range []string{"runtime", "api", "optimizer", "prepare"} {
				t.Run(mode, func(t *testing.T) {
					expr := DeepCopyExpr(bound)
					if mode == "api" {
						expr, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
						require.NoError(t, err)
					}
					if mode == "optimizer" || mode == "prepare" {
						node := &planpb.Node{ProjectList: []*planpb.Expr{expr}}
						planrule.NewConstantFold(mode == "prepare").Apply(node, nil, proc)
						expr = node.ProjectList[0]
					}
					requires, err := planpb.RequiresMORPCVersion111JSONStringConsumerOverload([]*planpb.Expr{expr})
					require.NoError(t, err)
					require.True(t, requires, "fold must retain durable/remote ELT identity")
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					if free != nil {
						defer free()
					}
					if tc.fail {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					if tc.null {
						require.True(t, result.IsNull(0))
						return
					}
					require.Equal(t, tc.want, string(result.GetBytesAt(0)))
				})
			}
		})
	}
}

func TestJSONStringEltStillBindsUnselectedInvalidFunction(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select elt(1,'ok',no_such_json_elt_function())", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	_, err = NewDefaultBinder(context.Background(), nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
	require.Error(t, err)
}
