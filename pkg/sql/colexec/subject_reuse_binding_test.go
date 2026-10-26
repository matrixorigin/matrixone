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

package colexec_test

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type orLikeTextBindingContext struct{ *plan2.MockCompilerContext }

func (ctx *orLikeTextBindingContext) Resolve(db, table string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	object, definition, err := ctx.MockCompilerContext.Resolve(db, table, snapshot)
	if err != nil || definition == nil {
		return object, definition, err
	}
	definition = proto.Clone(definition).(*plan.TableDef)
	for _, column := range definition.Cols {
		if column.Name == "l_comment" {
			column.Typ = types.T_text.ToType().PlanType()
		}
	}
	return object, definition, nil
}

func TestORLikeSubjectReusePublicBinding(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	ctx := &orLikeTextBindingContext{plan2.NewMockCompilerContext(false, proc)}
	statement, err := mysql.ParseOne(proc.Ctx, `select l_comment from tpch.lineitem where
lower(concat(ifnull(l_comment, ''), ' ', ifnull(l_comment, ''))) like '%error%'
or lower(concat(ifnull(l_comment, ''), ' ', ifnull(l_comment, ''))) like '%timeout%'
or lower(concat(ifnull(l_comment, ''), ' ', ifnull(l_comment, ''))) like '%retry%'`, 1)
	require.NoError(t, err)
	defer statement.Free()
	built, err := plan2.BuildPlan(ctx, statement, false)
	require.NoError(t, err)
	var candidate *plan.Expr
	var visit func(*plan.Expr)
	visit = func(expression *plan.Expr) {
		if expression == nil || expression.GetF() == nil {
			return
		}
		fid, _ := function.DecodeOverloadID(expression.GetF().Func.Obj)
		if fid == function.OR {
			candidate = expression
			return
		}
		for _, child := range expression.GetF().Args {
			visit(child)
		}
	}
	for _, node := range built.GetQuery().Nodes {
		for _, filter := range node.FilterList {
			visit(filter)
		}
	}
	require.NotNil(t, candidate, "real parser/binder/optimizer must retain the three-LIKE OR")
	t.Logf("bound OR: %s", candidate.String())
	executor, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	executor = colexec.ReuseFilterExpressionExecutor(candidate, executor, 1024)
	defer executor.Free()
	require.Equal(t, "*colexec.memoRootExpressionExecutor", fmt.Sprintf("%T", executor), "original bound SQL must satisfy strict admission without normalizing metadata")
}
