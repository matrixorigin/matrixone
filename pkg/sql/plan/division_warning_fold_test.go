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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDivisionWarningSubtreesSurviveConstantFolding(t *testing.T) {
	for _, folder := range []string{"binder", "optimizer", "prepare optimizer"} {
		for _, parent := range []string{"coalesce", "in", "not_in"} {
			for _, denominator := range []int64{0, 2} {
				t.Run(folder+"/"+parent+"/"+string(rune('0'+denominator)), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					defer proc.Free()
					proc.SetStmtProfile(&process.StmtProfile{})
					proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
					proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return "", nil })
					literal := func(value int64) *planpb.Expr {
						return &planpb.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}}}
					}
					division, err := BindFuncExprImplByPlanExpr(proc.Ctx, "div", []*planpb.Expr{literal(1), literal(denominator)})
					require.NoError(t, err)
					args := []*planpb.Expr{division, literal(7)}
					if parent != "coalesce" {
						args = []*planpb.Expr{literal(1), {Typ: division.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{division, literal(2)}}}}}
					}
					expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, parent, args)
					require.NoError(t, err)
					if folder == "binder" {
						expr, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, false)
						require.NoError(t, err)
					} else {
						node := &planpb.Node{ProjectList: []*planpb.Expr{expr}}
						rule.NewConstantFold(folder == "prepare optimizer").Apply(node, nil, proc)
						expr = node.ProjectList[0]
					}
					if denominator == 0 {
						require.NotNil(t, expr.GetF(), "parent must preserve the runtime division")
						features, err := planpb.RequiredRemoteExpressionFeatures(expr)
						require.NoError(t, err)
						require.True(t, features.ScalarDivision)
					} else {
						require.NotNil(t, expr.GetLit(), "safe nonzero arithmetic should still fold")
						if parent == "coalesce" {
							require.Equal(t, int64(0), expr.GetLit().GetI64Val())
						} else {
							require.Equal(t, parent == "not_in", expr.GetLit().GetBval())
						}
					}
				})
			}
		}
	}
}
