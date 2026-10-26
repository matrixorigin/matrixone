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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

type floatCastWarningSink struct{ codes []uint16 }

func (s *floatCastWarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}

func TestFloatCastDiagnosticFolding(t *testing.T) {
	for _, folder := range []string{"binder", "optimizer", "prepare"} {
		for _, assignment := range []bool{false, true} {
			for _, parent := range []string{"cast", "coalesce", "masked", "concat", "scalar-vector"} {
				t.Run(folder+"/"+parent+map[bool]string{false: "/explicit", true: "/assignment"}[assignment], func(t *testing.T) {
					proc := testutil.NewProcess(t)
					proc.SetStmtProfile(&process.StmtProfile{})
					proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", false)
					mode := "STRICT_TRANS_TABLES"
					proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return mode, nil })
					sink := &floatCastWarningSink{}
					proc.WarningSink = sink
					source := makePlan2StringConstExprWithType("inf")
					if parent == "concat" {
						var err error
						source, err = BindFuncExprImplByPlanExpr(proc.Ctx, "concat", []*planpb.Expr{makePlan2StringConstExprWithType("in"), makePlan2StringConstExprWithType("f")})
						require.NoError(t, err)
					}
					if parent == "scalar-vector" {
						value, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("inf"), 1, proc.Mp())
						require.NoError(t, err)
						data, err := value.MarshalBinary()
						require.NoError(t, err)
						value.Free(proc.Mp())
						source = &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 1, Data: data}}}
					}
					target := types.T_float64.ToType().PlanType()
					var expr *planpb.Expr
					var err error
					if assignment {
						expr, err = forceAssignmentCastExpr(proc.Ctx, source, target)
					} else {
						expr, err = appendSyntaxExplicitCastBeforeExpr(proc.Ctx, source, target)
					}
					require.NoError(t, err)
					if parent == "coalesce" {
						expr, err = BindFuncExprImplByPlanExpr(proc.Ctx, "coalesce", []*planpb.Expr{expr, makePlan2Float64ConstExprWithType(1)})
					}
					if parent == "masked" {
						expr, err = BindFuncExprImplByPlanExpr(proc.Ctx, "case", []*planpb.Expr{makePlan2BoolConstExprWithType(false), expr, makePlan2Float64ConstExprWithType(1)})
					}
					require.NoError(t, err)
					if folder == "binder" {
						expr, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, false)
						require.NoError(t, err)
					} else {
						node := &planpb.Node{ProjectList: []*planpb.Expr{expr}}
						rule.NewConstantFold(folder == "prepare").Apply(node, nil, proc)
						expr = node.ProjectList[0]
					}
					require.NotNil(t, expr.GetF(), "context-dependent conversion and its parents must survive PREPARE")
					require.Empty(t, sink.codes)
					// The same bound expression takes execution-time policy.
					for _, strict := range []bool{false, true} {
						mode = ""
						if strict {
							mode = "STRICT_TRANS_TABLES"
						}
						sink.codes = nil
						got, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
						if strict && parent != "masked" {
							var e *moerr.Error
							require.ErrorAs(t, err, &e)
							code := uint16(1292)
							if assignment {
								code = 1265
							}
							require.Equal(t, code, e.MySQLCode())
						} else {
							require.NoError(t, err)
							require.NotNil(t, free)
							value := vector.GetFixedAtNoTypeCheck[float64](got, 0)
							free()
							if parent == "masked" {
								require.Equal(t, float64(1), value)
								require.Empty(t, sink.codes)
							} else {
								require.Zero(t, value)
								require.Len(t, sink.codes, 1)
							}
						}
					}
				})
			}
		}
	}
}

func TestFloatCastSafeLiteralStillFolds(t *testing.T) {
	proc := testutil.NewProcess(t)
	expr, err := appendSyntaxExplicitCastBeforeExpr(proc.Ctx, makePlan2StringConstExprWithType("1.25"), types.T_float64.ToType().PlanType())
	require.NoError(t, err)
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, false)
	require.NoError(t, err)
	require.NotNil(t, folded.GetLit())
	require.Equal(t, float64(1.25), folded.GetLit().GetDval())
}
