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

package frontend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFrontendExpressionContextScope(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "like", []*plan2.Expr{plan2.MakePlan2StringConstExprWithType("MatrixOne"), plan2.MakePlan2StringConstExprWithType("Matrix%")})
	require.NoError(t, err)
	const savedID = "00000000-0000-0000-0000-000000000001"
	idExpr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*plan.Expr{expr, plan2.MakePlan2StringConstExprWithType(savedID), plan2.MakePlan2StringConstExprWithType(savedID)})
	require.NoError(t, err)
	idExpr, err = plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{idExpr, {Typ: plan.Type{Id: int32(types.T_uuid)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}})
	require.NoError(t, err)
	metadataPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_FUNCTION_SCAN, TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Name: "meta_scan"}}, TblFuncExprList: []*plan.Expr{idExpr}}}}}}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "set @scope_probe = 'MatrixOne' like 'Matrix%'", 1)
	require.NoError(t, err)
	defer stmt.Free()
	for _, stale := range []bool{false, true} {
		for _, state := range []string{"live", "canceled", "deadline"} {
			t.Run(fmt.Sprintf("stale=%v/%s", stale, state), func(t *testing.T) {
				previous, cancelPrevious := context.WithCancel(context.Background())
				defer cancelPrevious()
				ses.proc.ReplaceTopCtx(previous)
				ses.proc.ResetQueryContext()
				if stale {
					cancelPrevious()
				}
				queryCtx := ses.proc.Base.GetContextBase().BuildQueryCtx(previous)
				ses.proc.Ctx = queryCtx
				prior := ses.proc.Ctx
				_, queryCancel := process.GetQueryCtxFromProc(ses.proc)
				defer queryCancel()
				ses.proc.NextGroupConcatInputRowBase(73, 7)
				current := defines.AttachAccountId(context.Background(), 0)
				var cancel context.CancelFunc
				if state == "deadline" {
					current, cancel = context.WithDeadline(current, time.Unix(1, 0))
				} else {
					current, cancel = context.WithCancel(current)
					if state == "canceled" {
						cancel()
					}
				}
				defer cancel()
				ec := &ExecCtx{reqCtx: current, ses: ses, proc: ses.proc}
				ses.txnCompileCtx.execCtx = ec
				// These are the real prepared-SET and stored-procedure runtime consumers.
				value, _, err := getPreparedPlanExprValueWithMeta(expr, ses, ec, nil, nil, nil)
				if current.Err() != nil {
					require.ErrorIs(t, err, current.Err())
				} else {
					require.NoError(t, err)
					require.Equal(t, true, value)
				}
				value, err = GetSimpleExprValue(current, stmt.(*tree.SetVar).Assignments[0].Value, ses)
				if current.Err() != nil {
					require.ErrorIs(t, err, current.Err())
				} else {
					require.NoError(t, err)
					require.Equal(t, true, value)
				}
				ids, err := isResultQuery(current, ses.proc, metadataPlan)
				if current.Err() != nil {
					require.ErrorIs(t, err, current.Err())
				} else {
					require.NoError(t, err)
					require.Equal(t, []string{savedID}, ids)
				}
				require.Same(t, prior, ses.proc.Ctx)
				require.Same(t, previous, ses.proc.GetTopContext())
				after, _ := process.GetQueryCtxFromProc(ses.proc)
				require.Same(t, queryCtx, after)
				require.Equal(t, previous.Err(), queryCtx.Err())
				require.Equal(t, uint64(7), ses.proc.NextGroupConcatInputRowBase(73, 0))

			})
		}
	}
	t.Run("restore on construction error and panic", func(t *testing.T) {
		prior := ses.proc.Ctx
		ec := &ExecCtx{reqCtx: context.Background(), ses: ses, proc: ses.proc}
		_, _, err := getPreparedPlanExprValueWithMeta(&plan2.Expr{Typ: plan2.Type{Id: int32(types.T_bool)}}, ses, ec, nil, nil, nil)
		require.Error(t, err)
		require.Same(t, prior, ses.proc.Ctx)
		require.Panics(t, func() {
			restore := bindFrontendExpressionContext(ses.proc, context.Background())
			defer restore()
			panic("scoped evaluator panic")
		})
		require.Same(t, prior, ses.proc.Ctx)
	})
	t.Run("current cancellation propagates and live reuse succeeds", func(t *testing.T) {
		prior := ses.proc.Ctx
		current, cancel := context.WithCancel(context.Background())
		defer cancel()
		func() {
			restore := bindFrontendExpressionContext(ses.proc, current)
			defer restore()
			cancel()
			require.ErrorIs(t, ses.proc.Ctx.Err(), context.Canceled)
		}()
		require.Same(t, prior, ses.proc.Ctx)
		value, _, err := getPreparedPlanExprValueWithMeta(expr, ses, &ExecCtx{reqCtx: context.Background(), ses: ses, proc: ses.proc}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, true, value)
		require.Same(t, prior, ses.proc.Ctx)
	})
}
