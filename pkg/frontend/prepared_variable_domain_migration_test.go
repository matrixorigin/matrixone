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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMigrateConnectionFromRejectsResolvedFieldCaseDomain(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 1, "select field(case when ? then null else ? end,?)")
	t.Cleanup(ses.Close)
	execCtx.input.isBinaryProtExecute = false
	cw.binaryPrepare = false
	require.NoError(t, ses.SetUserDefinedVar("condition", int64(0), ""))
	typ := plan.Type{Id: int32(types.T_varbinary), Charset: uint32(types.CharsetBinary)}
	require.NoError(t, ses.setUserDefinedVarWithType("subject", "A", "", false, typ))
	require.NoError(t, ses.setUserDefinedVarWithType("candidate", "a", "", false, typ))
	args := []*plan.Expr{
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "condition"}}},
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "subject"}}},
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "candidate"}}},
	}
	evaluate := func() {
		_, runtime, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, &plan.Execute{Name: prepared.Name, Args: args}, "")
		require.NoError(t, err)
		if owned && stmt != nil {
			defer stmt.Free()
		}
		q := runtime.GetQuery()
		executor, err := colexec.NewExpressionExecutor(cw.proc, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0])
		require.NoError(t, err)
		defer executor.Free()
		out, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(0), vector.GetFixedAtWithTypeCheck[int64](out, 0))
	}
	evaluate()
	require.Len(t, prepared.fieldCaseDomains, 1)
	require.NoError(t, ses.SetUserDefinedVar("subject", "A", ""))
	require.NoError(t, ses.SetUserDefinedVar("candidate", "a", ""))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	resp := &query.MigrateConnFromResponse{}
	err := rt.migrateConnectionFrom(resp)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer), "resolved CASE state is not on the migration wire")
	require.Empty(t, resp.PrepareStmts)
	require.Len(t, prepared.fieldCaseDomains, 1)
	require.True(t, rt.mc.tryBeginRequest(), "rejection must leave the source usable")
	func() {
		defer rt.mc.endRequest()
		evaluate()
	}()
	ses.RemoveAllPrepareStmts()
	require.NoError(t, rt.migrateConnectionFrom(&query.MigrateConnFromResponse{}))
}

func TestMigrateConnectionFromPreservesPreparedVariableBinding(t *testing.T) {
	ses, scratch, cw, execCtx := newPreparedExecuteEnv(t, 125)
	t.Cleanup(ses.Close)
	staticType := plan.Type{Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetBinary)}
	require.NoError(t, ses.setUserDefinedVarWithTypeAndKindAndReplayability(
		"bound_s", "你", "", false, staticType, vector.PrepareParamNone, false, types.RuntimeStringText))
	stmt := tree.NewPrepareString("bound_migration", "select charset(@bound_s), char_length(coalesce(@bound_s, ?))")
	defer stmt.Free()
	p, err := buildPlan(execCtx.reqCtx, ses, ses.txnCompileCtx, stmt)
	require.NoError(t, err)
	parsed, err := mysql.Parse(execCtx.reqCtx, stmt.Sql, 1)
	require.NoError(t, err)
	prepared := &PrepareStmt{
		Name: "bound_migration", Sql: stmt.Sql, PreparePlan: p, PrepareStmt: parsed[0],
		NativeMode: ses.sqlModeHasMatrixOneNative(), protocolVersion: currentProtocolVersion(cw.proc),
		proc: cw.proc, ParamTypes: []byte{byte(defines.MYSQL_TYPE_NULL), 0},
		params: vector.NewVec(types.T_varchar.ToType()),
	}
	require.NoError(t, vector.AppendBytes(prepared.params, nil, true, cw.proc.Mp()))
	require.NoError(t, ses.SetPrepareStmt(execCtx.reqCtx, prepared.Name, prepared))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	original := p.String()

	for _, value := range []string{"你", "你好"} {
		// Migration's current snapshot differs from the frozen type and row domain.
		require.NoError(t, ses.setUserDefinedVarWithTypeAndKindAndReplayability(
			"bound_s", value, "", false,
			plan.Type{Id: int32(types.T_text), Charset: uint32(types.CharsetUTF8)},
			vector.PrepareParamNone, false, types.RuntimeStringBinary))
		resp := &query.MigrateConnFromResponse{}
		err := rt.migrateConnectionFrom(resp)
		require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
		require.Empty(t, resp.PrepareStmts)
		require.Equal(t, original, p.String())
		require.True(t, rt.mc.tryBeginRequest(), "rejected migration must release request admission")
		func() {
			defer rt.mc.endRequest()
			_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
			require.NoError(t, err)
			if owned {
				defer executionStmt.Free()
			}
			q := runtimePlan.GetQuery()
			projects := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList
			for i, expr := range projects {
				func() {
					executor, err := colexec.NewExpressionExecutor(cw.proc, expr)
					require.NoError(t, err)
					defer executor.Free()
					v, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					if i == 0 {
						require.Equal(t, "binary", v.GetStringAt(0))
					} else {
						require.Equal(t, int64(len([]rune(value))), vector.GetFixedAtNoTypeCheck[int64](v, 0))
					}
				}()
			}
			retryPlan, err := buildPlanForCompileRetry(execCtx.reqCtx, ses, ses.GetTxnCompileCtx(),
				prepared.PrepareStmt, false, &preparedExecutionRetry{
					bindings: cw.paramBindings, paramVals: cw.paramVals,
					preparedPlan: prepared.PreparePlan.GetDcl().GetPrepare().Plan,
				})
			require.NoError(t, err)
			retryQuery := retryPlan.GetQuery()
			retryProjects := retryQuery.Nodes[retryQuery.Steps[len(retryQuery.Steps)-1]].ProjectList
			executor, err := colexec.NewExpressionExecutor(cw.proc, retryProjects[1])
			require.NoError(t, err)
			defer executor.Free()
			retryValue, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, int64(len([]rune(value))), vector.GetFixedAtNoTypeCheck[int64](retryValue, 0))
		}()
		require.True(t, plan.HasBoundStringVariable(prepared.PreparePlan), "execution preparation and evaluation must not erase cached bindings")
		require.Equal(t, original, p.String())
	}

	require.True(t, ses.RemovePrepareStmt(prepared.Name))
	markerStmt := tree.NewPrepareString("marker_migration", "select char_length(?)")
	defer markerStmt.Free()
	markerPlan, err := buildPlan(execCtx.reqCtx, ses, ses.txnCompileCtx, markerStmt)
	require.NoError(t, err)
	require.NoError(t, ses.SetPrepareStmt(execCtx.reqCtx, "marker_migration", &PrepareStmt{
		Name: "marker_migration", Sql: markerStmt.Sql, PreparePlan: markerPlan,
	}))
	resp := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(resp))
	names := make([]string, 0, len(resp.PrepareStmts))
	for _, st := range resp.PrepareStmts {
		names = append(names, st.Name)
	}
	require.ElementsMatch(t, []string{scratch.Name, "marker_migration"}, names)
	ses.RemoveAllPrepareStmts()
	require.NoError(t, rt.migrateConnectionFrom(&query.MigrateConnFromResponse{}))
}

func TestMigrateConnectionFromWaitsForBoundStatementDeallocate(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_text), Charset: uint32(types.CharsetUTF8)},
		Expr: &plan.Expr_V{V: &plan.VarRef{Name: "s", BoundStringDomain: 1}},
	}}}}}}}
	prepared := &PrepareStmt{Name: "bound_deallocate", PreparePlan: p}
	require.NoError(t, ses.SetPrepareStmt(context.Background(), prepared.Name, prepared))
	t.Cleanup(ses.RemoveAllPrepareStmts)
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	require.True(t, rt.mc.tryBeginRequest())
	var finish sync.Once
	defer finish.Do(rt.mc.endRequest)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waiting := make(chan struct{})
	rt.mc.operationWaitHook = func() { close(waiting) }
	done := make(chan error, 1)
	go func() {
		done <- rt.migrateConnectionFromActionWithContext(ctx, query.MigrateConnFromAction_MigrateConnFromExport, &query.MigrateConnFromResponse{})
	}()
	joined := false
	t.Cleanup(func() {
		cancel()
		finish.Do(rt.mc.endRequest)
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("migration worker did not terminate during cleanup")
			}
		}
	})
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("migration did not wait for the request owner")
	}
	require.True(t, ses.RemovePrepareStmt(prepared.Name))
	finish.Do(rt.mc.endRequest)
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err, "migration must inspect plans only after deallocate finishes")
	case <-ctx.Done():
		t.Fatal("migration did not resume after deallocate")
	}
}
