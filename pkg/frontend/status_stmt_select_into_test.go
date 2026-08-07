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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/require"
)

type statusStmtTestRunner struct {
	result *util.RunResult
	err    error
}

func (r *statusStmtTestRunner) Run(uint64) (*util.RunResult, error) {
	return r.result, r.err
}

func newSelectIntoStatusTestContext(stmt *tree.Select, runner *statusStmtTestRunner) (*Session, *ExecCtx) {
	ses := &Session{
		feSessionImpl:   feSessionImpl{mrs: &MysqlResultSet{}},
		userDefinedVars: make(map[string]*UserDefinedVar),
	}
	execCtx := &ExecCtx{
		reqCtx:    context.Background(),
		stmt:      stmt,
		cw:        &TxnComputationWrapper{stmt: stmt, plan: newResultColumnTestPlan(len(stmt.IntoVars))},
		runner:    runner,
		ses:       ses,
		sqlOfStmt: "select 7 into @out",
	}
	return ses, execCtx
}

func newValidStatusTxnHandler(t *testing.T) *TxnHandler {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(workspace)
	txnOp.EXPECT().Status().Return(txn.TxnStatus_Active)
	return &TxnHandler{txnOp: txnOp}
}

func newBackSelectIntoStatusTestContext(t *testing.T, stmt *tree.Select, runner *statusStmtTestRunner) (*backSession, *Session, *ExecCtx) {
	upstream := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			mrs:        &MysqlResultSet{},
			txnHandler: newValidStatusTxnHandler(t),
			upstream:   upstream,
		},
	}
	execCtx := &ExecCtx{
		reqCtx:    context.Background(),
		stmt:      stmt,
		cw:        &TxnComputationWrapper{stmt: stmt, plan: newResultColumnTestPlan(len(stmt.IntoVars))},
		runner:    runner,
		ses:       backSes,
		sqlOfStmt: "select 7 into @out",
	}
	return backSes, upstream, execCtx
}

func seedSelectIntoCollector(collector *selectIntoUserVariables, value any) {
	collector.row = []any{value}
	collector.rowIsBin = []bool{false}
	collector.rowCount = 1
}

func TestExecuteStatusStmtSelectIntoAssignsUserVariable(t *testing.T) {
	stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
	ses, execCtx := newSelectIntoStatusTestContext(stmt, &statusStmtTestRunner{result: &util.RunResult{AffectRows: 3}})
	execCtx.selectInto = newSelectIntoUserVariables(stmt.IntoVars)
	seedSelectIntoCollector(execCtx.selectInto, int64(7))

	require.NoError(t, executeStatusStmt(ses, execCtx))
	value, err := ses.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, int64(7), value.Value)
}

func TestExecuteStatusStmtSelectIntoRejectsInvalidOrFailedExecution(t *testing.T) {
	t.Run("arity mismatch", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}, {Name: "other"}}}
		ses, execCtx := newSelectIntoStatusTestContext(stmt, &statusStmtTestRunner{result: &util.RunResult{}})
		execCtx.cw = &TxnComputationWrapper{stmt: stmt, plan: newResultColumnTestPlan(1)}
		err := executeStatusStmt(ses, execCtx)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongNumberOfColumnsInSelect))
	})

	t.Run("runner error", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		ses, execCtx := newSelectIntoStatusTestContext(stmt, &statusStmtTestRunner{err: errors.New("runner failed")})
		require.ErrorContains(t, executeStatusStmt(ses, execCtx), "runner failed")
	})

	t.Run("collector not initialized", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		ses, execCtx := newSelectIntoStatusTestContext(stmt, &statusStmtTestRunner{result: &util.RunResult{}})
		require.ErrorContains(t, executeStatusStmt(ses, execCtx), "collector is not initialized")
	})

	t.Run("apply error", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		ses, execCtx := newSelectIntoStatusTestContext(stmt, &statusStmtTestRunner{result: &util.RunResult{}})
		execCtx.selectInto = newSelectIntoUserVariables(stmt.IntoVars)
		execCtx.selectInto.rowCount = 2
		require.True(t, moerr.IsMoErrCode(executeStatusStmt(ses, execCtx), moerr.ErrTooManyRows))
	})
}

func TestExecuteStatusStmtInBackSelectIntoAssignsUserVariable(t *testing.T) {
	stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
	backSes, upstream, execCtx := newBackSelectIntoStatusTestContext(t, stmt, &statusStmtTestRunner{result: &util.RunResult{}})
	execCtx.selectInto = newSelectIntoUserVariables(stmt.IntoVars)
	seedSelectIntoCollector(execCtx.selectInto, int64(7))

	require.NoError(t, executeStatusStmtInBack(backSes, execCtx))
	value, err := upstream.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, int64(7), value.Value)
}

func TestExecuteStatusStmtInBackSelectIntoRejectsInvalidOrFailedExecution(t *testing.T) {
	t.Run("arity mismatch", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}, {Name: "other"}}}
		backSes, _, execCtx := newBackSelectIntoStatusTestContext(t, stmt, &statusStmtTestRunner{result: &util.RunResult{}})
		execCtx.cw = &TxnComputationWrapper{stmt: stmt, plan: newResultColumnTestPlan(1)}
		require.True(t, moerr.IsMoErrCode(executeStatusStmtInBack(backSes, execCtx), moerr.ErrWrongNumberOfColumnsInSelect))
	})

	t.Run("runner error", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		backSes, _, execCtx := newBackSelectIntoStatusTestContext(t, stmt, &statusStmtTestRunner{err: errors.New("runner failed")})
		require.ErrorContains(t, executeStatusStmtInBack(backSes, execCtx), "runner failed")
	})

	t.Run("collector not initialized", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		backSes, _, execCtx := newBackSelectIntoStatusTestContext(t, stmt, &statusStmtTestRunner{result: &util.RunResult{}})
		require.ErrorContains(t, executeStatusStmtInBack(backSes, execCtx), "collector is not initialized")
	})

	t.Run("apply error", func(t *testing.T) {
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		backSes, _, execCtx := newBackSelectIntoStatusTestContext(t, stmt, &statusStmtTestRunner{result: &util.RunResult{}})
		execCtx.selectInto = newSelectIntoUserVariables(stmt.IntoVars)
		execCtx.selectInto.rowCount = 2
		require.True(t, moerr.IsMoErrCode(executeStatusStmtInBack(backSes, execCtx), moerr.ErrTooManyRows))
	})
}
