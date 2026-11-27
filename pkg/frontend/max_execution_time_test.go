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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMaxExecutionTimePreservesCancellationAndProcessContext(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(t.Context())
	defer cancelParent()
	procCtx, cancelProc := context.WithCancel(parentCtx)
	defer cancelProc()
	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(procCtx)
	proc.ResetQueryContext()
	ses := &Session{feSessionImpl: feSessionImpl{
		sesSysVars: &SystemVariables{mp: map[string]interface{}{maxExecutionTime: int64(60_000)}},
	}}
	ses.SetFromRealUser(true)
	execCtx := &ExecCtx{reqCtx: parentCtx, proc: proc, input: &UserInput{}, stmt: &tree.Select{}}

	finish, err := startMaxExecutionTimer(ses, execCtx)
	require.NoError(t, err)
	require.NotNil(t, finish)
	require.Same(t, execCtx.reqCtx, proc.GetTopContext())
	require.Same(t, execCtx.reqCtx, execCtx.txnAdmissionCtx)
	require.Same(t, execCtx.reqCtx, proc.Ctx)
	cancelParent()
	require.ErrorIs(t, finish(context.Canceled), context.Canceled)
	require.Same(t, parentCtx, execCtx.reqCtx)
	require.Nil(t, execCtx.txnAdmissionCtx)
	require.Same(t, procCtx, proc.GetTopContext())
	require.Same(t, procCtx, proc.Ctx)
}

func TestMaxExecutionTimePreservesTransactionFatalError(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &Session{feSessionImpl: feSessionImpl{
		sesSysVars: &SystemVariables{mp: map[string]interface{}{maxExecutionTime: int64(1)}},
	}}
	ses.SetFromRealUser(true)
	execCtx := &ExecCtx{reqCtx: t.Context(), proc: proc, input: &UserInput{}, stmt: &tree.Select{}}

	finish, err := startMaxExecutionTimer(ses, execCtx)
	require.NoError(t, err)
	require.NotNil(t, finish)
	select {
	case <-execCtx.reqCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("statement timer did not expire")
	}
	deadlock := moerr.NewDeadLockDetectedNoCtx()
	require.Same(t, deadlock, finish(deadlock))
}

func TestMaxExecutionTimeRequiresReadOnlySelect(t *testing.T) {
	locking := &tree.Select{SelectLockInfo: &tree.SelectLockInfo{LockType: tree.SelectLockForUpdate}}
	require.True(t, isReadOnlySelectForTimeout(&tree.Select{
		Select: &tree.ParenSelect{Select: &tree.Select{}},
	}))
	require.False(t, isReadOnlySelectForTimeout(locking))
	require.False(t, isReadOnlySelectForTimeout(&tree.Select{
		Select: &tree.UnionClause{
			Left:  &tree.SelectClause{},
			Right: &tree.ParenSelect{Select: locking},
		},
	}))
	require.False(t, isReadOnlySelectForTimeout(&tree.Select{Select: &tree.ParenSelect{Select: locking}}))
	require.False(t, isReadOnlySelectForTimeout(&tree.Select{Ep: &tree.ExportParam{}}))
	require.False(t, isReadOnlySelectForTimeout(&tree.Select{IsPerform: true}))
	require.False(t, isReadOnlySelectForTimeout(&tree.Insert{}))
}

func TestMaxExecutionTimeClassifiesParsedNestedLocks(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		readOnly bool
	}{
		{"select * from (select id from t for update) as locked", false},
		{"with locked as (select id from t for share) select * from locked", false},
		{"select (select id from t lock in share mode)", false},
		{"select 1 where exists (select id from t for /* row lock */ update)", false},
		{"prepare s from select * from (select id from t for update) as locked", false},
		{"with r as (select id from t) select * from r", true},
		{"select 'for update', 'lock in share mode' /* for share */", true},
		{"select `for update` from t", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmts, err := mysql.Parse(t.Context(), tc.sql+"; select 1", 0)
			require.NoError(t, err)
			require.Len(t, stmts, 2)
			stmt := stmts[0]
			if prepared, ok := stmt.(*tree.PrepareStmt); ok {
				stmt = prepared.Stmt
			}
			require.Equal(t, tc.readOnly, isReadOnlySelectForTimeout(stmt))
			require.True(t, isReadOnlySelectForTimeout(stmts[1]))
		})
	}
}
