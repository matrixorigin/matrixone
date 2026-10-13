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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestStartMaxExecutionTimerExpiresAndRestoresContext(t *testing.T) {
	parentCtx := context.Background()
	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(parentCtx)
	ses := maxExecutionTimeTestSession(10)
	execCtx := &ExecCtx{
		reqCtx: parentCtx,
		proc:   proc,
		stmt:   &tree.Select{},
		input:  &UserInput{},
	}

	finish, err := startMaxExecutionTimer(ses, execCtx)
	require.NoError(t, err)
	require.NotEqual(t, parentCtx, execCtx.reqCtx)
	require.Equal(t, execCtx.reqCtx, proc.GetTopContext())

	select {
	case <-execCtx.reqCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("max_execution_time did not cancel the statement context")
	}

	err = finish(context.DeadlineExceeded)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryTimeout))
	require.Equal(t, parentCtx, execCtx.reqCtx)
	require.Equal(t, parentCtx, proc.GetTopContext())
}

func TestStartMaxExecutionTimerPreservesAncestorCancellation(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(parentCtx)
	ses := maxExecutionTimeTestSession(10_000)
	execCtx := &ExecCtx{
		reqCtx: parentCtx,
		proc:   proc,
		stmt:   &tree.Select{},
		input:  &UserInput{},
	}

	finish, err := startMaxExecutionTimer(ses, execCtx)
	require.NoError(t, err)
	cancelParent()
	<-execCtx.reqCtx.Done()

	want := context.Canceled
	require.ErrorIs(t, finish(want), want)
	require.Equal(t, parentCtx, execCtx.reqCtx)
	require.Equal(t, parentCtx, proc.GetTopContext())
}

func TestStartMaxExecutionTimerOnlyAppliesToReadOnlySelect(t *testing.T) {
	tests := []struct {
		name            string
		stmt            tree.Statement
		input           *UserInput
		derived         bool
		internalSession bool
		inMigration     bool
	}{
		{name: "zero disables timeout", stmt: &tree.Select{}, input: &UserInput{}},
		{name: "write statement", stmt: &tree.Insert{}, input: &UserInput{}},
		{
			name:  "internal select",
			stmt:  &tree.Select{},
			input: &UserInput{isInternalInput: true},
		},
		{name: "derived select", stmt: &tree.Select{}, input: &UserInput{}, derived: true},
		{name: "internal session", stmt: &tree.Select{}, input: &UserInput{}, internalSession: true},
		{name: "migration select", stmt: &tree.Select{}, input: &UserInput{}, inMigration: true},
		{
			name: "locking select",
			stmt: &tree.Select{SelectLockInfo: &tree.SelectLockInfo{
				LockType: tree.SelectLockForUpdate,
			}},
			input: &UserInput{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentCtx := context.Background()
			proc := testutil.NewProcess(t)
			proc.ReplaceTopCtx(parentCtx)
			milliseconds := int64(10)
			if tt.name == "zero disables timeout" {
				milliseconds = 0
			}
			ses := maxExecutionTimeTestSession(milliseconds)
			ses.ReplaceDerivedStmt(tt.derived)
			ses.isInternal = tt.internalSession
			execCtx := &ExecCtx{
				reqCtx:      parentCtx,
				proc:        proc,
				stmt:        tt.stmt,
				input:       tt.input,
				inMigration: tt.inMigration,
			}

			finish, err := startMaxExecutionTimer(ses, execCtx)
			require.NoError(t, err)
			require.Equal(t, parentCtx, execCtx.reqCtx)
			require.NoError(t, finish(nil))
			require.Equal(t, parentCtx, proc.GetTopContext())
		})
	}
}

func TestMaxExecutionTimeAppliesToTextPreparedSelect(t *testing.T) {
	ses := maxExecutionTimeTestSession(10)
	ses.prepareStmts = map[string]*PrepareStmt{
		"read_stmt":  {PrepareStmt: &tree.Select{}},
		"write_stmt": {PrepareStmt: &tree.Insert{}},
	}

	applies, err := maxExecutionTimeApplies(
		context.Background(),
		ses,
		tree.NewExecute(tree.Identifier("read_stmt")),
	)
	require.NoError(t, err)
	require.True(t, applies)

	applies, err = maxExecutionTimeApplies(
		context.Background(),
		ses,
		tree.NewExecute(tree.Identifier("write_stmt")),
	)
	require.NoError(t, err)
	require.False(t, applies)
}

func maxExecutionTimeTestSession(milliseconds int64) *Session {
	return &Session{feSessionImpl: feSessionImpl{
		sesSysVars: &SystemVariables{mp: map[string]interface{}{
			maxExecutionTime: milliseconds,
		}},
	}}
}
