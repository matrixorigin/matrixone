// Copyright 2021 - 2024 Matrix Origin
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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func Test_mysqlResp(t *testing.T) {
	var resp *MysqlResp
	resp.SetStr(DBNAME, "test")
	resp.GetStr(DBNAME)
	resp.SetU32(CONNID, 100)
	resp.GetU32(CONNID)
	resp.ResetStatistics()
}

func TestSessionLastAffectedRows(t *testing.T) {
	ses := &Session{}
	require.Equal(t, int64(0), ses.GetLastAffectedRows())

	ses.SetLastAffectedRows(7)
	require.Equal(t, int64(7), ses.GetLastAffectedRows())

	// -1 sentinel used after result-set statements.
	ses.SetLastAffectedRows(-1)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
}

func TestMarkRowCountFailed(t *testing.T) {
	// session + proc both reset to -1
	ses := &Session{}
	proc := &process.Process{Base: &process.BaseProcess{AffectedRows: new(int64)}}
	ses.SetLastAffectedRows(9)
	proc.SetAffectedRows(9)
	markRowCountFailed(ses, proc)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
	require.Equal(t, int64(-1), proc.GetAffectedRows())

	// nil proc is tolerated; the session is still updated. This mirrors the
	// COM_STMT_EXECUTE parse-failure path, which never reaches doComQuery.
	ses2 := &Session{}
	ses2.SetLastAffectedRows(5)
	markRowCountFailed(ses2, nil)
	require.Equal(t, int64(-1), ses2.GetLastAffectedRows())
}

func TestRestoreRowCount(t *testing.T) {
	ses := &Session{}
	proc := &process.Process{Base: &process.BaseProcess{AffectedRows: new(int64)}}
	// A protocol-only command (e.g. DEALLOCATE PREPARE from COM_STMT_CLOSE) just
	// recorded 0; restore the preceding statement's value.
	ses.SetLastAffectedRows(0)
	proc.SetAffectedRows(0)
	restoreRowCount(ses, proc, 7)
	require.Equal(t, int64(7), ses.GetLastAffectedRows())
	require.Equal(t, int64(7), proc.GetAffectedRows())

	// nil proc is tolerated.
	ses2 := &Session{}
	restoreRowCount(ses2, nil, 3)
	require.Equal(t, int64(3), ses2.GetLastAffectedRows())
}

func TestRecordLastAffectedRowsFieldList(t *testing.T) {
	ses := &Session{}
	proc := &process.Process{Base: &process.BaseProcess{AffectedRows: new(int64)}}
	// A prior DML set ROW_COUNT() to 5.
	ses.SetLastAffectedRows(5)
	proc.SetAffectedRows(5)

	// COM_FIELD_LIST terminates its metadata result with EOF and sets ROW_COUNT()
	// to the result-set sentinel.
	execCtx := &ExecCtx{stmt: &InternalCmdFieldList{}, proc: proc}
	recordLastAffectedRows(ses, execCtx)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
	require.Equal(t, int64(-1), proc.GetAffectedRows())
}

func TestRecordLastAffectedRows(t *testing.T) {
	cases := []struct {
		name   string
		stmt   tree.Statement
		affect uint64
		want   int64
	}{
		{"insert", &tree.Insert{}, 5, 5},
		{"update", &tree.Update{}, 3, 3},
		{"delete", &tree.Delete{}, 1, 1},
		{"replace", &tree.Replace{}, 2, 2},
		{"load", &tree.Load{}, 7, 7},
		// status-returning select (SELECT ... INTO ...) keeps its affected rows.
		{"select into -> affect", &tree.Select{Ep: &tree.ExportParam{}}, 9, 9},
		// plain result-set select reports -1 regardless of AffectRows.
		{"select -> -1", &tree.Select{}, 4, -1},
		// DDL is a status statement but affects no rows.
		{"ddl -> 0", &tree.CreateTable{}, 0, 0},
		// CALL propagates the final statement's affected rows from the procedure.
		{"call", &tree.CallStmt{}, 6, 6},
	}
	for _, c := range cases {
		ses := &Session{}
		proc := &process.Process{Base: &process.BaseProcess{AffectedRows: new(int64)}}
		execCtx := &ExecCtx{
			stmt:      c.stmt,
			runResult: &util.RunResult{AffectRows: c.affect},
			proc:      proc,
		}
		recordLastAffectedRows(ses, execCtx)
		// Written to both the process (for same-proc multi-statement COM_QUERY)
		// and the session (for the next COM_QUERY).
		require.Equal(t, c.want, proc.GetAffectedRows(), "%s: proc", c.name)
		require.Equal(t, c.want, ses.GetLastAffectedRows(), "%s: session", c.name)
	}
}

func TestAffectedRowsForBackgroundStatement(t *testing.T) {
	cases := []struct {
		name   string
		stmt   tree.Statement
		result *util.RunResult
		want   int64
	}{
		{"insert", &tree.Insert{}, &util.RunResult{AffectRows: 5}, 5},
		{"select", &tree.Select{}, nil, -1},
		{"ddl", &tree.CreateTable{}, &util.RunResult{}, 0},
		{"call", &tree.CallStmt{}, &util.RunResult{AffectRows: 3}, 3},
		{"call without result", &tree.CallStmt{}, nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			execCtx := &ExecCtx{stmt: c.stmt, runResult: c.result}
			require.Equal(t, c.want, affectedRowsForStatement(execCtx))
		})
	}
}

func TestInterpreterBackgroundAffectedRows(t *testing.T) {
	back := &backExec{backSes: &backSession{}}
	interpreter := &Interpreter{bh: back}

	interpreter.setAffectedRows(7)
	require.Equal(t, int64(7), interpreter.lastAffectedRows)
	require.Equal(t, int64(7), back.GetLastAffectedRows())

	back.SetLastAffectedRows(-1)
	interpreter.recordAffectedRows()
	require.Equal(t, int64(-1), interpreter.lastAffectedRows)
}

type evalCondBackgroundExec struct {
	BackgroundExec
	rows    int64
	result  []interface{}
	execErr error
}

func (e *evalCondBackgroundExec) ClearExecResultSet() {
	e.result = nil
}

func (e *evalCondBackgroundExec) Exec(context.Context, string) error {
	e.rows = -1
	if e.execErr == nil {
		e.result = []interface{}{&evalCondResult{value: 1}}
	}
	return e.execErr
}

func (e *evalCondBackgroundExec) GetExecResultSet() []interface{} {
	return e.result
}

func (e *evalCondBackgroundExec) GetLastAffectedRows() int64 {
	return e.rows
}

func (e *evalCondBackgroundExec) SetLastAffectedRows(rows int64) {
	e.rows = rows
}

type evalCondResult struct {
	ExecResult
	value int64
}

func (e *evalCondResult) GetRowCount() uint64 {
	return 1
}

func (e *evalCondResult) GetInt64(context.Context, uint64, uint64) (int64, error) {
	return e.value, nil
}

func TestEvalCondRestoresAffectedRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		execErr error
	}{
		{name: "success"},
		{name: "failure", execErr: errors.New("condition failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			back := &evalCondBackgroundExec{execErr: tc.execErr}
			varScope := []map[string]interface{}{}
			interpreter := &Interpreter{
				ctx:              context.Background(),
				bh:               back,
				varScope:         &varScope,
				lastAffectedRows: 7,
			}
			back.SetLastAffectedRows(7)

			cond, err := interpreter.EvalCond("1 = 1")
			if tc.execErr != nil {
				require.ErrorIs(t, err, tc.execErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, cond)
			}
			require.Equal(t, int64(7), interpreter.lastAffectedRows)
			require.Equal(t, int64(7), back.GetLastAffectedRows())
		})
	}
}
