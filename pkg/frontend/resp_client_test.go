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
