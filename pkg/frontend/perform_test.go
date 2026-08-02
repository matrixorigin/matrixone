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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type performTestRunner struct {
	result *util.RunResult
	err    error
}

func (r *performTestRunner) Run(uint64) (*util.RunResult, error) {
	return r.result, r.err
}

type performTestBinaryWriter struct {
	calls int
	err   error
}

func (w *performTestBinaryWriter) Write(*ExecCtx, *perfcounter.CounterSet, *batch.Batch) error {
	w.calls++
	return w.err
}

func (w *performTestBinaryWriter) Close() {}

func TestExecutePerformAsStatusStatement(t *testing.T) {
	testPlan := newResultColumnTestPlan(1)
	stmt := &tree.Select{IsPerform: true}
	runner := &performTestRunner{result: &util.RunResult{AffectRows: 9}}
	ses := &Session{feSessionImpl: feSessionImpl{mrs: &MysqlResultSet{}}}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   stmt,
		cw:     &TxnComputationWrapper{stmt: stmt, plan: testPlan},
		runner: runner,
	}

	require.NoError(t, executeStatusStmt(ses, execCtx))
	require.NotNil(t, ses.rs)
	require.Len(t, ses.rs.ResultCols, 1)
	require.Equal(t, uint64(0), execCtx.runResult.AffectRows)
}

func TestExecutePerformPropagatesRunnerError(t *testing.T) {
	testPlan := newResultColumnTestPlan(1)
	stmt := &tree.Select{IsPerform: true}
	wantErr := errors.New("perform failed")
	ses := &Session{feSessionImpl: feSessionImpl{mrs: &MysqlResultSet{}}}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   stmt,
		cw:     &TxnComputationWrapper{stmt: stmt, plan: testPlan},
		runner: &performTestRunner{err: wantErr},
	}

	require.ErrorIs(t, executeStatusStmt(ses, execCtx), wantErr)
	require.Nil(t, execCtx.runResult)
}

func TestExecutePerformPropagatesCancellation(t *testing.T) {
	testPlan := newResultColumnTestPlan(1)
	stmt := &tree.Select{IsPerform: true}
	ses := &Session{feSessionImpl: feSessionImpl{mrs: &MysqlResultSet{}}}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   stmt,
		cw:     &TxnComputationWrapper{stmt: stmt, plan: testPlan},
		runner: &performTestRunner{err: context.Canceled},
	}

	require.ErrorIs(t, executeStatusStmt(ses, execCtx), context.Canceled)
	require.Nil(t, execCtx.runResult)
}

func TestPerformResultIsSavedButNotWrittenToClient(t *testing.T) {
	saver := &performTestBinaryWriter{}
	resper := &MysqlResp{
		mysqlRrWr: &testMysqlWriter{},
		binWr:     saver,
	}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   &tree.Select{IsPerform: true},
		ses:    &Session{},
	}

	require.NoError(t, resper.RespResult(execCtx, nil, nil))
	require.Equal(t, 1, saver.calls)
}

func TestPerformPropagatesResultSaverError(t *testing.T) {
	wantErr := errors.New("save perform result failed")
	saver := &performTestBinaryWriter{err: wantErr}
	resper := &MysqlResp{
		mysqlRrWr: &testMysqlWriter{},
		binWr:     saver,
	}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   &tree.Select{IsPerform: true},
		ses:    &Session{},
	}

	require.ErrorIs(t, resper.RespResult(execCtx, nil, nil), wantErr)
	require.Equal(t, 1, saver.calls)
}

func TestPerformDoesNotCountSentRows(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	bat := allocTestBatch(mp, []string{"a"}, []types.Type{types.T_int64.ToType()}, 3)
	defer bat.Clean(mp)

	saver := &performTestBinaryWriter{}
	resper := &MysqlResp{mysqlRrWr: &testMysqlWriter{}, binWr: saver}
	ses := &Session{feSessionImpl: feSessionImpl{respr: resper}}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   &tree.Select{IsPerform: true},
		ses:    ses,
	}

	require.NoError(t, getDataFromPipeline(ses, execCtx, bat, nil))
	require.Equal(t, int64(0), ses.sentRows.Load())
	require.Equal(t, 1, saver.calls)
}

func TestPerformBackgroundFetchersDiscardResults(t *testing.T) {
	execCtx := &ExecCtx{stmt: &tree.Select{IsPerform: true}}
	bat := batch.NewWithSize(0)
	bat.SetRowCount(1)

	ses := &Session{feSessionImpl: feSessionImpl{mrs: &MysqlResultSet{}}}
	require.NoError(t, batchFetcher(ses, execCtx, bat, nil))
	require.Empty(t, ses.GetAllMysqlResultSet())

	back := &backSession{feSessionImpl: feSessionImpl{mrs: &MysqlResultSet{}}}
	require.NoError(t, batchFetcher2(back, execCtx, bat, nil))
	require.NoError(t, fakeDataSetFetcher2(back, execCtx, bat, nil))
	require.NoError(t, backSesOutputCallback(back, execCtx, bat, nil))
	require.Empty(t, back.GetAllMysqlResultSet())
}

func TestPerformPreparedStatementHasNoResultColumns(t *testing.T) {
	testPlan := newResultColumnTestPlan(1)
	columns := getPreparedResultColumnsFor(&tree.Select{IsPerform: true}, testPlan, false)
	require.Empty(t, columns)

	columns = getPreparedResultColumnsFor(&tree.Select{}, testPlan, false)
	require.Len(t, columns, 1)
}

func TestPerformReprepareKeepsNoResultColumns(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 13060, "perform select 1")
	defer prepareStmt.Close()

	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	var columnCount int
	writer.makeColumnDefDataFunc = func(_ context.Context, columns []*plan.ColDef) ([][]byte, error) {
		columnCount = len(columns)
		return nil, nil
	}

	oldPlan := prepareStmt.PreparePlan
	ses.AddTempTable("db1", "unrelated", "temp-unrelated")
	_, _, executionStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
	require.Zero(t, columnCount)
	selectStmt, ok := executionStmt.(*tree.Select)
	require.True(t, ok)
	require.True(t, selectStmt.IsPerform)
}

func TestRecordLastAffectedRowsForPerform(t *testing.T) {
	ses := &Session{}
	proc := &process.Process{Base: &process.BaseProcess{AffectedRows: new(int64)}}
	execCtx := &ExecCtx{
		stmt:      &tree.Select{IsPerform: true},
		runResult: &util.RunResult{AffectRows: 0},
		proc:      proc,
	}

	recordLastAffectedRows(ses, execCtx)
	require.Equal(t, int64(0), ses.GetLastAffectedRows())
	require.Equal(t, int64(0), proc.GetAffectedRows())
}

func TestPerformStatusResponseMoreResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		isLastStmt bool
		wantMore   bool
	}{
		{name: "followed by another statement", wantMore: true},
		{name: "last statement", isLastStmt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
			resper := NewMysqlResp(writer)
			ses := &Session{
				seqLastValue: new(string),
				feSessionImpl: feSessionImpl{
					mrs:        &MysqlResultSet{},
					txnHandler: &TxnHandler{},
				},
			}
			proc := &process.Process{Base: &process.BaseProcess{}}
			proc.InitSeq()
			execCtx := &ExecCtx{
				reqCtx:     context.Background(),
				stmt:       &tree.Select{IsPerform: true},
				isLastStmt: tc.isLastStmt,
				proc:       proc,
				runResult:  &util.RunResult{AffectRows: 0},
			}

			require.NoError(t, resper.respStatus(ses, execCtx))
			require.Len(t, writer.responses, 1)
			require.Equal(t, OkResponse, writer.responses[0].category)
			require.Equal(t, uint64(0), writer.responses[0].affectedRows)
			if tc.wantMore {
				require.NotZero(t, writer.responses[0].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
			} else {
				require.Zero(t, writer.responses[0].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
			}
		})
	}
}

func TestPerformUsesSelectPrivileges(t *testing.T) {
	selectPrivileges := determinePrivilegeSetOfStatement(&tree.Select{})
	performPrivileges := determinePrivilegeSetOfStatement(&tree.Select{IsPerform: true})
	require.Equal(t, selectPrivileges, performPrivileges)
}
