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
	"strings"
	"sync"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	mysql "github.com/matrixorigin/mysql"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// The existing watermark fixture parses parenthesized PK predicates. Adapt the
// equivalent frontend predicate without stubbing the persisted error or decision.
type cdcRetryDiagnosticExecutor struct{ ie.InternalExecutor }

func (e cdcRetryDiagnosticExecutor) Query(ctx context.Context, query string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	if strings.HasPrefix(query, "SELECT err_msg ") {
		query = strings.Replace(query, " WHERE ", " WHERE (", 1) + ")"
	}
	return e.InternalExecutor.Query(ctx, query, opts)
}

// Exercise real admission, diagnostic persistence and the next admission read,
// rather than merely formatting retry metadata in the test.
func TestCDCRetryAdmissionPersistsTypedErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"native deadlock", moerr.NewDeadLockDetectedNoCtx(), true},
		{"production deadlock", &mysql.MySQLError{Number: 20701}, true},
		{"upstream deadlock", &gomysql.MySQLError{Number: 20701}, true},
		{"MySQL deadlock", &mysql.MySQLError{Number: 1213}, true},
		{"bind changed", &mysql.MySQLError{Number: 20702}, true},
		{"syntax", &mysql.MySQLError{Number: 1064}, false},
		{"permission", &mysql.MySQLError{Number: 1045}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubs := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
				return nil, nil
			})
			t.Cleanup(stubs.Reset)
			stubs.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
			calls := 0
			stubs.Stub(&cdc.GetTableDef, func(context.Context, client.TxnOperator, engine.Engine, uint64) (*plan.TableDef, error) {
				calls++
				return nil, tc.err
			})
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			updater, sqlExec := cdc.InitCDCWatermarkUpdaterForTest(t)
			updater.Start()
			t.Cleanup(updater.Stop)
			diagnostics := cdcRetryDiagnosticExecutor{sqlExec}
			exec := &CDCTaskExecutor{
				spec: &task.CreateCdcDetails{TaskId: "retry-admission", TaskName: "retry-admission", Accounts: []*task.Account{{Id: 1}}},
				tables: cdc.PatternTuples{Pts: []*cdc.PatternTuple{{
					Source: cdc.PatternTable{Database: "src", Table: "t"},
					Sink:   cdc.PatternTable{Database: "dst", Table: "t"},
				}}},
				cnEngine: eng, ie: diagnostics, watermarkUpdater: updater,
				runningReaders: &sync.Map{}, stateMachine: NewExecutorStateMachine(),
			}
			require.NoError(t, exec.stateMachine.Transition(TransitionStart))
			require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
			info := &cdc.DbTableInfo{SourceDbName: "src", SourceTblName: "t", SourceTblId: 1}
			tables := map[uint32]cdc.TblMap{1: {"src.t": info}}
			attempts := 1
			if tc.retry {
				attempts = cdc.MaxRetryCount + 1
			}
			for attempt := 1; attempt <= attempts; attempt++ {
				require.Error(t, exec.handleNewTables(tables))
				require.Equal(t, attempt, calls, "retry metadata must admit the next factory call")
				row, err := sqlExec.GetTableDataByPK("mo_catalog", "mo_cdc_watermark", []string{"1", "retry-admission", "src", "t"})
				require.NoError(t, err)
				require.Len(t, row, 6)
				meta := cdc.ParseErrorMetadata(row[5])
				require.NotNil(t, meta)
				wantRetry := tc.retry && attempt <= cdc.MaxRetryCount
				require.Equal(t, wantRetry, cdc.ShouldRetry(meta))
				blocked, err := GetTableErrMsg(t.Context(), 1, diagnostics, exec.spec.TaskId, info)
				require.NoError(t, err)
				require.Equal(t, !wantRetry, blocked)
			}
			require.Error(t, exec.handleNewTables(tables))
			require.Equal(t, attempts, calls, "permanent/exhausted diagnostics must block before target effects")
			require.Equal(t, StateFailed, exec.stateMachine.State())
		})
	}
}
