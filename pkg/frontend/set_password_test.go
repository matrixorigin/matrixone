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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// setPasswordSqlForms returns every parser-admitted SET PASSWORD form: the self
// form, the admin form, the quoted-host form and the unknown-subject form. Every
// parsed variant must hit the same fixed rejection in the real frontend owner.
func setPasswordSqlForms() []string {
	return []string{
		"set password = 'P_w-live-self-9z'",
		"set password for u_probe_red = 'P_w-live-admin-8y'",
		"set password for 'u_probe_red'@'%' = 'P_w-live-qhost-7x'",
		"set password for ghost_user_nobody = 'P_w-live-ghost-6w'",
	}
}

// TestExecInFrontendSetPasswordRejectedForAllForms pins the direct-owner contract:
// execInFrontend must never return a nil error for *tree.SetPassword. Before the
// fix every form fell through the dispatch switch and the nil error reached
// respClientWhenSuccess, which turned it into a false OK on the wire while the
// old credential kept working. Each form runs as its own subtest so its session
// is closed immediately, including on assertion failures.
func TestExecInFrontendSetPasswordRejectedForAllForms(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msgs := make(map[string]string)
	for _, sql := range setPasswordSqlForms() {
		t.Run(sql, func(t *testing.T) {
			stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.IsType(t, &tree.SetPassword{}, stmts[0])

			ses := newTestSession(t, ctrl)
			defer ses.Close()
			execCtx := newTestExecCtx(ctx, ctrl)
			execCtx.ses = ses
			execCtx.stmt = stmts[0]
			execCtx.sqlOfStmt = sql

			_, err = execInFrontend(ses, execCtx)
			require.Error(t, err, "form %q must not return a false OK", sql)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
				"form %q must fail with ErrNotSupported, got: %v", sql, err)
			require.NotContains(t, err.Error(), "P_w-live", "no password echo in %q", sql)
			require.NotContains(t, err.Error(), "u_probe_red", "no subject/statement echo in %q", sql)
			msgs[sql] = err.Error()
		})
	}
	// one fixed error for every parsed form, no data-dependent text
	require.Len(t, msgs, len(setPasswordSqlForms()))
	var firstMsg string
	for _, sql := range setPasswordSqlForms() {
		if firstMsg == "" {
			firstMsg = msgs[sql]
			continue
		}
		require.Equal(t, firstMsg, msgs[sql], "form %q must produce the fixed message", sql)
	}
}

// txnTerminalCounts records which terminal operations the statement-owned
// transaction actually experienced. Blanket AnyTimes alone would mask the
// mandatory transactional cleanup, so the counters make that experience
// measurable and assertable.
type txnTerminalCounts struct {
	rollbacks int
	commits   int
}

// freshDoComQuerySession mirrors a real fresh autocommit connection: no active
// transaction at statement start, so the doComQuery admin-in-transaction gate
// stays bypassed exactly like on the wire, plus a working txn client and engine
// through the parameter unit so the statement can own its transaction. The
// shared parameter-unit TxnClient is saved and restored via t.Cleanup,
// registered at the moment of the mutation, so no later test ever inherits
// this fixture's mocks.
func freshDoComQuerySession(t *testing.T, ctrl *gomock.Controller) (*Session, *ExecCtx, *txnTerminalCounts) {
	t.Helper()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses}

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	// txn.go:777 th.storage.New(...) must bind the engine workspace to the
	// freshly created txn before the statement can run
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	counts := &txnTerminalCounts{}
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	// disttae.txnIsValid accepts the txn only with a non-nil workspace and an
	// active status
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
	txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	// run-SQL admission goes through the richer TryEnter path
	// (client.TryEnterRunSqlWithTokenAndSQL); token 0 keeps the legacy no-op
	// bookkeeping in finishRunSQL
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).DoAndReturn(
		func(context.Context) error { counts.rollbacks++; return nil }).AnyTimes()
	txnOperator.EXPECT().Commit(gomock.Any()).DoAndReturn(
		func(context.Context) error { counts.commits++; return nil }).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	pu := getPu("")
	savedTxnClient := pu.TxnClient
	pu.TxnClient = txnClient
	t.Cleanup(func() { pu.TxnClient = savedTxnClient })

	ses.txnHandler.storage = eng
	ses.txnHandler.txnOp = nil
	ses.txnHandler.txnCtx = ctx
	return ses, execCtx, counts
}

// TestDoComQuerySetPasswordRejected exercises the real COM_QUERY path through
// doComQuery on a fresh session: the statement that used to reach the client as
// a success OK must reach it as a fixed error carrying no password, subject or
// statement text, the admin and self forms must produce one identical message,
// and the statement-owned transaction must be finished by the existing error
// policy: rolled back, never force-committed. Each form runs as its own
// subtest so cleanup is immediate and orderly even on assertion failures.
func TestDoComQuerySetPasswordRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	adminForm := "set password for u_probe_red = 'P_w-live-admin-8y'"
	selfForm := "set password = 'P_w-live-self-9z'"
	msgs := make(map[string]string)
	for _, sql := range []string{adminForm, selfForm} {
		t.Run(sql, func(t *testing.T) {
			ses, execCtx, counts := freshDoComQuerySession(t, ctrl)
			// skipping authentication decouples this regression from
			// privilege/catalog setup, exactly like the neighboring
			// doComQuery tests do
			ses.SetTenantInfo(nil)
			ses.SetCmd(COM_QUERY)
			reqCtx, recordStubs := mockRecordStatement(execCtx.reqCtx)
			execCtx.reqCtx = reqCtx
			// immediate orderly cleanup, also on assertion failures below:
			// session close first (its statement recording still works),
			// then the recording stubs.
			defer func() {
				ses.Close()
				recordStubs.Reset()
			}()

			err := doComQuery(ses, execCtx, &UserInput{sql: sql})
			require.Error(t, err, "form %q must not reach the client as OK", sql)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
				"form %q must fail with ErrNotSupported, got: %v", sql, err)
			require.NotContains(t, err.Error(), "P_w-live", "no password echo in %q", sql)
			require.NotContains(t, err.Error(), "u_probe_red", "no subject/statement echo in %q", sql)
			require.GreaterOrEqual(t, counts.rollbacks, 1,
				"form %q: the statement-owned transaction must reach the rollback terminal (existing rollback-on-error policy)", sql)
			require.Zero(t, counts.commits,
				"form %q: a rejected statement must not commit its transaction", sql)
			msgs[sql] = err.Error()
		})
	}
	require.Len(t, msgs, 2)
	require.Equal(t, msgs[adminForm], msgs[selfForm],
		"admin and self forms must produce the identical fixed message")
}
