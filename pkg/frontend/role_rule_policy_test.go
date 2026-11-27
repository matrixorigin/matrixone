// Copyright 2024 Matrix Origin
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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestRoleRuleCacheProofRejectsUnknownOrChangedVersions(t *testing.T) {
	ctx := t.Context()
	view := timestamp.Timestamp{PhysicalTime: 10}
	reader := &authorizationVersionReader{}
	principal := authorizationPrincipal{account: 1, user: 2, role: 3}
	proof := roleRuleCacheProof{principal: principal, snapshot: view}
	var observed [roleRuleDependencyCount]engine.TableContentVersion
	require.True(t, proof.valid(ctx, principal, reader, view, observed[:]))
	for i := range proof.versions {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			reader.versions[i].Revision++
			require.False(t, proof.valid(ctx, principal, reader, view, observed[:]))
			reader.versions[i].Revision--
		})
	}
	require.False(t, proof.valid(ctx, principal, nil, view, observed[:]))
	require.False(t, proof.valid(ctx, authorizationPrincipal{}, reader, view, observed[:]))
	require.False(t, proof.valid(ctx, principal, reader, view.Prev(), observed[:]))
}

func TestExecRequestRoleRuleCatalogFailureAllowsRollback(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), sysAccountID)
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	pu := getPu(ses.GetService())
	pu.SV.SkipCheckPrivilege = true
	storage := mock_frontend.NewMockEngine(ctrl)
	storage.EXPECT().Hints().Return(engine.Hints{}).AnyTimes()
	ses.txnHandler.storage = storage
	op := newTestTxnOp()
	op.meta = txn.TxnMeta{ID: []byte{1}, Status: txn.TxnStatus_Active}
	ses.txnHandler.txnOp = op
	ses.txnHandler.txnCtx = ctx
	ses.txnHandler.SetOptionBits(OPTION_BEGIN)
	failure := moerr.NewInternalError(ctx, "test catalog read failure")
	stubs := gostub.Stub(&captureRoleRuleCache, func(context.Context, *Session) (*roleRuleCache, error) {
		return nil, failure
	})
	defer stubs.Reset()

	run := func(statement string) *Response {
		t.Helper()
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		defer ec.Close()
		resp, err := ExecRequest(ses, ec, &Request{cmd: COM_QUERY, data: []byte(statement)})
		require.NoError(t, err)
		return resp
	}
	for _, statement := range []string{"select 1", "rollback; select 1"} {
		resp := run(statement)
		require.NotNil(t, resp)
		require.Equal(t, ErrorResponse, resp.category)
		require.Same(t, op, ses.GetTxnHandler().GetTxn())
	}
	run("rollback")
	require.Equal(t, 1, op.rollbackCalls)
	require.Nil(t, ses.GetTxnHandler().GetTxn())
	require.False(t, ses.GetTxnHandler().InActiveTxn())
}

func TestRoleRuleMigrationRejectsPreparedStateAtSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	require.NoError(t, ses.SetPrepareStmt(t.Context(), "p", &PrepareStmt{Name: "p", Sql: "select 1"}))
	resp := &query.MigrateConnFromResponse{}
	require.ErrorIs(t, rt.migrateConnectionFromWithContext(t.Context(), resp), moerr.GetOkExpectedNotSafeToStartTransfer())
	require.Empty(t, resp.PrepareStmts)
	_, err := ses.GetPrepareStmt(t.Context(), "p")
	require.NoError(t, err)
}
