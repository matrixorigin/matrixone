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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	for _, opt := range []int64{0, 1} {
		for _, command := range []CommandType{COM_QUERY, COM_STMT_PREPARE} {
			t.Run(fmt.Sprintf("%s/rollback=%d", command, opt), func(t *testing.T) {
				ctx := defines.AttachAccountId(t.Context(), sysAccountID)
				ctrl := gomock.NewController(t)
				ses := newTestSession(t, ctrl)
				defer ses.Close()
				require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_error", opt))
				getPu(ses.GetService()).SV.SkipCheckPrivilege = true
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
				run := func(cmd CommandType, statement string) *Response {
					t.Helper()
					ec := newTestExecCtx(ctx, ctrl)
					defer ec.Close()
					resp, err := ExecRequest(ses, ec, &Request{cmd: cmd, data: []byte(statement)})
					require.NoError(t, err)
					return resp
				}

				resp := run(command, "select 1")
				require.NotNil(t, resp)
				require.Equal(t, ErrorResponse, resp.category)
				require.ErrorIs(t, resp.GetData().(error), failure)
				if opt == 0 {
					require.Same(t, op, ses.GetTxnHandler().GetTxn())
					require.Zero(t, op.rollbackCalls)
					resp = run(COM_QUERY, "rollback; select 1")
					require.Equal(t, ErrorResponse, resp.category)
					require.Same(t, op, ses.GetTxnHandler().GetTxn())
					run(COM_QUERY, "rollback")
				}
				require.Equal(t, 1, op.rollbackCalls)
				require.Nil(t, ses.GetTxnHandler().GetTxn())
				require.False(t, ses.GetTxnHandler().InActiveTxn())
			})
		}
	}
}

type statementPolicyVersionReader struct {
	engine.Engine
	onRead func(timestamp.Timestamp, int)
}

func (r *statementPolicyVersionReader) ReadTableContentVersions(_ context.Context, snapshot timestamp.Timestamp, deps []engine.TableContentDependency, _ []engine.TableContentVersion) bool {
	r.onRead(snapshot, len(deps))
	return true
}

func TestRoleRuleAndObjectPrivilegesShareStatementSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "fixture", User: "reader", DefaultRole: "reader", TenantID: 3, UserID: 4, DefaultRoleID: 5})

	first := timestamp.Timestamp{PhysicalTime: 10}
	later := timestamp.Timestamp{PhysicalTime: 20}
	client := &authorizationSnapshotReader{snapshot: first}
	principal := currentAuthorizationPrincipal(ses)
	cache := ses.GetPrivilegeCache()
	cache.certificate = authorizationCertificate{principal: principal, reusableFrom: first}
	cache.grantRoleID = int64(principal.role)
	grant := privilegeEntriesMap[PrivilegeTypeShowDatabases]
	cache.add(grant.objType, grant.privilegeLevel, grant.databaseName, grant.tableName, grant.privilegeId)
	policy := &roleRuleCache{proof: roleRuleCacheProof{principal: principal, snapshot: first}}
	pu := getPu(ses.GetService())
	pu.TxnClient = client

	// Emulate a newer catalog publication after policy selection, before
	// COM_QUERY runs the ordinary object privilege admission.
	var observations []timestamp.Timestamp
	var cancel context.CancelFunc
	pu.StorageEngine = &statementPolicyVersionReader{onRead: func(snapshot timestamp.Timestamp, count int) {
		observations = append(observations, snapshot)
		if count == authorizationDependencyCount {
			cancel() // Stop after admission has observed its view.
		}
	}}
	stubs := gostub.Stub(&captureRoleRuleCache, func(ctx context.Context, owner *Session) (*roleRuleCache, error) {
		snapshot, err := readStatementAuthorizationSnapshot(ctx, owner, true)
		require.NoError(t, err)
		require.True(t, policy.proof.valid(ctx, principal, pu.StorageEngine.(engine.TableContentReader), snapshot, owner.ruleCacheObserved[:]))
		client.snapshot = later
		return policy, nil
	})
	stubs.Stub(&privilegeCacheIsEnabled, func(context.Context, *Session) (bool, error) { return true, nil })
	defer stubs.Reset()

	request := func() {
		t.Helper()
		var ctx context.Context
		ctx, cancel = context.WithCancel(defines.AttachAccountId(t.Context(), principal.account))
		defer cancel()
		ec := newTestExecCtx(ctx, ctrl)
		defer ec.Close()
		resp, err := ExecRequest(ses, ec, &Request{cmd: COM_QUERY, data: []byte("show databases")})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, ErrorResponse, resp.category)
		require.ErrorIs(t, resp.GetData().(error), context.Canceled)
	}
	request()
	request()
	require.Equal(t, []timestamp.Timestamp{first, first, later, later}, observations)
}

func TestRoleRuleStatementSupportRejectsUnboundReads(t *testing.T) {
	for _, stmt := range []tree.Statement{
		&tree.Merge{},
		&tree.ShowTables{Where: &tree.Where{Expr: &tree.Subquery{}}},
	} {
		rewrite, supported := roleRuleStatementSupport(stmt)
		require.False(t, rewrite)
		require.False(t, supported)
	}
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
