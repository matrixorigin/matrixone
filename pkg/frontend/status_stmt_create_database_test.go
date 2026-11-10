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
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestDatabaseWasCreated(t *testing.T) {
	for _, tc := range []struct {
		name      string
		runResult *util.RunResult
		want      bool
	}{
		{name: "physical creation", runResult: &util.RunResult{AffectRows: 1}, want: true},
		{name: "if not exists no-op", runResult: &util.RunResult{}},
		{name: "missing run result", runResult: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, databaseWasCreated(tc.runResult))
		})
	}
}

type statusCreateDatabaseRunner struct {
	result *util.RunResult
}

func (r *statusCreateDatabaseRunner) Run(uint64) (*util.RunResult, error) {
	return r.result, nil
}

func TestExecuteStatusCreateDatabaseUsesPhysicalCreationResult(t *testing.T) {
	for _, tc := range []struct {
		name      string
		runResult *util.RunResult
	}{
		{name: "physical creation", runResult: &util.RunResult{AffectRows: 1}},
		{name: "if not exists no-op", runResult: &util.RunResult{}},
		{name: "missing run result", runResult: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := &Session{}
			ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, DefaultRoleID: moAdminRoleID})
			ses.txnHandler = &TxnHandler{txnOp: mock_frontend.NewMockTxnOperator(gomock.NewController(t))}
			bh := &backgroundExecTest{}
			bh.init()
			stub := gostub.StubFunc(&NewShareTxnBackgroundExec, bh)
			defer stub.Reset()
			execCtx := &ExecCtx{
				reqCtx: context.Background(),
				stmt:   &tree.CreateDatabase{Name: tree.Identifier("ownership_db")},
				runner: &statusCreateDatabaseRunner{result: tc.runResult},
			}

			require.NoError(t, executeStatusStmt(ses, execCtx))
			require.Equal(t, tc.runResult, execCtx.runResult)
			if databaseWasCreated(tc.runResult) {
				require.Len(t, bh.executedSQLs, 1, "the creation owner initializes settings without BEGIN or COMMIT")
			} else {
				require.Empty(t, bh.executedSQLs)
			}
		})
	}
}

func TestGrantDatabaseOwnershipAfterCreate(t *testing.T) {
	stmt := &tree.CreateDatabase{Name: tree.Identifier("ownership_db")}

	t.Run("if not exists no-op", func(t *testing.T) {
		require.NoError(t, grantDatabaseOwnershipAfterCreate(
			context.Background(), nil, stmt, &util.RunResult{},
		))
	})
	t.Run("missing run result", func(t *testing.T) {
		require.NoError(t, grantDatabaseOwnershipAfterCreate(
			context.Background(), nil, stmt, nil,
		))
	})
	t.Run("physical creation", func(t *testing.T) {
		ses := &Session{}
		ses.SetTenantInfo(&TenantInfo{DefaultRoleID: moAdminRoleID})
		require.NoError(t, grantDatabaseOwnershipAfterCreate(
			context.Background(), ses, stmt, &util.RunResult{AffectRows: 1},
		))
	})
}

func TestRespStatusCreateDatabaseDoesNotWriteCatalog(t *testing.T) {
	for _, tc := range []struct {
		name         string
		runResult    *util.RunResult
		wantAffected uint64
	}{
		{name: "physical creation", runResult: &util.RunResult{AffectRows: 1}, wantAffected: 1},
		{name: "if not exists no-op", runResult: &util.RunResult{}},
		{name: "missing run result", runResult: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := &Session{
				seqLastValue:  new(string),
				feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}},
			}
			ses.SetTenantInfo(&TenantInfo{
				Tenant:        sysAccountName,
				User:          rootName,
				DefaultRole:   moAdminRoleName,
				TenantID:      sysAccountID,
				UserID:        rootID,
				DefaultRoleID: moAdminRoleID,
			})

			bh := &backgroundExecTest{}
			bh.init()
			bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer bhStub.Reset()
			sharedStub := gostub.StubFunc(&NewShareTxnBackgroundExec, bh)
			defer sharedStub.Reset()

			writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
			proc := &process.Process{Base: &process.BaseProcess{}}
			proc.InitSeq()
			execCtx := &ExecCtx{
				reqCtx:    context.Background(),
				stmt:      &tree.CreateDatabase{Name: tree.Identifier("metadata_db")},
				proc:      proc,
				runResult: tc.runResult,
			}

			require.NoError(t, NewMysqlResp(writer).respStatus(ses, execCtx))
			require.Len(t, writer.responses, 1)
			require.Equal(t, tc.wantAffected, writer.responses[0].affectedRows)
			require.Empty(t, bh.executedSQLs)
		})
	}
}

func TestCreatedDatabaseCompatibilityUsesStatementOwner(t *testing.T) {
	for _, scenario := range []string{"success", "insert failure", "no-op", "missing owner", "missing tenant", "invalid version", "system"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			ses := &Session{}
			ses.SetTenantInfo(&TenantInfo{Tenant: "account'\\name", TenantID: 7})
			ses.txnHandler = &TxnHandler{txnOp: op}
			ses.sesSysVars = &SystemVariables{mp: map[string]interface{}{"version": "configured'\\version"}}
			stmt := &tree.CreateDatabase{Name: tree.Identifier("db'\\name")}
			result := &util.RunResult{AffectRows: 1}
			switch scenario {
			case "no-op":
				result.AffectRows = 0
			case "missing owner":
				ses.txnHandler.txnOp = nil
			case "missing tenant":
				ses.SetTenantInfo(nil)
			case "invalid version":
				ses.sesSysVars.Set("version", int64(7))
			case "system":
				stmt.Name = "MO_CATALOG"
			}
			bh := &backgroundExecTest{}
			bh.init()
			bh.parserSQLMode = "NO_BACKSLASH_ESCAPES"
			injected := errors.New("compatibility INSERT failed")
			if scenario == "insert failure" {
				bh.sqlPrefixErr["insert into mo_catalog.mo_mysql_compatibility_mode"] = injected
			}
			calls := 0
			stub := gostub.Stub(&NewShareTxnBackgroundExec, func(got context.Context, owner FeSession, raw bool) BackgroundExec {
				calls++
				require.Same(t, ctx, got)
				require.Same(t, ses, owner)
				require.Same(t, op, owner.GetTxnHandler().GetTxn())
				require.False(t, raw)
				return bh
			})
			defer stub.Reset()
			err := initializeCreatedDatabaseCompatibility(ctx, ses, stmt, result)
			switch scenario {
			case "insert failure":
				require.ErrorIs(t, err, injected)
			case "missing owner", "missing tenant", "invalid version":
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			if scenario == "success" || scenario == "insert failure" {
				require.Equal(t, 1, calls)
				require.Len(t, bh.executedSQLs, 1)
				require.True(t, strings.HasPrefix(bh.executedSQLs[0], "insert into mo_catalog.mo_mysql_compatibility_mode"))
				require.Contains(t, bh.executedSQLs[0], "values(7,'account''\\\\name','db''\\\\name','version_compatibility','configured''\\\\version',false)")
				require.Empty(t, bh.parserSQLMode)
			} else {
				require.Zero(t, calls)
				require.Empty(t, bh.executedSQLs)
			}
		})
	}
}
