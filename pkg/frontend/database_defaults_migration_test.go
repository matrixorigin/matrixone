// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

type defaultsMigrationContextKey struct{}

type defaultsMigrationBackground struct {
	*accountRecordingBackgroundExec
	closed int
}

func (b *defaultsMigrationBackground) Close() { b.closed++ }

func TestDatabaseDefaultsMigrationWithoutRequestContext(t *testing.T) {
	setProtocolVersionForTest(t, "", defines.MORPCVersion118)
	for _, state := range []string{"between_requests", "finished_request", "active_request"} {
		t.Run(state, func(t *testing.T) {
			ses := newTestSession(t, gomock.NewController(t))
			defer ses.Close()
			tenant := *ses.GetTenantInfo()
			tenant.TenantID = 7
			ses.SetTenantInfo(&tenant)
			ses.SetAccountId(99) // Last-query accounting is not the authenticated owner.
			ses.SetDatabaseName("migration_defaults")
			connection, cancelConnection := context.WithCancel(context.WithValue(t.Context(), defaultsMigrationContextKey{}, "connection"))
			t.Cleanup(cancelConnection)
			handler := ses.GetTxnHandler()
			handler.mu.Lock()
			handler.connCtx = connection
			handler.mu.Unlock()
			compiler := ses.GetTxnCompileCtx()
			compiler.execCtx = &ExecCtx{ses: ses}
			if state != "between_requests" {
				request, cancel := context.WithCancel(defines.AttachAccountId(t.Context(), 99))
				t.Cleanup(cancel)
				if state == "finished_request" {
					cancel()
				}
				compiler.execCtx.reqCtx = request
			}
			base := &backgroundExecTest{}
			base.init()
			const query = "select cast(dd.collation_id as varchar(10)), cast(dd.collation_revision as varchar(10)), cast(dd.version as varchar(20)) from mo_catalog.mo_database db join mo_catalog.mo_database_defaults dd on dd.account_id=db.account_id and dd.database_id=db.dat_id where db.account_id=7 and db.datname='migration_defaults'"
			base.sql2result[query] = defaultsRestoreRows([][]interface{}{{"10", "1", "3"}})
			background := &defaultsMigrationBackground{accountRecordingBackgroundExec: &accountRecordingBackgroundExec{backgroundExecTest: base}}
			previous := NewBackgroundExec
			t.Cleanup(func() { NewBackgroundExec = previous })
			var opened int
			expectOperation := true
			NewBackgroundExec = func(ctx context.Context, owner FeSession, _ ...*BackgroundExecOption) BackgroundExec {
				require.Same(t, ses, owner)
				accountID, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(7), accountID, "session tenant, not migration RPC or expired request tenant")
				if expectOperation {
					require.Equal(t, "operation", ctx.Value(defaultsMigrationContextKey{}))
				} else {
					require.Equal(t, "connection", ctx.Value(defaultsMigrationContextKey{}))
				}
				require.NoError(t, context.Cause(ctx), "finished request must not cancel a new migration operation")
				opened++
				return background
			}
			operation := context.WithValue(defines.AttachAccountId(t.Context(), 42), defaultsMigrationContextKey{}, "operation")
			variables, err := ses.snapshotSessionSystemVars(operation)
			require.NoError(t, err)
			values := make(map[string]string)
			for _, variable := range variables {
				if variable.Name == "character_set_database" || variable.Name == "collation_database" {
					values[variable.Name] = variable.Value.GetLit().GetSval()
				}
			}
			require.Equal(t, map[string]string{"character_set_database": "utf8mb4", "collation_database": "utf8mb4_unicode_ci"}, values)
			require.Equal(t, 2, opened)
			require.Equal(t, opened, background.closed, "close every independently owned background executor")
			require.Equal(t, []string{query, query}, base.executedSQLs)
			require.Equal(t, uint32(7), background.accountID)
			if state == "between_requests" {
				expectOperation = false
				value, err := ses.GetSessionSysVar("collation_database")
				require.NoError(t, err)
				require.Equal(t, "utf8mb4_unicode_ci", value, "ordinary getter also handles a missing request context")
				expectOperation = true
			}
			injected := errors.New("migration catalog read failed")
			base.sql2err[query] = injected
			_, err = ses.snapshotSessionSystemVars(operation)
			require.ErrorIs(t, err, injected)
			require.Equal(t, opened, background.closed, "catalog errors must also close their reader")
			beforeCancel := opened
			if state == "between_requests" {
				cancelConnection()
				_, err = ses.GetSessionSysVar("collation_database")
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, beforeCancel, opened, "disconnect cancels the ordinary getter before acquiring a reader")
			}

			canceled, cancel := context.WithCancel(operation)
			cancel()
			_, err = ses.snapshotSessionSystemVars(canceled)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, beforeCancel, opened, "canceled migration must not acquire a catalog reader")
			ses.SetTenantInfo(nil)
			_, err = ses.snapshotSessionSystemVars(operation)
			require.ErrorContains(t, err, "missing session tenant")
			require.Equal(t, beforeCancel, opened, "missing authenticated identity must not acquire a reader")
			ses.SetTenantInfo(&tenant)
		})
	}
}

func TestDatabaseDefaultsGetterWithoutRequestContext(t *testing.T) {
	setProtocolVersionForTest(t, "", defines.MORPCVersion118)
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{ses: ses}
	for _, database := range []string{"", "information_schema"} {
		ses.SetDatabaseName(database)
		charset, err := ses.GetSessionSysVar("character_set_database")
		require.NoError(t, err)
		require.Equal(t, "utf8mb4", charset)
		collation, err := ses.GetSessionSysVar("collation_database")
		require.NoError(t, err)
		require.Equal(t, "utf8mb4_general_ci", collation)
	}
	_, handled, err := databaseDefaultSystemVariableWithContext(t.Context(), ses, "autocommit")
	require.NoError(t, err)
	require.False(t, handled)
	// collation_server is global-only. Model a different inherited global
	// snapshot without permitting an otherwise invalid SET SESSION.
	require.ErrorContains(t, ses.SetSessionSysVar(t.Context(), "collation_server", "utf8mb4_bin"), "system variable is global")
	ses.sesSysVars.Set("collation_server", "utf8mb4_bin")
	value, err := ses.GetSessionSysVar("collation_database")
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_bin", value)
}
