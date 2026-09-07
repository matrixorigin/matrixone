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
	"strings"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestDatabaseWasCreated(t *testing.T) {
	for _, tc := range []struct {
		name      string
		result    *util.RunResult
		wasCreate bool
	}{
		{name: "physical creation", result: &util.RunResult{AffectRows: 1}, wasCreate: true},
		{name: "if not exists no-op", result: &util.RunResult{}},
		{name: "missing result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wasCreate, databaseWasCreated(tc.result))
		})
	}
}

func TestGrantDatabaseOwnershipSkipsNoopCreate(t *testing.T) {
	stmt := &tree.CreateDatabase{Name: tree.Identifier("ownership_db")}
	require.NoError(t, grantDatabaseOwnershipAfterCreate(
		context.Background(), nil, stmt, &util.RunResult{},
	))
	require.NoError(t, grantDatabaseOwnershipAfterCreate(
		context.Background(), nil, stmt, nil,
	))
}

func TestRespStatusCreateDatabaseWritesCompatibilityOnlyAfterCreation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     *util.RunResult
		wantInsert bool
	}{
		{name: "physical creation", result: &util.RunResult{AffectRows: 1}, wantInsert: true},
		{name: "if not exists no-op", result: &util.RunResult{}},
		{name: "missing result"},
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

			writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
			proc := &process.Process{Base: &process.BaseProcess{}}
			proc.InitSeq()
			execCtx := &ExecCtx{
				reqCtx:    context.Background(),
				stmt:      &tree.CreateDatabase{Name: tree.Identifier("metadata_db")},
				proc:      proc,
				runResult: tc.result,
			}

			require.NoError(t, NewMysqlResp(writer).respStatus(ses, execCtx))
			require.Len(t, writer.responses, 1)
			if tc.wantInsert {
				require.Len(t, bh.executedSQLs, 3)
				require.Equal(t, "begin", bh.executedSQLs[0])
				require.True(t, strings.HasPrefix(
					bh.executedSQLs[1],
					"insert into mo_catalog.mo_mysql_compatibility_mode(",
				))
				require.Equal(t, "commit;", bh.executedSQLs[2])
			} else {
				require.Empty(t, bh.executedSQLs)
			}
		})
	}
}
