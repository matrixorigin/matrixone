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
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"testing"
)

// These adapters expose existing owners only to the external engine-backed test.
func AuthorizationParameterUnitForTest() *config.ParameterUnit { return getPu("") }
func NewAuthorizationSessionForTest(t *testing.T, ctx context.Context) *Session {
	pu := getPu("")
	setSessionAlloc("", NewLeakCheckAllocator())
	io, err := NewIOSession(&testConn{}, pu, "")
	require.NoError(t, err)
	ses := NewSession(ctx, "", NewMysqlClientProtocol("", 0, io, 1024, pu.SV), nil)
	ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "auth_u", DefaultRole: "auth_r", UserID: 42, DefaultRoleID: 43})
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	require.NoError(t, ses.InitSystemVariables(ctx, bh))
	enabled, err := privilegeCacheIsEnabled(ctx, ses)
	require.NoError(t, err)
	require.True(t, enabled)
	t.Cleanup(ses.Close)
	return ses
}
func AuthorizationAtSnapshotForTest(ctx context.Context, ses *Session, snapshot timestamp.Timestamp) (bool, bool, error) {
	execution := &ExecCtx{reqCtx: ctx, ses: ses, authorizationSnapshot: snapshot}
	compiler := ses.GetTxnCompileCtx()
	previous := compiler.execCtx
	compiler.execCtx = execution
	defer func() { compiler.execCtx = previous }()
	entry := privilegeEntriesMap[PrivilegeTypeSelect]
	entry.databaseName, entry.tableName = "app", "t"
	ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, &privilege{entries: []privilegeEntry{entry}})
	return ok, !ses.cache.certificate.reusableFrom.IsEmpty(), err
}
