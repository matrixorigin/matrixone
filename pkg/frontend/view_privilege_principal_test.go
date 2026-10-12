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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestViewPrivilegePrincipal(t *testing.T) {
	for _, tc := range []struct {
		name, security string
		grantAccount   uint32
		grantDB        string
		allowed        bool
	}{
		{"publisher definer", viewSecurityDefiner, 9, "publisher", true},
		{"colliding subscriber role cannot authorize publisher", viewSecurityDefiner, 7, "publisher", false},
		{"publisher role cannot use alias grants", viewSecurityDefiner, 9, "sub", false},
		{"subscriber invoker", viewSecurityInvoker, 7, "sub", true},
		{"invoker cannot borrow publisher grants", viewSecurityInvoker, 9, "sub", false},
	} {
		for _, cached := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/uncached", true: "/cached"}[cached], func(t *testing.T) {
				ctrl := gomock.NewController(t)
				ses := newTestSession(t, ctrl)
				t.Cleanup(ses.Close)
				ses.SetTenantInfo(&TenantInfo{Tenant: "subscriber", TenantID: 7, DefaultRoleID: 50})
				ctx := defines.AttachAccountId(t.Context(), 7)
				type queryKey struct {
					account uint32
					sql     string
				}
				results := make(map[queryKey]ExecResult)
				positive := &MysqlResultSet{}
				positive.AddRow([]interface{}{int64(50)})
				for _, name := range []string{"outer", "inner"} {
					meta, err := getSqlForCheckViewMeta(defines.AttachAccountId(ctx, 9), "publisher", name)
					require.NoError(t, err)
					mrs := &MysqlResultSet{}
					mrs.AddColumn(&MysqlColumn{})
					mrs.AddColumn(&MysqlColumn{})
					mrs.AddRow([]interface{}{`{"security_type":"` + tc.security + `"}`, int64(50)})
					results[queryKey{9, meta}] = mrs
				}
				grant := func(account uint32, obj objectType, db, name string) {
					q, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(ctx, obj, 50, PrivilegeTypeSelect, db, name)
					require.NoError(t, err)
					results[queryKey{account, q}] = positive
				}
				grant(7, objectTypeView, "sub", "outer")
				grant(tc.grantAccount, objectTypeView, tc.grantDB, "inner")
				grant(tc.grantAccount, objectTypeTable, tc.grantDB, "t")
				// Same numeric role and physical name in the caller cache must not
				// satisfy a publisher check, or acquire a foreign grant on success.
				ses.GetPrivilegeCache().add(objectTypeView, privilegeLevelDatabaseTable, "publisher", "inner", PrivilegeTypeSelect)
				bh := mock_frontend.NewMockBackgroundExec(ctrl)
				var result ExecResult
				bh.EXPECT().ClearExecResultSet().AnyTimes()
				bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(c context.Context, sql string) error {
					account, err := defines.GetAccountId(c)
					require.NoError(t, err)
					result = results[queryKey{account, sql}]
					if result == nil {
						result = &MysqlResultSet{}
					}
					return nil
				}).AnyTimes()
				bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} { return []interface{}{result} }).AnyTimes()
				step := func(name string) *plan.ViewStep {
					return &plan.ViewStep{DatabaseName: "publisher", ViewName: name, SubscriptionName: "sub", Snapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: 9}}}
				}
				ref := &plan.ObjectRef{SchemaName: "publisher", ObjName: "t", SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 9}}
				priv := &privilege{}
				convertPrivilegeTipsToPrivilege(priv, privilegeTipsArray{{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: "sub", tableName: "t", viewPath: []*plan.ViewStep{step("outer"), step("inner")}, objectRef: ref}})
				roles := &btree.Set[int64]{}
				roles.Insert(50)
				allowed, role, err := determineRoleSetHasPrivilegeSet(ctx, bh, ses, roles, priv, cached)
				require.NoError(t, err)
				require.Equal(t, tc.allowed, allowed)
				if allowed {
					require.Equal(t, int64(50), role)
				}
				require.Equal(t, "sub", ref.SubscriptionName)
				require.False(t, ses.GetPrivilegeCache().has(objectTypeTable, privilegeLevelDatabaseTable, "publisher", "t", PrivilegeTypeSelect))
			})
		}
	}
}

func TestViewAdminRoleIsAccountQualified(t *testing.T) {
	for _, tc := range []struct {
		account uint32
		role    int64
		allowed bool
	}{
		{0, 0, true}, {7, 2, true}, {7, 0, false}, {0, 2, false},
	} {
		ctx := defines.AttachAccountId(t.Context(), tc.account)
		bh := &backgroundExecTest{}
		bh.init()
		bh.resultSets = []interface{}{&MysqlResultSet{}}
		allowed, err := verifyViewPrivilegeForRole(ctx, bh, nil, nil, tc.role, PrivilegeTypeSelect, "db", "v", false)
		require.NoError(t, err)
		require.Equal(t, tc.allowed, allowed)
	}
}

func TestCloneTargetContext(t *testing.T) {
	for _, tc := range []struct{ caller, target, user, role, owner uint32 }{
		{7, 7, 12, 50, 51}, {0, 7, uint32(GetAdminUserId()), accountAdminRoleID, accountAdminRoleID}, {7, 0, 0, moAdminRoleID, moAdminRoleID},
	} {
		ctx := defines.AttachDDLOwnerRoleId(defines.AttachAccount(t.Context(), tc.caller, 12, 50), 51)
		got := cloneTargetContext(ctx, tc.caller, tc.target)
		account, err := defines.GetAccountId(got)
		require.NoError(t, err)
		require.Equal(t, tc.target, account)
		require.Equal(t, tc.user, defines.GetUserId(got))
		require.Equal(t, tc.role, defines.GetRoleId(got))
		owner, ok := defines.GetDDLOwnerRoleId(got)
		require.True(t, ok)
		require.Equal(t, tc.owner, owner)
	}
}
