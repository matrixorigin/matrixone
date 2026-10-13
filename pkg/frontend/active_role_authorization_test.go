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

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestLoadActiveRolesForAuthorization(t *testing.T) {
	for _, secondary := range []bool{false, true} {
		for _, tc := range []struct {
			name                                string
			role                                uint32
			trusted, absent, revoked, malformed bool
			failure                             error
		}{
			{name: "member", role: 3}, {name: "revoked", role: 3, revoked: true},
			{name: "replaced user", role: 3, absent: true}, {name: "malformed", role: 3, malformed: true},
			{name: "public", role: 1, trusted: true}, {name: "deleted public user", role: 1, trusted: true, absent: true},
			{name: "accountadmin", role: 2, trusted: true}, {name: "replaced admin", role: 2, trusted: true, absent: true},
			{name: "read", role: 3, failure: errors.New("read failed")},
			{name: "canceled", role: 3, failure: context.Canceled}, {name: "deadline", role: 3, failure: context.DeadlineExceeded},
		} {
			t.Run(tc.name+map[bool]string{false: "/primary", true: "/secondary"}[secondary], func(t *testing.T) {
				account := &TenantInfo{Tenant: "account", TenantID: 10, User: "a'b", UserID: 7, DefaultRoleID: tc.role}
				account.SetUseSecondaryRole(secondary)
				bh := &backgroundExecTest{}
				bh.init()
				query := getSqlForActiveRolesForAuthorization(account, tc.trusted)
				require.Contains(t, query, "u.user_id = 7")
				require.Contains(t, query, "u.user_name = 'a''b'")
				var rows [][]interface{}
				if !tc.absent {
					var value interface{} = int64(tc.role)
					if tc.revoked {
						value = nil
					}
					if tc.malformed {
						value = "not a role ID"
					}
					rows = [][]interface{}{{value}}
					if secondary {
						rows = append(rows, []interface{}{int64(4)})
					}
				}
				bh.sql2result[query] = newMrsForRestoreStringRows([]string{"role"}, rows)
				if tc.failure != nil {
					bh.sql2err[query] = tc.failure
				}
				roles := &btree.Set[int64]{}
				// Preexisting output must not establish fresh primary membership.
				if tc.revoked {
					roles.Insert(int64(tc.role))
				}
				ses := &Session{}
				ses.SetTenantInfo(account)
				err := loadActiveRolesForAuthorization(t.Context(), bh, ses, roles)
				switch {
				case tc.failure != nil:
					require.ErrorIs(t, err, tc.failure)
				case tc.absent:
					require.ErrorContains(t, err, "authenticated user no longer matches")
				case tc.revoked:
					require.ErrorContains(t, err, "active role is no longer granted")
				case tc.malformed:
					require.Error(t, err)
				default:
					require.NoError(t, err)
					require.True(t, roles.Contains(int64(tc.role)))
					require.Equal(t, secondary, roles.Contains(4))
				}
				require.Equal(t, []string{query}, bh.executedSQLs)
			})
		}
	}
}

// These policy tests previously needed no executor. Represent the current
// authenticated administrator explicitly instead of bypassing its catalog check.
func mockAuthorizationUser(t *testing.T, ses *Session) *backgroundExecTest {
	t.Helper()
	require.True(t, ses.GetTenantInfo().IsAdminRole())
	bh := &backgroundExecTest{}
	bh.init()
	query := getSqlForActiveRolesForAuthorization(ses.GetTenantInfo(), true)
	bh.sql2result[query] = newMrsForRoleIdOfUserId([][]interface{}{{int64(ses.GetTenantInfo().GetUserID())}})
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)
	return bh
}
