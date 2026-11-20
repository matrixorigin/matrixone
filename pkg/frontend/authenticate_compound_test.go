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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestCompoundPrivilegesAcrossRoles(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			secondary bool
			grants    map[int64][]string
			inherited map[int64][]int64
			reverse   bool
			prewarm   bool
			want      bool
		}{
			{name: "split_active_roles", secondary: true, grants: map[int64][]string{5: {"a"}, 7: {"b"}}, want: true},
			{name: "reverse_roles", secondary: true, grants: map[int64][]string{5: {"b"}, 7: {"a"}}, want: true},
			{name: "reverse_items", secondary: true, grants: map[int64][]string{5: {"a"}, 7: {"b"}}, reverse: true, want: true},
			{name: "prewarmed", secondary: true, grants: map[int64][]string{5: {"a"}, 7: {"b"}}, prewarm: true, want: true},
			{name: "single_role", grants: map[int64][]string{5: {"a", "b"}}, want: true},
			{name: "missing_permission", secondary: true, grants: map[int64][]string{5: {"a"}}},
			{name: "secondary_disabled", grants: map[int64][]string{5: {"a"}, 7: {"b"}}},
			{name: "parent_and_child", grants: map[int64][]string{5: {"a"}, 7: {"b"}}, inherited: map[int64][]int64{5: {7}}, want: true},
			{name: "different_depths", grants: map[int64][]string{7: {"a"}, 9: {"b"}}, inherited: map[int64][]int64{5: {7}, 7: {9}}, want: true},
			{name: "cycle_missing_permission", grants: map[int64][]string{7: {"a"}}, inherited: map[int64][]int64{5: {7}, 7: {5}}},
		} {
			t.Run(fmt.Sprintf("cache_%t/%s", cacheEnabled, tc.name), func(t *testing.T) {
				ctx := defines.AttachAccountId(t.Context(), 3)
				priv := compoundTablePrivileges("a", "b")
				if tc.reverse {
					priv.entries[0].compound.items[0], priv.entries[0].compound.items[1] = priv.entries[0].compound.items[1], priv.entries[0].compound.items[0]
				}
				ses, _ := newCompoundPrivilegeSession(t, tc.grants, tc.inherited, cacheEnabled, tc.secondary)
				if tc.prewarm {
					for _, table := range []string{"a", "b"} {
						ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, compoundTablePrivileges(table))
						require.NoError(t, err)
						require.True(t, ok)
					}
				}
				ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
				require.NoError(t, err)
				require.Equal(t, tc.want, ok)
			})
		}
	}
}

func TestCompoundPrivilegeChecksAreStatementLocal(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		t.Run(fmt.Sprint(cacheEnabled), func(t *testing.T) {
			ctx := defines.AttachAccountId(t.Context(), 3)
			priv := compoundTablePrivileges("a", "b")
			ses, _ := newCompoundPrivilegeSession(t, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, cacheEnabled, true)
			for _, secondary := range []bool{true, false, true} {
				ses.GetTenantInfo().SetUseSecondaryRole(secondary)
				ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
				require.NoError(t, err)
				require.Equal(t, secondary, ok, "reuse the same immutable requirement")
			}
		})
	}
}

func TestCompoundPrivilegesPreserveMatchedRole(t *testing.T) {
	priv := compoundTablePrivileges("a", "b")
	priv.needMatchedRole = true
	ses, _ := newCompoundPrivilegeSession(t, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, true, true)
	ok, _, _, err := determineUserHasPrivilegeSet(defines.AttachAccountId(t.Context(), 3), ses, priv)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCompoundPrivilegeCatalogError(t *testing.T) {
	for _, failure := range []error{errors.New("catalog unavailable"), context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx := defines.AttachAccountId(t.Context(), 3)
			ses, bh := newCompoundPrivilegeSession(t, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, true, true)
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.databaseName, entry.tableName = "compound_db", "b"
			levels, err := getPrivilegeLevelsOfObjectType(ctx, objectTypeTable)
			require.NoError(t, err)
			query, err := getSqlForPrivilege(ctx, 7, entry, levels[0])
			require.NoError(t, err)
			bh.sql2err[query] = failure
			ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, compoundTablePrivileges("a", "b"))
			require.ErrorIs(t, err, failure)
			require.False(t, ok)
			require.Contains(t, bh.executedSQLs, "rollback;")
			require.True(t, ses.GetPrivilegeCache().certificate.reusableFrom.IsEmpty())
		})
	}
}

func TestCompoundPrivilegesKeepRoleCacheIsolation(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 3)
	ses, bh := newCompoundPrivilegeSession(t, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, true, true)
	roles := &btree.Set[int64]{}
	roles.Insert(5)
	roles.Insert(7)
	priv := compoundTablePrivileges("a", "b")
	ok, _, err := determineRoleSetHasPrivilegeSet(ctx, bh, ses, roles, priv, true, newCompoundPrivilegeChecks(priv))
	require.NoError(t, err)
	require.True(t, ok)
	cache := ses.GetPrivilegeCache()
	require.Equal(t, int64(7), cache.grantRoleID)
	entry := privilegeEntriesMap[PrivilegeTypeSelect]
	entry.databaseName, entry.tableName = "compound_db", "a"
	levels, err := getPrivilegeLevelsOfObjectType(ctx, objectTypeTable)
	require.NoError(t, err)
	ok, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, cache, 7, entry, levels, true)
	require.NoError(t, err)
	require.False(t, ok, "compound success must not publish role 5's grant as role 7's")
}

func TestCompoundPrivilegesPerObjectCovers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   PrivilegeType
		level privilegeLevelType
	}{
		{"all", PrivilegeTypeTableAll, privilegeLevelDatabaseTable},
		{"ownership", PrivilegeTypeTableOwnership, privilegeLevelDatabaseTable},
		{"database_select", PrivilegeTypeSelect, privilegeLevelDatabaseStar},
		{"global_select", PrivilegeTypeSelect, privilegeLevelStarStar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := defines.AttachAccountId(t.Context(), 3)
			ses, bh := newCompoundPrivilegeSession(t, nil, nil, true, true)
			setCompoundPrivilegeGrant(t, bh, 5, objectTypeTable, "a", tc.typ, tc.level)
			setCompoundPrivilegeGrant(t, bh, 7, objectTypeTable, "b", PrivilegeTypeInsert, privilegeLevelDatabaseTable)
			priv := compoundTablePrivileges("a", "b")
			priv.entries[0].compound.items[1].privilegeTyp = PrivilegeTypeInsert
			ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
			require.NoError(t, err)
			require.True(t, ok)
			// An unrelated SELECT/ALL/ownership grant cannot supply this write.
			priv.entries[0].compound.items[1].tableName = "missing"
			ok, _, _, err = determineUserHasPrivilegeSet(ctx, ses, priv)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func TestCompoundPrivilegesKeepViewSecurityContext(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		for _, tc := range []struct {
			name, security string
			baseRole       int64
			want           bool
		}{
			{"definer", viewSecurityDefiner, 9, true},
			{"caller_cannot_supply_definer_grant", viewSecurityDefiner, 5, false},
			{"invoker", viewSecurityInvoker, 5, true},
			{"definer_cannot_supply_invoker_grant", viewSecurityInvoker, 9, false},
		} {
			t.Run(fmt.Sprintf("cache_%t/%s", cacheEnabled, tc.name), func(t *testing.T) {
				ctx := defines.AttachAccountId(t.Context(), 3)
				priv := compoundTablePrivileges("a", "b")
				priv.entries[0].compound.items[0].originViews = []string{"compound_db.v"}
				ses, bh := newCompoundPrivilegeSession(t, map[int64][]string{tc.baseRole: {"a"}, 7: {"b"}}, nil, cacheEnabled, true)
				setCompoundPrivilegeGrant(t, bh, 5, objectTypeView, "v", PrivilegeTypeSelect, privilegeLevelDatabaseTable)
				query, err := getSqlForCheckViewMeta(ctx, "compound_db", "v")
				require.NoError(t, err)
				bh.sql2result[query] = newMrsForRestoreStringRows([]string{"viewdef", "owner"}, [][]interface{}{
					{fmt.Sprintf(`{"security_type":%q}`, tc.security), int64(9)},
				})
				ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
				require.NoError(t, err)
				require.Equal(t, tc.want, ok)
			})
		}
	}
}

func TestCompoundPrivilegeAlternativesStaySeparate(t *testing.T) {
	first, second := compoundTablePrivileges("a", "missing"), compoundTablePrivileges("missing", "b")
	first.entries = append(first.entries, second.entries...)
	ses, _ := newCompoundPrivilegeSession(t, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, true, true)
	ok, _, _, err := determineUserHasPrivilegeSet(defines.AttachAccountId(t.Context(), 3), ses, first)
	require.NoError(t, err)
	require.False(t, ok, "partial success in different alternatives must not satisfy either compound")
}

func TestCompoundPrivilegeChecksScope(t *testing.T) {
	for _, tc := range []struct {
		name string
		priv *privilege
	}{
		{"no_entries", &privilege{}},
		{"general", &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeGeneral}}}},
		{"nil_compound", &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound}}}},
		{"single_item", compoundTablePrivileges("a")},
		{"account_scope", &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: []privilegeItem{{objType: objectTypeAccount}, {objType: objectTypeTable}}}}}}},
		{"special_grant", &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: []privilegeItem{{objType: objectTypeTable, privilegeTyp: PrivilegeTypeCanGrantRoleToOthersInCreateUser}, {objType: objectTypeTable}}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Nil(t, newCompoundPrivilegeChecks(tc.priv))
		})
	}
	priv := compoundTablePrivileges("a", "b")
	priv.entries[0].compound.items[0].objType = objectTypeView
	require.Equal(t, []bool{false, false}, newCompoundPrivilegeChecks(priv)[priv.entries[0].compound])
}

func compoundTablePrivileges(tables ...string) *privilege {
	tips := make(privilegeTipsArray, 0, len(tables))
	for _, table := range tables {
		tips = append(tips, privilegeTips{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: "compound_db", tableName: table})
	}
	priv := &privilege{objType: objectTypeTable}
	convertPrivilegeTipsToPrivilege(priv, tips)
	return priv
}

// The session owns a deterministic catalog and a valid certificate. This permits
// actual warm privilege-cache reads without starting an engine or stubbing admission.
func newCompoundPrivilegeSession(t *testing.T, grants map[int64][]string, inherited map[int64][]int64, cacheEnabled, secondary bool) (*Session, *backgroundExecTest) {
	t.Helper()
	ses := newSes(gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetTenantInfo(&TenantInfo{Tenant: "account", User: "reader", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	stub := gostub.Stub(&privilegeCacheIsEnabled, func(context.Context, *Session) (bool, error) { return cacheEnabled, nil })
	t.Cleanup(stub.Reset)
	bh := &backgroundExecTest{}
	bh.init()
	stub = gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)
	for _, all := range []bool{false, true} {
		ses.GetTenantInfo().SetUseSecondaryRole(all)
		rows := [][]interface{}{{int64(5)}}
		if all {
			rows = append(rows, []interface{}{int64(7)})
		}
		bh.sql2result[getSqlForActiveRolesForAuthorization(ses.GetTenantInfo(), false)] = newMrsForRoleIdOfUserId(rows)
	}
	ses.GetTenantInfo().SetUseSecondaryRole(secondary)
	reader := &authorizationVersionReader{}
	snapshot := timestamp.Timestamp{PhysicalTime: 100}
	getPu("").StorageEngine = reader
	getPu("").TxnClient = &authorizationSnapshotReader{snapshot: snapshot}
	ses.GetPrivilegeCache().certificate = authorizationCertificate{principal: currentAuthorizationPrincipal(ses), reusableFrom: snapshot}
	for _, role := range []int64{5, 7, 9} {
		var children [][]interface{}
		for _, child := range inherited[role] {
			children = append(children, []interface{}{child, false})
		}
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(role)] = newMrsForInheritedRoleIdOfRoleId(children)
		for _, table := range []string{"a", "b", "missing", "v"} {
			objType := objectTypeTable
			if table == "v" {
				objType = objectTypeView
			}
			for _, typ := range []PrivilegeType{PrivilegeTypeSelect, PrivilegeTypeInsert, PrivilegeTypeDelete, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
				entry := privilegeEntriesMap[typ]
				entry.objType, entry.databaseName, entry.tableName = objType, "compound_db", table
				levels, err := getPrivilegeLevelsOfObjectType(t.Context(), objType)
				require.NoError(t, err)
				for _, level := range levels {
					query, err := getSqlForPrivilege(t.Context(), role, entry, level)
					require.NoError(t, err)
					bh.sql2result[query] = newMrsForWithGrantOptionPrivilege(nil)
				}
			}
		}
	}
	for role, tables := range grants {
		for _, table := range tables {
			setCompoundPrivilegeGrant(t, bh, role, objectTypeTable, table, PrivilegeTypeSelect, privilegeLevelDatabaseTable)
		}
	}
	return ses, bh
}

func setCompoundPrivilegeGrant(t *testing.T, bh *backgroundExecTest, role int64, objType objectType, table string, typ PrivilegeType, level privilegeLevelType) {
	t.Helper()
	entry := privilegeEntriesMap[typ]
	entry.objType, entry.databaseName, entry.tableName = objType, "compound_db", table
	query, err := getSqlForPrivilege(t.Context(), role, entry, level)
	require.NoError(t, err)
	bh.sql2result[query] = newMrsForWithGrantOptionPrivilege([][]interface{}{{role, false}})
}
