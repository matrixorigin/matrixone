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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
			{name: "missing_permission", secondary: true, grants: map[int64][]string{5: {"a"}}, want: false},
			{name: "secondary_disabled", grants: map[int64][]string{5: {"a"}, 7: {"b"}}, want: false},
			{name: "parent_and_child", grants: map[int64][]string{5: {"a"}, 7: {"b"}}, inherited: map[int64][]int64{5: {7}}, want: true},
			{name: "different_inheritance_depths", grants: map[int64][]string{7: {"a"}, 9: {"b"}}, inherited: map[int64][]int64{5: {7}, 7: {9}}, want: true},
			{name: "inheritance_cycle_missing_permission", grants: map[int64][]string{7: {"a"}}, inherited: map[int64][]int64{5: {7}, 7: {5}}, want: false},
		} {
			t.Run(fmt.Sprintf("cache_%t/%s", cacheEnabled, tc.name), func(t *testing.T) {
				ctx := context.Background()
				priv := compoundTablePrivileges("a", "b")
				if tc.reverse {
					priv.entries[0].compound.items[0], priv.entries[0].compound.items[1] = priv.entries[0].compound.items[1], priv.entries[0].compound.items[0]
				}
				ses, bh := newCompoundPrivilegeSession(t, priv, tc.grants, tc.inherited, cacheEnabled)
				ses.GetTenantInfo().SetUseSecondaryRole(tc.secondary)
				if tc.prewarm {
					for _, table := range []string{"a", "b"} {
						ok, _, err := determineUserHasPrivilegeSet(ctx, ses, compoundTablePrivileges(table))
						require.NoError(t, err)
						require.True(t, ok)
					}
				}
				ok, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
				require.NoError(t, err)
				require.Equal(t, tc.want, ok, "queries: %v", bh.executedSQLs)
			})
		}
	}
}

func TestCompoundPrivilegeCatalogError(t *testing.T) {
	for _, want := range []error{moerr.NewInternalErrorNoCtx("catalog unavailable"), context.Canceled} {
		t.Run(want.Error(), func(t *testing.T) {
			priv := compoundTablePrivileges("a", "b")
			ses, bh := newCompoundPrivilegeSession(t, priv, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, false)
			ses.GetTenantInfo().SetUseSecondaryRole(true)
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.databaseName, entry.tableName = "compound_db", "b"
			levels, err := getPrivilegeLevelsOfObjectType(context.Background(), objectTypeTable)
			require.NoError(t, err)
			sql, err := getSqlForPrivilege(context.Background(), 7, entry, levels[0])
			require.NoError(t, err)
			bh.sql2err[sql] = want
			ok, _, err := determineUserHasPrivilegeSet(context.Background(), ses, priv)
			require.ErrorIs(t, err, want)
			require.False(t, ok)
		})
	}
}

func TestCompoundPrivilegeChecksAreStatementLocal(t *testing.T) {
	priv := compoundTablePrivileges("a", "b")
	ses, _ := newCompoundPrivilegeSession(t, priv, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, false)
	for _, secondary := range []bool{true, false, true} {
		ses.GetTenantInfo().SetUseSecondaryRole(secondary)
		ok, _, err := determineUserHasPrivilegeSet(context.Background(), ses, priv)
		require.NoError(t, err)
		require.Equal(t, secondary, ok)
	}
}

func TestCompoundPrivilegesPreserveMatchedRole(t *testing.T) {
	priv := compoundTablePrivileges("a", "b")
	priv.needMatchedRole = true
	ses, _ := newCompoundPrivilegeSession(t, priv, map[int64][]string{5: {"a"}, 7: {"b"}}, nil, true)
	ses.GetTenantInfo().SetUseSecondaryRole(true)
	ok, _, err := determineUserHasPrivilegeSet(context.Background(), ses, priv)
	require.NoError(t, err)
	require.False(t, ok, "a statement requiring one matched role must not combine roles")
}

func TestCompoundPrivilegesKeepViewSecurityContext(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			security string
			baseRole int64
			want     bool
		}{
			{name: "definer", security: viewSecurityDefiner, baseRole: 9, want: true},
			{name: "caller_cannot_supply_definer_grant", security: viewSecurityDefiner, baseRole: 5},
			{name: "invoker", security: viewSecurityInvoker, baseRole: 5, want: true},
			{name: "definer_cannot_supply_invoker_grant", security: viewSecurityInvoker, baseRole: 9},
		} {
			t.Run(fmt.Sprintf("cache_%t/%s", cacheEnabled, tc.name), func(t *testing.T) {
				ctx := defines.AttachAccountId(context.Background(), 0)
				priv := compoundTablePrivileges("a", "b")
				priv.entries[0].compound.items[0].originViews = []string{"compound_db.v"}
				ses, bh := newCompoundPrivilegeSession(t, priv, map[int64][]string{tc.baseRole: {"a"}, 7: {"b"}}, nil, cacheEnabled)
				ses.GetTenantInfo().SetUseSecondaryRole(true)
				for _, role := range []int64{5, 7} {
					for _, typ := range []PrivilegeType{PrivilegeTypeSelect, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
						entry := privilegeEntriesMap[typ]
						entry.objType, entry.databaseName, entry.tableName = objectTypeView, "compound_db", "v"
						levels, err := getPrivilegeLevelsOfObjectType(ctx, objectTypeView)
						require.NoError(t, err)
						for _, level := range levels {
							sql, err := getSqlForPrivilege(ctx, role, entry, level)
							require.NoError(t, err)
							var rows [][]interface{}
							if role == 5 && typ == PrivilegeTypeSelect && (level == privilegeLevelDatabaseTable || level == privilegeLevelTable) {
								rows = [][]interface{}{{role, false}}
							}
							bh.sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
						}
					}
				}
				meta := &MysqlResultSet{}
				for i, typ := range []defines.MysqlType{defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_LONGLONG} {
					col := &MysqlColumn{}
					col.SetName(fmt.Sprintf("meta_%d", i))
					col.SetColumnType(typ)
					meta.AddColumn(col)
				}
				meta.AddRow([]interface{}{fmt.Sprintf(`{"security_type":%q}`, tc.security), int64(9)})
				sql, err := getSqlForCheckViewMeta(ctx, "compound_db", "v")
				require.NoError(t, err)
				bh.sql2result[sql] = meta
				ok, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
				require.NoError(t, err)
				require.Equal(t, tc.want, ok)
			})
		}
	}
}

func TestCompoundPrivilegeChecksScope(t *testing.T) {
	for _, tc := range []struct {
		name string
		priv *privilege
	}{
		{name: "no_entries", priv: &privilege{}},
		{name: "general", priv: &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeGeneral}}}},
		{name: "nil_compound", priv: &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound}}}},
		{name: "single_item", priv: compoundTablePrivileges("a")},
		{name: "account_scope", priv: &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: []privilegeItem{{objType: objectTypeAccount}, {objType: objectTypeTable}}}}}}},
		{name: "special_grant", priv: &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: []privilegeItem{{objType: objectTypeTable, privilegeTyp: PrivilegeTypeCanGrantRoleToOthersInCreateUser}, {objType: objectTypeTable}}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Nil(t, newCompoundPrivilegeChecks(tc.priv))
		})
	}
	priv := compoundTablePrivileges("a", "b")
	priv.entries[0].compound.items[0].objType = objectTypeView
	checks := newCompoundPrivilegeChecks(priv)
	require.Equal(t, []bool{false, false}, checks[priv.entries[0].compound])
}

func compoundTablePrivileges(tables ...string) *privilege {
	items := make([]privilegeItem, 0, len(tables))
	for _, table := range tables {
		items = append(items, privilegeItem{
			privilegeTyp: PrivilegeTypeSelect,
			objType:      objectTypeTable,
			dbName:       "compound_db",
			tableName:    table,
		})
	}
	return &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: items}}}}
}

// Each test owns a session and a deterministic catalog, not a running service.
func newCompoundPrivilegeSession(t *testing.T, priv *privilege, grants map[int64][]string, inherited map[int64][]int64, cacheEnabled bool) (*Session, *backgroundExecTest) {
	t.Helper()
	ctrl := gomock.NewController(t)
	ses := newSes(priv, ctrl)
	t.Cleanup(ses.Close)
	ses.GetTenantInfo().SetDefaultRoleID(5)
	stub := gostub.Stub(&privilegeCacheIsEnabled, func(context.Context, *Session) (bool, error) { return cacheEnabled, nil })
	t.Cleanup(stub.Reset)
	bh := &backgroundExecTest{}
	bh.init()
	stub = gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)
	bh.sql2result[getSqlForRoleIdOfUserId(0)] = newMrsForRoleIdOfUserId([][]interface{}{{5, false}, {7, false}})
	for _, role := range []int64{5, 7, 9} {
		var children [][]interface{}
		for _, child := range inherited[role] {
			children = append(children, []interface{}{child, false})
		}
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(role)] = newMrsForInheritedRoleIdOfRoleId(children)
		for _, table := range []string{"a", "b", "missing"} {
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.databaseName, entry.tableName = "compound_db", table
			levels, err := getPrivilegeLevelsOfObjectType(context.Background(), objectTypeTable)
			require.NoError(t, err)
			for _, level := range levels {
				sql, err := getSqlForPrivilege(context.Background(), role, entry, level)
				require.NoError(t, err)
				var rows [][]interface{}
				// Only table-specific SELECT is granted; wildcard checks must stay empty.
				if level == privilegeLevelDatabaseTable || level == privilegeLevelTable {
					for _, granted := range grants[role] {
						if granted == table {
							rows = [][]interface{}{{role, false}}
						}
					}
				}
				bh.sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
			}
		}
	}
	return ses, bh
}
