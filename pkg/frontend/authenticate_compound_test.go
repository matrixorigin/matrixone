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

func TestCompoundPrivilegesAcrossInheritedRoles(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			grants    map[int64][]string
			inherited map[int64][]int64
			reverse   bool
			want      bool
		}{
			{name: "parent_and_child", grants: map[int64][]string{5: {"a"}, 7: {"b"}}, inherited: map[int64][]int64{5: {7}}, want: true},
			{name: "reverse_grants", grants: map[int64][]string{5: {"b"}, 7: {"a"}}, inherited: map[int64][]int64{5: {7}}, want: true},
			{name: "reverse_items", grants: map[int64][]string{5: {"a"}, 7: {"b"}}, inherited: map[int64][]int64{5: {7}}, reverse: true, want: true},
			{name: "different_depths", grants: map[int64][]string{7: {"a"}, 9: {"b"}}, inherited: map[int64][]int64{5: {7}, 7: {9}}, want: true},
			{name: "siblings", grants: map[int64][]string{7: {"a"}, 9: {"b"}}, inherited: map[int64][]int64{5: {7, 9}}, want: true},
			{name: "reverse_siblings", grants: map[int64][]string{7: {"b"}, 9: {"a"}}, inherited: map[int64][]int64{5: {7, 9}}, want: true},
			{name: "single_role", grants: map[int64][]string{5: {"a", "b"}}, want: true},
			{name: "missing_permission", grants: map[int64][]string{5: {"a"}}, inherited: map[int64][]int64{5: {7}}},
			{name: "not_inherited", grants: map[int64][]string{5: {"a"}, 7: {"b"}}},
			{name: "cycle_missing_permission", grants: map[int64][]string{7: {"a"}}, inherited: map[int64][]int64{5: {7}, 7: {5}}},
		} {
			t.Run(fmt.Sprintf("cache_%t/%s", cacheEnabled, tc.name), func(t *testing.T) {
				ctx := defines.AttachAccountId(t.Context(), 0)
				priv := compoundTablePrivileges("a", "b")
				if tc.reverse {
					priv.entries[0].compound.items[0], priv.entries[0].compound.items[1] = priv.entries[0].compound.items[1], priv.entries[0].compound.items[0]
				}
				ses, _ := newCompoundPrivilegeSession(t, priv, tc.grants, tc.inherited, cacheEnabled)
				ok, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
				require.NoError(t, err)
				require.Equal(t, tc.want, ok)
			})
		}
	}
}

func TestCompoundPrivilegeCatalogError(t *testing.T) {
	for _, want := range []error{moerr.NewInternalErrorNoCtx("catalog unavailable"), context.Canceled} {
		t.Run(want.Error(), func(t *testing.T) {
			ctx := defines.AttachAccountId(t.Context(), 0)
			priv := compoundTablePrivileges("a", "b")
			ses, bh := newCompoundPrivilegeSession(t, priv, map[int64][]string{5: {"a"}, 7: {"b"}}, map[int64][]int64{5: {7}}, false)
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.databaseName, entry.tableName = "compound_db", "b"
			levels, err := getPrivilegeLevelsOfObjectType(ctx, objectTypeTable)
			require.NoError(t, err)
			sql, err := getSqlForPrivilege(ctx, 7, entry, levels[0])
			require.NoError(t, err)
			bh.sql2err[sql] = want
			ok, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
			require.ErrorIs(t, err, want)
			require.False(t, ok)
			require.Contains(t, bh.executedSQLs, "rollback;")
		})
	}
}

func TestCompoundPrivilegeChecksAreStatementLocal(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 0)
	priv := compoundTablePrivileges("a", "b")
	ses, _ := newCompoundPrivilegeSession(t, priv, map[int64][]string{5: {"a"}, 7: {"b"}}, map[int64][]int64{5: {7}}, false)
	for _, role := range []uint32{5, 7, 5} {
		ses.GetTenantInfo().SetDefaultRoleID(role)
		ok, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
		require.NoError(t, err)
		require.Equal(t, role == 5, ok)
	}
}

func TestCompoundPrivilegesPreserveMatchedRole(t *testing.T) {
	priv := compoundTablePrivileges("a", "b")
	priv.needMatchedRole = true
	ses, _ := newCompoundPrivilegeSession(t, priv, map[int64][]string{5: {"a"}, 7: {"b"}}, map[int64][]int64{5: {7}}, true)
	ok, _, err := determineUserHasPrivilegeSet(defines.AttachAccountId(t.Context(), 0), ses, priv)
	require.NoError(t, err)
	require.False(t, ok, "a statement requiring one matched role must not combine roles")
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
	tips := make(privilegeTipsArray, 0, len(tables))
	for _, table := range tables {
		tips = append(tips, privilegeTips{
			typ: PrivilegeTypeSelect, objType: objectTypeTable,
			databaseName: "compound_db", tableName: table,
		})
	}
	priv := &privilege{objType: objectTypeTable}
	convertPrivilegeTipsToPrivilege(priv, tips)
	return priv
}

func newCompoundPrivilegeSession(t *testing.T, priv *privilege, grants map[int64][]string, inherited map[int64][]int64, cacheEnabled bool) (*Session, *backgroundExecTest) {
	t.Helper()
	ses := newSes(priv, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.GetTenantInfo().SetDefaultRoleID(5)
	stub := gostub.Stub(&privilegeCacheIsEnabled, func(context.Context, *Session) (bool, error) { return cacheEnabled, nil })
	t.Cleanup(stub.Reset)
	bh := &backgroundExecTest{}
	bh.init()
	stub = gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)
	for _, role := range []int64{5, 7, 9} {
		bh.sql2result[getSqlForCheckUserGrantForAuthorization(role, 0)] = newMrsForCheckUserGrant([][]interface{}{{role, int64(0), false}})
		var children [][]interface{}
		for _, child := range inherited[role] {
			children = append(children, []interface{}{child, false})
		}
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(role)] = newMrsForInheritedRoleIdOfRoleId(children)
		for _, table := range []string{"a", "b"} {
			for _, typ := range []PrivilegeType{PrivilegeTypeSelect, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
				entry := privilegeEntriesMap[typ]
				entry.databaseName, entry.tableName = "compound_db", table
				levels, err := getPrivilegeLevelsOfObjectType(t.Context(), objectTypeTable)
				require.NoError(t, err)
				for _, level := range levels {
					sql, err := getSqlForPrivilege(t.Context(), role, entry, level)
					require.NoError(t, err)
					var rows [][]interface{}
					if typ == PrivilegeTypeSelect && (level == privilegeLevelDatabaseTable || level == privilegeLevelTable) {
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
	}
	return ses, bh
}
