// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

// Plan-derived privileges require every source and target. ALL or ownership
// may cover the requirement on their own object, not a different object.
func TestPlanPrivilegeCoverageIsPerObject(t *testing.T) {
	type grant struct {
		db, table string
		typ       PrivilegeType
		level     privilegeLevelType
	}
	source := privilegeTips{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: "src", tableName: "s"}
	target := privilegeTips{typ: PrivilegeTypeInsert, objType: objectTypeTable, databaseName: "dst", tableName: "t"}
	deleteTarget := privilegeTips{typ: PrivilegeTypeDelete, objType: objectTypeTable, databaseName: "dst", tableName: "t"}
	tableGrant := func(db, table string, typ PrivilegeType) grant {
		return grant{db: db, table: table, typ: typ, level: privilegeLevelDatabaseTable}
	}
	for _, tc := range []struct {
		name   string
		tips   privilegeTipsArray
		grants []grant
		want   bool
	}{
		{"source select and target all", privilegeTipsArray{source, target}, []grant{tableGrant("src", "s", PrivilegeTypeSelect), tableGrant("dst", "t", PrivilegeTypeTableAll)}, true},
		{"target all cannot cover source", privilegeTipsArray{source, target}, []grant{tableGrant("dst", "t", PrivilegeTypeTableAll)}, false},
		{"target ownership cannot cover source", privilegeTipsArray{target, source}, []grant{tableGrant("dst", "t", PrivilegeTypeTableOwnership)}, false},
		{"source all and target insert", privilegeTipsArray{source, target}, []grant{tableGrant("src", "s", PrivilegeTypeTableAll), tableGrant("dst", "t", PrivilegeTypeInsert)}, true},
		{"source all cannot cover target", privilegeTipsArray{source, target}, []grant{tableGrant("src", "s", PrivilegeTypeTableAll)}, false},
		{"unrelated all cannot cover either", privilegeTipsArray{source, target}, []grant{tableGrant("other", "t", PrivilegeTypeTableAll)}, false},
		{"same target missing delete", privilegeTipsArray{target, deleteTarget}, []grant{tableGrant("dst", "t", PrivilegeTypeInsert)}, false},
		{"same target both actions", privilegeTipsArray{target, deleteTarget}, []grant{tableGrant("dst", "t", PrivilegeTypeInsert), tableGrant("dst", "t", PrivilegeTypeDelete)}, true},
		{"same target all covers both", privilegeTipsArray{target, deleteTarget}, []grant{tableGrant("dst", "t", PrivilegeTypeTableAll)}, true},
		{"single object ownership", privilegeTipsArray{source}, []grant{tableGrant("src", "s", PrivilegeTypeTableOwnership)}, true},
		{"global all", privilegeTipsArray{source, target}, []grant{{typ: PrivilegeTypeTableAll, level: privilegeLevelStarStar}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
			cache := ses.GetPrivilegeCache()
			cache.grantRoleID = 5
			bh := &backgroundExecTest{}
			bh.init()
			// The fake catalog returns an empty result for every candidate
			// except explicitly granted (object, scope, privilege) tuples.
			for _, tip := range tc.tips {
				for _, typ := range []PrivilegeType{PrivilegeTypeSelect, PrivilegeTypeInsert, PrivilegeTypeDelete, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
					entry := privilegeEntriesMap[typ]
					entry.objType, entry.databaseName, entry.tableName = tip.objType, tip.databaseName, tip.tableName
					levels, err := getPrivilegeLevelsOfObjectType(t.Context(), entry.objType)
					require.NoError(t, err)
					for _, level := range levels {
						query, err := getSqlForPrivilege2(t.Context(), ses, 5, entry, level)
						require.NoError(t, err)
						bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, nil)
					}
				}
			}
			for _, g := range tc.grants {
				cache.add(objectTypeTable, g.level, g.db, g.table, g.typ)
				entry := privilegeEntriesMap[g.typ]
				entry.objType, entry.databaseName, entry.tableName = objectTypeTable, g.db, g.table
				query, err := getSqlForPrivilege2(t.Context(), ses, 5, entry, g.level)
				require.NoError(t, err)
				bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, [][]interface{}{{int64(g.typ)}})
			}
			priv := &privilege{objType: objectTypeTable}
			convertPrivilegeTipsToPrivilege(priv, tc.tips)
			roles := &btree.Set[int64]{}
			roles.Insert(5)
			cached, err := checkPrivilegeInCache(t.Context(), ses, priv, true)
			require.NoError(t, err)
			if cached != tc.want {
				t.Errorf("cache evaluator: got %t, want %t", cached, tc.want)
			}
			cold, _, err := determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, false, newCompoundPrivilegeChecks(priv))
			require.NoError(t, err)
			require.Equal(t, tc.want, cold, "catalog evaluator")
		})
	}
}

func TestEmptyPlanPrivilegeTipsCannotAuthorize(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	priv := &privilege{objType: objectTypeTable}
	convertPrivilegeTipsToPrivilege(priv, nil)
	require.Empty(t, priv.entries)
	cached, err := checkPrivilegeInCache(t.Context(), ses, priv, true)
	require.NoError(t, err)
	require.False(t, cached)
	bh := &backgroundExecTest{}
	bh.init()
	roles := &btree.Set[int64]{}
	roles.Insert(5)
	cold, _, err := determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, false, newCompoundPrivilegeChecks(priv))
	require.NoError(t, err)
	require.False(t, cold)
	require.Empty(t, bh.executedSQLs)
}

func TestPlanPrivilegeCoverageRespectsLightGuards(t *testing.T) {
	source := privilegeTips{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: "src", tableName: "s"}
	target := privilegeTips{typ: PrivilegeTypeInsert, objType: objectTypeTable, databaseName: "dst", tableName: "t"}
	clusterSource := privilegeTips{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: moCatalog, tableName: "cluster_t", isClusterTable: true, clusterTableOperation: clusterTableSelect}
	clusterTarget := privilegeTips{typ: PrivilegeTypeInsert, objType: objectTypeTable, databaseName: moCatalog, tableName: "cluster_t", isClusterTable: true, clusterTableOperation: clusterTableModify}
	for _, tc := range []struct {
		name, policy string
		tips         privilegeTipsArray
		want         bool
	}{
		{"protected source remains readable", "src", privilegeTipsArray{source, target}, true},
		{"protected target remains denied", "dst", privilegeTipsArray{source, target}, false},
		{"cluster source remains readable", "", privilegeTipsArray{clusterSource, target}, true},
		{"cluster target remains denied", "", privilegeTipsArray{source, clusterTarget}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
			ses.SetFromRealUser(true)
			ses.GetTxnCompileCtx().SetExecCtx(&ExecCtx{reqCtx: t.Context(), ses: ses, protectedPolicyLoaded: true, protectedPolicyRaw: tc.policy})
			cache := ses.GetPrivilegeCache()
			cache.grantRoleID = 5
			bh := &backgroundExecTest{}
			bh.init()
			for _, tip := range tc.tips {
				for _, typ := range []PrivilegeType{tip.typ, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
					entry := privilegeEntriesMap[typ]
					entry.objType, entry.databaseName, entry.tableName = tip.objType, tip.databaseName, tip.tableName
					levels, err := getPrivilegeLevelsOfObjectType(t.Context(), entry.objType)
					require.NoError(t, err)
					for _, level := range levels {
						query, err := getSqlForPrivilege2(t.Context(), ses, 5, entry, level)
						require.NoError(t, err)
						var rows [][]interface{}
						if typ == PrivilegeTypeTableAll && level == privilegeLevelDatabaseTable {
							rows = [][]interface{}{{int64(typ)}}
						}
						bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, rows)
					}
				}
				cache.add(tip.objType, privilegeLevelDatabaseTable, tip.databaseName, tip.tableName, PrivilegeTypeTableAll)
			}
			priv := &privilege{objType: objectTypeTable, writeDatabaseAndTableDirectly: true}
			convertPrivilegeTipsToPrivilege(priv, tc.tips)
			roles := &btree.Set[int64]{}
			roles.Insert(5)
			cached, err := checkPrivilegeInCache(t.Context(), ses, priv, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, cached, "cache evaluator")
			cold, _, err := determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, false, newCompoundPrivilegeChecks(priv))
			require.NoError(t, err)
			require.Equal(t, tc.want, cold, "catalog evaluator")
		})
	}
}

func TestPlanPrivilegeCoverageFailsClosedOnCatalogError(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	bh := &backgroundExecTest{}
	bh.init()
	entry := privilegeEntriesMap[PrivilegeTypeSelect]
	entry.objType, entry.databaseName, entry.tableName = objectTypeTable, "src", "s"
	levels, err := getPrivilegeLevelsOfObjectType(t.Context(), objectTypeTable)
	require.NoError(t, err)
	query, err := getSqlForPrivilege2(t.Context(), ses, 5, entry, levels[0])
	require.NoError(t, err)
	failure := errors.New("catalog read failed")
	bh.sql2err[query] = failure
	priv := &privilege{objType: objectTypeTable}
	convertPrivilegeTipsToPrivilege(priv, privilegeTipsArray{{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: "src", tableName: "s"}})
	roles := &btree.Set[int64]{}
	roles.Insert(5)
	ok, _, err := determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, false, newCompoundPrivilegeChecks(priv))
	require.False(t, ok)
	require.ErrorIs(t, err, failure)
	require.Equal(t, []string{query}, bh.executedSQLs, "an error must not fall through to covering grants")
}

func TestPlanPrivilegeCoverageCanceledPolicyFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	ses.SetFromRealUser(true)
	ses.GetTxnCompileCtx().SetExecCtx(&ExecCtx{reqCtx: t.Context(), ses: ses, protectedPolicyLoaded: true})
	priv := &privilege{objType: objectTypeTable, writeDatabaseAndTableDirectly: true}
	convertPrivilegeTipsToPrivilege(priv, privilegeTipsArray{{typ: PrivilegeTypeInsert, objType: objectTypeTable, databaseName: "dst", tableName: "t"}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ok, err := checkPrivilegeInCache(ctx, ses, priv, true)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	bh := &backgroundExecTest{}
	bh.init()
	roles := &btree.Set[int64]{}
	roles.Insert(5)
	ok, _, err = determineRoleSetHasPrivilegeSet(ctx, bh, ses, roles, priv, false, newCompoundPrivilegeChecks(priv))
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, bh.executedSQLs)
}
