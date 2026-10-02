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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

type authorizationVersionReader struct {
	engine.Engine
	versions        [authorizationDependencyCount]engine.TableContentVersion
	reads, changeAt int
	afterRead       func()
}

func (r *authorizationVersionReader) ReadTableContentVersions(ctx context.Context, _ timestamp.Timestamp, _ []engine.TableContentDependency, out []engine.TableContentVersion) bool {
	r.reads++
	if r.afterRead != nil {
		defer r.afterRead()
	}
	if r.changeAt == r.reads {
		r.versions[0].Revision++
	}
	copy(out, r.versions[:])
	return ctx.Err() == nil
}

type authorizationSnapshotReader struct {
	client.TxnClient
	snapshot timestamp.Timestamp
}

func (r *authorizationSnapshotReader) ReadSnapshot(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
	return r.snapshot, ctx.Err()
}

// SQL admission is tested independently in the multi-CN restore/revoke cases.
// This isolates warm admission cost; the engine-backed test covers same-S SQL.
func TestAuthorizationCacheWarmAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", DefaultRole: "r", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	reader := &authorizationVersionReader{}
	snapshot := timestamp.Timestamp{PhysicalTime: 100}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, reader, nil, nil)
	pu.SV.SetDefaultValues()
	pu.TxnClient = &authorizationSnapshotReader{snapshot: snapshot}
	setPu("", pu)
	ctx := defines.AttachAccountId(t.Context(), 3)
	enabled := gostub.Stub(&privilegeCacheIsEnabled, func(context.Context, *Session) (bool, error) { return true, nil })
	defer enabled.Reset()
	cache := ses.GetPrivilegeCache()
	cache.grantRoleID = 5
	cache.certificate = authorizationCertificate{principal: currentAuthorizationPrincipal(ses), versions: reader.versions, reusableFrom: snapshot}
	entry := privilegeEntriesMap[PrivilegeTypeSelect]
	entry.objType, entry.databaseName, entry.tableName = objectTypeTable, "app", "t"
	priv := &privilege{entries: []privilegeEntry{entry}}
	cache.add(objectTypeTable, privilegeLevelDatabaseTable, "app", "t", PrivilegeTypeSelect)
	allocs := testing.AllocsPerRun(100, func() {
		ok, _, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
		if !ok || err != nil {
			t.Fatalf("warm admission: %v %v", ok, err)
		}
	})
	require.Zero(t, allocs, "warm authorization must create neither SQL transactions nor heap scratch buffers")
	require.Equal(t, 101, reader.reads)
	statement := &tree.Select{}
	query := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "app", ObjName: "t"}}}}}}
	prepared := &PrepareStmt{PrepareStmt: statement, PreparePlan: &plan.Plan{Plan: &plan.Plan_Dcl{Dcl: &plan.DataControl{Control: &plan.DataControl_Prepare{Prepare: &plan.Prepare{Plan: query}}}}}}
	_, err := authenticatePreparedStatement(ctx, ses, statement, query, prepared)
	require.NoError(t, err)
	saved := prepared.authorization
	require.NotNil(t, saved)
	reader.reads = 0
	allocs = testing.AllocsPerRun(100, func() {
		if _, err := authenticatePreparedStatement(ctx, ses, statement, query, prepared); err != nil {
			t.Fatal(err)
		}
	})
	require.Zero(t, allocs, "complete warm prepared authorization must not rebuild requirements")
	require.Equal(t, 101, reader.reads, "every EXECUTE captures the current versions")
	require.Same(t, saved, prepared.authorization)
	otherPlan := &plan.Plan{Plan: query.Plan}
	require.Nil(t, prepared.authorizationRequirements(ses, statement, otherPlan), "a retry or unrelated plan cannot use saved requirements")
	// Cancellation after a successful capture must not return a cached allow.
	canceled, cancel := context.WithCancel(ctx)
	reader.afterRead = cancel
	ok, _, _, err := determineUserHasPrivilegeSet(canceled, ses, priv)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
	reader.afterRead = nil
	// A change already visible when C0 is read must defeat the cached allow.
	reader.reads, reader.changeAt = 0, 1
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getSqlForActiveRolesForAuthorization(ses.GetTenantInfo(), false)] = newMrsForRoleIdOfUserId([][]interface{}{{int64(5)}})
	levels, err := getPrivilegeLevelsOfObjectType(ctx, objectTypeTable)
	require.NoError(t, err)
	for _, level := range levels {
		query, err := getSqlForPrivilege2(ctx, ses, 5, entry, level)
		require.NoError(t, err)
		bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, nil)
	}
	bh.sql2result[getSqlForInheritedRoleIdOfRoleId(5)] = newMrsForRoleIdOfUserId(nil)
	background := gostub.StubFunc(&NewBackgroundExec, bh)
	defer background.Reset()
	ok, _, _, err = determineUserHasPrivilegeSet(ctx, ses, priv)
	require.False(t, ok)
	require.NoError(t, err)
	require.Contains(t, bh.executedSQLs, getSqlForActiveRolesForAuthorization(ses.GetTenantInfo(), false))
	require.True(t, cache.certificate.reusableFrom.IsEmpty())
}

func TestPrivilegeCacheBounds(t *testing.T) {
	for _, typ := range []objectType{objectTypeTable, objectTypeView, objectTypeDatabase} {
		t.Run(typ.String(), func(t *testing.T) {
			var cache privilegeCache
			level := privilegeLevelDatabaseTable
			if typ == objectTypeDatabase {
				level = privilegeLevelDatabase
			}
			for i := 0; i < 1100; i++ {
				cache.add(typ, level, fmt.Sprintf("db%d", i), "t", PrivilegeTypeSelect)
				require.LessOrEqual(t, cache.scopeEntries, 1024)
				require.LessOrEqual(t, cache.nameBytes, 256<<10)
			}
			require.False(t, cache.has(typ, level, "db0", "t", PrivilegeTypeSelect))
			require.True(t, cache.has(typ, level, "db1099", "t", PrivilegeTypeSelect))
			// One oversized name cannot consume the session's entire budget.
			cache.add(typ, level, strings.Repeat("x", (256<<10)+1), "t", PrivilegeTypeSelect)
			require.LessOrEqual(t, cache.nameBytes, 256<<10)
			cache.invalidate()
			require.Zero(t, cache.scopeEntries)
			require.Zero(t, cache.nameBytes)
		})
	}
}

func TestNativeStatementAuthorizationBoundary(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		guard bool
	}{
		{"create stage s url='file:///tmp/x'", true},
		{"drop stage s", true},
		{"set global sql_mode=''", true},
		{"set @x=1, global sql_mode=''", true},
		{"alter database app set mysql_compatibility_mode='8.0'", true},
		{"set role r", false},
		{"set secondary role all", false},
		{"set @x=1", false},
		{"rollback", false},
		{"commit", false},
		{"prepare s from 'select 1'", false},
		{"execute s", false},
		{"deallocate prepare s", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.Equal(t, tc.guard, nativeStatementNeedsAuthorization(stmt))
		})
	}
}

func TestAuthorizationAccountVersionIsSharedByRoleAdmission(t *testing.T) {
	for _, tc := range []struct {
		name            string
		current         uint64
		absent, retired bool
	}{
		{name: "current", current: 17},
		{name: "version changed", current: 18},
		{name: "account replaced", absent: true},
		{name: "routine retired", retired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := &Session{}
			tenant := &TenantInfo{Tenant: "a", User: "u", DefaultRole: "r", TenantID: 3, UserID: 4, DefaultRoleID: 5}
			ses.SetTenantInfo(tenant)
			routine := &Routine{}
			ses.setRoutine(routine)
			owner := &AccountRoutineManager{accountId2Routine: map[int64]map[*Routine]uint64{3: {routine: 17}}}
			if tc.retired {
				delete(owner.accountId2Routine[3], routine)
			}
			ses.setRoutineManager(&RoutineManager{accountRoutine: owner})
			bh := &backgroundExecTest{}
			bh.init()
			query := "select version from mo_catalog.mo_account where account_id = 3 and account_name = 'a'"
			var rows [][]interface{}
			if !tc.absent {
				rows = [][]interface{}{{tc.current}}
			}
			bh.sql2result[query] = newMrsForRestoreStringRows([]string{"version"}, rows)
			identity := getSqlForActiveRolesForAuthorization(tenant, false)
			bh.sql2result[identity] = newMrsForRoleIdOfUserId([][]interface{}{{int64(5)}})
			roles := &btree.Set[int64]{}
			err := loadActiveRolesForAuthorization(defines.AttachAccountId(t.Context(), 3), bh, ses, roles)
			if tc.name == "current" {
				require.NoError(t, err)
				require.True(t, roles.Contains(5))
				require.Equal(t, []string{query, identity}, bh.executedSQLs)
			} else {
				require.ErrorContains(t, err, "do not have privilege: authenticated account")
				require.Empty(t, roles.Keys())
				require.NotContains(t, bh.executedSQLs, identity, "WGO and ordinary admission must reject the account before using unchanged user/role tuples")
			}
		})
	}
}

// An allowed source read must not authorize a forbidden target write. Exercise
// both orders because the result of a compound requirement is order-independent.
func TestPrivilegeCacheCompoundDoesNotCarryAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	ses.SetFromRealUser(true)
	cache := ses.GetPrivilegeCache()
	cache.add(objectTypeTable, privilegeLevelDatabaseTable, "app", "source", PrivilegeTypeSelect)
	cache.add(objectTypeTable, privilegeLevelDatabaseTable, moCatalog, "target", PrivilegeTypeInsert)
	bh := &backgroundExecTest{}
	bh.init()
	entry := privilegeEntriesMap[PrivilegeTypeSelect]
	entry.databaseName, entry.tableName = "app", "source"
	levels, err := getPrivilegeLevelsOfObjectType(t.Context(), objectTypeTable)
	require.NoError(t, err)
	for _, level := range levels {
		query, err := getSqlForPrivilege2(t.Context(), ses, 5, entry, level)
		require.NoError(t, err)
		bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, [][]interface{}{{int64(PrivilegeTypeSelect)}})
	}
	roles := &btree.Set[int64]{}
	roles.Insert(5)
	read := privilegeItem{objType: objectTypeTable, privilegeTyp: PrivilegeTypeSelect, dbName: "app", tableName: "source"}
	write := privilegeItem{objType: objectTypeTable, privilegeTyp: PrivilegeTypeInsert, dbName: moCatalog, tableName: "target"}
	for _, items := range [][]privilegeItem{{read, write}, {write, read}, {read, read}} {
		priv := &privilege{writeDatabaseAndTableDirectly: true, entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: items}}}}
		ok, err := checkPrivilegeInCache(t.Context(), ses, priv, true)
		require.NoError(t, err)
		require.Equal(t, items[1].privilegeTyp == PrivilegeTypeSelect && items[0].privilegeTyp == PrivilegeTypeSelect, ok, "compound items: %+v", items)
		cold, _, err := determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, false)
		require.NoError(t, err)
		require.Equal(t, cold, ok, "cache and SQL evaluators must agree")
	}
}

func TestPrivilegeCacheRequiresSQLRole(t *testing.T) {
	for _, typ := range []objectType{objectTypeTable, objectTypeView} {
		t.Run(typ.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			bh := &backgroundExecTest{}
			bh.init()
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.objType, entry.databaseName, entry.tableName = typ, "app", "t"
			levels, err := getPrivilegeLevelsOfObjectType(t.Context(), typ)
			require.NoError(t, err)
			for _, role := range []int64{7, 8} {
				for _, level := range levels {
					query, err := getSqlForPrivilege2(t.Context(), ses, role, entry, level)
					require.NoError(t, err)
					var rows [][]interface{}
					if role == 7 {
						rows = [][]interface{}{{int64(PrivilegeTypeSelect)}}
					}
					bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, rows)
				}
			}
			cache := ses.GetPrivilegeCache()
			ok, err := verifyPrivilegeEntryInMultiPrivilegeLevels(t.Context(), bh, ses, cache, 7, entry, levels, true)
			require.NoError(t, err)
			require.True(t, ok)
			before := len(bh.executedSQLs)
			ok, err = verifyPrivilegeEntryInMultiPrivilegeLevels(t.Context(), bh, ses, cache, 8, entry, levels, true)
			require.NoError(t, err)
			require.False(t, ok)
			require.Greater(t, len(bh.executedSQLs), before, "another role must consult SQL")
			before = len(bh.executedSQLs)
			ok, err = verifyPrivilegeEntryInMultiPrivilegeLevels(t.Context(), bh, ses, cache, 7, entry, levels, true)
			require.NoError(t, err)
			require.True(t, ok)
			require.Len(t, bh.executedSQLs, before, "failed probes preserve the proven role's facts")
			// Eviction occurs inside a positive fill and must retain that fill's role.
			for i := 0; i < 1100; i++ {
				cache.add(typ, privilegeLevelDatabaseTable, fmt.Sprintf("db%d", i), "t", PrivilegeTypeSelect)
			}
			require.Equal(t, int64(7), cache.grantRoleID)
			require.True(t, cache.has(typ, privilegeLevelDatabaseTable, "db1099", "t", PrivilegeTypeSelect))
		})
	}
}
