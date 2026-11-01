// Copyright 2021 Matrix Origin
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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

// Pair a real parser/planner witness with typed lock identities and independent
// catalog grants. A SELECT-only user must never acquire an exclusive SELECT lock.
func TestSelectLockPrivilege(t *testing.T) {
	for _, tc := range []struct {
		name, sql  string
		targets    []string
		grants     map[string]PrivilegeType
		denySelect bool
		allowed    bool
	}{
		{name: "ordinary read", sql: "select n_nationkey from nation", allowed: true},
		{name: "shared read", sql: "select n_nationkey from nation for share", allowed: true},
		{name: "legacy shared read", sql: "select n_nationkey from nation lock in share mode", allowed: true},
		{name: "read only exclusive", sql: "select n_nationkey from nation for update", targets: []string{"nation"}},
		{name: "nowait exclusive", sql: "select n_nationkey from nation for update nowait", targets: []string{"nation"}},
		{name: "constant false", sql: "select n_nationkey from nation where 1=0 for update", targets: []string{"nation"}},
		{name: "update alternative", sql: "select n_nationkey from nation for update", targets: []string{"nation"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeUpdate}, allowed: true},
		{name: "delete alternative", sql: "select n_nationkey from nation for update", targets: []string{"nation"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeDelete}, allowed: true},
		{name: "write alone lacks select", sql: "select n_nationkey from nation for update", targets: []string{"nation"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeUpdate}, denySelect: true},
		{name: "nested exclusive", sql: "select n_nationkey from nation where n_regionkey in (select r_regionkey from region for update)", targets: []string{"region"}},
		{name: "derived exclusive", sql: "select * from (select n_nationkey from nation for update) d", targets: []string{"nation"}},
		{name: "cte exclusive", sql: "with d as (select n_nationkey from nation for update) select * from d", targets: []string{"nation"}},
		{name: "unlocked source needs no write", sql: "select n_nationkey from nation where n_regionkey in (select r_regionkey from region) for update", targets: []string{"nation"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeUpdate}, allowed: true},
		{name: "outer grant cannot authorize inner lock", sql: "select n_nationkey from nation where n_regionkey in (select r_regionkey from region for update)", targets: []string{"region"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeUpdate}},
		{name: "mixed alternatives", sql: "select n_nationkey from nation join region on n_regionkey=r_regionkey for update", targets: []string{"nation", "region"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeUpdate, "region": PrivilegeTypeDelete}, allowed: true},
		{name: "sibling lacks write", sql: "select n_nationkey from nation join region on n_regionkey=r_regionkey for update", targets: []string{"nation", "region"}, grants: map[string]PrivilegeType{"nation": PrivilegeTypeTableOwnership}},
		{name: "shared outer exclusive inner", sql: "select n_nationkey from nation where n_regionkey in (select r_regionkey from region for update) for share", targets: []string{"region"}},
		{name: "dml own locks", sql: "update nation set n_name='new' where n_nationkey=1", grants: map[string]PrivilegeType{"nation": PrivilegeTypeUpdate}, allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			p, err := plan2.BuildPlan(plan2.NewMockCompilerContext(true, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			var targets []string
			for _, ref := range p.GetQuery().SelectLockReferences {
				targets = append(targets, ref.ObjectRef.ObjName)
			}
			require.ElementsMatch(t, tc.targets, targets)
			// The immutable binding evidence survives both cache copying and the wire.
			copy := plan2.DeepCopyPlan(p)
			wire, err := p.Marshal()
			require.NoError(t, err)
			roundtrip := &plan.Plan{}
			require.NoError(t, roundtrip.Unmarshal(wire))
			require.Equal(t, p.GetQuery().SelectLockReferences, copy.GetQuery().SelectLockReferences)
			require.Equal(t, p.GetQuery().SelectLockReferences, roundtrip.GetQuery().SelectLockReferences)
			for _, node := range p.GetQuery().Nodes {
				for _, target := range node.LockTargets {
					if len(tc.targets) == 0 && p.GetQuery().StmtType == plan.Query_SELECT {
						require.Equal(t, lockpb.LockMode_Shared, target.Mode)
					}
				}
			}
			ctrl := gomock.NewController(t)
			ses := newSes(determinePrivilegeSetOfStatement(stmt), ctrl)
			t.Cleanup(ses.Close)
			ses.SetTenantInfo(&TenantInfo{Tenant: "test_account", User: "reader", DefaultRole: "reader_role", TenantID: 1, UserID: 1, DefaultRoleID: 2})
			results := makeSql2ExecResult2(1, nil, nil, nil, nil, nil, nil, nil, nil)
			for _, table := range []string{"nation", "region"} {
				grants := map[PrivilegeType]bool{PrivilegeTypeSelect: !tc.denySelect}
				if typ, ok := tc.grants[table]; ok {
					grants[typ] = true
				}
				addTablePrivilegeResultsForRole(t, results, 2, "tpch", table, grants)
			}
			results[getSqlForInheritedRoleIdOfRoleId(2)] = newMrsForInheritedRoleIdOfRoleId(nil)
			stub := gostub.StubFunc(&NewBackgroundExec, newBh(ctrl, results))
			defer stub.Reset()
			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, p)
			require.NoError(t, err)
			require.Equal(t, tc.allowed, ok)
		})
	}
}

func TestSelectLockPrivilegeCacheAlternatives(t *testing.T) {
	for _, grant := range []PrivilegeType{PrivilegeTypeSelect, PrivilegeTypeUpdate, PrivilegeTypeDelete, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
		t.Run(grant.String(), func(t *testing.T) {
			ses := newTestSession(t, gomock.NewController(t))
			t.Cleanup(ses.Close)
			ses.SetTenantInfo(getDefaultAccount())
			ses.GetPrivilegeCache().add(objectTypeTable, privilegeLevelDatabaseTable, "app", "t", grant)
			priv := &privilege{objType: objectTypeTable, entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound, compound: &compoundEntry{items: []privilegeItem{{privilegeTyp: PrivilegeTypeSelect, selectLock: true, objType: objectTypeTable, dbName: "app", tableName: "t", clusterTableOperation: clusterTableSelect}}}}}}
			ok, err := checkPrivilegeInCache(t.Context(), ses, priv, true)
			require.NoError(t, err)
			require.Equal(t, grant != PrivilegeTypeSelect, ok)
		})
	}
}

func TestSelectLockReferenceIdentity(t *testing.T) {
	step := &plan.ViewStep{DatabaseName: "app", ViewName: "v", Snapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: 1}}}
	ref := &plan.SelectLockReference{ObjectRef: &plan.ObjectRef{SchemaName: "app", ObjName: "t"}, ViewPath: []*plan.ViewStep{step}}
	q := &plan.Query{SelectLockReferences: []*plan.SelectLockReference{ref, ref, {ObjectRef: ref.ObjectRef, ViewPath: []*plan.ViewStep{{DatabaseName: "app", ViewName: "other", Snapshot: step.Snapshot}}}}}
	tips, err := selectLockPrivilegeTips(q)
	require.NoError(t, err)
	require.Len(t, tips, 2)
	copy := plan2.DeepCopyQuery(q)
	copy.SelectLockReferences[0].ObjectRef.ObjName = "changed"
	copy.SelectLockReferences[0].ViewPath[0].ViewName = "changed"
	require.Equal(t, "t", ref.ObjectRef.ObjName)
	require.Equal(t, "v", step.ViewName)
	for _, invalid := range []*plan.SelectLockReference{nil, {}, {ObjectRef: ref.ObjectRef, ViewPath: []*plan.ViewStep{nil}}} {
		_, err = selectLockPrivilegeTips(&plan.Query{SelectLockReferences: []*plan.SelectLockReference{invalid}})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	}
}

func TestSelectLockViewAlternativesAndErrors(t *testing.T) {
	// Exercise the real view-grant checker; a SELECT grant cannot satisfy the lock
	// item, and downstream error identities must survive the new alternatives.
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)
	ses.SetTenantInfo(getDefaultAccount())
	ctx := defines.AttachAccountId(context.Background(), 1)
	for _, grant := range []PrivilegeType{PrivilegeTypeSelect, PrivilegeTypeUpdate, PrivilegeTypeDelete} {
		results := map[string]ExecResult{}
		for _, typ := range []PrivilegeType{PrivilegeTypeUpdate, PrivilegeTypeDelete, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
			entry := privilegeEntriesMap[typ]
			entry.objType = objectTypeView
			entry.databaseName = "app"
			entry.tableName = "v"
			levels, err := getPrivilegeLevelsOfObjectType(ctx, objectTypeView)
			require.NoError(t, err)
			for _, level := range levels {
				sql, err := getSqlForPrivilege(ctx, 3, entry, level)
				require.NoError(t, err)
				var rows [][]interface{}
				if grant == typ {
					rows = [][]interface{}{{typ, true}}
				}
				results[sql] = newMrsForWithGrantOptionPrivilege(rows)
			}
		}
		allowed, err := verifyViewPrivilegeForRole(ctx, newBh(ctrl, results), ses, nil, 3, PrivilegeTypeSelect, "app", "v", false, true)
		require.NoError(t, err)
		require.Equal(t, grant != PrivilegeTypeSelect, allowed)
	}
	failure := moerr.NewTxnNeedRetryNoCtx()
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(failure)
	_, err := verifyViewPrivilegeForRole(ctx, bh, ses, nil, 3, PrivilegeTypeSelect, "app", "v", false, true)
	require.ErrorIs(t, err, failure)
}

func BenchmarkSelectLockPrivilegeTips(b *testing.B) {
	for _, tc := range []struct {
		name  string
		count int
	}{{"ordinary", 0}, {"one target", 1}, {"repeated target", 32}} {
		b.Run(tc.name, func(b *testing.B) {
			q := &plan.Query{}
			for i := 0; i < tc.count; i++ {
				q.SelectLockReferences = append(q.SelectLockReferences, &plan.SelectLockReference{ObjectRef: &plan.ObjectRef{SchemaName: "app", ObjName: "t"}})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := selectLockPrivilegeTips(q); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSelectLockViewPrincipalRoles(t *testing.T) {
	for _, tc := range []struct {
		name, security string
		grantAccount   uint32
		grantDB        string
		allowed        bool
	}{
		{"invoker inherits base grant", viewSecurityInvoker, 7, "sub", true},
		{"definer inherits base grant", viewSecurityDefiner, 9, "publisher", true},
		{"caller grant cannot cross definer", viewSecurityDefiner, 7, "sub", false},
	} {
		for _, poisonCache := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/uncached", true: "/foreign cache hit"}[poisonCache], func(t *testing.T) {
				ctrl := gomock.NewController(t)
				ses := newTestSession(t, ctrl)
				t.Cleanup(ses.Close)
				ses.SetTenantInfo(&TenantInfo{Tenant: "subscriber", TenantID: 7, DefaultRoleID: 50})
				ctx := defines.AttachAccountId(t.Context(), 7)
				type key struct {
					account uint32
					sql     string
				}
				results := map[key]ExecResult{}
				for _, account := range []uint32{7, 9} {
					results[key{account, getSqlForInheritedRoleIdOfRoleId(50)}] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{{int64(51)}})
					results[key{account, getSqlForInheritedRoleIdOfRoleId(51)}] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{{int64(50)}}) // cycle is bounded
				}
				metadata, err := getSqlForCheckViewMeta(defines.AttachAccountId(ctx, 9), "publisher", "outer")
				require.NoError(t, err)
				mrs := &MysqlResultSet{}
				mrs.AddColumn(&MysqlColumn{})
				mrs.AddColumn(&MysqlColumn{})
				mrs.AddRow([]interface{}{`{"security_type":"` + tc.security + `"}`, int64(50)})
				results[key{9, metadata}] = mrs
				grant := func(account uint32, role int64, obj objectType, typ PrivilegeType, db, name string) {
					sql, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(ctx, obj, role, typ, db, name)
					require.NoError(t, err)
					results[key{account, sql}] = newMrsForWithGrantOptionPrivilege([][]interface{}{{typ, true}})
				}
				grant(7, 50, objectTypeView, PrivilegeTypeUpdate, "sub", "outer")
				grant(tc.grantAccount, 51, objectTypeTable, PrivilegeTypeDelete, tc.grantDB, "t")
				if poisonCache {
					ses.GetPrivilegeCache().add(objectTypeTable, privilegeLevelDatabaseTable, "publisher", "t", PrivilegeTypeUpdate)
				}
				bh := mock_frontend.NewMockBackgroundExec(ctrl)
				var result ExecResult
				bh.EXPECT().ClearExecResultSet().AnyTimes()
				bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(c context.Context, sql string) error {
					account, err := defines.GetAccountId(c)
					require.NoError(t, err)
					result = results[key{account, sql}]
					if result == nil {
						result = &MysqlResultSet{}
					}
					return nil
				}).AnyTimes()
				bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} { return []interface{}{result} }).AnyTimes()
				roles := &btree.Set[int64]{}
				roles.Insert(50)
				require.NoError(t, expandRoleSetWithInheritedRoles(ctx, bh, roles))
				item := privilegeItem{privilegeTyp: PrivilegeTypeSelect, selectLock: true, objType: objectTypeTable, dbName: "sub", tableName: "t", objectRef: &plan.ObjectRef{SchemaName: "publisher", ObjName: "t", SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 9}}, viewPath: []*plan.ViewStep{{DatabaseName: "publisher", ViewName: "outer", SubscriptionName: "sub", Snapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: 9}}}}}
				allowed, err := determineSelectLockViewPrivilege(ctx, bh, ses, roles, item)
				require.NoError(t, err)
				require.Equal(t, tc.allowed, allowed)
				require.False(t, ses.GetPrivilegeCache().has(objectTypeTable, privilegeLevelDatabaseTable, "publisher", "t", PrivilegeTypeDelete))
			})
		}
	}
}

func TestSelectLockInheritedRoleErrors(t *testing.T) {
	failure := moerr.NewTxnNeedRetryNoCtx()
	ctrl := gomock.NewController(t)
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(failure)
	roles := &btree.Set[int64]{}
	roles.Insert(50)
	require.ErrorIs(t, expandRoleSetWithInheritedRoles(t.Context(), bh, roles), failure)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, expandRoleSetWithInheritedRoles(ctx, bh, roles), context.Canceled)
}
