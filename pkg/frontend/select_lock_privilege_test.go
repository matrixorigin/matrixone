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
	"fmt"
	"strings"
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
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
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
			p, err := plan2.BuildPlan(plan2.NewMockCompilerContext(true, newPlanTestProcess(t)), stmt, false, nil)
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
			// The same grant may also be stored through the table spelling,
			// whose exact-relation rows the view checker probes as well.
			entry.objType = objectTypeTable
			for _, level := range exactRelationPrivilegeLevels {
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

// Count the catalog work itself, independent of wall-clock scheduling. The
// legacy arm enters the real per-reference authorizer; the batch arm exercises
// its shared role/view evaluator with the same catalog and logical targets.
func TestSelectLockAuthorizationBatchCatalogWork(t *testing.T) {
	for _, security := range []string{viewSecurityInvoker, viewSecurityDefiner} {
		for _, batched := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/batch=%t", security, batched), func(t *testing.T) {
				f := newSelectLockBatchCatalog(t, security, 8)
				if batched {
					roles := &btree.Set[int64]{}
					roles.Insert(50)
					require.NoError(t, expandRoleSetWithInheritedRoles(f.ctx, f.bh, roles))
					batch := &selectLockAuthorizationBatch{items: f.items}
					ok, err := batch.check(f.ctx, f.bh, f.ses, roles, func() (bool, error) { return true, nil })
					require.NoError(t, err)
					require.True(t, ok)
					require.Equal(t, 8, batch.checked)
					require.True(t, roles.Contains(50))
				} else {
					stub := gostub.StubFunc(&NewBackgroundExec, f.bh)
					defer stub.Reset()
					ok, _, err := authenticateSelectLockReferences(f.ctx, f.ses, f.query)
					require.NoError(t, err)
					require.True(t, ok)
					require.Equal(t, 8, f.counts[selectLockCatalogKey{7, "begin;"}])
					require.Equal(t, 8, f.closes)
				}
				multiplier := 8
				if batched {
					multiplier = 1
				}
				for role := int64(50); role < 59; role++ {
					require.Equal(t, multiplier, f.counts[selectLockCatalogKey{7, getSqlForInheritedRoleIdOfRoleId(role)}])
					if security == viewSecurityDefiner {
						require.Equal(t, multiplier, f.counts[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(role)}])
					}
				}
				meta, err := getSqlForCheckViewMeta(defines.AttachAccountId(f.ctx, 9), "publisher", "v")
				require.NoError(t, err)
				require.Equal(t, 8, f.counts[selectLockCatalogKey{9, meta}], "metadata is checked separately for every target")
				for i := 0; i < 8; i++ {
					account, db := uint32(7), "sub"
					if security == viewSecurityDefiner {
						account, db = 9, "publisher"
					}
					role, typ := int64(58), PrivilegeTypeDelete
					if i%2 != 0 {
						role, typ = 57, PrivilegeTypeUpdate
					}
					sql, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(f.ctx, objectTypeTable, role, typ, db, fmt.Sprintf("t%d", i))
					require.NoError(t, err)
					require.Equal(t, 1, f.counts[selectLockCatalogKey{account, sql}], "each distinct base target still needs its own grant")
				}
				t.Logf("caller inheritance reads=%d; per-target metadata reads=8; batch evaluator=%t", 9*multiplier, batched)
			})
		}
	}
}

type selectLockCatalogKey struct {
	account uint32
	sql     string
}

type selectLockBatchCatalog struct {
	ctx     context.Context
	ses     *Session
	bh      *mock_frontend.MockBackgroundExec
	query   *plan.Query
	items   []privilegeItem
	results map[selectLockCatalogKey]ExecResult
	counts  map[selectLockCatalogKey]int
	fail    map[selectLockCatalogKey]error
	before  func(selectLockCatalogKey)
	closes  int
}

func newSelectLockBatchCatalog(t *testing.T, security string, targets int) *selectLockBatchCatalog {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &selectLockBatchCatalog{
		ctx: defines.AttachAccountId(t.Context(), 7), ses: newSes(nil, ctrl),
		bh: mock_frontend.NewMockBackgroundExec(ctrl), query: &plan.Query{},
		results: map[selectLockCatalogKey]ExecResult{}, counts: map[selectLockCatalogKey]int{}, fail: map[selectLockCatalogKey]error{},
	}
	t.Cleanup(f.ses.Close)
	f.ses.SetTenantInfo(&TenantInfo{Tenant: "subscriber", TenantID: 7, UserID: 1, DefaultRoleID: 50})
	f.results[selectLockCatalogKey{7, getSqlForCheckUserGrantForAuthorization(50, 1)}] = newMrsForCheckUserGrant([][]interface{}{{int64(50), int64(1), false}})
	for _, account := range []uint32{7, 9} {
		for role := int64(50); role < 59; role++ {
			var rows [][]interface{}
			if role < 58 {
				rows = [][]interface{}{{role + 1}}
			}
			f.results[selectLockCatalogKey{account, getSqlForInheritedRoleIdOfRoleId(role)}] = newMrsForInheritedRoleIdOfRoleId(rows)
		}
	}
	meta, err := getSqlForCheckViewMeta(defines.AttachAccountId(f.ctx, 9), "publisher", "v")
	require.NoError(t, err)
	metadata := &MysqlResultSet{}
	metadata.AddColumn(&MysqlColumn{})
	metadata.AddColumn(&MysqlColumn{})
	metadata.AddRow([]interface{}{`{"security_type":"` + security + `"}`, int64(50)})
	f.results[selectLockCatalogKey{9, meta}] = metadata
	grant := func(account uint32, role int64, obj objectType, typ PrivilegeType, db, name string) {
		sql, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(f.ctx, obj, role, typ, db, name)
		require.NoError(t, err)
		f.results[selectLockCatalogKey{account, sql}] = newMrsForWithGrantOptionPrivilege([][]interface{}{{typ, true}})
	}
	grant(7, 50, objectTypeView, PrivilegeTypeUpdate, "sub", "v")
	for i := 0; i < targets; i++ {
		name := fmt.Sprintf("t%d", i)
		account, db := uint32(7), "sub"
		if security == viewSecurityDefiner {
			account, db = 9, "publisher"
		}
		role, typ := int64(58), PrivilegeTypeDelete
		if i%2 != 0 {
			role, typ = 57, PrivilegeTypeUpdate
		}
		grant(account, role, objectTypeTable, typ, db, name)
		ref := &plan.SelectLockReference{
			ObjectRef: &plan.ObjectRef{SchemaName: "publisher", ObjName: name, SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 9}},
			ViewPath:  []*plan.ViewStep{{DatabaseName: "publisher", ViewName: "v", SubscriptionName: "sub", Snapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: 9}}}},
		}
		f.query.SelectLockReferences = append(f.query.SelectLockReferences, ref)
		f.items = append(f.items, privilegeItem{privilegeTyp: PrivilegeTypeSelect, selectLock: true, objType: objectTypeTable, dbName: "sub", tableName: name, objectRef: ref.ObjectRef, viewPath: ref.ViewPath})
	}
	var current selectLockCatalogKey
	f.bh.EXPECT().ClearExecResultSet().AnyTimes()
	f.bh.EXPECT().Close().Do(func() { f.closes++ }).AnyTimes()
	f.bh.EXPECT().GetExecStatsArray().Return(statistic.StatsArray{}).AnyTimes()
	f.bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string) error {
		account, err := defines.GetAccountId(ctx)
		require.NoError(t, err)
		current = selectLockCatalogKey{account, sql}
		f.counts[current]++
		if f.before != nil {
			f.before(current)
		}
		return f.fail[current]
	}).AnyTimes()
	f.bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
		result := f.results[current]
		if result == nil {
			result = &MysqlResultSet{}
		}
		return []interface{}{result}
	}).AnyTimes()
	return f
}

func TestSelectLockAuthorizationBatchBoundaries(t *testing.T) {
	for _, tc := range []string{"success", "sibling lacks write", "definer cannot rescue invoker", "eligibility changes", "boundary error", "metadata error", "cancel between targets", "cancel with eligibility loss"} {
		t.Run(tc, func(t *testing.T) {
			f := newSelectLockBatchCatalog(t, viewSecurityDefiner, 2)
			roles := &btree.Set[int64]{}
			roles.Insert(50)
			require.NoError(t, expandRoleSetWithInheritedRoles(f.ctx, f.bh, roles))
			failure := moerr.NewTxnNeedRetryNoCtx()
			beforeNext := func() (bool, error) { return true, nil }
			wantChecked, wantOK := 2, true
			switch tc {
			case "sibling lacks write":
				for key := range f.results {
					if strings.Contains(key.sql, "'t1'") && strings.Contains(key.sql, "mo_role_privs") {
						delete(f.results, key)
					}
				}
				wantChecked, wantOK = 1, false
			case "definer cannot rescue invoker":
				// The second target is an INVOKER view whose base has only the
				// first definer's grant, with identical numeric role IDs.
				step := *f.items[1].viewPath[0]
				step.ViewName = "invoker"
				f.items[1].viewPath = []*plan.ViewStep{&step}
				meta, err := getSqlForCheckViewMeta(defines.AttachAccountId(f.ctx, 9), "publisher", "invoker")
				require.NoError(t, err)
				mrs := &MysqlResultSet{}
				mrs.AddColumn(&MysqlColumn{})
				mrs.AddColumn(&MysqlColumn{})
				mrs.AddRow([]interface{}{`{"security_type":"INVOKER"}`, int64(50)})
				f.results[selectLockCatalogKey{9, meta}] = mrs
				grant, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(f.ctx, objectTypeView, 50, PrivilegeTypeUpdate, "sub", "invoker")
				require.NoError(t, err)
				f.results[selectLockCatalogKey{7, grant}] = newMrsForWithGrantOptionPrivilege([][]interface{}{{PrivilegeTypeUpdate, true}})
				leakedViewGrant, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(f.ctx, objectTypeView, 61, PrivilegeTypeUpdate, "sub", "invoker")
				require.NoError(t, err)
				f.results[selectLockCatalogKey{7, leakedViewGrant}] = newMrsForWithGrantOptionPrivilege([][]interface{}{{PrivilegeTypeUpdate, true}})
				// Give the first definer a different role closure, then poison
				// the caller namespace with a grant usable only by that closure.
				outerMeta, err := getSqlForCheckViewMeta(defines.AttachAccountId(f.ctx, 9), "publisher", "v")
				require.NoError(t, err)
				outer := &MysqlResultSet{}
				outer.AddColumn(&MysqlColumn{})
				outer.AddColumn(&MysqlColumn{})
				outer.AddRow([]interface{}{`{"security_type":"DEFINER"}`, int64(60)})
				f.results[selectLockCatalogKey{9, outerMeta}] = outer
				for role := int64(60); role <= 61; role++ {
					var rows [][]interface{}
					if role == 60 {
						rows = [][]interface{}{{int64(61)}}
					}
					f.results[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(role)}] = newMrsForInheritedRoleIdOfRoleId(rows)
				}
				for i, account := range []uint32{9, 7} {
					db := "publisher"
					if account == 7 {
						db = "sub"
					}
					sql, err := getSqlForCheckRoleHasTableLevelPrivilegeWithObjType(f.ctx, objectTypeTable, 61, PrivilegeTypeDelete, db, fmt.Sprintf("t%d", i))
					require.NoError(t, err)
					f.results[selectLockCatalogKey{account, sql}] = newMrsForWithGrantOptionPrivilege([][]interface{}{{PrivilegeTypeDelete, true}})
				}
				wantChecked, wantOK = 1, false
			case "eligibility changes":
				beforeNext = func() (bool, error) { return false, nil }
				wantChecked = 1 // successful prefix must resume, rather than deny
			case "boundary error":
				beforeNext = func() (bool, error) { return false, failure }
				wantChecked, wantOK = 1, false
			case "metadata error":
				meta, err := getSqlForCheckViewMeta(defines.AttachAccountId(f.ctx, 9), "publisher", "v")
				require.NoError(t, err)
				f.fail[selectLockCatalogKey{9, meta}] = failure
				wantChecked, wantOK = 0, false
			case "cancel between targets", "cancel with eligibility loss":
				ctx, cancel := context.WithCancel(f.ctx)
				t.Cleanup(cancel)
				f.ctx = ctx
				beforeNext = func() (bool, error) { cancel(); return tc != "cancel with eligibility loss", nil }
				wantChecked, wantOK = 1, false
			}
			batch := &selectLockAuthorizationBatch{items: f.items}
			ok, err := batch.check(f.ctx, f.bh, f.ses, roles, beforeNext)
			if tc == "boundary error" || tc == "metadata error" {
				require.ErrorIs(t, err, failure)
			} else if tc == "cancel between targets" || tc == "cancel with eligibility loss" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, wantOK, ok)
			require.Equal(t, wantChecked, batch.checked)
			require.Equal(t, []int64{50, 51, 52, 53, 54, 55, 56, 57, 58}, roles.Keys(), "target resolution must not mutate caller roles")
		})
	}
}

func TestSelectLockDefinerRoleReuse(t *testing.T) {
	f := newSelectLockBatchCatalog(t, viewSecurityDefiner, 0)
	var memo selectLockDefinerRoles
	load := func(account uint32, role int64) error {
		roles := &btree.Set[int64]{}
		roles.Insert(role)
		return memo.load(defines.AttachAccountId(f.ctx, account), f.bh, viewPrivilegePrincipal{accountID: account, roleID: role}, roles)
	}
	for _, account := range []uint32{7, 7, 9, 9, 7} {
		require.NoError(t, load(account, 50))
	}
	require.Equal(t, 2, f.counts[selectLockCatalogKey{7, getSqlForInheritedRoleIdOfRoleId(50)}])
	require.Equal(t, 1, f.counts[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(50)}])
	// A failed expansion cannot publish even a partial closure.
	key := selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(51)}
	failure := moerr.NewTxnNeedRetryNoCtx()
	f.fail[key] = failure
	require.ErrorIs(t, load(9, 50), failure)
	delete(f.fail, key)
	require.NoError(t, load(9, 50))
	require.Equal(t, 3, f.counts[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(50)}])
	// Same account, different seed role: the previous closure cannot satisfy
	// the new principal, and switching back must reload the one-entry memo.
	f.results[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(70)}] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{{int64(71)}})
	f.results[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(71)}] = newMrsForInheritedRoleIdOfRoleId(nil)
	different := &btree.Set[int64]{}
	different.Insert(70)
	require.NoError(t, memo.load(defines.AttachAccountId(f.ctx, 9), f.bh, viewPrivilegePrincipal{accountID: 9, roleID: 70}, different))
	require.Equal(t, []int64{70, 71}, different.Keys())
	require.NoError(t, load(9, 50))
	require.Equal(t, 4, f.counts[selectLockCatalogKey{9, getSqlForInheritedRoleIdOfRoleId(50)}])
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	roles := &btree.Set[int64]{}
	roles.Insert(50)
	require.ErrorIs(t, memo.load(ctx, f.bh, viewPrivilegePrincipal{accountID: 9, roleID: 50}, roles), context.Canceled)
}
