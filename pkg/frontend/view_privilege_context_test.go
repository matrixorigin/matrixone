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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestViewPrivilegeEdgeKeepsAccountSnapshotAndLegacyNames(t *testing.T) {
	snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}, Tenant: &plan.SnapshotTenant{TenantID: 7, TenantName: "source"}}
	key, err := plan2.FormatViewPrivilegeKey("db#part", "view#part", snapshot, 7)
	require.NoError(t, err)
	database, view, got, account, qualified, err := parseViewPrivilegeEdge(t.Context(), key, nil)
	require.NoError(t, err)
	require.Equal(t, "db#part", database)
	require.Equal(t, "view#part", view)
	require.Equal(t, snapshot, got)
	require.EqualValues(t, 7, account)
	require.True(t, qualified)

	legacy := objectkey.Encode("db#part", "view#part")
	database, view, got, _, qualified, err = parseViewPrivilegeEdge(t.Context(), legacy, snapshot)
	require.NoError(t, err)
	require.Equal(t, "db#part", database)
	require.Equal(t, "view#part", view)
	require.Same(t, snapshot, got)
	require.False(t, qualified)

	_, _, _, _, _, err = parseViewPrivilegeEdge(t.Context(), "\x00mo_view_privilege\x00!", nil)
	require.Error(t, err)
}

func TestViewAdminRoleIsTenantScoped(t *testing.T) {
	for _, tc := range []struct {
		account uint32
		role    int64
		want    bool
	}{
		{sysAccountID, moAdminRoleID, true},
		{7, accountAdminRoleID, true},
		{sysAccountID, accountAdminRoleID, false},
		{7, moAdminRoleID, false},
		{7, 9, false},
	} {
		require.Equal(t, tc.want, isViewAdmin(tc.account, tc.role), "%+v", tc)
	}
}

func TestViewRoleExistsPropagatesQueryErrors(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	query := "select role_id from mo_catalog.mo_role where role_id = 0;"
	bh.sql2result[query] = newMrsForRestoreStringRows([]string{"role_id"}, nil)
	exists, err := viewRoleExists(t.Context(), bh, 7, 0)
	require.NoError(t, err)
	require.False(t, exists)
	bh.sql2result[query] = newMrsForRestoreStringRows([]string{"role_id"}, [][]interface{}{{int64(0)}})
	exists, err = viewRoleExists(t.Context(), bh, 7, 0)
	require.NoError(t, err)
	require.True(t, exists)
	bh.sql2err[query] = context.Canceled
	exists, err = viewRoleExists(t.Context(), bh, 7, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, exists)
}

func TestResolveViewPrivilegeSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 100}}).AnyTimes()
	ses := &Session{}
	ses.txnHandler = &TxnHandler{txnOp: op}
	ctx := defines.AttachAccountId(t.Context(), 7)
	tcc := &TxnCompilerContext{execCtx: &ExecCtx{ses: ses, reqCtx: ctx}}
	cases := []struct {
		name     string
		snapshot *plan.Snapshot
		account  uint32
		wantErr  bool
	}{
		{"current", nil, 7, false},
		{"same-account historical", &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 99}}, 7, false},
		{"foreign historical", &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 99}, Tenant: &plan.SnapshotTenant{TenantID: 8}}, 8, false},
		{"same-account future", &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 101}}, 7, false},
		{"foreign future", &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 101}, Tenant: &plan.SnapshotTenant{TenantID: 8}}, 0, true},
		{"foreign equal", &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 100}, Tenant: &plan.SnapshotTenant{TenantID: 8}}, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			account, snapshot, err := tcc.ResolveViewPrivilegeSnapshot(tc.snapshot)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.account, account)
			require.Same(t, tc.snapshot, snapshot)
		})
	}
}

func TestExtractPrivilegeTipsIncludesEveryViewChain(t *testing.T) {
	query := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{
		{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"}, OriginViews: []string{"outer", "table_view"}},
		{NodeType: plan.Node_VALUE_SCAN, OriginViews: []string{"outer"}}, // already covered by the table item
		{NodeType: plan.Node_VALUE_SCAN, OriginViews: []string{"outer", "constant_view"}},
		{NodeType: plan.Node_PROJECT, OriginViews: []string{"outer", "constant_view"}}, // duplicate
		{NodeType: plan.Node_FUNCTION_SCAN, OriginViews: []string{"other_view"}},
		{NodeType: plan.Node_VALUE_SCAN}, // SELECT 1 needs no view grant
	}}}}
	tips := extractPrivilegeTipsFromPlan(query)
	require.Len(t, tips, 3)
	require.False(t, tips[0].viewOnly)
	require.Equal(t, []string{"outer", "table_view"}, tips[0].originViews)
	require.True(t, tips[1].viewOnly)
	require.Equal(t, []string{"outer", "constant_view"}, tips[1].originViews)
	require.True(t, tips[2].viewOnly)
	require.Equal(t, []string{"other_view"}, tips[2].originViews)
	priv := &privilege{objType: objectTypeTable}
	convertPrivilegeTipsToPrivilege(priv, tips)
	require.Len(t, priv.entries, 1)
	require.True(t, priv.entries[0].compound.items[1].viewOnly)
	require.NotEqual(t, viewChainKey([]string{"a", "bc"}), viewChainKey([]string{"ab", "c"}))
}

func TestViewOnlyRequirementWithoutChainFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
	cache := ses.GetPrivilegeCache()
	cache.add(objectTypeTable, privilegeLevelStarStar, "", "", PrivilegeTypeSelect)
	item := privilegeItem{privilegeTyp: PrivilegeTypeSelect, objType: objectTypeTable}
	priv := &privilege{entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound,
		compound: &compoundEntry{items: []privilegeItem{item}, perObjectCovers: true}}}}
	ok, err := checkPrivilegeInCache(t.Context(), ses, priv, true)
	require.NoError(t, err)
	require.True(t, ok, "control: the cache contains the broad table grant")
	priv.entries[0].compound.items[0].viewOnly = true
	ok, err = checkPrivilegeInCache(t.Context(), ses, priv, true)
	require.NoError(t, err)
	require.False(t, ok)
	roles := &btree.Set[int64]{}
	roles.Insert(5)
	bh := &backgroundExecTest{}
	bh.init()
	ok, _, err = determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, false)
	require.Error(t, err)
	require.False(t, ok)
}
