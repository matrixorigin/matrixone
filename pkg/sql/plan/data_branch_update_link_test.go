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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func collectMultiUpdateLinkContexts(t *testing.T, q *planpb.Query) (links []*planpb.UpdateCtx, owners []*planpb.UpdateCtx) {
	t.Helper()
	for _, node := range q.Nodes {
		if node.NodeType != planpb.Node_MULTI_UPDATE {
			continue
		}
		for _, ctx := range node.UpdateCtxList {
			if !IsDataBranchUpdateLinkPlanCtx(ctx) {
				continue
			}
			links = append(links, ctx)
			if int(ctx.TargetUpdateCtxIdx) < len(node.UpdateCtxList) {
				owners = append(owners, node.UpdateCtxList[ctx.TargetUpdateCtxIdx])
			}
		}
	}
	return links, owners
}

func TestUpdatePkMoveLinkPlannedForPkTouchingUpdate(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t, "update dept set deptno = 20, dname = 'x' where deptno = 10")
	require.NoError(t, err)

	links, owners := collectMultiUpdateLinkContexts(t, logicPlan.GetQuery())
	require.Len(t, links, 1, "a genuine pk-touching UPDATE must plan exactly one link ctx")
	require.Len(t, owners, 1)
	require.Equal(t, "dept", owners[0].TableDef.Name)
	require.True(t, links[0].IgnoreAffectedRows)
	require.Empty(t, links[0].InsertCols)
	require.Empty(t, links[0].DeleteCols)
	require.Equal(t, catalog.MO_DATA_BRANCH_UPDATE_LINK, links[0].ObjRef.ObjName)
	require.Equal(t, catalog.MO_CATALOG, links[0].ObjRef.SchemaName)
	require.NotNil(t, links[0].TableDef)
	require.Equal(t, catalog.MO_DATA_BRANCH_UPDATE_LINK, links[0].TableDef.Name)
	require.Equal(t, catalog.DataBranchUpdateLinkColLinkID, links[0].TableDef.Pkey.PkeyColName)
}

func TestUpdatePkMoveLinkPlannedForNonKeyUpdate(t *testing.T) {
	// A pk-unchanged UPDATE still needs the marker-capable link ctx: when a
	// same-key edit replaces a workspace-minted row an earlier statement's
	// move recorded, the operator emits a same-key continuity marker through
	// this ctx (durable rows emit nothing, so ordinary updates stay free).
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t, "update dept set dname = 'x' where deptno = 10")
	require.NoError(t, err)

	links, owners := collectMultiUpdateLinkContexts(t, logicPlan.GetQuery())
	require.Len(t, links, 1, "a genuine pk-unchanged UPDATE must plan exactly one marker-capable link ctx")
	require.Len(t, owners, 1)
	require.Equal(t, "dept", owners[0].TableDef.Name)
}

func TestUpdatePkMoveLinkCtxSkipsMetaNameLocks(t *testing.T) {
	// Regression: the synthetic link ctx targets a second database
	// (mo_catalog). If it counted toward LockMeta's name pins, its
	// cross-database ObjRef would flip ordinary single-table UPDATEs from
	// the sole mo_tables name pin they retain on main into
	// LockMeta.doLock's multi-database branch — additionally pinning the
	// mo_database rows of both databases and the mo_tables row of the link
	// catalog. That widening lets a concurrent DROP DATABASE queue on the
	// UPDATE's Shared database pin before it ever reaches the retained
	// mo_tables pin (the issue-29400 shared-gate choreography), so both
	// the plant site and the actually planned ctx must carry the skip.
	built := buildUpdatePkMoveLinkPlanCtx(1)
	require.True(t, built.ObjRef.NotLockMeta,
		"the internal link write must not acquire catalog meta-name locks")
	require.True(t, IsDataBranchUpdateLinkPlanCtx(built))

	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t, "update dept set dname = 'x' where deptno = 10")
	require.NoError(t, err)
	links, _ := collectMultiUpdateLinkContexts(t, logicPlan.GetQuery())
	require.Len(t, links, 1)
	require.True(t, links[0].ObjRef.NotLockMeta,
		"planned link ctx must skip catalog meta-name locks for the internal link write")
}

func TestUpdatePkMoveLinkNotPlanned(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{name: "plain insert", sql: "insert into dept values (1, 'a', 'b')"},
		{name: "replace keeps delete+insert semantics", sql: "replace into dept values (1, 'a', 'b')"},
		{name: "odku keeps delete+insert semantics", sql: "insert into dept values (1, 'a', 'b') on duplicate key update loc = 'y'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			links, _ := collectMultiUpdateLinkContexts(t, logicPlan.GetQuery())
			require.Empty(t, links, "statement %q must not record pk-move links", tc.sql)
		})
	}
}

func TestUpdatePkMoveLinkNotPlannedForCascadeChildren(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_CASCADE, planpb.ForeignKeyDef_RESTRICT)

	logicPlan, err := runOneStmt(mock, t, "update dept set deptno = 20 where deptno = 10")
	require.NoError(t, err)
	links, owners := collectMultiUpdateLinkContexts(t, logicPlan.GetQuery())
	require.NotEmpty(t, links)
	for _, owner := range owners {
		require.NotEqual(t, "emp", owner.TableDef.Name,
			"cascade-generated child update ctxs must not record pk-move links")
	}
	require.Equal(t, 1, len(links), "only the root pk-touching update records links")
}

func TestUpdatePkMoveLinkEligibility(t *testing.T) {
	userRef := &ObjectRef{SchemaName: "somedb", ObjName: "t"}
	userDef := &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
	require.True(t, updatePkMoveLinkEligible(userRef, userDef))

	require.False(t, updatePkMoveLinkEligible(nil, userDef))
	require.False(t, updatePkMoveLinkEligible(userRef, nil))
	require.False(t, updatePkMoveLinkEligible(userRef, &TableDef{}))
	require.False(t, updatePkMoveLinkEligible(
		&ObjectRef{SchemaName: catalog.MO_CATALOG, ObjName: "mo_tables"}, userDef))
	require.False(t, updatePkMoveLinkEligible(userRef, &TableDef{
		Pkey:      userDef.Pkey,
		Partition: &planpb.Partition{PartitionDefs: []*planpb.PartitionDef{{}}},
	}))
	clusterDef := &TableDef{Pkey: userDef.Pkey, TableType: catalog.SystemClusterRel}
	require.False(t, updatePkMoveLinkEligible(userRef, clusterDef))

	// ordinary user table relkind is allowed
	ordinaryDef := &TableDef{Pkey: userDef.Pkey, TableType: catalog.SystemOrdinaryRel}
	require.True(t, updatePkMoveLinkEligible(userRef, ordinaryDef))
}

func TestUpdatePkMoveLinkTableDefMatchesDDLColumns(t *testing.T) {
	def := buildUpdatePkMoveLinkTableDef()
	require.NotNil(t, def)
	// The visible columns mirror frontend.MoCatalogDataBranchUpdateLinkDDL, the
	// hidden row id column trails.
	require.GreaterOrEqual(t, len(def.Cols), 9)
	want := []struct {
		name  string
		isVar bool
	}{
		{catalog.DataBranchUpdateLinkColLinkID, false},
		{catalog.DataBranchUpdateLinkColTableID, false},
		{catalog.DataBranchUpdateLinkColOldPKSha, false},
		{catalog.DataBranchUpdateLinkColOldRow, false},
		{catalog.DataBranchUpdateLinkColOldPK, true},
		{catalog.DataBranchUpdateLinkColNewPKSha, false},
		{catalog.DataBranchUpdateLinkColNewPK, true},
		{catalog.DataBranchUpdateLinkColNewRow, false},
		{catalog.DataBranchUpdateLinkColWriteTS, false},
		{catalog.DataBranchUpdateLinkColTxnID, false},
	}
	for i, w := range want {
		require.Equal(t, w.name, def.Cols[i].Name)
		require.Equal(t, w.isVar, def.Cols[i].Typ.Id == int32(types.T_varbinary))
	}
	require.Equal(t, catalog.Row_ID, def.Cols[len(def.Cols)-1].Name)
}
