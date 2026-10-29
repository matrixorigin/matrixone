// Copyright 2026 MatrixOne
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

package plan

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func lockingQuery(nodes ...*plan.Node) *plan.Query {
	lockNode := &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
	}
	return &plan.Query{Nodes: append(nodes, lockNode)}
}

func scan(nodeID int32, role plan.ScanReadRole) *plan.Node {
	return &plan.Node{
		NodeId:       nodeID,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r"},
		ScanReadRole: role,
	}
}

// The bind-time presence summary (design D-8 layer 1) derives from any
// LockTarget carrying locking_read; a plan without presence stays unmarked.
func TestPlanHasLockingCurrentRead(t *testing.T) {
	require.False(t, PlanHasLockingCurrentRead(nil))
	require.False(t, PlanHasLockingCurrentRead(&plan.Query{}),
		"no LockTargets means no presence")
	require.False(t, PlanHasLockingCurrentRead(&plan.Query{
		Nodes: []*plan.Node{{LockTargets: []*plan.LockTarget{{TableId: 42}}}},
	}), "a plan without the presence flag is unmarked")
	require.True(t, PlanHasLockingCurrentRead(lockingQuery(scan(0, plan.ScanReadRole_ScanOuterLocking))),
		"any LockTarget with the presence flag marks the plan")
}

// The R14-F2 closure walk (design D-10.3/G4) reads presence and roles from
// the plan alone: an unset-role TABLE_SCAN inside a presence plan is a lost
// mark; INDEX_SEARCH_SCAN and the covered-plan strip are outside the stage-1
// admission set; explicit-nested occurrences are exempt by representation so
// same-table scalar-subquery plans stay decidable without join-shape or
// table-identity inference.
func TestWalkLockingReadClosure(t *testing.T) {
	// Lost mark: one unset-role scan beside a valid marked scan.
	closure := WalkLockingReadClosure(lockingQuery(
		scan(0, plan.ScanReadRole_ScanOuterLocking),
		scan(1, plan.ScanReadRole_ScanNestedBlock),
	))
	require.True(t, closure.LostMark)
	require.False(t, closure.OutsideAdmission)

	// Clean shape: marked scan plus an explicit-nested occurrence (the
	// same-table scalar-subquery/LEFT-lowered shape).
	closure = WalkLockingReadClosure(lockingQuery(
		scan(0, plan.ScanReadRole_ScanOuterLocking),
		scan(1, plan.ScanReadRole_ScanNestedBlockExplicit),
	))
	require.False(t, closure.LostMark)
	require.False(t, closure.OutsideAdmission)

	// Search-served surface: an INDEX_SEARCH_SCAN anywhere in the plan.
	closure = WalkLockingReadClosure(lockingQuery(
		scan(0, plan.ScanReadRole_ScanOuterLocking),
		&plan.Node{NodeId: 1, NodeType: plan.Node_INDEX_SEARCH_SCAN},
	))
	require.False(t, closure.LostMark)
	require.True(t, closure.OutsideAdmission)

	// Covered-plan strip: representationally distinct from a lost mark.
	closure = WalkLockingReadClosure(lockingQuery(
		scan(0, plan.ScanReadRole_ScanOuterLocking),
		scan(1, plan.ScanReadRole_ScanCoveredPlanStrip),
	))
	require.False(t, closure.LostMark)
	require.True(t, closure.OutsideAdmission)

	// Unset roles never trigger anything without presence.
	closure = WalkLockingReadClosure(&plan.Query{Nodes: []*plan.Node{
		scan(0, plan.ScanReadRole_ScanNestedBlock),
	}})
	require.False(t, closure.LostMark)
	require.False(t, closure.OutsideAdmission)
	require.False(t, closure.UnsupportedOuterKind)
}

// The rev-14 P2 outer-kind member of the closure walk: an OUTER-locking
// occurrence admits only on the stage-1 relation kinds — ordinary tables and
// covered index surfaces through their marked owning parent. A locked
// cluster/external table (or an orphan index surface) must trip the closure
// so the whole statement declines instead of current-reading its ordinary
// locked peers while the unsupported occurrence keeps the snapshot
// (D-10 whole-statement conservation). Intentional snapshot occurrences
// stay exempt BY REPRESENTATION regardless of kind.
func TestWalkLockingReadClosureUnsupportedOuterKind(t *testing.T) {
	kindedScan := func(id int32, role plan.ScanReadRole, kind string) *plan.Node {
		node := scan(id, role)
		node.TableDef.TableType = kind
		return node
	}
	indexScan := func(id int32, role plan.ScanReadRole, parent int64) *plan.Node {
		node := kindedScan(id, role, "i")
		node.IndexScanInfo = plan.IndexScanInfo{ParentTableId: parent}
		return node
	}

	// (a) Mixed join shape: an ordinary locked scan plus a locked CLUSTER
	// table occurrence — the reviewer's mixed ordinary/cluster shape.
	closure := WalkLockingReadClosure(lockingQuery(
		scan(0, plan.ScanReadRole_ScanOuterLocking),
		kindedScan(1, plan.ScanReadRole_ScanOuterLocking, "cluster"),
	))
	require.True(t, closure.UnsupportedOuterKind,
		"a locked cluster-table occurrence must decline the whole statement")
	require.False(t, closure.LostMark)
	require.False(t, closure.OutsideAdmission)

	// (b) The same cluster table under an INTENTIONAL snapshot occurrence
	// stays exempt: explicit-nested provenance is a designed snapshot
	// read, never a conservation violation.
	closure = WalkLockingReadClosure(lockingQuery(
		scan(0, plan.ScanReadRole_ScanOuterLocking),
		kindedScan(1, plan.ScanReadRole_ScanNestedBlockExplicit, "cluster"),
	))
	require.False(t, closure.UnsupportedOuterKind)

	// (c) A covered secondary-index surface admits through its marked owning
	// parent: the plan's LockTarget on table 42 carries LockingRead.
	closure = WalkLockingReadClosure(lockingQuery(
		indexScan(0, plan.ScanReadRole_ScanOuterLocking, 42),
	))
	require.False(t, closure.UnsupportedOuterKind,
		"the covered index surface is an admitted stage-1 member")

	// (d) The same index surface WITHOUT its admission link — unset parent
	// or a parent outside the marked lock targets — trips the closure.
	closure = WalkLockingReadClosure(lockingQuery(
		indexScan(0, plan.ScanReadRole_ScanOuterLocking, 0),
	))
	require.True(t, closure.UnsupportedOuterKind, "an unset ParentTableId is unsupported")

	closure = WalkLockingReadClosure(lockingQuery(
		indexScan(0, plan.ScanReadRole_ScanOuterLocking, 77),
	))
	require.True(t, closure.UnsupportedOuterKind, "a parent outside the marked targets is unsupported")

	// (e) An unmarked lock target is not an admission link (the plan's
	// presence comes from the other, marked target).
	qry := &plan.Query{Nodes: []*plan.Node{
		indexScan(0, plan.ScanReadRole_ScanOuterLocking, 42),
		{NodeType: plan.Node_LOCK_OP,
			LockTargets: []*plan.LockTarget{
				{TableId: 42},
				{TableId: 55, LockingRead: true},
			}},
	}}
	closure = WalkLockingReadClosure(qry)
	require.True(t, closure.UnsupportedOuterKind,
		"an unmarked lock target provides no admission link")

	// (f) A plan without locking-read presence is never flagged.
	closure = WalkLockingReadClosure(&plan.Query{Nodes: []*plan.Node{
		kindedScan(0, plan.ScanReadRole_ScanOuterLocking, "cluster"),
	}})
	require.False(t, closure.UnsupportedOuterKind)
}

// The P2 view-provenance members through the FULL BINDER: transparent view
// chains keep the outer locking provenance through EVERY view expansion (a
// two-hop v2 -> v1 -> table chain current-reads at the base table, where the
// pre-fix expansion dropped provenance on the second hop and left a stale
// snapshot read inside a locking view), while genuine nested query blocks
// (derived tables, whether inside or outside a locking view) bind
// explicit-nested and keep their snapshot reads.
func TestViewChainLockingProvenance(t *testing.T) {
	// The full-binder fixture runs exactly like the delivered single-version
	// cluster (rev 15, R15-F1): roles and LockTarget presence are built for
	// every locking statement, with no capability arming.
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 0, nil }

	registerView := func(name, body string) {
		viewData, err := json.Marshal(ViewData{
			Stmt:            body,
			DefaultDatabase: "tpch",
			SecurityType:    "DEFINER",
		})
		require.NoError(t, err)
		ctx.tables[name] = &TableDef{
			Name: name, DbName: "tpch", DbId: 10, TblId: 100 + uint64(name[1]),
			TableType: catalog.SystemViewRel,
			Cols: []*ColDef{{
				Name: "n_name", OriginName: "n_name",
				Typ: Type{Id: int32(types.T_varchar), Width: 25},
			}},
			ViewSql: &plan.ViewDef{View: string(viewData)},
		}
		ctx.objects[name] = &ObjectRef{SchemaName: "tpch", ObjName: name, Db: 10, Obj: 100}
	}
	registerView("v1", "create view v1 as select n_name from nation")
	registerView("v2", "create view v2 as select n_name from v1")
	registerView("v3",
		"create view v3 as select n_name from (select n_name from nation) d")

	scanRoles := func(qry *plan.Query) map[string]plan.ScanReadRole {
		out := make(map[string]plan.ScanReadRole)
		for _, node := range qry.GetNodes() {
			if node == nil || node.NodeType != plan.Node_TABLE_SCAN {
				continue
			}
			if tbl := node.GetTableDef(); tbl != nil && tbl.GetName() != "" {
				out[tbl.GetName()] = node.GetScanReadRole()
			}
		}
		return out
	}
	markedTableIDs := func(qry *plan.Query) map[uint64]bool {
		out := make(map[uint64]bool)
		for _, node := range qry.GetNodes() {
			for _, target := range node.GetLockTargets() {
				if target.GetLockingRead() {
					out[target.GetTableId()] = true
				}
			}
		}
		return out
	}
	build := func(sql string) (*plan.Query, error) {
		t.Helper()
		stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmts[0].Free()
		opt := NewBaseOptimizer(ctx)
		query, err := opt.Optimize(stmts[0], false)
		if err != nil {
			return nil, err
		}
		return query, nil
	}

	// The direct ordinary table stays the control shape.
	qry, err := build("select n_name from nation for update")
	require.NoError(t, err)
	nationID := qry.GetNodes()[0].GetTableDef().GetTblId()
	marks := markedTableIDs(qry)
	require.True(t, marks[nationID],
		"the locked base occurrence derives a marked LockTarget")

	// One view hop: the base table inside the view keeps the provenance.
	qry, err = build("select n_name from v1 for update")
	require.NoError(t, err)
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, scanRoles(qry)["nation"],
		"a transparent view body keeps the outer locking provenance")

	// Two view hops — the pre-fix regression shape: the second expansion
	// must NOT drop the inherited provenance back to the snapshot.
	qry, err = build("select n_name from v2 for update")
	require.NoError(t, err)
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, scanRoles(qry)["nation"],
		"provenance survives every transparent view expansion of the chain")
	require.True(t, PlanHasLockingCurrentRead(qry))
	require.False(t, WalkLockingReadClosure(qry).LostMark,
		"the base scan keeps its outer role, never an unset role")

	// A derived table is a GENUINE nested block even inside a locking
	// statement: it keeps the snapshot read, whether it sits in the outer
	// statement or inside the view body.
	qry, err = build("select n_name from v3 for update")
	require.NoError(t, err)
	require.Equal(t, plan.ScanReadRole_ScanNestedBlockExplicit, scanRoles(qry)["nation"],
		"the view body's derived table does not leak view provenance into the nested block")

	qry, err = build("select d.n_name from (select n_name from nation) d for update")
	require.NoError(t, err)
	require.Equal(t, plan.ScanReadRole_ScanNestedBlockExplicit, scanRoles(qry)["nation"],
		"an outer derived table binds explicit-nested, outside views too")
}

// The PlanLockingReadQuery adapter resolves the same query through a
// *plan.Plan and a *plan.Query, and stays nil-safe.
func TestPlanLockingReadQueryAdapter(t *testing.T) {
	qry, err := PlanLockingReadQuery((*plan.Plan)(nil))
	require.NoError(t, err)
	require.Nil(t, qry)

	qry, err = PlanLockingReadQuery(&plan.Plan{})
	require.NoError(t, err)
	require.Nil(t, qry, "a Plan without a carrier resolves no query")

	direct := lockingQuery(scan(0, plan.ScanReadRole_ScanOuterLocking))
	qry, err = PlanLockingReadQuery(&plan.Plan{
		Plan: &plan.Plan_Query{Query: direct},
	})
	require.NoError(t, err)
	require.Same(t, direct, qry)

	qry, err = PlanLockingReadQuery(direct)
	require.NoError(t, err)
	require.Same(t, direct, qry)

	qry, err = PlanLockingReadQuery("not-a-plan")
	require.NoError(t, err)
	require.Nil(t, qry)
}

// The pre-PR review P2 deep-copy presence member at the plan layer: the
// per-EXECUTE parameter fill consumer (FillValuesOfParamsInPlan — the
// prepared-statement path whose DeepCopyPlan used to drop LockTarget presence
// silently, disabling capability validation, closure checks and LockOp
// marking for every filled execution) keeps the bind-time locking-read
// presence and scan-role representation through the whole fill round trip.
func TestFillValuesOfParamsKeepsLockingReadPresence(t *testing.T) {
	paramType := types.T_int32.ToType().PlanType()
	lockedScan := &plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r", Name: "t", TblId: 42},
		ObjRef:       &plan.ObjectRef{SchemaName: "test", ObjName: "t"},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
		FilterList: []*plan.Expr{{
			// Prepared plans store ZERO-based normalized param positions
			// (NormalizePrepareParamRefs), so the single parameter is Pos 0.
			Typ:  paramType,
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}},
	}
	lockNode := &plan.Node{
		NodeId:   1,
		NodeType: plan.Node_LOCK_OP,
		Children: []int32{0},
		LockTargets: []*plan.LockTarget{
			{TableId: 42, LockingRead: true},
			{TableId: 43},
		},
	}
	preparePlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Steps: []int32{1},
		Nodes: []*plan.Node{lockedScan, lockNode},
	}}}
	require.True(t, PlanHasLockingCurrentRead(preparePlan.GetQuery()),
		"the prepared marked plan carries bind-time presence")

	filled, err := FillValuesOfParamsInPlan(context.Background(), preparePlan, []any{int32(7)})
	require.NoError(t, err)
	require.True(t, PlanHasLockingCurrentRead(filled.GetQuery()),
		"the parametro-fill deep copy keeps the marked presence for Run admission")
	filledQry := filled.GetQuery()
	require.Len(t, filledQry.Nodes, 2)
	scanCopy := filledQry.Nodes[0]
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, scanCopy.GetScanReadRole(),
		"the filled scan keeps its locking role representation")
	filledExpr := scanCopy.GetFilterList()[0]
	_, stillParam := filledExpr.Expr.(*plan.Expr_P)
	require.False(t, stillParam, "the one parameter was filled with a constant")
	require.Len(t, filledQry.Nodes[1].LockTargets, 2)
	require.True(t, filledQry.Nodes[1].LockTargets[0].GetLockingRead(),
		"the marked target keeps presence through the fill's DeepCopyPlan")
	require.False(t, filledQry.Nodes[1].LockTargets[1].GetLockingRead())

	// The original prepared plan is never mutated by the fill.
	_, origParam := preparePlan.GetQuery().Nodes[0].GetFilterList()[0].Expr.(*plan.Expr_P)
	require.True(t, origParam, "the original prepared plan keeps its ParamRef (fill copies, never mutates)")
}
