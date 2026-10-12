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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func requireOnDupUpdateColumns(t *testing.T, logicPlan *planpb.Plan, included, excluded []int32) {
	t.Helper()
	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_DEDUP ||
			node.OnDuplicateAction != planpb.Node_UPDATE {
			continue
		}
		found = true
		for _, col := range included {
			require.Contains(t, node.DedupJoinCtx.UpdateColIdxList, col)
		}
		for _, col := range excluded {
			require.NotContains(t, node.DedupJoinCtx.UpdateColIdxList, col)
		}
	}
	require.True(t, found, "expected an ON DUPLICATE KEY UPDATE dedup join")
}

func requireODKUBaseLockUsesResolvedTarget(
	t *testing.T,
	logicPlan *planpb.Plan,
	tableDef *planpb.TableDef,
) {
	t.Helper()
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_LOCK_OP || len(node.Children) != 1 {
			continue
		}
		for _, target := range node.LockTargets {
			if target.TableId != tableDef.TblId {
				continue
			}
			lockInput := query.Nodes[node.Children[0]]
			require.Equal(t, planpb.Node_PRE_INSERT_UK, lockInput.NodeType)
			require.True(t, lockInput.PreInsertUkCtx.GetOdkuTargetArbitration())
			resolvedTargetPos := int32(len(lockInput.ProjectList) - 1)
			require.Equal(t, resolvedTargetPos, target.PrimaryColIdxInBat,
				"base-row lock must consume the arbiter's resolved target identity")
			require.Equal(t, lockInput.ProjectList[resolvedTargetPos].Typ.Id, target.PrimaryColTyp.Id)
			return
		}
	}
	t.Fatal("expected an ODKU base-table lock on the resolved target identity")
}

func TestInsertOnDupIncomingPrimaryKeyNoop(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = values(a), b = values(b)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, []int32{1}, []int32{0})
}

func TestInsertOnDupIncomingPrimaryKeyOnlyNoop(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = values(a)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, nil, []int32{0})
}

func TestInsertOnDupPrimaryKeyLiteralIdentityIsRuntimeGuarded(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = (1), b = values(b)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, []int32{0, 1}, nil)
	requireODKUPKIdentityGuard(t, logicPlan, 0)
}

func TestInsertOnDupGuardedPrimaryKeyDoesNotEnableActionReplay(t *testing.T) {
	for _, tc := range []struct {
		name       string
		assignment string
	}{
		{name: "primary-key-only", assignment: "a = 1"},
		{name: "primary-key-and-payload", assignment: "a = 1, b = values(b)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			logicPlan, err := runOneStmt(mock, t,
				"insert into constraint_test.t1(a, b) values (1, 'x') "+
					"on duplicate key update "+tc.assignment)
			require.NoError(t, err)

			ctx := odkuDedupCtx(t, logicPlan)
			require.False(t, ctx.EmitActionRows,
				"a successfully guarded, unchanged primary key must not trigger redundant NOT NULL action replay")
			requireODKUPKIdentityGuard(t, logicPlan, 0)
		})
	}
}

func TestInsertOnDupCharPrimaryKeyGuardPreservesType(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	for _, table := range []*planpb.TableDef{
		mock.ctxt.tables["t1"],
		mock.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "t1")],
	} {
		require.NotNil(t, table)
		for i := range table.Cols {
			if table.Cols[i].Name == "a" {
				table.Cols[i].Typ.Id = int32(types.T_char)
				table.Cols[i].Typ.Width = 4
			}
		}
	}

	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values ('x', 'seed') "+
			"on duplicate key update a = 'x ', b = values(b)")
	require.NoError(t, err)

	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_DEDUP ||
			node.OnDuplicateAction != planpb.Node_UPDATE {
			continue
		}
		for i, colIdx := range node.DedupJoinCtx.UpdateColIdxList {
			if colIdx != 0 {
				continue
			}
			guarded := node.DedupJoinCtx.UpdateColExprList[i]
			require.Equal(t, int32(types.T_char), guarded.Typ.Id)
			require.Equal(t, int32(4), guarded.Typ.Width)
			guard := guarded.GetF()
			require.NotNil(t, guard)
			require.Equal(t, "if", guard.Func.ObjName)
			require.Len(t, guard.Args, 3)
			nullBranch := guard.Args[2]
			require.True(t, nullBranch.GetLit().Isnull)
			require.Equal(t, int32(types.T_char), nullBranch.Typ.Id)
			require.Equal(t, int32(4), nullBranch.Typ.Width)
			require.False(t, nullBranch.Typ.NotNullable)
			return
		}
	}
	t.Fatal("expected a CHAR primary-key identity guard")
}

func TestInsertOnDupPrimaryKeyPreparedMarkerIsRuntimeGuarded(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"prepare pk_identity from 'insert into constraint_test.t1(a, b) "+
			"values (?, ?) on duplicate key update a = ?, b = values(b)'")
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	requireOnDupUpdateColumns(t, prepare.Plan, []int32{0, 1}, nil)
	requireODKUPKIdentityGuard(t, prepare.Plan, 0)
}

func requireODKUPKIdentityGuard(t *testing.T, logicPlan *planpb.Plan, updateColIdx int32) {
	t.Helper()
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_DEDUP ||
			node.OnDuplicateAction != planpb.Node_UPDATE {
			continue
		}
		for i, colIdx := range node.DedupJoinCtx.UpdateColIdxList {
			if colIdx != updateColIdx {
				continue
			}
			guarded := node.DedupJoinCtx.UpdateColExprList[i].GetF()
			require.NotNil(t, guarded)
			require.Equal(t, "if", guarded.Func.ObjName)
			require.True(t, node.DedupJoinCtx.UpdateColExprList[i].Typ.NotNullable,
				"successful identity guards always return the existing non-NULL primary key")
			require.NotEmpty(t, guarded.Args)
			assertion := guarded.Args[0].GetF()
			require.NotNil(t, assertion)
			require.Equal(t, "assert", assertion.Func.ObjName)
			require.Equal(t, "odku_pk_mutation", assertion.Args[2].GetLit().GetSval())
			require.False(t, planExprContainsFunction(assertion.Args[0], "serial"),
				"integer primary-key identity should not serialize both values")
			return
		}
	}
	t.Fatalf("expected runtime identity guard for primary-key column index %d", updateColIdx)
}

func TestInsertOnDupIncomingCompositePrimaryKeyNoop(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"insert into tpch.partsupp values (1, 2, 3, 4.50, 'x') "+
			"on duplicate key update ps_partkey = values(ps_partkey), "+
			"ps_suppkey = values(ps_suppkey), ps_availqty = values(ps_availqty)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, []int32{2}, []int32{0, 1})
}

func TestInsertOnDupPrimaryKeyMutationStillRejected(t *testing.T) {
	for _, assignment := range []string{
		"a = a + 1, b = values(b)",
		"a = 1, a = a + 1, b = values(b)",
		"a = a + 1, a = 1, b = values(b)",
	} {
		t.Run(assignment, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			_, err := runOneStmt(mock, t,
				"insert into constraint_test.t1(a, b) values (1, 'x') "+
					"on duplicate key update "+assignment)
			require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
		})
	}
}

func TestInsertOnDupPrimaryKeyFromDifferentIncomingColumnStillRejected(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, '2') "+
			"on duplicate key update a = values(b)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func TestInsertOnDupCompositePrimaryKeyFromDifferentIncomingColumnStillRejected(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	_, err := runOneStmt(mock, t,
		"insert into tpch.partsupp values (1, 2, 3, 4.50, 'x') "+
			"on duplicate key update ps_partkey = values(ps_suppkey), "+
			"ps_suppkey = values(ps_suppkey)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func TestInsertOnDupWrappedIncomingPrimaryKeyStillRejected(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = cast(values(a) as signed)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func TestInsertOnDupPrimaryKeyLiteralIdentityWithSecondaryUniqueIsRuntimeGuarded(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY') "+
			"on duplicate key update deptno = 1, loc = values(loc)")
	require.NoError(t, err)
	requireODKUPKIdentityGuard(t, logicPlan, 0)
	requireODKUBaseLockUsesResolvedTarget(t, logicPlan, mock.ctxt.tables["dept"])
}

func TestInsertOnDupPreparedPrimaryKeyIdentityWithSecondaryUniqueIsRuntimeGuarded(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"prepare pk_identity_unique from 'insert into constraint_test.dept(deptno, dname, loc) "+
			"values (?, ?, ?) on duplicate key update deptno = ?'")
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	requireODKUPKIdentityGuard(t, prepare.Plan, 0)
	requireODKUBaseLockUsesResolvedTarget(t, prepare.Plan, mock.ctxt.tables["dept"])
}

func TestBindPrimaryKeyIdentityExprRetainsFloatBitIdentity(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_float64)}
	expr := &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Dval{Dval: 1.5},
		}},
	}
	identity, err := bindPrimaryKeyIdentityExpr(nil, expr, typ)
	require.NoError(t, err)
	require.True(t, planExprContainsFunction(identity, "serial"))
}

func planExprContainsFunction(expr *planpb.Expr, name string) bool {
	if expr == nil {
		return false
	}
	function := expr.GetF()
	if function == nil {
		return false
	}
	if function.Func.ObjName == name {
		return true
	}
	for _, arg := range function.Args {
		if planExprContainsFunction(arg, name) {
			return true
		}
	}
	return false
}

func TestIsOnDupIncomingColumn(t *testing.T) {
	expr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 3}}}
	require.True(t, isOnDupIncomingColumn(expr, 7, 3))
	require.False(t, isOnDupIncomingColumn(expr, 8, 3))
	require.False(t, isOnDupIncomingColumn(expr, 7, 4))
	require.False(t, isOnDupIncomingColumn(&planpb.Expr{}, 7, 3))
}

func TestInsertOnDupPreservesAssignmentOrderAndDuplicates(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'Alice') "+
			"on duplicate key update b = concat(b, 'x'), b = concat(b, 'y'), b = concat(b, 'z')")
	require.NoError(t, err)

	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_DEDUP ||
			node.OnDuplicateAction != planpb.Node_UPDATE {
			continue
		}
		require.Equal(t, []int32{1, 1, 1}, node.DedupJoinCtx.UpdateColIdxList)
		require.Len(t, node.DedupJoinCtx.UpdateColExprList, 3)
		require.NotNil(t, node.DedupJoinCtx.AffectedRowsCol)
		require.NotNil(t, node.DedupJoinCtx.PhysicalChangedRowsCol)
		return
	}
	t.Fatal("expected ODKU dedup join")
}

func TestInsertOnDupLocksResolvedConflictTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tableName string
		sql       string
	}{
		{
			name:      "real primary key with secondary unique conflict",
			tableName: "dept",
			sql: "insert into constraint_test.dept(deptno, dname, loc) " +
				"values (999, 'Sales', 'NY') on duplicate key update loc = 'LA'",
		},
		{
			name:      "synthetic primary key with unique conflict",
			tableName: "fake_pk_t",
			sql: "insert into constraint_test.fake_pk_t(a, b) " +
				"values (1, 'x') on duplicate key update b = 'y'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			requireODKUBaseLockUsesResolvedTarget(t, logicPlan, mock.ctxt.tables[tc.tableName])
		})
	}
}

func TestInsertOnDupCarriesFoundRowsMode(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		countUpdateChangedRows bool
		wantFoundRows          bool
	}{
		{name: "default changed rows", countUpdateChangedRows: true},
		{name: "client found rows", wantFoundRows: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			mock.CurrentContext().GetProcess().Base.SessionInfo.CountUpdateChangedRows = tc.countUpdateChangedRows
			logicPlan, err := runOneStmt(mock, t,
				"insert into constraint_test.t1(a, b) values (1, 'x') on duplicate key update b = values(b)")
			require.NoError(t, err)
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.DedupJoinCtx != nil && node.OnDuplicateAction == planpb.Node_UPDATE {
					require.Equal(t, tc.wantFoundRows, node.DedupJoinCtx.CountFoundRows)
					return
				}
			}
			t.Fatal("expected ODKU dedup join")
		})
	}
}
