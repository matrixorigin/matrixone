// Copyright 2021 - 2024 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func mainUpdateCtx(t *testing.T, p *Plan) *planpb.UpdateCtx {
	t.Helper()
	q := p.GetQuery()
	require.NotNil(t, q)
	var found *planpb.Node
	for _, n := range q.Nodes {
		if n.NodeType == planpb.Node_MULTI_UPDATE {
			require.Nil(t, found, "expected a single MULTI_UPDATE node")
			found = n
		}
	}
	require.NotNil(t, found, "no MULTI_UPDATE node in plan")
	require.NotEmpty(t, found.UpdateCtxList)
	return found.UpdateCtxList[0]
}

func odkuDedupCtx(t *testing.T, p *Plan) *planpb.DedupJoinCtx {
	t.Helper()
	for _, n := range p.GetQuery().Nodes {
		if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP &&
			n.OnDuplicateAction == planpb.Node_UPDATE {
			require.NotNil(t, n.DedupJoinCtx)
			return n.DedupJoinCtx
		}
	}
	t.Fatal("no ODKU DEDUP join in plan")
	return nil
}

func hasGuardedConstraintAssert(p *Plan, assertName string) bool {
	for _, node := range p.GetQuery().Nodes {
		for _, filter := range node.FilterList {
			assertFn := filter.GetF()
			if assertFn == nil || assertFn.Func == nil || assertFn.Func.ObjName != assertName || len(assertFn.Args) == 0 {
				continue
			}
			guard := assertFn.Args[0].GetF()
			if guard == nil || guard.Func == nil || guard.Func.ObjName != "or" || len(guard.Args) != 2 {
				continue
			}
			notEligible := guard.Args[0].GetF()
			if notEligible != nil && notEligible.Func != nil && notEligible.Func.ObjName == "not" &&
				len(notEligible.Args) == 1 && notEligible.Args[0].GetCol() != nil {
				return true
			}
		}
	}
	return false
}

func hasConstraintAssert(p *Plan, assertName string) bool {
	for _, node := range p.GetQuery().Nodes {
		for _, filter := range node.FilterList {
			assertFn := filter.GetF()
			if assertFn != nil && assertFn.Func != nil && assertFn.Func.ObjName == assertName {
				return true
			}
		}
	}
	return false
}

func hasActionNotNullAssert(p *Plan) bool {
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType != planpb.Node_FILTER || !node.FilterIsBarrier {
			continue
		}
		for _, filter := range node.FilterList {
			assertFn := filter.GetF()
			if assertFn == nil || assertFn.Func == nil ||
				(assertFn.Func.ObjName != "assert" && assertFn.Func.ObjName != "_check_constraint_assert") ||
				len(assertFn.Args) == 0 {
				continue
			}
			isNotNull := assertFn.Args[0].GetF()
			if isNotNull == nil || isNotNull.Func == nil ||
				(isNotNull.Func.ObjName != "isnotnull" && isNotNull.Func.ObjName != "is_not_null") ||
				len(isNotNull.Args) != 1 {
				continue
			}
			if col := isNotNull.Args[0].GetCol(); col != nil {
				return true
			}
		}
	}
	return false
}

func TestDeepCopyUpdateCtxPreservesChangedRowsCol(t *testing.T) {
	original := []*planpb.UpdateCtx{{ChangedRowsCol: &planpb.ColRef{RelPos: 3, ColPos: 7}}}
	copied := DeepCopyUpdateCtxList(original)
	require.Equal(t, original[0].ChangedRowsCol, copied[0].ChangedRowsCol)
	require.NotSame(t, original[0].ChangedRowsCol, copied[0].ChangedRowsCol)
}

func TestDeepCopyDedupJoinCtxPreservesActionMetadata(t *testing.T) {
	original := &planpb.DedupJoinCtx{
		EmitActionRows: true,
		ActionFinalCol: &planpb.ColRef{RelPos: 3, ColPos: 7},
		ForeignKeyChecks: []planpb.ODKUForeignKeyCheck{{
			ColIdxList: []int32{1, 2}, EligibilityCol: &planpb.ColRef{RelPos: 3, ColPos: 8},
		}},
	}
	copied := DeepCopyDedupJoinCtx(original)
	require.Equal(t, original, copied)
	require.NotSame(t, original.ActionFinalCol, copied.ActionFinalCol)
	require.NotSame(t, original.ForeignKeyChecks[0].EligibilityCol,
		copied.ForeignKeyChecks[0].EligibilityCol)
	copied.ForeignKeyChecks[0].ColIdxList[0] = 99
	require.Equal(t, int32(1), original.ForeignKeyChecks[0].ColIdxList[0])
}

// hasNoopFilter reports whether the plan contains a FILTER node whose predicate
// is the ODKU no-op guard: isnull(old rowid) OR NOT( col <=> col [AND ...] ).
// The isnull(rowid) branch keeps non-conflicting rows (all-NULL old image)
// flowing to the INSERT side instead of being dropped by the equality chain.
func hasNoopFilter(p *Plan) bool {
	q := p.GetQuery()
	if q == nil {
		return false
	}
	for _, n := range q.Nodes {
		if n.NodeType != planpb.Node_FILTER {
			continue
		}
		for _, cond := range n.FilterList {
			if notExpr := noopFilterNotBranch(cond); notExpr != nil {
				if inner := notExpr.Args[0].GetF(); inner != nil && inner.Func != nil {
					switch inner.Func.ObjName {
					case "<=>", "and":
						return true
					}
				}
			}
		}
	}
	return false
}

// noopFilterNotBranch matches the no-op guard shape
// or(isnull(old rowid), not(...)) and returns the not(...) function,
// or nil if expr does not match.
func noopFilterNotBranch(expr *planpb.Expr) *planpb.Function {
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "or" || len(f.Args) != 2 {
		return nil
	}
	isNull := f.Args[0].GetF()
	if isNull == nil || isNull.Func == nil || isNull.Func.ObjName != "isnull" ||
		len(isNull.Args) != 1 || isNull.Args[0].GetCol() == nil {
		return nil
	}
	notExpr := f.Args[1].GetF()
	if notExpr == nil || notExpr.Func == nil || notExpr.Func.ObjName != "not" || len(notExpr.Args) != 1 {
		return nil
	}
	return notExpr
}

// TestUpsertAffectRowsPlan verifies that ODKU carries logical affected-row
// weights independently of its final physical-write decision. REPLACE retains
// the legacy delete+insert count; plain INSERT remains unchanged.
func TestUpsertAffectRowsPlan(t *testing.T) {
	mock := NewMockOptimizer(true)

	t.Run("ODKU carries logical count and physical marker", func(t *testing.T) {
		// dept goes through the dedup-join + MULTI_UPDATE path; loc is not part of
		// any key, so it is a legal ON DUPLICATE KEY UPDATE target.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = loc")
		require.NoError(t, err)
		ctx := mainUpdateCtx(t, p)
		require.False(t, ctx.CountDeleteAffectRows)
		require.NotNil(t, ctx.AffectedRowsWeightCol)
		require.NotNil(t, ctx.PhysicalChangedRowsCol)
		require.False(t, hasNoopFilter(p), "physical filtering belongs in MULTI_UPDATE")
		dedup := odkuDedupCtx(t, p)
		require.NotNil(t, dedup.AffectedRowsCol)
		require.NotNil(t, dedup.PhysicalChangedRowsCol)
		require.False(t, dedup.EmitActionRows,
			"tables without CHECK or child FKs must retain the one-row-per-group fast path")
	})

	t.Run("ODKU unrelated update skips FK action stream", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.emp(empno, ename, job, deptno) values (1, 'A', 'B', 1) on duplicate key update sal = sal")
		require.NoError(t, err)
		ctx := odkuDedupCtx(t, p)
		require.False(t, ctx.EmitActionRows,
			"an unrelated update must retain one-row-per-key execution on an FK table")
		require.Nil(t, ctx.ActionFinalCol)
		require.Len(t, ctx.ForeignKeyChecks, 1)
		require.True(t, hasGuardedConstraintAssert(p, "assert"),
			"the single final-row FK probe must bypass an unchanged historical tuple")
	})

	t.Run("ODKU affected FK assertion is guarded by tuple eligibility", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.emp(empno, ename, job, deptno) values (1, 'A', 'B', 1) on duplicate key update deptno = values(deptno)")
		require.NoError(t, err)
		require.True(t, hasGuardedConstraintAssert(p, "assert"))
		ctx := odkuDedupCtx(t, p)
		require.True(t, ctx.EmitActionRows)
		require.NotNil(t, ctx.ActionFinalCol)
		require.Len(t, ctx.ForeignKeyChecks, 1)
		require.NotEmpty(t, ctx.ForeignKeyChecks[0].ColIdxList)
		require.NotNil(t, ctx.ForeignKeyChecks[0].EligibilityCol)
		hasFinalBarrier := false
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType != planpb.Node_FILTER || !node.FilterIsBarrier || len(node.FilterList) != 1 {
				continue
			}
			if col := node.FilterList[0].GetCol(); col != nil && node.FilterList[0].Typ.Id == int32(types.T_bool) {
				hasFinalBarrier = true
			}
		}
		require.True(t, hasFinalBarrier,
			"the synthetic final-action predicate must never be pushed below DEDUP UPDATE")
	})

	t.Run("ODKU unrelated update skips CHECK action stream", func(t *testing.T) {
		m := NewMockOptimizer(true)
		addPositiveCheck(t, m, "emp", "deptno")
		p, err := runOneStmt(m, t,
			"insert into constraint_test.emp(empno, ename, job, deptno) values (1, 'A', 'B', 1) on duplicate key update sal = sal")
		require.NoError(t, err)
		require.False(t, odkuDedupCtx(t, p).EmitActionRows)
		require.True(t, hasGuardedConstraintAssert(p, "_check_constraint_assert"),
			"an unrelated CHECK applies to new inserts but not an existing update row")
	})

	t.Run("ODKU action checks remain independent when FK checks are disabled", func(t *testing.T) {
		newMock := func(t *testing.T) *MockOptimizer {
			t.Helper()
			m := NewMockOptimizer(true)
			m.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
				switch name {
				case "foreign_key_checks":
					return int64(0), nil
				case "sql_mode":
					return "", nil
				default:
					return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
				}
			}
			return m
		}

		t.Run("CHECK", func(t *testing.T) {
			m := newMock(t)
			addPositiveCheck(t, m, "emp", "deptno")

			p, err := runOneStmt(m, t,
				"insert into constraint_test.emp(empno, ename, job, deptno) values (1, 'A', 'B', 1) "+
					"on duplicate key update deptno = values(deptno)")
			require.NoError(t, err)
			ctx := odkuDedupCtx(t, p)
			require.True(t, ctx.EmitActionRows,
				"CHECK still requires the ordered action stream")
			require.Empty(t, ctx.ForeignKeyChecks,
				"disabled FK checks must not consume unmaterialized eligibility columns")
			require.True(t, hasConstraintAssert(p, "_check_constraint_assert"),
				"disabling FK checks must not disable CHECK validation")
		})

		t.Run("NOT NULL", func(t *testing.T) {
			m := newMock(t)
			table := DeepCopyTableDef(m.ctxt.tables["t_on_update"], true)
			table.Cols[1].Typ.NotNullable = true
			table.Cols[1].Default.NullAbility = false
			table.Fkeys = []*planpb.ForeignKeyDef{{
				Name:        "fk_val",
				Cols:        []uint64{table.Cols[1].ColId},
				ForeignTbl:  88888,
				ForeignCols: []uint64{1},
			}}
			m.ctxt.tables["t_on_update"] = table

			p, err := runOneStmt(m, t,
				"insert into constraint_test.t_on_update(id, val) values (1, 10) "+
					"on duplicate key update val = if(values(val) = 10, null, values(val))")
			require.NoError(t, err)
			ctx := odkuDedupCtx(t, p)
			require.True(t, ctx.EmitActionRows,
				"nullable expression into NOT NULL still requires the ordered action stream")
			require.Empty(t, ctx.ForeignKeyChecks,
				"disabled FK checks must not consume unmaterialized eligibility columns")
			require.True(t, hasActionNotNullAssert(p),
				"disabling FK checks must not disable NOT NULL action validation")
		})
	})

	t.Run("ODKU validates each action that can write a NOT NULL column", func(t *testing.T) {
		original := mock.ctxt.tables["t_on_update"]
		table := DeepCopyTableDef(original, true)
		table.Cols[1].Typ.NotNullable = true
		table.Cols[1].Default.NullAbility = false
		mock.ctxt.tables["t_on_update"] = table
		t.Cleanup(func() { mock.ctxt.tables["t_on_update"] = original })

		p, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update(id, val) values (1, 10), (1, 20) "+
				"on duplicate key update val = if(values(val) = 10, null, values(val))")
		require.NoError(t, err)
		require.True(t, odkuDedupCtx(t, p).EmitActionRows)
		require.True(t, hasActionNotNullAssert(p),
			"NOT NULL must be asserted before the final-action filter can discard an invalid intermediate action")

		fastPath, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update(id, val) values (1, 10) "+
				"on duplicate key update val = values(val)")
		require.NoError(t, err)
		require.False(t, odkuDedupCtx(t, fastPath).EmitActionRows,
			"a proven non-null assignment must retain the one-row-per-key fast path")
	})

	t.Run("unique-only table validates actions before retaining the final row", func(t *testing.T) {
		original := mock.ctxt.tables["fake_pk_t"]
		table := DeepCopyTableDef(original, true)
		table.Cols[1].Typ.NotNullable = true
		table.Cols[1].Default.NullAbility = false
		mock.ctxt.tables["fake_pk_t"] = table
		t.Cleanup(func() { mock.ctxt.tables["fake_pk_t"] = original })

		p, err := runOneStmt(mock, t,
			"insert into constraint_test.fake_pk_t(a, b) values (1, 'bad'), (1, 'good') "+
				"on duplicate key update b = if(values(b) = 'bad', null, values(b))")
		require.NoError(t, err)
		require.True(t, odkuDedupCtx(t, p).EmitActionRows)
		require.True(t, hasActionNotNullAssert(p))
		hasFinalBarrier := false
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType != planpb.Node_FILTER || !node.FilterIsBarrier || len(node.FilterList) != 1 {
				continue
			}
			if col := node.FilterList[0].GetCol(); col != nil && node.FilterList[0].Typ.Id == int32(types.T_bool) {
				hasFinalBarrier = true
			}
		}
		require.True(t, hasFinalBarrier,
			"the fake-PK/unique-key path must discard non-final action rows after validation")
	})

	t.Run("REPLACE flags main ctx", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"replace into constraint_test.emp(empno, ename, job) values (1, 'A', 'B')")
		require.NoError(t, err)
		require.True(t, mainUpdateCtx(t, p).CountDeleteAffectRows,
			"REPLACE main UpdateCtx should set CountDeleteAffectRows")
		require.False(t, hasNoopFilter(p),
			"REPLACE always rewrites a conflicting row, so it has no no-op filter")
	})

	t.Run("plain INSERT has no no-op filter", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.emp(empno, ename, job) values (1, 'A', 'B')")
		require.NoError(t, err)
		require.False(t, hasNoopFilter(p),
			"plain INSERT must not add a no-op filter")
	})

	t.Run("ODKU change check excludes ON UPDATE columns", func(t *testing.T) {
		// t_on_update has: id (PK), val, updated_at (ON UPDATE CURRENT_TIMESTAMP).
		// ODKU with v=v should skip updated_at in the no-op filter so that the
		// auto-update expression does not defeat the no-op guard.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update(id, val) values (1, 10) on duplicate key update val = val")
		require.NoError(t, err)
		dedup := odkuDedupCtx(t, p)
		require.Equal(t, []int32{1}, dedup.UpdateCheckColIdxList,
			"only explicit val assignment determines whether the logical action changed")
	})

	t.Run("ODKU primary-key-only no-op filters conflicting rows", func(t *testing.T) {
		// Flink emits id = VALUES(id) even when id is the only assignment. The
		// planner removes that semantic no-op from the physical update set, so the
		// resulting guard must keep only rows without an existing-row match. This
		// also prevents implicit ON UPDATE expressions from firing on conflicts.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update(id, val) values (1, 10) on duplicate key update id = values(id)")
		require.NoError(t, err)
		require.Empty(t, odkuDedupCtx(t, p).UpdateCheckColIdxList,
			"removed PK self-assignment is always a no-op on a conflict")
		require.NotNil(t, mainUpdateCtx(t, p).PhysicalChangedRowsCol)
	})

	t.Run("ODKU no-op filter keeps non-conflicting rows via rowid guard", func(t *testing.T) {
		// An all-NULL insert row into a nullable unique key never conflicts, so
		// its old image is all-NULL and every <=> in the no-op chain evaluates
		// to true. The isnull(old rowid) OR-branch must be present so such rows
		// are inserted instead of silently dropped. hasNoopFilter only matches
		// the or(isnull(rowid), not(...)) shape, so a true result asserts the
		// guard exists.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = loc")
		require.NoError(t, err)
		require.NotNil(t, odkuDedupCtx(t, p).PhysicalChangedRowsCol,
			"DEDUP emits true for a non-conflicting insert without a rowid filter")
	})

	t.Run("ODKU no-op filter compares only written columns on secondary-unique conflict", func(t *testing.T) {
		// dept has PK deptno and a secondary UNIQUE key on dname. An ODKU whose
		// conflict is resolved through dname can carry an incoming deptno that
		// differs from the existing row's deptno. The update (loc = loc) writes
		// only loc, so the row is a genuine no-op and MySQL returns
		// affected-rows = 0. The no-op guard must therefore compare the final
		// written value of loc only — not the immutable PK deptno nor the
		// conflict-key dname against the raw incoming image, which would
		// spuriously turn the no-op into a counted update.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (999, 'Sales', 'NY') on duplicate key update loc = loc")
		require.NoError(t, err)
		require.Equal(t, []int32{2}, odkuDedupCtx(t, p).UpdateCheckColIdxList,
			"only loc, not immutable deptno/dname, determines whether the update changed")
	})

	t.Run("ODKU no-op filter references materialized value for computed update", func(t *testing.T) {
		// A computed assignment (loc = substring(loc, 1, 2)) is evaluated once by
		// the dedup-update join and written into the new-image column. The no-op
		// guard must compare against that materialized column, never re-evaluate
		// the expression — otherwise a non-deterministic assignment would be
		// evaluated twice and the no-op decision could disagree with the value
		// actually stored.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = substring(loc, 1, 2)")
		require.NoError(t, err)
		dedup := odkuDedupCtx(t, p)
		require.Len(t, dedup.UpdateColExprList, 1,
			"the computed assignment must be evaluated exactly once")
		require.Equal(t, []int32{2}, dedup.UpdateCheckColIdxList)
	})

	t.Run("DeepCopyNode preserves CountDeleteAffectRows on MULTI_UPDATE", func(t *testing.T) {
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.dept(deptno, dname, loc) values (1, 'A', 'B') on duplicate key update loc = loc")
		require.NoError(t, err)
		var mu *planpb.Node
		for _, n := range p.GetQuery().Nodes {
			if n.NodeType == planpb.Node_MULTI_UPDATE {
				mu = n
				break
			}
		}
		require.NotNil(t, mu)
		require.NotNil(t, mu.UpdateCtxList[0].AffectedRowsWeightCol)
		require.NotNil(t, mu.UpdateCtxList[0].PhysicalChangedRowsCol)
		copied := DeepCopyNode(mu)
		require.Equal(t, mu.UpdateCtxList[0].AffectedRowsWeightCol, copied.UpdateCtxList[0].AffectedRowsWeightCol)
		require.NotSame(t, mu.UpdateCtxList[0].AffectedRowsWeightCol, copied.UpdateCtxList[0].AffectedRowsWeightCol)
		require.Equal(t, mu.UpdateCtxList[0].PhysicalChangedRowsCol, copied.UpdateCtxList[0].PhysicalChangedRowsCol)
		require.NotSame(t, mu.UpdateCtxList[0].PhysicalChangedRowsCol, copied.UpdateCtxList[0].PhysicalChangedRowsCol)
	})

	t.Run("ODKU no-op filter excludes generated column derived from ON UPDATE", func(t *testing.T) {
		// t_on_update_gen has: id (PK), val, updated_at (ON UPDATE CURRENT_TIMESTAMP),
		// g (stored, g AS (updated_at)). ODKU with val=val must skip both updated_at
		// and its dependent generated column g, otherwise g's recomputed value would
		// defeat the no-op guard.
		p, err := runOneStmt(mock, t,
			"insert into constraint_test.t_on_update_gen(id, val) values (1, 10) on duplicate key update val = val")
		require.NoError(t, err)
		require.Equal(t, []int32{1}, odkuDedupCtx(t, p).UpdateCheckColIdxList,
			"ON UPDATE and its generated dependent must not turn val=val into a change")
	})
}
