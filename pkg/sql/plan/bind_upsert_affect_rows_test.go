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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// mainUpdateCtxCountDelete returns the main-table UpdateCtx's
// CountDeleteAffectRows flag of the (single) MULTI_UPDATE node in the plan.
func mainUpdateCtxCountDelete(t *testing.T, p *Plan) bool {
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
	return found.UpdateCtxList[0].CountDeleteAffectRows
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

// TestUpsertAffectRowsPlan keeps writer flags separate from the ordered
// owner's per-action change comparison and logical affected-row result.
func TestUpsertAffectRowsPlan(t *testing.T) {
	mock := NewMockOptimizer(true)
	actions := func(t *testing.T, p *Plan) *planpb.OdkuActionCtx {
		t.Helper()
		var found *planpb.OdkuActionCtx
		for _, node := range p.GetQuery().Nodes {
			if c := node.PreInsertUkCtx.GetOdkuActions(); c != nil {
				require.Nil(t, found)
				found = c
			}
		}
		require.NotNil(t, found)
		return found
	}
	for _, tc := range []struct {
		name, sql string
		columns   []int32
		automatic uint32
	}{
		{"self assignment", "insert into constraint_test.dept(deptno,dname,loc) values(1,'A','B') on duplicate key update loc=loc", []int32{2}, 0},
		{"secondary conflict compares written columns", "insert into constraint_test.dept(deptno,dname,loc) values(999,'Sales','NY') on duplicate key update loc=loc", []int32{2}, 0},
		{"computed assignment evaluated once", "insert into constraint_test.dept(deptno,dname,loc) values(1,'A','B') on duplicate key update loc=substring(loc,1,2)", []int32{2}, 0},
		{"implicit automatic column excluded", "insert into constraint_test.t_on_update(id,val) values(1,10) on duplicate key update val=val", []int32{1}, 1},
		{"automatic generated dependency excluded", "insert into constraint_test.t_on_update_gen(id,val) values(1,10) on duplicate key update val=val", []int32{1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			require.True(t, mainUpdateCtxCountDelete(t, p))
			require.False(t, hasNoopFilter(p), "post-publication SQL no-op filtering must not re-evaluate an action")
			c := actions(t, p)
			require.Equal(t, tc.columns, c.ChangeCheckColumns)
			require.Equal(t, uint32(1), c.ChangeCheckAssignmentCount)
			require.Equal(t, tc.automatic, c.AutoUpdateAssignmentCount)
			require.Equal(t, int32(types.T_Rowid), c.OldColumns[c.RowIdColumn].Typ.Id, "new owners retain a nullable original identity")
		})
	}
	for _, tc := range []struct {
		name, sql   string
		countDelete bool
	}{
		{"replace", "replace into constraint_test.emp(empno,ename,job) values(1,'A','B')", true},
		{"insert", "insert into constraint_test.emp(empno,ename,job) values(1,'A','B')", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.countDelete, mainUpdateCtxCountDelete(t, p))
			require.False(t, hasNoopFilter(p))
			for _, node := range p.GetQuery().Nodes {
				require.Nil(t, node.PreInsertUkCtx.GetOdkuActions())
			}
		})
	}
	t.Run("deep copy preserves action phases and writer flags", func(t *testing.T) {
		p, err := runOneStmt(mock, t, "insert into constraint_test.t_on_update_gen(id,val) values(1,10) on duplicate key update val=val")
		require.NoError(t, err)
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == planpb.Node_MULTI_UPDATE {
				copied := DeepCopyNode(node)
				require.True(t, copied.UpdateCtxList[0].CountDeleteAffectRows)
			}
			if node.PreInsertUkCtx.GetOdkuActions() != nil {
				copied := DeepCopyNode(node)
				require.Equal(t, node.PreInsertUkCtx, copied.PreInsertUkCtx)
				require.NotSame(t, node.PreInsertUkCtx.OdkuActions, copied.PreInsertUkCtx.OdkuActions)
			}
		}
	})
}
