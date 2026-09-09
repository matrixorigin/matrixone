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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIssue28308ForeignKeyDeleteAffectedRowsOwnership(t *testing.T) {
	t.Run("cascade delete", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_CASCADE, planpb.ForeignKeyDef_RESTRICT)

		logicPlan, err := runOneStmt(mock, t, "delete from dept where deptno = 10")
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		var parentDelete, childDelete int
		for _, node := range query.Nodes {
			if node.NodeType != planpb.Node_DELETE || node.DeleteCtx == nil || node.DeleteCtx.TableDef == nil {
				continue
			}
			switch node.DeleteCtx.TableDef.Name {
			case "dept":
				parentDelete++
				require.True(t, node.DeleteCtx.AddAffectedRows,
					"the direct DELETE must own the statement affected-row count")
			case "emp":
				childDelete++
				require.False(t, node.DeleteCtx.AddAffectedRows,
					"a cascading child DELETE is an implicit side effect")
			}
		}
		require.Equal(t, 1, parentDelete)
		require.Equal(t, 1, childDelete)
	})

	t.Run("set null update remains implicit", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_SET_NULL, planpb.ForeignKeyDef_RESTRICT)

		logicPlan, err := runOneStmt(mock, t, "delete from dept where deptno = 10")
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		var parentDelete, childDeletes, childInserts int
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_DELETE && node.DeleteCtx != nil && node.DeleteCtx.TableDef != nil &&
				node.DeleteCtx.TableDef.Name == "dept" {
				parentDelete++
				require.True(t, node.DeleteCtx.AddAffectedRows)
			}
			if node.NodeType == planpb.Node_DELETE && node.DeleteCtx != nil && node.DeleteCtx.TableDef != nil &&
				node.DeleteCtx.TableDef.Name == "emp" {
				childDeletes++
				require.False(t, node.DeleteCtx.AddAffectedRows,
					"the delete half of SET NULL is an implicit side effect")
			}
			if node.NodeType == planpb.Node_INSERT && node.InsertCtx != nil && node.InsertCtx.TableDef != nil &&
				node.InsertCtx.TableDef.Name == "emp" {
				childInserts++
				require.False(t, node.InsertCtx.AddAffectedRows,
					"the insert half of SET NULL is an implicit side effect")
			}
		}
		require.Equal(t, 1, parentDelete)
		require.Equal(t, 1, childDeletes)
		require.Equal(t, 1, childInserts)
	})

	t.Run("replace cascade delete remains implicit", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_CASCADE, planpb.ForeignKeyDef_RESTRICT)

		logicPlan, err := runOneStmt(mock, t, "replace into dept values (1, 'Sales', 'New York')")
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		var parentUpdate, childDelete int
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_MULTI_UPDATE {
				for _, updateCtx := range node.UpdateCtxList {
					if updateCtx.TableDef != nil && updateCtx.TableDef.Name == "dept" {
						parentUpdate++
						require.False(t, updateCtx.IgnoreAffectedRows,
							"the direct REPLACE action must own the statement affected-row count")
					}
				}
			}
			if node.NodeType != planpb.Node_DELETE || node.DeleteCtx == nil || node.DeleteCtx.TableDef == nil {
				continue
			}
			switch node.DeleteCtx.TableDef.Name {
			case "emp":
				childDelete++
				require.False(t, node.DeleteCtx.AddAffectedRows,
					"a REPLACE-triggered cascading child DELETE is an implicit side effect")
			}
		}
		require.Equal(t, 1, parentUpdate)
		require.Equal(t, 1, childDelete)
	})

	t.Run("self reference cascade keeps direct rows as owner", func(t *testing.T) {
		mock := NewMockOptimizer(true)

		logicPlan, err := runOneStmt(mock, t, "delete from self_ref_cascade where id = 1")
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		var directDelete, recursiveDelete int
		for _, node := range query.Nodes {
			if node.NodeType != planpb.Node_DELETE || node.DeleteCtx == nil || node.DeleteCtx.TableDef == nil ||
				node.DeleteCtx.TableDef.Name != "self_ref_cascade" {
				continue
			}
			if node.DeleteCtx.AddAffectedRows {
				directDelete++
			} else {
				recursiveDelete++
			}
		}
		require.Equal(t, 1, directDelete)
		require.Equal(t, 1, recursiveDelete,
			"self-referential descendants must use a separate non-counting DELETE")
	})

	t.Run("self reference set null keeps direct rows as owner", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		mock.ctxt.tables["self_ref_cascade"].Fkeys[0].OnDelete = planpb.ForeignKeyDef_SET_NULL

		logicPlan, err := runOneStmt(mock, t, "delete from self_ref_cascade where id = 1")
		require.NoError(t, err)

		query := logicPlan.GetQuery()
		var directDelete, recursiveDeletes, recursiveInserts int
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_DELETE && node.DeleteCtx != nil && node.DeleteCtx.TableDef != nil &&
				node.DeleteCtx.TableDef.Name == "self_ref_cascade" {
				if node.DeleteCtx.AddAffectedRows {
					directDelete++
				} else {
					recursiveDeletes++
				}
			}
			if node.NodeType == planpb.Node_INSERT && node.InsertCtx != nil && node.InsertCtx.TableDef != nil &&
				node.InsertCtx.TableDef.Name == "self_ref_cascade" {
				recursiveInserts++
				require.False(t, node.InsertCtx.AddAffectedRows)
			}
		}
		require.Equal(t, 1, directDelete)
		require.Equal(t, 1, recursiveDeletes)
		require.Equal(t, 1, recursiveInserts)
	})

	t.Run("multiple self reference set nulls share one implicit action", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		for _, fk := range mock.ctxt.tables["self_ref_multi_cascade"].Fkeys {
			fk.OnDelete = planpb.ForeignKeyDef_SET_NULL
		}

		logicPlan, err := runOneStmt(mock, t, "delete from self_ref_multi_cascade where id = 1")
		require.NoError(t, err)
		assertLockTargetTypesMatchInput(t, logicPlan.GetQuery())

		query := logicPlan.GetQuery()
		var directDelete, recursiveDeletes, recursiveInserts int
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_DELETE && node.DeleteCtx != nil && node.DeleteCtx.TableDef != nil &&
				node.DeleteCtx.TableDef.Name == "self_ref_multi_cascade" {
				if node.DeleteCtx.AddAffectedRows {
					directDelete++
				} else {
					recursiveDeletes++
				}
			}
			if node.NodeType == planpb.Node_INSERT && node.InsertCtx != nil && node.InsertCtx.TableDef != nil &&
				node.InsertCtx.TableDef.Name == "self_ref_multi_cascade" {
				recursiveInserts++
				require.False(t, node.InsertCtx.AddAffectedRows)
			}
		}
		require.Equal(t, 1, directDelete)
		require.Equal(t, 1, recursiveDeletes)
		require.Equal(t, 1, recursiveInserts)
	})
}

func TestIssue28308SelfReferentialSetNullKeepsSiblingRestrict(t *testing.T) {
	for _, tc := range []struct {
		name     string
		combined bool
	}{
		{name: "single set null", combined: false},
		{name: "combined set null", combined: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			configureIssue28308SelfReferentialBoundary(t, mock, tc.combined)

			logicPlan, err := runOneStmt(mock, t,
				"delete from self_ref_multi_cascade where id = 1")
			require.NoError(t, err)
			require.True(t, hasIssue28308SelfReferentialRestrictCheck(logicPlan.GetQuery()),
				"the sibling ON UPDATE RESTRICT check must consume the NULL-safe changed-row stream")
		})
	}
}

func hasIssue28308SelfReferentialRestrictCheck(query *planpb.Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_FILTER || len(node.FilterList) == 0 ||
			!issue28308ExprContainsFunction(node.FilterList[0], "assert") || len(node.Children) != 1 {
			continue
		}
		join := query.Nodes[node.Children[0]]
		if join.NodeType != planpb.Node_JOIN ||
			len(join.Children) != 2 || len(join.OnList) == 0 {
			continue
		}
		for _, childID := range join.Children {
			child := query.Nodes[childID]
			if child.NodeType == planpb.Node_FILTER &&
				len(child.FilterList) > 0 &&
				issue28308ExprContainsFunction(child.FilterList[0], "not") &&
				issue28308ExprContainsFunction(child.FilterList[0], "<=>") {
				return true
			}
		}
	}
	return false
}

func issue28308ExprContainsFunction(expr *planpb.Expr, name string) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.GetFunc().GetObjName() == name {
		return true
	}
	for _, arg := range fn.Args {
		if issue28308ExprContainsFunction(arg, name) {
			return true
		}
	}
	return false
}

func configureIssue28308SelfReferentialBoundary(
	t *testing.T,
	mock *MockOptimizer,
	combined bool,
) {
	t.Helper()
	table := mock.ctxt.tables["self_ref_multi_cascade"]
	require.NotNil(t, table)

	if combined {
		cols := DeepCopyColDefList(table.Cols)
		p := DeepCopyColDef(cols[2])
		p.Name = "p"
		p.OriginName = "p"
		p.ColId = 3
		rowID := cols[len(cols)-1]
		rowID.ColId = 4
		table.Cols = append(cols[:3], p, rowID)
	} else {
		table.Cols = DeepCopyColDefList(table.Cols)
	}
	table.Name2ColIndex = make(map[string]int32, len(table.Cols))
	for pos, col := range table.Cols {
		table.Name2ColIndex[col.Name] = int32(pos)
	}

	setNull := func(name string, childCol, parentCol uint64) *planpb.ForeignKeyDef {
		return &planpb.ForeignKeyDef{
			Name:        name,
			Cols:        []uint64{childCol},
			ForeignTbl:  0,
			ForeignCols: []uint64{parentCol},
			OnDelete:    planpb.ForeignKeyDef_SET_NULL,
			OnUpdate:    planpb.ForeignKeyDef_NO_ACTION,
		}
	}
	restrict := func(name string, childCol, parentCol uint64) *planpb.ForeignKeyDef {
		return &planpb.ForeignKeyDef{
			Name:        name,
			Cols:        []uint64{childCol},
			ForeignTbl:  0,
			ForeignCols: []uint64{parentCol},
			OnDelete:    planpb.ForeignKeyDef_NO_ACTION,
			OnUpdate:    planpb.ForeignKeyDef_RESTRICT,
		}
	}
	table.Fkeys = []*planpb.ForeignKeyDef{
		setNull("fk_set_null_a", 1, 0),
	}
	if combined {
		table.Fkeys = append(table.Fkeys,
			setNull("fk_set_null_b", 2, 0),
			restrict("fk_restrict_p", 3, 1))
	} else {
		table.Fkeys = append(table.Fkeys, restrict("fk_restrict_b", 2, 1))
	}
	table.RefChildTbls = []uint64{0}
}
