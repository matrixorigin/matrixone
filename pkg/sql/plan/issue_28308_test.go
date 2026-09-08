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
