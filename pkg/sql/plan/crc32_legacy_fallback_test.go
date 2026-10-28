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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Add columns before rowid without changing existing key or FK types.
func addLegacyCRC32GeneratedFixture(t *testing.T, table *planpb.TableDef) {
	t.Helper()
	last := len(table.Cols) - 1
	require.GreaterOrEqual(t, last, 0)
	require.Equal(t, int32(types.T_Rowid), table.Cols[last].Typ.Id)
	var maxID uint64
	for _, col := range table.Cols {
		if col.ColId > maxID {
			maxID = col.ColId
		}
	}
	expr := legacyCRC32Expr()
	expr.GetF().Args[0].GetCol().ColPos = int32(last)
	expr.GetF().Args[0].GetCol().Name = "crc_input"
	rowID := table.Cols[last]
	table.Cols = append(table.Cols[:last],
		&planpb.ColDef{ColId: maxID + 1, Name: "crc_input", Typ: planpb.Type{Id: int32(types.T_json)}, Default: &planpb.Default{NullAbility: true}},
		&planpb.ColDef{ColId: maxID + 2, Name: "crc_legacy", Typ: planpb.Type{Id: int32(types.T_uint64)}, Default: &planpb.Default{NullAbility: true},
			GeneratedCol: &planpb.GeneratedCol{Expr: expr, OriginString: "crc32(crc_input)", IsStored: true}}, rowID)
	table.Name2ColIndex = make(map[string]int32, len(table.Cols))
	for i, col := range table.Cols {
		table.Name2ColIndex[col.Name] = int32(i)
	}
}

// Exercise the public SQL planner with catalog-owned old overloads. The
// controls establish which SQL shapes select the legacy planner.
func TestCRC32LegacyGeneratedFallbackRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, table, sql string
		modern           bool
	}{
		{"modern_load", "self_ref", "load data inline format='csv', data='1,1,x' into table self_ref fields terminated by ',' (id,parent_id,name)", true},
		{"self_fk_load", "self_ref", "load data inline format='csv', data='1,1,x' into table self_ref fields terminated by ',' (id,parent_id,name)", false},
		{"no_key_odku", "insert_fk_no_key_c", "insert into constraint_test.insert_fk_no_key_c(id,pid) values (1,1) on duplicate key update pid=values(pid)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := mock.ctxt.tables[tc.table]
			require.NotNil(t, table)
			if tc.modern {
				table.Fkeys = nil
			}
			control, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			var modern, legacy bool
			for _, node := range control.GetQuery().Nodes {
				modern = modern || node.NodeType == planpb.Node_MULTI_UPDATE
				legacy = legacy || node.NodeType == planpb.Node_INSERT
			}
			if tc.modern {
				require.True(t, modern)
			} else {
				require.True(t, legacy)
			}

			addLegacyCRC32GeneratedFixture(t, table)
			sql := tc.sql
			if tc.modern {
				// Supply a real JSON input so the generated expression is not
				// reduced to a constant NULL by the optimizer.
				sql = "load data inline format='csv', data='1,1,x,123' into table self_ref fields terminated by ',' (id,parent_id,name,crc_input)"
			}
			got, err := runOneStmt(mock, t, sql)
			if !tc.modern {
				require.Nil(t, got)
				require.ErrorContains(t, err, "legacy DML planner cannot write a legacy CRC32 JSON generated column")
				return
			}
			require.NoError(t, err)
			found := false
			for _, node := range got.GetQuery().Nodes {
				for _, e := range node.ProjectList {
					require.NoError(t, planpb.VisitExprTree(e, func(expr *planpb.Expr) error {
						f := expr.GetF()
						if f != nil && f.Func != nil && f.Func.Obj == function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload) && len(f.Args) > 0 && f.Args[0] != nil && f.Args[0].Typ.Id == int32(types.T_json) {
							found = true
						}
						return nil
					}))
				}
			}
			require.True(t, found, "modern LOAD must evaluate the catalog-bound overload")
		})
	}
}

func TestCRC32LegacyGeneratedFallbackForeignKeySetNull(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	table := mock.ctxt.tables["self_ref_cascade"]
	table.Fkeys[0].OnDelete = planpb.ForeignKeyDef_SET_NULL
	const sql = "delete from self_ref_cascade where id = 1"
	control, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	var recursiveInsert bool
	for _, node := range control.GetQuery().Nodes {
		if node.NodeType == planpb.Node_INSERT && node.InsertCtx != nil && node.InsertCtx.TableDef != nil && node.InsertCtx.TableDef.Name == "self_ref_cascade" {
			recursiveInsert = true
		}
	}
	require.True(t, recursiveInsert, "SET NULL must use the legacy update insert path")
	addLegacyCRC32GeneratedFixture(t, table)
	got, err := runOneStmt(mock, t, sql)
	require.Nil(t, got)
	require.ErrorContains(t, err, "legacy DML planner cannot write a legacy CRC32 JSON generated column")
}

func TestCRC32LegacyGeneratedFallbackIncludesVirtual(t *testing.T) {
	for _, stored := range []bool{false, true} {
		table := &planpb.TableDef{Cols: []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{
			Expr: legacyCRC32Expr(), IsStored: stored,
		}}}}
		require.ErrorContains(t, rejectLegacyCRC32GeneratedFallback(context.Background(), table),
			"legacy DML planner cannot write a legacy CRC32 JSON generated column")
	}
	scalar := legacyCRC32Expr()
	scalar.GetF().Args[0].Typ.Id = int32(types.T_varchar)
	table := &planpb.TableDef{Cols: []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: scalar}}}}
	require.NoError(t, rejectLegacyCRC32GeneratedFallback(context.Background(), table))
	textJSON := legacyCRC32Expr()
	textJSON.GetF().Func.Obj = function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload)
	table.Cols[0].GeneratedCol.Expr = textJSON
	require.NoError(t, rejectLegacyCRC32GeneratedFallback(context.Background(), table))
}
