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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func generatedColumnRefExpr(typ planpb.Type, pos int32, name string) *planpb.Expr {
	return &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: pos,
				Name:   name,
			},
		},
	}
}

func generatedDependencyTestTable() *planpb.TableDef {
	typ := planpb.Type{Id: int32(types.T_int64)}
	cols := []*planpb.ColDef{
		{Name: "source", Typ: typ},
		{Name: "middle", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: generatedColumnRefExpr(typ, 0, "source")}},
		{Name: "tail", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: generatedColumnRefExpr(typ, 1, "middle")}},
		{Name: "late_generated", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: generatedColumnRefExpr(typ, 4, "late_source")}},
		{Name: "late_source", Typ: typ},
		{Name: "other", Typ: typ},
	}
	name2ColIndex := make(map[string]int32, len(cols))
	for i, col := range cols {
		name2ColIndex[col.Name] = int32(i)
	}
	return &planpb.TableDef{Cols: cols, Name2ColIndex: name2ColIndex}
}

func TestCollectGeneratedColumnDependents(t *testing.T) {
	tableDef := generatedDependencyTestTable()

	possiblyChanged, err := collectGeneratedColumnDependents(
		context.Background(), tableDef, map[string]struct{}{"source": {}},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		"source": {},
		"middle": {},
		"tail":   {},
	}, possiblyChanged)
	require.False(t, columnPossiblyChanged(tableDef, possiblyChanged, "other"))
	require.True(t, columnPossiblyChanged(tableDef, possiblyChanged, catalog.CreateAlias("tail")))
	require.False(t, columnPossiblyChanged(tableDef, possiblyChanged, "late_generated"))

	possiblyChanged, err = collectGeneratedColumnDependents(
		context.Background(), tableDef, map[string]struct{}{"late_source": {}},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		"late_source":    {},
		"late_generated": {},
	}, possiblyChanged)

	possiblyChanged, err = collectGeneratedColumnDependents(
		context.Background(), tableDef, map[string]struct{}{"other": {}},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"other": {}}, possiblyChanged)
}

func TestCollectGeneratedColumnDependentsRejectsInvalidColPos(t *testing.T) {
	for _, tc := range []struct {
		name string
		pos  int32
	}{
		{name: "negative", pos: -1},
		{name: "out of range", pos: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tableDef := generatedDependencyTestTable()
			tableDef.Cols[1].GeneratedCol.Expr = generatedColumnRefExpr(
				tableDef.Cols[0].Typ, tc.pos, "source",
			)
			_, err := collectGeneratedColumnDependents(
				context.Background(), tableDef, map[string]struct{}{"source": {}},
			)
			require.ErrorContains(t, err, "invalid generated column reference position")
		})
	}
}

func registerMockGeneratedIndexTable(t *testing.T, mock *MockOptimizer, base *planpb.TableDef, indexTableName string, keyCol *planpb.ColDef) {
	t.Helper()
	primaryCol := base.Cols[mockTableColPos(t, base, base.Pkey.PkeyColName)]
	rowIDCol := base.Cols[mockTableColPos(t, base, catalog.Row_ID)]
	indexKey := &planpb.ColDef{
		Name:    catalog.IndexTableIndexColName,
		Typ:     keyCol.Typ,
		Default: &planpb.Default{NullAbility: true},
	}
	indexPrimary := &planpb.ColDef{
		Name:    catalog.IndexTablePrimaryColName,
		Typ:     primaryCol.Typ,
		Default: &planpb.Default{NullAbility: true},
	}
	indexRowID := &planpb.ColDef{
		Name:    catalog.Row_ID,
		Typ:     rowIDCol.Typ,
		Default: &planpb.Default{NullAbility: true},
	}
	hidden := &planpb.TableDef{
		Name:      indexTableName,
		TblId:     99051,
		TableType: catalog.SystemIndexRel,
		Cols:      []*planpb.ColDef{indexKey, indexPrimary, indexRowID},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: catalog.IndexTableIndexColName,
			Names:       []string{catalog.IndexTableIndexColName},
			Cols:        []uint64{0},
			CompPkeyCol: indexKey,
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
			catalog.Row_ID:                   2,
		},
	}
	objRef := &planpb.ObjectRef{SchemaName: "constraint_test", ObjName: indexTableName}
	mock.ctxt.tables[indexTableName] = hidden
	mock.ctxt.objects[indexTableName] = objRef
}

func mockTableColPos(t *testing.T, tableDef *planpb.TableDef, name string) int32 {
	t.Helper()
	for i, col := range tableDef.Cols {
		if col.Name == name {
			return int32(i)
		}
	}
	t.Fatalf("column %q not found in table %q", name, tableDef.Name)
	return -1
}

func TestCollectGeneratedColumnDependentsIncludesOnUpdateDependents(t *testing.T) {
	mock := NewMockOptimizer(true)
	base := mock.ctxt.tables["t_on_update_gen"]
	base.Name2ColIndex = make(map[string]int32, len(base.Cols))
	for i, col := range base.Cols {
		base.Name2ColIndex[col.Name] = int32(i)
	}

	possiblyChanged, err := collectGeneratedColumnDependents(
		context.Background(), base, map[string]struct{}{"updated_at": {}},
	)
	require.NoError(t, err)
	require.Contains(t, possiblyChanged, "updated_at")
	require.Contains(t, possiblyChanged, "g")
}

func configureMockGeneratedIndex(t *testing.T, mock *MockOptimizer, unique bool) string {
	t.Helper()
	base := mock.ctxt.tables["t_on_update_gen"]
	require.NotNil(t, base)
	sourcePos := mockTableColPos(t, base, "val")
	generatedPos := mockTableColPos(t, base, "g")
	sourceCol := base.Cols[sourcePos]
	generatedCol := base.Cols[generatedPos]
	generatedCol.Typ = sourceCol.Typ
	generatedCol.GeneratedCol = &planpb.GeneratedCol{
		Expr:     generatedColumnRefExpr(sourceCol.Typ, sourcePos, sourceCol.Name),
		IsStored: true,
	}
	// Keep this fixture focused on explicit ODKU dependencies rather than the
	// separate ON UPDATE no-op behavior covered by bind_upsert_affect_rows_test.go.
	base.Cols[mockTableColPos(t, base, "updated_at")].OnUpdate = nil

	indexTableName := catalog.SecondaryIndexTableNamePrefix + "odku-generated-g"
	base.Indexes = []*planpb.IndexDef{{
		IndexName:      "idx_generated_g",
		Parts:          []string{catalog.CreateAlias("g")},
		Unique:         unique,
		IndexTableName: indexTableName,
		TableExist:     true,
	}}
	registerMockGeneratedIndexTable(t, mock, base, indexTableName, generatedCol)
	return indexTableName
}

func configureMockGeneratedPrimaryKey(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	base := mock.ctxt.tables["t_on_update_gen"]
	require.NotNil(t, base)
	sourcePos := mockTableColPos(t, base, "val")
	idCol := base.Cols[mockTableColPos(t, base, "id")]
	idCol.GeneratedCol = &planpb.GeneratedCol{
		Expr:     generatedColumnRefExpr(idCol.Typ, sourcePos, "val"),
		IsStored: true,
	}
	base.Cols[mockTableColPos(t, base, "updated_at")].OnUpdate = nil
	base.Indexes = nil
}

func TestInsertOnDupGeneratedUniqueKeyRejected(t *testing.T) {
	mock := NewMockOptimizer(true)
	configureMockGeneratedIndex(t, mock, true)

	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t_on_update_gen (id, val, updated_at) values (1, 1, null) "+
			"on duplicate key update val = values(val)")
	require.ErrorContains(t, err, "unsupported DML: update unique key on duplicate")
}

func TestInsertOnDupUnrelatedColumnWithGeneratedUniqueKeySucceeds(t *testing.T) {
	mock := NewMockOptimizer(true)
	configureMockGeneratedIndex(t, mock, true)

	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t_on_update_gen (id, val, updated_at) values (1, 1, null) "+
			"on duplicate key update updated_at = values(updated_at)")
	require.NoError(t, err)
}

func TestInsertOnDupGeneratedPrimaryKeyRejected(t *testing.T) {
	mock := NewMockOptimizer(true)
	configureMockGeneratedPrimaryKey(t, mock)

	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t_on_update_gen (val, updated_at) values (1, null) "+
			"on duplicate key update val = values(val)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func findUpdateCtxByTableName(query *planpb.Query, tableName string) *planpb.UpdateCtx {
	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_MULTI_UPDATE {
			continue
		}
		for _, updateCtx := range node.UpdateCtxList {
			if updateCtx.TableDef != nil && updateCtx.TableDef.Name == tableName {
				return updateCtx
			}
		}
	}
	return nil
}

func hasTableScanForTable(query *planpb.Query, tableName string) bool {
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == tableName {
			return true
		}
	}
	return false
}

func TestInsertOnDupGeneratedNonUniqueIndexMaintainsOldAndNewKeys(t *testing.T) {
	mock := NewMockOptimizer(true)
	indexTableName := configureMockGeneratedIndex(t, mock, false)

	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t_on_update_gen (id, val, updated_at) values (1, 1, null) "+
			"on duplicate key update val = values(val)")
	require.NoError(t, err)

	indexUpdateCtx := findUpdateCtxByTableName(logicPlan.GetQuery(), indexTableName)
	require.NotNil(t, indexUpdateCtx)
	require.NotEmpty(t, indexUpdateCtx.InsertCols)
	require.Len(t, indexUpdateCtx.DeleteCols, 2)
	require.True(t, hasTableScanForTable(logicPlan.GetQuery(), indexTableName))
}

func TestInsertOnDupUnrelatedColumnSkipsGeneratedIndexDelete(t *testing.T) {
	mock := NewMockOptimizer(true)
	indexTableName := configureMockGeneratedIndex(t, mock, false)

	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t_on_update_gen (id, val, updated_at) values (1, 1, null) "+
			"on duplicate key update updated_at = values(updated_at)")
	require.NoError(t, err)

	indexUpdateCtx := findUpdateCtxByTableName(logicPlan.GetQuery(), indexTableName)
	require.NotNil(t, indexUpdateCtx)
	require.NotEmpty(t, indexUpdateCtx.InsertCols)
	require.Empty(t, indexUpdateCtx.DeleteCols)
	require.False(t, hasTableScanForTable(logicPlan.GetQuery(), indexTableName))
}
