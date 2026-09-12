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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestOriginalAlterSourceColumnUsesCurrentLineage(t *testing.T) {
	original := generatedDependencyTestTable()
	copyTable := &planpb.TableDef{Cols: make([]*planpb.ColDef, len(original.Cols))}
	for i, col := range original.Cols {
		colCopy := *col
		copyTable.Cols[i] = &colCopy
	}
	alterCtx := initAlterTableContext(original, copyTable, "test")

	copyTable.Cols[0].Name = "renamed_source"
	alterCtx.renameColumnSource("source", "renamed_source")
	source, ok, err := originalAlterSourceColumn(
		context.Background(), original, copyTable, alterCtx, "renamed_source",
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "source", source,
		"a renamed target must retain its original copy-source lineage")

	copyTable.Cols = append(copyTable.Cols, &planpb.ColDef{Name: "added"})
	alterCtx.alterColMap["added"] = selectExpr{sexprType: exprConstValue, sexprStr: "0"}
	source, ok, err = originalAlterSourceColumn(
		context.Background(), original, copyTable, alterCtx, "added",
	)
	require.NoError(t, err)
	require.False(t, ok, "constant-populated columns must not seed original dependencies")
	require.Empty(t, source)

	delete(alterCtx.alterColMap, "added")
	_, ok, err = originalAlterSourceColumn(
		context.Background(), original, copyTable, alterCtx, "added",
	)
	require.NoError(t, err)
	require.False(t, ok, "an absent mapping must not be inferred from a matching name")

	alterCtx.alterColMap["renamed_source"] = selectExpr{
		sexprType: exprColumnName,
		sexprStr:  "missing_original",
	}
	_, _, err = originalAlterSourceColumn(
		context.Background(), original, copyTable, alterCtx, "renamed_source",
	)
	require.ErrorContains(t, err, "cannot resolve original source column")
}

func TestAppendAlterGeneratedDependentsRebuildsChainedIndexes(t *testing.T) {
	tableDef := generatedDependencyTestTable()
	tableDef.Indexes = []*planpb.IndexDef{
		{
			IndexName: "idx_middle",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{catalog.CreateAlias("middle")},
		},
		{
			IndexName: "idx_tail",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{catalog.CreateAlias("tail")},
		},
		{
			IndexName: "idx_other",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{catalog.CreateAlias("other")},
		},
	}

	affectedCols, primaryKeyAffected, err := appendAlterGeneratedDependents(
		context.Background(), tableDef, []string{"source"}, map[string]struct{}{"source": {}},
	)
	require.NoError(t, err)
	require.False(t, primaryKeyAffected)
	require.Equal(t, []string{"source", "middle", "tail"}, affectedCols)

	affectedIndexes, err := collectAffectedIndexNamesForAlter(tableDef.Indexes, affectedCols)
	require.NoError(t, err)
	require.Equal(t, []string{"idx_middle", "idx_tail"}, affectedIndexes)
	require.NotContains(t, affectedIndexes, "idx_other",
		"unrelated indexes must remain eligible for COPY cloning")
}

func TestAppendAlterGeneratedDependentsSkipsTablesWithoutGeneratedColumns(t *testing.T) {
	tableDef := &planpb.TableDef{
		// Legacy table metadata can omit Name2ColIndex. With no generated
		// expressions there is no dependency graph to resolve.
		Cols: []*planpb.ColDef{{Name: "b"}},
	}
	affectedCols, primaryKeyAffected, err := appendAlterGeneratedDependents(
		context.Background(), tableDef, []string{"b"}, map[string]struct{}{"b": {}},
	)
	require.NoError(t, err)
	require.False(t, primaryKeyAffected)
	require.Equal(t, []string{"b"}, affectedCols)
}

func TestAlterCopyAffectedStoredGeneratedColumnsUsesFinalSourceTypes(t *testing.T) {
	original := generatedDependencyTestTable()
	for i, col := range original.Cols {
		col.ColId = uint64(i + 1)
		if col.GeneratedCol != nil {
			col.GeneratedCol.IsStored = true
		}
	}

	virtual := &planpb.ColDef{
		ColId: 7,
		Name:  "virtual",
		Typ:   planpb.Type{Id: int32(types.T_int64)},
		GeneratedCol: &planpb.GeneratedCol{
			Expr: generatedColumnRefExpr(planpb.Type{Id: int32(types.T_int64)}, 0, "source"),
		},
	}
	original.Cols = append(original.Cols, virtual)
	original.Name2ColIndex[virtual.Name] = int32(len(original.Cols) - 1)

	copyTable := &planpb.TableDef{
		Cols:          make([]*planpb.ColDef, len(original.Cols)),
		Name2ColIndex: original.Name2ColIndex,
	}
	changeColDefMap := make(map[uint64]*planpb.ColDef, len(original.Cols))
	for i, col := range original.Cols {
		copyCol := *col
		copyTable.Cols[i] = &copyCol
		changeColDefMap[col.ColId] = &planpb.ColDef{Name: col.Name}
	}
	copyTable.Cols[0].Typ = planpb.Type{Id: int32(types.T_int32)}

	affected, err := AlterCopyAffectedStoredGeneratedColumns(
		context.Background(), original, copyTable, changeColDefMap,
	)
	require.NoError(t, err)
	require.Equal(t, map[uint64]string{
		original.Cols[1].ColId: "middle",
		original.Cols[2].ColId: "tail",
	}, affected,
		"a converted source must include the full transitive stored-generated closure, but exclude unrelated and virtual generated columns")
}

func addAlterTestIndex(t *testing.T, mock *MockOptimizer, base *planpb.TableDef, indexName, columnName string, unique bool) {
	t.Helper()
	if base.Name2ColIndex == nil {
		base.Name2ColIndex = make(map[string]int32, len(base.Cols))
	}
	for i, col := range base.Cols {
		base.Name2ColIndex[col.Name] = int32(i)
	}
	indexTableName := catalog.SecondaryIndexTableNamePrefix + "alter-generated-" + indexName
	base.Indexes = append(base.Indexes, &planpb.IndexDef{
		IndexName:      indexName,
		IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
		Parts:          []string{columnName},
		Unique:         unique,
		IndexTableName: indexTableName,
		TableExist:     true,
	})
	registerMockGeneratedIndexTable(t, mock, base, indexTableName, base.Cols[mockTableColPos(t, base, columnName)])
	registerAlterIndexVisibilityRows(t, mock, base)
}

func registerAlterIndexVisibilityRows(t *testing.T, mock *MockOptimizer, base *planpb.TableDef) {
	t.Helper()
	proc := mock.ctxt.GetProcess()
	require.NotNil(t, mock.ctxt.processHolder)
	mock.ctxt.processHolder.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		require.Equal(t, fmt.Sprintf(
			"SELECT name, is_visible FROM mo_catalog.mo_indexes WHERE table_id = %d",
			base.TblId,
		), sql)
		result := executor.NewMemResult(
			[]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, proc.Mp(),
		)
		result.NewBatchWithRowCount(len(base.Indexes))
		names := make([]string, len(base.Indexes))
		visible := make([]int8, len(base.Indexes))
		for i, index := range base.Indexes {
			names[i] = index.IndexName
			visible[i] = 1
		}
		require.NoError(t, executor.AppendStringRows(result, 0, names))
		require.NoError(t, executor.AppendFixedRows(result, 1, visible))
		return result.GetResult(), nil
	})
}

func newGeneratedIndexAlterMock(t *testing.T) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(false)
	configureMockGeneratedIndex(t, mock, true)
	base := mock.ctxt.tables["t_on_update_gen"]
	addAlterTestIndex(t, mock, base, "idx_val", "val", false)
	addAlterTestIndex(t, mock, base, "idx_unaffected", "id", false)
	return mock
}

func TestAlterModifyRebuildsOnlyDirectAndGeneratedDependentIndexes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "modify",
			sql:  "alter table constraint_test.t_on_update_gen modify column val bigint",
		},
		{
			name: "same-name change",
			sql:  "alter table constraint_test.t_on_update_gen change column val val bigint",
		},
		{
			name: "reorder then modify",
			sql:  "alter table constraint_test.t_on_update_gen modify column val int after updated_at, modify column val bigint",
		},
		{
			name: "rename unrelated then modify",
			sql:  "alter table constraint_test.t_on_update_gen change column updated_at changed_at timestamp, modify column val bigint",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newGeneratedIndexAlterMock(t)
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)

			alter := logicPlan.GetDdl().GetAlterTable()
			require.NotNil(t, alter)
			require.NotNil(t, alter.Options)
			require.Contains(t, strings.ToLower(alter.CreateTmpTableSql), "generated always as (val) stored",
				"the COPY DDL must serialize and rebind the generated expression")
			require.False(t, alter.Options.SkipIndexesCopy["idx_val"],
				"an index on the directly modified source column must be rebuilt")
			require.False(t, alter.Options.SkipIndexesCopy["idx_generated_g"],
				"an index on a dependent generated column must be rebuilt")
			require.True(t, alter.Options.SkipIndexesCopy["idx_unaffected"],
				"an unrelated index must remain cloneable")
			require.False(t, alter.Options.SkipUniqueIdxDedup["idx_generated_g"],
				"COPY must retain dedup for a generated unique key")

			if tc.name == "reorder then modify" {
				copyTable := alter.GetCopyTableDef()
				valPos := mockTableColPos(t, mock.ctxt.tables["t_on_update_gen"], "val")
				val := FindColumn(copyTable.Cols, "val")
				g := FindColumn(copyTable.Cols, "g")
				require.NotNil(t, val)
				require.NotNil(t, g)
				require.NotEqual(t, valPos, mockTableColPos(t, copyTable, "val"))
				require.Equal(t, int32(mockTableColPos(t, copyTable, "val")), g.GeneratedCol.Expr.GetCol().ColPos,
					"the generated expression must reference the source's final position")
			}
		})
	}
}

func TestAlterAddThenModifyDoesNotSeedOriginalGeneratedDependencies(t *testing.T) {
	mock := newGeneratedIndexAlterMock(t)
	logicPlan, err := runOneStmt(mock, t,
		"alter table constraint_test.t_on_update_gen add column added int not null default 0, modify column added bigint")
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.True(t, alter.Options.SkipIndexesCopy["idx_generated_g"],
		"the added column has no source value that could change the original generated key")
}

func TestAlterSourceAffectingGeneratedPrimaryKeyRebuildsAllIndexes(t *testing.T) {
	mock := NewMockOptimizer(false)
	configureMockGeneratedPrimaryKey(t, mock)
	base := mock.ctxt.tables["t_on_update_gen"]
	addAlterTestIndex(t, mock, base, "idx_pk_payload", "updated_at", true)

	logicPlan, err := runOneStmt(mock, t,
		"alter table constraint_test.t_on_update_gen modify column val bigint")
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.Contains(t, strings.ToLower(alter.CreateTmpTableSql), "generated always as (val) stored",
		"the generated primary-key expression must survive temporary-table serialization")
	require.False(t, alter.Options.SkipIndexesCopy["idx_pk_payload"],
		"secondary index entries carry the primary key and must be rebuilt when its generated value can change")
}
