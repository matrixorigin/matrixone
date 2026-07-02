// Copyright 2024 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func makeIvfIncludeAlterIndexDef(indexName string, includedColumns []string) *planpb.IndexDef {
	return &planpb.IndexDef{
		IndexName:       indexName,
		IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
		Parts:           []string{"embedding"},
		IncludedColumns: append([]string(nil), includedColumns...),
	}
}

func makeIvfIncludeAlterIndexDefForTable(indexName, tableType, tableName string, includedColumns []string) *planpb.IndexDef {
	indexDef := makeIvfIncludeAlterIndexDef(indexName, includedColumns)
	indexDef.IndexAlgoTableType = tableType
	indexDef.IndexTableName = tableName
	return indexDef
}

func TestHandleDropColumnWithIndexRemovesIvfIndexForIncludeColumn(t *testing.T) {
	def := TableDef{
		Indexes: []*IndexDef{
			makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "category"}),
			makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "category"}),
			makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "category"}),
		},
	}

	err := handleDropColumnWithIndex(context.Background(), "title", &def)
	require.NoError(t, err)
	require.Empty(t, def.Indexes)
}

func TestUpdateRenameColumnInTableDefRenamesIvfIncludeMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := &planpb.TableDef{
		TblId:  42,
		DbName: "db1",
		Cols: []*ColDef{
			{Name: "id", OriginName: "id"},
			{Name: "title", OriginName: "title"},
			{Name: "category", OriginName: "category"},
		},
		Pkey: &PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Indexes: []*planpb.IndexDef{
			makeIvfIncludeAlterIndexDefForTable("idx_ivf", catalog.SystemSI_IVFFLAT_TblType_Metadata, "meta_tbl", []string{"title", "category"}),
			makeIvfIncludeAlterIndexDefForTable("idx_ivf", catalog.SystemSI_IVFFLAT_TblType_Centroids, "centroids_tbl", []string{"title", "category"}),
			makeIvfIncludeAlterIndexDefForTable("idx_ivf", catalog.SystemSI_IVFFLAT_TblType_Entries, "entries_tbl", []string{"title", "category"}),
		},
	}

	sqls, err := updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[1],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("headline"),
		},
	)
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], `set included_columns = '["headline","category"]'`)
	require.Contains(t, sqls[0], "name = 'idx_ivf'")

	for _, indexDef := range tableDef.Indexes {
		require.Equal(t, []string{"headline", "category"}, indexDef.IncludedColumns)
		require.NotContains(t, indexDef.IndexAlgoParams, "include_columns")
	}
	require.Equal(t, "headline", tableDef.Cols[1].Name)
	require.Equal(t, "headline", tableDef.Cols[1].OriginName)
}

func TestRenameIncludedColumnsForAlgoSyncsAlgoParams(t *testing.T) {
	tableDef := &planpb.TableDef{
		TblId: 42,
		Indexes: []*planpb.IndexDef{
			{
				IndexName:       "idx_gpu",
				IndexAlgo:       catalog.MoIndexCagraAlgo.ToString(),
				IndexAlgoParams: `{"op_type":"vector_l2_ops","include_columns":"title,category"}`,
				IncludedColumns: []string{"title", "category"},
			},
			{
				IndexName:       "idx_gpu",
				IndexAlgo:       catalog.MoIndexCagraAlgo.ToString(),
				IndexAlgoParams: `{"op_type":"vector_l2_ops","include_columns":"title,category"}`,
				IncludedColumns: []string{"title", "category"},
			},
		},
	}

	sqls, err := renameIncludedColumnsForAlgo(tableDef, catalog.MoIndexCagraAlgo.ToString(), "title", "headline", true)
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], `included_columns = '["headline","category"]'`)
	require.Contains(t, sqls[0], `algo_params = '{"included_columns":"headline,category","op_type":"vector_l2_ops"}'`)

	for _, indexDef := range tableDef.Indexes {
		require.Equal(t, []string{"headline", "category"}, indexDef.IncludedColumns)
		params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
		require.NoError(t, err)
		require.Equal(t, "headline,category", params[catalog.IncludedColumns])
		require.NotContains(t, params, "include_columns")
	}
}

func TestResolveAlterTableAlgorithmCopiesIvfIncludeRename(t *testing.T) {
	tableDef := &planpb.TableDef{
		Indexes: []*planpb.IndexDef{
			makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "category"}),
		},
	}

	algorithm, err := ResolveAlterTableAlgorithm(
		context.Background(),
		[]tree.AlterTableOption{
			&tree.AlterTableRenameColumnClause{
				OldColumnName: tree.NewUnresolvedColName("title"),
				NewColumnName: tree.NewUnresolvedColName("headline"),
			},
		},
		tableDef,
	)
	require.NoError(t, err)
	require.Equal(t, planpb.AlterTable_COPY, algorithm)

	algorithm, err = ResolveAlterTableAlgorithm(
		context.Background(),
		[]tree.AlterTableOption{
			&tree.AlterTableRenameColumnClause{
				OldColumnName: tree.NewUnresolvedColName("note"),
				NewColumnName: tree.NewUnresolvedColName("memo"),
			},
		},
		tableDef,
	)
	require.NoError(t, err)
	require.Equal(t, planpb.AlterTable_INPLACE, algorithm)
}

func TestCollectAffectedIndexNamesForAlterIncludesIvfIncludeColumns(t *testing.T) {
	indexes := []*planpb.IndexDef{
		makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "category"}),
		makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "category"}),
		{
			IndexName: "idx_note",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{"note"},
		},
	}

	names, err := collectAffectedIndexNamesForAlter(indexes, []string{"title", "note"})
	require.NoError(t, err)
	require.Equal(t, []string{"idx_ivf", "idx_note"}, names)
}
