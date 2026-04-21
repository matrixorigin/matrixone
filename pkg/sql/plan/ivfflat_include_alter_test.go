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

func TestHandleDropColumnWithIndexRemovesIvfIndexForIncludeColumn(t *testing.T) {
	def := TableDef{
		Indexes: []*IndexDef{
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
				Parts:           []string{"embedding"},
			},
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
				Parts:           []string{"embedding"},
			},
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
				Parts:           []string{"embedding"},
			},
		},
	}

	err := handleDropColumnWithIndex(context.Background(), "title", &def)
	require.NoError(t, err)
	require.Empty(t, def.Indexes)
}

func TestUpdateRenameColumnInTableDefRenamesIvfIncludeMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := &planpb.TableDef{
		TblId: 42,
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
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
				Parts:           []string{"embedding"},
			},
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
				Parts:           []string{"embedding"},
			},
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
				Parts:           []string{"embedding"},
			},
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
	require.Contains(t, sqls[0], `set algo_params = '{"include_columns":"[\\"headline\\",\\"category\\"]","lists":"2","op_type":"vector_l2_ops"}'`)
	require.Contains(t, sqls[0], "name = 'idx_ivf'")

	for _, indexDef := range tableDef.Indexes {
		require.Contains(t, indexDef.IndexAlgoParams, `"include_columns":"[\"headline\",\"category\"]"`)
	}
	require.Equal(t, "headline", tableDef.Cols[1].Name)
	require.Equal(t, "headline", tableDef.Cols[1].OriginName)
}

func TestCollectAffectedIndexNamesForAlterIncludesIvfIncludeColumns(t *testing.T) {
	indexes := []*planpb.IndexDef{
		{
			IndexName:       "idx_ivf",
			IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
			Parts:           []string{"embedding"},
		},
		{
			IndexName:       "idx_ivf",
			IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops","include_columns":"[\"title\",\"category\"]"}`,
			Parts:           []string{"embedding"},
		},
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
