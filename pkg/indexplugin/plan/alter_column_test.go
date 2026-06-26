// Copyright 2026 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

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

	sqls, err := RenameIncludedColumnsForAlgo(tableDef, catalog.MoIndexCagraAlgo.ToString(), "title", "headline", true)
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
