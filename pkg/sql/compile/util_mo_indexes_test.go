// Copyright 2021 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestGenInsertMOIndexesSqlIncludesIncludedColumns(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), ALLOCID_INDEX_KEY).Return(uint64(272510), nil).Times(1)

	proc := testutil.NewProc(t)
	tableDef := &plan.TableDef{
		Name2ColIndex: map[string]int32{"embedding": 0},
		Cols: []*plan.ColDef{
			{Name: "embedding", OriginName: "embedding"},
		},
	}
	ct := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.IndexDef{
				Indexes: []*plan.IndexDef{
					{
						IndexName:          "idx_vec",
						Parts:              []string{"embedding"},
						IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
						IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
						IndexAlgoParams:    `{"lists":"2","op_type":"vector_l2_ops"}`,
						IndexTableName:     "__mo_index_entries_idx_vec",
						TableExist:         true,
						IncludedColumns:    []string{"title", "category"},
					},
				},
			},
		},
	}

	sql, err := genInsertMOIndexesSql(mockEngine, proc, "123456", 272464, ct, tableDef)
	require.NoError(t, err)
	require.Contains(t, sql, `'__mo_index_entries_idx_vec', '["title","category"]')`)
}
