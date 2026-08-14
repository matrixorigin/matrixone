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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestGenInsertMOIndexesSqlUsesRollingUpgradeSafeColumnList(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), ALLOCID_INDEX_KEY).Return(uint64(272510), nil).Times(1)

	proc := testutil.NewProc(t)
	const algoParams = `{"included_columns":"[\"title\",\"category\"]","lists":"2","op_type":"vector_l2_ops"}`
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
						IndexAlgoParams:    algoParams,
						IndexTableName:     "__mo_index_entries_idx_vec",
						TableExist:         true,
						Visible:            true,
						IncludedColumns:    []string{"title", "category"},
					},
				},
			},
		},
	}

	sql, err := genInsertMOIndexesSql(mockEngine, proc, "123456", 272464, ct, tableDef)
	require.NoError(t, err)
	header := strings.SplitN(sql, " values", 2)[0]
	require.Equal(t, "insert into mo_catalog.mo_indexes "+moIndexesColumnList, header)
	require.NotContains(t, header, catalog.IncludedColumns)
	require.Contains(t, sql, sqlquote.String(algoParams))
	require.Contains(t, sql, sqlquote.String(algoParams)+", 1, 0, ")
	require.Contains(t, sql, "'__mo_index_entries_idx_vec')")
}

func TestGenInsertMOIndexesSqlEscapesStringValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), ALLOCID_INDEX_KEY).Return(uint64(272511), nil).Times(1)

	proc := testutil.NewProc(t)
	comment := "index's comment\\with unicode 维度"
	tableDef := &plan.TableDef{
		Name2ColIndex: map[string]int32{"note": 0},
		Cols:          []*plan.ColDef{{Name: "note", OriginName: "note"}},
	}
	ct := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.IndexDef{Indexes: []*plan.IndexDef{{
				IndexName:       "idx_note",
				Parts:           []string{"note"},
				Comment:         comment,
				IndexAlgo:       catalog.MoIndexBTreeAlgo.ToString(),
				IndexAlgoParams: "{}",
			}}},
		},
	}

	sql, err := genInsertMOIndexesSql(mockEngine, proc, "123456", 272465, ct, tableDef)
	require.NoError(t, err)
	statements, err := mysql.Parse(proc.Ctx, sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	insert, ok := statements[0].(*tree.Insert)
	require.True(t, ok)
	values, ok := insert.Rows.Select.(*tree.ValuesClause)
	require.True(t, ok)
	require.Len(t, values.Rows, 1)
	require.Len(t, values.Rows[0], 15)
	commentValue, ok := values.Rows[0][10].(*tree.NumVal)
	require.True(t, ok)
	require.Equal(t, comment, commentValue.String())
}

func TestGenInsertMOIndexesSqlPersistsInvisibleIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), ALLOCID_INDEX_KEY).Return(uint64(272511), nil)

	proc := testutil.NewProc(t)
	tableDef := &plan.TableDef{
		Name2ColIndex: map[string]int32{"a": 0},
		Cols:          []*plan.ColDef{{Name: "a", OriginName: "a"}},
	}
	ct := &engine.ConstraintDef{Cts: []engine.Constraint{&engine.IndexDef{
		Indexes: []*plan.IndexDef{{
			IndexName: "idx_a",
			Parts:     []string{"a"},
			Visible:   false,
		}},
	}}}

	sql, err := genInsertMOIndexesSql(mockEngine, proc, "123456", 272464, ct, tableDef)
	require.NoError(t, err)
	require.Contains(t, sql, "'', '', '', 0, 0, ''")
}
