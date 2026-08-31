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

package compile

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestGenInsertMOIndexesSqlAlwaysEmitsOptionsValue(t *testing.T) {
	tests := []struct {
		name     string
		option   *planpb.IndexOption
		wantTail string
	}{
		{
			name:     "nil option",
			wantTail: ", 1, null, '__mo_index_secondary_test');",
		},
		{
			name:     "option without parser",
			option:   &planpb.IndexOption{CreateExtraTable: true},
			wantTail: ", 1, null, '__mo_index_secondary_test');",
		},
		{
			name:     "gojieba parser",
			option:   &planpb.IndexOption{ParserName: "gojieba", NgramTokenSize: 3},
			wantTail: ", 1, 'parser=gojieba,ngram_token_size=3', '__mo_index_secondary_test');",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockEngine := mock_frontend.NewMockEngine(ctrl)
			mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), ALLOCID_INDEX_KEY).Return(uint64(1), nil)

			proc := testutil.NewProcess(t)
			tableDef := &planpb.TableDef{
				Name2ColIndex: map[string]int32{"body": 0},
				Cols:          []*planpb.ColDef{{Name: "body", OriginName: "body"}},
			}
			ct := &engine.ConstraintDef{Cts: []engine.Constraint{
				&engine.IndexDef{Indexes: []*planpb.IndexDef{{
					IndexName:          "ft_biz_name",
					Parts:              []string{"body"},
					IndexAlgo:          catalog.MOIndexFullTextAlgo.ToString(),
					IndexAlgoParams:    `{"parser":"gojieba"}`,
					IndexTableName:     "__mo_index_secondary_test",
					IndexAlgoTableType: "",
					TableExist:         true,
					Option:             tt.option,
				}}},
			}}

			sql, err := genInsertMOIndexesSql(mockEngine, proc, "123", 456, ct, tableDef)
			require.NoError(t, err)
			require.Equal(t, "insert into mo_catalog.mo_indexes values(1, 456, 123, 'ft_biz_name', 'FULLTEXT', 'fulltext', '', '{\"parser\":\"gojieba\"}', 1, 0, '', 'body'"+tt.wantTail, sql)
		})
	}
}
