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

package iscp

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestIndexInitSQLFencesSourceAndMatchesLiveGroup(t *testing.T) {
	for _, tc := range []struct {
		name        string
		expected    uint64
		index       string
		function    string
		duplicate   bool
		wantErrText string
	}{
		{name: "matching", expected: 10, index: "idx", function: "ivf_create"},
		{name: "replaced source", expected: 99, index: "idx", function: "ivf_create", wantErrText: "source identity changed"},
		{name: "missing index", expected: 10, index: "missing", function: "ivf_create", wantErrText: "plugin is unavailable"},
		{name: "wrong case", expected: 10, index: "IDX", function: "ivf_create", wantErrText: "plugin is unavailable"},
		{name: "duplicate role", expected: 10, index: "idx", function: "ivf_create", duplicate: true, wantErrText: "duplicate index initialization role"},
		{name: "wrong function owner", expected: 10, index: "idx", function: "hnsw_create", wantErrText: "function does not match its owner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			ctx := defines.AttachAccountId(context.Background(), 0)
			eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil).AnyTimes()
			db.EXPECT().GetDatabaseId(gomock.Any()).Return("8").AnyTimes()
			defs := []*plan.IndexDef{
				{IndexName: "idx", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata, IndexTableName: "meta"},
				{IndexName: "idx", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids, IndexTableName: "cent"},
				{IndexName: "idx", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries, IndexTableName: "entries"},
			}
			if tc.duplicate {
				defs = append(defs, defs[0])
			}
			db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, name string, _ any) (engine.Relation, error) {
				rel := mock_frontend.NewMockRelation(ctrl)
				ids := map[string]uint64{"source": 10, "meta": 11, "cent": 12, "entries": 13}
				rel.EXPECT().GetTableName().Return(name).AnyTimes()
				rel.EXPECT().GetTableID(gomock.Any()).Return(ids[name]).AnyTimes()
				kind := catalog.SystemOrdinaryRel
				if name == "meta" {
					kind = catalog.SystemSI_IVFFLAT_TblType_Metadata
				}
				if name == "cent" {
					kind = catalog.SystemSI_IVFFLAT_TblType_Centroids
				}
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{Name: name, TableType: kind}).AnyTimes()
				rel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{Name: name, TableType: kind, Indexes: defs}).AnyTimes()
				rel.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{ParentTableID: 10}).AnyTimes()
				return rel, nil
			}).AnyTimes()
			sql := `select * from ` + tc.function + `('{}','{"db":"db","src":"source","index":"cent","metadata":"meta"}') f`
			got, err := resolveInitSQLMaintenance(ctx, eng, nil, "db", "source", tc.index, []uint64{tc.expected}, []string{sql})
			if tc.wantErrText != "" {
				require.ErrorContains(t, err, tc.wantErrText)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(10), got.spec.BaseID)
			require.Len(t, got.spec.Targets, 2, "entries are not consumed by ivf_create")
			require.NoError(t, got.validator.ValidateIndexMaintenance("ivf_create", []string{"{}", `{"db":"db","src":"source","index":"cent","metadata":"meta"}`}, got.spec))
		})
	}
}

func TestIndexInitSQLLeavesLegacyStatementsUnattested(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 42)
	for _, sql := range []string{"select 1", "insert into db.cent select (select 1), 1, null"} {
		got, err := resolveInitSQLMaintenance(ctx, nil, nil, "db", "source", "idx", nil, []string{sql})
		require.NoError(t, err)
		require.Nil(t, got)
	}
}

func TestIndexInitSQLRequiresFencedSourceForMaintenance(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 42)
	sql := `select * from ivf_create('{}','{"db":"db"}') f`
	for _, expected := range [][]uint64{nil, {0}, {10, 11}} {
		got, err := resolveInitSQLMaintenance(ctx, nil, nil, "db", "source", "idx", expected, []string{sql})
		require.ErrorContains(t, err, "source identity is unavailable")
		require.Nil(t, got)
	}
}
