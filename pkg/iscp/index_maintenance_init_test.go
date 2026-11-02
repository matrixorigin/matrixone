// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0. See the LICENSE file.

package iscp

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestIndexInitSQLFencesSourceAndMatchesLiveGroup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected uint64
		index    string
		wantErr  bool
	}{
		{"matching", 10, "idx", false},
		{"replaced source", 99, "idx", true},
		{"missing index", 10, "missing", true},
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
			sql := `select * from ivf_create('{}','{"db":"db","src":"source","index":"cent","metadata":"meta"}') f`
			got, err := resolveInitSQLMaintenance(ctx, eng, nil, "db", "source", tc.index, []uint64{tc.expected}, []string{sql})
			if tc.wantErr {
				require.Error(t, err)
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
