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

package frontend

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestTryBuildCurrentMoTablesCPKeyFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetRel := mock_frontend.NewMockRelation(ctrl)
	moTablesRel := mock_frontend.NewMockRelation(ctrl)

	targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "bench_db",
	}).Times(1)
	targetRel.EXPECT().GetTableName().Return("bench_table").Times(1)

	cpkeyName := strings.ToLower(catalog.SystemRelAttr_CPKey)
	moTablesRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name:   cpkeyName,
				Seqnum: uint32(catalog.MO_TABLES_CPKEY_IDX),
			},
		},
		Name2ColIndex: map[string]int32{
			cpkeyName: 0,
		},
	}).Times(1)

	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
		context.Background(),
		42,
		targetRel,
		moTablesRel,
		mp,
	)
	require.NoError(t, err)
	require.Empty(t, fallbackReason)
	require.NotNil(t, pkFilter)
	require.Equal(t, catalog.MO_TABLES_CPKEY_IDX, pkFilter.PrimarySeqnum)
	require.Len(t, pkFilter.Segments, 1)

	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(42)
	packer.EncodeStringType([]byte("bench_db"))
	packer.EncodeStringType([]byte("bench_table"))

	zm := index.ZM(pkFilter.Segments[0])
	require.Equal(t, packer.Bytes(), zm.GetMinBuf())
	require.Equal(t, packer.Bytes(), zm.GetMaxBuf())
}

func TestPackMoDatabaseCPKey(t *testing.T) {
	packedKey := packMoDatabaseCPKey(42, "bench_db")

	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(42)
	packer.EncodeStringType([]byte("bench_db"))

	require.Equal(t, packer.Bytes(), packedKey)
}

func TestDatabaseCreatedTimeToCollectLowerBound(t *testing.T) {
	createdTime := types.UnixMicroToTimestamp(1234567)

	lowerBound, err := databaseCreatedTimeToCollectLowerBound(createdTime)
	require.NoError(t, err)

	require.Equal(t, types.BuildTS(1234567*1000, 0).Prev(), lowerBound)
}

func TestTryBuildCurrentMoTablesCPKeyFilterMissingTargetDBNameFallsBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetRel := mock_frontend.NewMockRelation(ctrl)
	moTablesRel := mock_frontend.NewMockRelation(ctrl)
	targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(1)

	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
		context.Background(),
		1,
		targetRel,
		moTablesRel,
		mp,
	)
	require.NoError(t, err)
	require.Nil(t, pkFilter)
	require.Equal(t, "missing-target-db-name", fallbackReason)
}

func TestTryBuildCurrentMoTablesCPKeyFilterFallbackReasons(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("missing target relation", func(t *testing.T) {
		pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
			context.Background(),
			1,
			nil,
			nil,
			mp,
		)
		require.NoError(t, err)
		require.Nil(t, pkFilter)
		require.Equal(t, "missing-target-rel", fallbackReason)
	})

	t.Run("missing mo_tables relation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)

		pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
			context.Background(),
			1,
			targetRel,
			nil,
			mp,
		)
		require.NoError(t, err)
		require.Nil(t, pkFilter)
		require.Equal(t, "missing-mo-tables-rel", fallbackReason)
	})

	t.Run("missing target table def", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)
		moTablesRel := mock_frontend.NewMockRelation(ctrl)
		targetRel.EXPECT().GetTableDef(gomock.Any()).Return(nil).Times(1)

		pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
			context.Background(),
			1,
			targetRel,
			moTablesRel,
			mp,
		)
		require.NoError(t, err)
		require.Nil(t, pkFilter)
		require.Equal(t, "missing-target-table-def", fallbackReason)
	})

	t.Run("missing target table name", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)
		moTablesRel := mock_frontend.NewMockRelation(ctrl)
		targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "bench_db",
		}).Times(1)
		targetRel.EXPECT().GetTableName().Return("").Times(1)

		pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
			context.Background(),
			1,
			targetRel,
			moTablesRel,
			mp,
		)
		require.NoError(t, err)
		require.Nil(t, pkFilter)
		require.Equal(t, "missing-target-table-name", fallbackReason)
	})

	t.Run("missing mo_tables table def", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)
		moTablesRel := mock_frontend.NewMockRelation(ctrl)
		targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "bench_db",
		}).Times(1)
		targetRel.EXPECT().GetTableName().Return("bench_table").Times(1)
		moTablesRel.EXPECT().GetTableDef(gomock.Any()).Return(nil).Times(1)

		pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
			context.Background(),
			1,
			targetRel,
			moTablesRel,
			mp,
		)
		require.NoError(t, err)
		require.Nil(t, pkFilter)
		require.Equal(t, "missing-mo-tables-table-def", fallbackReason)
	})

	t.Run("missing mo_tables cpkey col", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)
		moTablesRel := mock_frontend.NewMockRelation(ctrl)
		targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "bench_db",
		}).Times(1)
		targetRel.EXPECT().GetTableName().Return("bench_table").Times(1)
		moTablesRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			Cols:          []*plan.ColDef{},
			Name2ColIndex: map[string]int32{},
		}).Times(1)

		pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
			context.Background(),
			1,
			targetRel,
			moTablesRel,
			mp,
		)
		require.NoError(t, err)
		require.Nil(t, pkFilter)
		require.Equal(t, "missing-mo-tables-cpkey-col", fallbackReason)
	})
}

func TestTryBuildCurrentMoTablesCPKeyFilterUsesExactCPKeyNameFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	targetRel := mock_frontend.NewMockRelation(ctrl)
	moTablesRel := mock_frontend.NewMockRelation(ctrl)

	targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "bench_db",
	}).Times(1)
	targetRel.EXPECT().GetTableName().Return("bench_table").Times(1)

	moTablesRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name:   catalog.SystemRelAttr_CPKey,
				Seqnum: uint32(catalog.MO_TABLES_CPKEY_IDX),
			},
		},
		Name2ColIndex: map[string]int32{
			catalog.SystemRelAttr_CPKey: 0,
		},
	}).Times(1)

	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	pkFilter, fallbackReason, err := tryBuildCurrentMoTablesCPKeyFilter(
		context.Background(),
		42,
		targetRel,
		moTablesRel,
		mp,
	)
	require.NoError(t, err)
	require.Empty(t, fallbackReason)
	require.NotNil(t, pkFilter)
}

func TestDatabaseCreatedTimeToCollectLowerBoundRejectsInvalidTimestamp(t *testing.T) {
	_, err := databaseCreatedTimeToCollectLowerBound(types.Timestamp(types.GetUnixEpochSecs()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid database created_time")
}

func TestGetDatabaseCreatedTimeLowerBoundGuardAndCachePaths(t *testing.T) {
	ses := newValidateSession(t)
	ses.SetAccountId(42)

	t.Run("missing target relation", func(t *testing.T) {
		_, err := getDatabaseCreatedTimeLowerBound(
			context.Background(),
			ses,
			nil,
			types.BuildTS(1, 0),
			nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing target relation")
	})

	t.Run("missing target database name", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)
		targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(1)

		_, err := getDatabaseCreatedTimeLowerBound(
			context.Background(),
			ses,
			targetRel,
			types.BuildTS(1, 0),
			nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing target database name")
	})

	t.Run("cache hit skips lookup", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		targetRel := mock_frontend.NewMockRelation(ctrl)
		targetRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "bench_db",
		}).Times(1)
		targetRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(7)).Times(1)

		snapshotTS := types.BuildTS(123, 0)
		expected := types.BuildTS(456, 0)
		cache := map[dbCreatedTimeLowerBoundKey]types.TS{
			{
				accountID:  42,
				databaseID: 7,
				database:   "bench_db",
				snapshot:   snapshotTS,
			}: expected,
		}

		lowerBound, err := getDatabaseCreatedTimeLowerBound(
			context.Background(),
			ses,
			targetRel,
			snapshotTS,
			cache,
		)
		require.NoError(t, err)
		require.Equal(t, expected, lowerBound)
	})
}
