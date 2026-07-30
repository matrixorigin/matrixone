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
