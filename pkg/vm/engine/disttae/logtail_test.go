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

package disttae

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

func TestApplyPreparedMetadataToCatalogCache(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(
		bat.Vecs[0], types.Rowid{}, false, mp))
	require.NoError(t, vector.AppendFixed(
		bat.Vecs[1], types.BuildTS(42, 7), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	protoBatch, err := batch.BatchToProtoBatch(bat)
	require.NoError(t, err)
	catalogCache := cache.NewCatalog()
	applyToCatalogCache(catalogCache, &api.Entry{
		EntryType:    api.Entry_Update,
		DatabaseName: catalog.MO_CATALOG,
		TableName:    catalog.MO_PUBS,
		Bat:          protoBatch,
	})

	require.Equal(t,
		timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
		catalogCache.GetPreparedMetadataTS())
	require.True(t, isPreparedMetadataTable(catalog.MO_CATALOG, catalog.MO_SUBS))
	require.True(t, isPreparedMetadataTable(catalog.MO_CATALOG, catalog.MO_PUBS))
	require.True(t, isPreparedMetadataTable(catalog.MO_CATALOG, catalog.MOAccountTable))
	require.True(t, isPreparedMetadataTable(catalog.MO_CATALOG, catalog.MO_SNAPSHOTS))
	require.False(t, isPreparedMetadataTable("user_db", catalog.MO_SUBS))
	require.False(t, isPreparedMetadataTable(catalog.MO_CATALOG, catalog.MO_TABLES))
}
