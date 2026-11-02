// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestClusterReplayDispatcherBindsCatalogGeneration(t *testing.T) {
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	packer := types.NewPacker()
	defer packer.Close()
	cc := cache.NewCatalog()
	eng := &Engine{partitions: make(map[[2]uint64]*logtailreplay.Partition)}
	eng.catalog.Store(cc)
	meta, err := catalog.GenCreateTableTuple(catalog.Table{AccountId: 7, DatabaseId: 10, DatabaseName: "db", TableId: 22, TableName: "metrics"}, mp, packer)
	require.NoError(t, err)
	defer meta.Clean(mp)
	_, err = fillRandomRowidAndZeroTs(meta, mp)
	require.NoError(t, err)
	vector.MustFixedColWithTypeCheck[types.TS](meta.GetVector(cache.MO_TIMESTAMP_IDX))[0] = types.BuildTS(1, 0)
	cc.InsertTable(meta)
	item := cc.GetTableByName(7, 10, "metrics")
	require.NotNil(t, item)
	// A complete declared schema is installed before dispatch, as after the
	// catalog bootstrap/column publication boundary. No batch name declares it.
	item.TableDef = clusterReplayTableDef()
	input, _ := clusterReplaySpecBatch(t, mp, packer, []clusterRowSpec{{device: 0, metric: 0, tsOrdinal: 18, value: 19}}, 5, 0, clusterReplayBatchOptions{})
	frame := &logtail.TableLogtail{Table: &api.TableID{AccId: 7, DbId: 10, TbId: 22, TbName: "metrics", DbName: "db"}, Commands: []api.Entry{{EntryType: api.Entry_Insert, TableId: 22, TableName: "metrics", Bat: input}}}
	through := types.BuildTS(20, 0).ToTimestamp()
	require.Nil(t, eng.captureReplayClusterKey(frame, through), "bootstrap is not complete")
	eng.pClient.dcaConfirmAndApply()
	captured := eng.captureReplayClusterKey(frame, through)
	require.NotNil(t, captured)
	require.Equal(t, uint32(0), captured.Version)
	for _, tc := range []struct {
		name   string
		change func()
	}{
		{"ordinary", func() { item.TableDef.ClusterBy = nil }},
		{"not_hidden", func() { item.TableDef.Cols[7].Hidden = false }},
		{"not_clustered", func() { item.TableDef.Cols[7].ClusterBy = false }},
		{"wrong_table", func() { frame.Table.TbId = 23 }},
		{"wrong_tenant", func() { frame.Table.AccId = 8 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item.TableDef = clusterReplayTableDef()
			frame.Table.TbId = 22
			frame.Table.AccId = 7
			tc.change()
			require.Nil(t, eng.captureReplayClusterKey(frame, through))
		})
	}
	frame.Table.TbId = 22
	frame.Table.AccId = 7
	// The queued descriptor is immutable when a later catalog generation is
	// published before the table worker applies this frame.
	item.TableDef = clusterReplayTableDef()
	item.TableDef.Version = 1
	item.Version = 1
	next := eng.captureReplayClusterKey(frame, through)
	require.NotNil(t, next)
	require.Equal(t, uint32(0), captured.Version)
	require.Equal(t, uint32(1), next.Version)
	partition := eng.GetOrCreateLatestPart(t.Context(), 7, 10, 22)
	state, publish := partition.MutateState()
	state.HandleLogtailEntry(t.Context(), nil, &frame.Commands[0], 6, packer, mp, captured)
	publish()
	require.True(t, partition.Snapshot().ClusterIndexReadyFor(*captured))
	previous := partition.Snapshot()
	state, publish = partition.MutateState()
	newer, _ := clusterReplaySpecBatch(t, mp, packer, []clusterRowSpec{{device: 0, metric: 0, tsOrdinal: 19, value: 20}}, 6, 1, clusterReplayBatchOptions{})
	state.HandleRowsInsert(t.Context(), newer, 6, packer, mp, next)
	publish()
	require.False(t, partition.Snapshot().WantsClusterKeyMetadata())
	require.Nil(t, eng.captureReplayClusterKey(frame, through), "disabled published state skips metadata")
	require.True(t, previous.ClusterIndexReadyFor(*captured), "old snapshot keeps its schema/index")
	require.Equal(t, 2, partition.Snapshot().ApproxInMemRows())
}
