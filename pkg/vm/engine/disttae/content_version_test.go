// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestTableContentVersionsRequireSnapshotAndContinuity(t *testing.T) {
	ctx := context.Background()
	e := &Engine{partitions: make(map[[2]uint64]*logtailreplay.Partition)}
	p := e.GetOrCreateLatestPart(ctx, 0, 10, 42)
	state, publish := p.MutateState()
	state.UpdateDuration(types.TS{}, types.MaxTs())
	state.UpdateAppliedTo(types.BuildTS(10, 0))
	state.RecordContentChange(types.BuildTS(10, 0))
	publish()
	e.pClient.subscribed.eng = e
	e.pClient.subscribed.m = make(map[uint64]*subEntry)
	e.pClient.subscribed.setTableSubscribed(10, 42)
	e.pClient.replayVersion.Store(nextReplayVersion.Add(1))
	e.pClient.receivedLogTailTime.ready.Store(true)
	dependencies := []engine.TableContentDependency{{DatabaseID: 10, TableID: 42}}
	var first, next [1]engine.TableContentVersion
	snapshot := timestamp.Timestamp{PhysicalTime: 11}
	read := func() bool { return e.ReadTableContentVersions(ctx, snapshot, dependencies, next[:]) }
	require.True(t, e.ReadTableContentVersions(ctx, snapshot, dependencies, first[:]))

	// A newer applied watermark without content changes keeps the same proof.
	state, publish = p.MutateState()
	state.UpdateAppliedTo(types.BuildTS(12, 0))
	publish()
	require.True(t, read())
	require.Equal(t, first, next)

	// The state may already contain changes beyond the reader's snapshot.
	// Matching captures cannot certify facts that this older reader cannot see.
	state, publish = p.MutateState()
	state.RecordContentChange(types.BuildTS(12, 0))
	publish()
	require.False(t, read())
	snapshot = timestamp.Timestamp{PhysicalTime: 13}
	require.True(t, read())
	require.NotEqual(t, first, next)
	first = next

	// Reusing the same table ID and data revision after a subscription gap is
	// insufficient: the subscription owner must establish fresh continuity.
	e.pClient.subscribed.clearTable(10, 42)
	require.False(t, read())
	e.pClient.subscribed.setTableSubscribed(10, 42)
	require.True(t, read())
	require.NotEqual(t, first, next)
	first = next

	// Local partition replacement may happen without an unsubscribe round
	// trip. Publishing into the replacement must retire the old proof too.
	replacement := logtailreplay.NewPartition("", nil, 0, 10, 42, nil)
	state, publish = replacement.MutateState()
	state.UpdateDuration(types.TS{}, types.MaxTs())
	state.UpdateAppliedTo(types.BuildTS(12, 0))
	state.RecordContentChange(types.BuildTS(10, 0))
	state.RecordContentChange(types.BuildTS(12, 0))
	publish()
	e.Lock()
	e.partitions[[2]uint64{10, 42}] = replacement
	e.Unlock()
	e.pClient.subscribed.bindPartition(10, 42, replacement)
	require.True(t, read())
	require.Equal(t, first[0].Revision, next[0].Revision)
	require.NotEqual(t, first[0].Subscription, next[0].Subscription)
	first = next

	e.pClient.invalidateReadContinuity()
	require.False(t, read())
	e.pClient.receivedLogTailTime.ready.Store(true)
	require.True(t, read())
	require.NotEqual(t, first, next)

	// A queued update needed by this snapshot cannot be skipped by a cached
	// grant. Cancellation and missing dependencies never create a proof.
	pending := timestamp.Timestamp{PhysicalTime: 20}
	e.pClient.subscribed.setTablePendingUpdate(10, 42, pending)
	snapshot = timestamp.Timestamp{PhysicalTime: 21}
	require.False(t, read())
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.False(t, e.ReadTableContentVersions(canceled, snapshot, dependencies, next[:]))
	dependencies[0].TableID = 43
	require.False(t, read())
	require.Len(t, e.partitions, 1, "proof validation must not create missing partitions")
}

// Exercise the production publisher and independent visible row/object state,
// rather than manually advancing the version the reader is meant to verify.
func TestContentVersionsFollowPublishedRowsAndObjects(t *testing.T) {
	ctx := t.Context()
	pool := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()
	e := &Engine{partitions: make(map[[2]uint64]*logtailreplay.Partition), mp: pool,
		globalStats: &GlobalStats{tailC: make(chan *logtail.TableLogtail, 8)},
		packerPool:  fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, func(*types.Packer) {}),
	}
	e.pClient.subscribed.eng = e
	e.pClient.subscribed.m = make(map[uint64]*subEntry)
	e.pClient.replayVersion.Store(nextReplayVersion.Add(1))
	e.pClient.receivedLogTailTime.ready.Store(true)
	table := &api.TableID{DbId: 10, TbId: 42, DbName: "app", TbName: "t"}
	stamp := timestamp.Timestamp{PhysicalTime: 10}
	require.NoError(t, updatePartitionOfPush(ctx, e, &logtail.TableLogtail{Table: table, Ts: &stamp}, true, time.Now(), true, stamp))
	e.pClient.subscribed.setTableSubscribed(10, 42)
	deps := []engine.TableContentDependency{{DatabaseID: 10, TableID: 42}}
	var before, after [1]engine.TableContentVersion
	read := func(out []engine.TableContentVersion) {
		require.True(t, e.ReadTableContentVersions(ctx, stamp.Next(), deps, out))
	}
	read(before[:])
	// Empty watermark publications leave the certificate reusable.
	stamp.PhysicalTime++
	require.NoError(t, updatePartitionOfPush(ctx, e, &logtail.TableLogtail{Table: table, Ts: &stamp}, true, time.Now(), false, stamp))
	read(after[:])
	require.Equal(t, before, after)
	for _, kind := range []api.Entry_EntryType{api.Entry_Insert, api.Entry_DataObject, api.Entry_TombstoneObject} {
		stamp.PhysicalTime++
		commit := types.TimestampToTS(stamp)
		var bat *batch.Batch
		if kind == api.Entry_Insert {
			bat = batch.NewWithSchema(false, []string{"rowid", "commit", "id"}, []types.Type{types.T_Rowid.ToType(), types.T_TS.ToType(), types.T_int64.ToType()})
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(1, 1), false, pool))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], commit, false, pool))
			require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(7), false, pool))
		} else {
			attrs := append([]string{"rowid", "commit"}, taelogtail.ObjectInfoAttr...)
			typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, taelogtail.ObjectInfoTypes...)
			bat = batch.NewWithSchema(false, attrs, typs)
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
			require.NoError(t, objectio.SetObjectStatsSize(stats, 100))
			require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
			require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(2, 2), false, pool))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], commit, false, pool))
			require.NoError(t, vector.AppendBytes(bat.Vecs[2], stats[:], false, pool))
			require.NoError(t, vector.AppendFixed(bat.Vecs[3], uint64(10), false, pool))
			require.NoError(t, vector.AppendFixed(bat.Vecs[4], uint64(42), false, pool))
			for i := 5; i < 10; i++ {
				value := commit
				if i == 6 {
					value = types.TS{}
				}
				require.NoError(t, vector.AppendFixed(bat.Vecs[i], value, false, pool))
			}
		}
		proto, err := batch.BatchToProtoBatch(bat)
		require.NoError(t, err)
		tail := &logtail.TableLogtail{Table: table, Ts: &stamp, Commands: []api.Entry{{DatabaseId: 10, TableId: 42, DatabaseName: "app", TableName: "t", EntryType: kind, Bat: proto}}}
		require.NoError(t, updatePartitionOfPush(ctx, e, tail, true, time.Now(), false, stamp))
		bat.Clean(pool)
		read(after[:])
		require.NotEqual(t, before, after)
		state := e.GetOrCreateLatestPart(ctx, 0, 10, 42).Snapshot()
		if kind == api.Entry_Insert {
			it := state.NewRowsIter(commit, nil, false)
			require.True(t, it.Next())
			require.Equal(t, types.BuildTestRowid(1, 1), it.Entry().RowID)
			require.NoError(t, it.Close())
		} else {
			it, err := state.NewObjectsIter(commit, true, kind == api.Entry_TombstoneObject)
			require.NoError(t, err)
			require.True(t, it.Next())
			object := it.Entry()
			require.Equal(t, uint32(1), object.Rows())
			require.NoError(t, it.Close())
		}
		before = after
	}
	// Missing the publication timestamp must disable reuse, even with valid rows.
	part := e.GetOrCreateLatestPart(ctx, 0, 10, 42)
	state, publish := part.MutateState()
	state.RecordContentChange(types.TS{})
	publish()
	require.False(t, e.ReadTableContentVersions(ctx, stamp.Next(), deps, after[:]))
}
