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

package disttae

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

// receivedUpdate passes response through an off-heap receive buffer, as
// the logtail client does: the result's batches are views of the buffer.
// free releases the buffer, poisoned when poison-on-free is enabled.
func receivedUpdate(t *testing.T, response *logtail.UpdateResponse) (*logtail.UpdateResponse, func()) {
	wire, err := response.Marshal()
	require.NoError(t, err)
	pool := mpool.MustNewZero()
	buf := mpool.NewMessageBuffer(pool, 1<<30)
	data, err := buf.Resize(len(wire))
	require.NoError(t, err)
	copy(data, wire)
	got := &logtail.UpdateResponse{}
	require.NoError(t, got.Unmarshal(data))
	for _, tail := range got.LogtailList {
		for _, entry := range tail.Commands {
			if entry.Bat.Len() > 0 {
				require.True(t, entry.Bat.IsView())
			}
		}
	}
	return got, func() {
		buf.Free()
		require.Zero(t, pool.CurrNB())
	}
}

func newBufferTestEngine(t *testing.T, mp *mpool.MPool, tw client.TimestampWaiter) *Engine {
	packer := types.NewPacker()
	t.Cleanup(packer.Close)
	e := &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{tailC: make(chan statsLogtail, 64)},
		mp:          mp,
		packerPool: fileservice.NewPool(1,
			func() *types.Packer { return packer },
			func(p *types.Packer) { p.Reset() }, nil),
	}
	e.catalog.Store(cache.NewCatalog())
	e.pClient.receivedLogTailTime.initLogTailTimestamp(tw)
	e.pClient.receivedLogTailTime.ready.Store(true)
	return e
}

// A user table's logtail queued to a consumer routine is consumed after
// dispatch returns, when the receive buffer is already reused: the queued
// logtail owns its batches, so the rows replay intact from a freed,
// poisoned buffer.
func TestQueuedLogtailOutlivesReceiveBuffer(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	defer mpool.DisableDebugPoisonOnFree()
	ctx := t.Context()
	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	defer tw.Close()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	e := newBufferTestEngine(t, mp, tw)
	e.pClient.subscribed = subscribedTable{eng: e, m: map[uint64]*subEntry{101: {dbID: 10, state: Subscribed}}}

	values := []string{"first-value-longer-than-inline", "second-value-longer-than-inline"}
	rowBatch := &batch.Batch{Vecs: []*vector.Vector{testutil.MakeVarcharVector(values, nil, mp)}}
	t.Cleanup(func() { rowBatch.Clean(mp) })
	rowBatch.SetRowCount(len(values))
	rows, rowAttrs, err := fillRandomRowidAndZeroTs(rowBatch, []string{"pk"}, mp)
	require.NoError(t, err)
	to := timestamp.Timestamp{PhysicalTime: 200}
	response, free := receivedUpdate(t, &logtail.UpdateResponse{To: &to, LogtailList: []logtail.TableLogtail{{
		Table:    &api.TableID{DbId: 10, TbId: 101, DbName: "db", TbName: "t1"},
		Commands: []api.Entry{{EntryType: api.Entry_Insert, TableId: 101, TableName: "t1", Attrs: rowAttrs, Bat: api.NewEncodedBatch(rows)}},
	}}})

	routines := newTestRoutineControllers(8)
	require.NoError(t, dispatchUpdateResponse(ctx, e, response, routines, time.Now()))
	free() // the client reuses the buffer once dispatch returns

	controller := routines[101%consumerNumber]
	require.NotZero(t, len(controller.signalChan))
	for len(controller.signalChan) > 0 {
		require.NoError(t, (<-controller.signalChan).action(ctx, e, controller))
	}
	state := e.GetOrCreateLatestPart(ctx, 0, 10, 101).Snapshot()
	iter := state.NewRowsIter(types.BuildTS(300, 0), nil, false)
	defer iter.Close()
	var got []string
	for iter.Next() {
		entry := iter.Entry()
		got = append(got, entry.Batch.Vecs[2].GetStringAt(int(entry.Offset)))
	}
	require.ElementsMatch(t, values, got, "the replayed rows are the committed values")
}

// Catalog updates consumed during dispatch, and those delayed until the
// catalog replay is confirmed, both survive the receive buffer being freed
// and poisoned before they are applied.
func TestDelayedCatalogUpdateOutlivesReceiveBuffer(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "delayed"}[delayed], func(t *testing.T) {
			mpool.EnableDebugPoisonOnFree()
			defer mpool.DisableDebugPoisonOnFree()
			ctx := t.Context()
			tw := client.NewTimestampWaiter(runtime.GetLogger(""))
			defer tw.Close()
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			e := newBufferTestEngine(t, mp, tw)
			if !delayed {
				e.pClient.dcaConfirmAndApply()
			}
			packer := types.NewPacker()
			t.Cleanup(packer.Close)
			makeTail := func(bat *batch.Batch, id uint64, name string, seq uint32) logtail.TableLogtail {
				_, attrs, err := fillRandomRowidAndZeroTs(bat, catalogTupleAttrsForTest(id), mp)
				require.NoError(t, err)
				ts := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[cache.MO_TIMESTAMP_IDX])
				for i := range ts {
					ts[i] = types.BuildTS(100, 0)
				}
				encoded, err := batch.MarshalVectors(bat.Vecs)
				require.NoError(t, err)
				t.Cleanup(func() { bat.Clean(mp) })
				return logtail.TableLogtail{Table: &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: id, DbName: catalog.MO_CATALOG, TbName: name, PrimarySeqnum: seq},
					Commands: []api.Entry{{EntryType: api.Entry_Insert, TableId: id, TableName: name, DatabaseId: catalog.MO_CATALOG_ID, DatabaseName: catalog.MO_CATALOG, Attrs: attrs, Bat: api.NewEncodedBatch(encoded)}}}
			}
			cols, err := catalog.GenColumnsFromDefs(7, "t", "db", 90, 80, []engine.TableDef{&engine.AttributeDef{Attr: engine.Attribute{Name: "n", Type: types.T_uint64.ToType()}}})
			require.NoError(t, err)
			cols[0].Comment = "kept-comment"
			cb, err := catalog.GenCreateColumnTuples(cols, mp, packer)
			require.NoError(t, err)
			tb, err := catalog.GenCreateTableTuple(catalog.Table{AccountId: 7, TableId: 90, DatabaseId: 80, TableName: "t", DatabaseName: "db", Version: 1}, mp, packer)
			require.NoError(t, err)
			to := timestamp.Timestamp{PhysicalTime: 100}
			response, free := receivedUpdate(t, &logtail.UpdateResponse{To: &to, LogtailList: []logtail.TableLogtail{
				makeTail(tb, catalog.MO_TABLES_ID, catalog.MO_TABLES, catalog.MO_TABLES_CPKEY_IDX),
				makeTail(cb, catalog.MO_COLUMNS_ID, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ATT_CPKEY_IDX)}})
			require.NoError(t, dispatchUpdateResponse(ctx, e, response, newTestRoutineControllers(16), time.Now()))
			free()
			if delayed {
				e.pClient.dcaConfirmAndApply()
			}
			q := &cache.TableItem{AccountId: 7, DatabaseId: 80, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: 200}}
			require.True(t, e.GetLatestCatalogCache().GetTable(q))
			require.Equal(t, "kept-comment", q.ReadPlanTableDef().Cols[0].Comment)
		})
	}
}

// The stats consumer gets a summary taken at enqueue, so it never reads a
// logtail's batches after the receive buffer is reused.
func TestStatsLogtailIsTakenAtEnqueue(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	defer mpool.DisableDebugPoisonOnFree()
	mp := mpool.MustNewZero()
	objects := batch.NewWithSize(1)
	objects.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(objects.Vecs[0], int64(i), false, mp))
	}
	objects.SetRowCount(3)
	defer objects.Clean(mp)
	encoded, err := objects.MarshalBinary()
	require.NoError(t, err)
	to := timestamp.Timestamp{PhysicalTime: 1}
	response, free := receivedUpdate(t, &logtail.UpdateResponse{To: &to, LogtailList: []logtail.TableLogtail{{
		Table:    &api.TableID{AccId: 1, DbId: 2, TbId: 3, TbName: "t", DbName: "d"},
		Commands: []api.Entry{{EntryType: api.Entry_DataObject, TableName: "t", Bat: api.NewEncodedBatch(encoded)}},
	}, {
		Table: &api.TableID{TbId: 4},
	}}})

	gs := &GlobalStats{tailC: make(chan statsLogtail, 4)}
	gs.enqueue(&response.LogtailList[0])
	gs.enqueue(&response.LogtailList[1])
	free()
	require.Len(t, gs.tailC, 1, "a logtail without changes is not enqueued")
	change := <-gs.tailC
	require.Equal(t, 3, change.metaChanges)
	require.False(t, change.hasCheckpoint)
	require.Equal(t, uint64(3), change.key.TableID)
	require.Equal(t, "d", change.key.DbName)
}
