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
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/stretchr/testify/require"
)

func TestTemporalInputReconnectPublishesFreshClient(t *testing.T) {
	subscriber := newLogTailSubscriber()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { require.NoError(t, subscriber.closeTransport()) })
	factory := func(context.Context, string, string, morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error) {
		return nil, newCloseRaceStream(), nil
	}
	require.Nil(t, subscriber.client())
	require.NoError(t, subscriber.init(ctx, "test", "test", factory))
	first := subscriber.client()
	require.NotNil(t, first)
	require.NoError(t, subscriber.init(ctx, "test", "test", factory))
	second := subscriber.client()
	require.NotNil(t, second)
	require.NotSame(t, first, second)
	require.NoError(t, subscriber.closeClient())
	require.Nil(t, subscriber.client())
	initErr := errors.New("stream unavailable")
	failed := func(context.Context, string, string, morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error) {
		return nil, nil, initErr
	}
	require.ErrorIs(t, subscriber.init(ctx, "test", "test", failed), initErr)
	require.Nil(t, subscriber.client())
}

// Controlled interleavings in the block walk, after object admission charges
// the shared budget. No production hook or scheduler timing is needed.
type planningInputWorkContext struct {
	context.Context
	budget *int
	below  int
	action func()
	fired  bool
	skip   int
}

func (c *planningInputWorkContext) Err() error {
	if !c.fired && *c.budget < c.below {
		if c.skip > 0 {
			c.skip--
			return c.Context.Err()
		}
		c.fired = true
		if c.action != nil {
			c.action()
		}
	}
	return c.Context.Err()
}

func TestTemporalInputSnapshotDoesNotAdmitOrWait(t *testing.T) {
	f := newTemporalNativeFixture(t)
	e := f.base.origin.eng.(*Engine)
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	c := &e.pClient
	c.eng = e
	c.subscribed.m = map[uint64]*subEntry{98: {dbID: 10, state: Subscribed}}
	c.receivedLogTailTime.ready.Store(true)
	c.receivedLogTailTime.tList = make([]atomic.Value, 1)
	c.receivedLogTailTime.tList[0].Store(f.read.Snapshot.Prev())
	state, _, ok := c.planningInputSnapshot(10, 98, f.read)
	require.False(t, ok)
	require.Nil(t, state)
	require.Empty(t, e.partitions, "optional costing cannot create a partition")
	part := e.GetOrCreateLatestPart(f.ctx, 7, 10, 98)
	ps, done := part.MutateState()
	ps.UpdateDuration(types.TS{}, types.MaxTs())
	done()
	state, entry, ok := c.planningInputSnapshot(10, 98, f.read)
	require.True(t, ok)
	require.Same(t, part.Snapshot(), state)
	require.Same(t, c.subscribed.m[98], entry)
	for _, scenario := range []string{"pending", "waterline", "not ready", "unsubscribed", "wrong database", "subscription contended", "engine contended", "historical"} {
		t.Run(scenario, func(t *testing.T) {
			c.receivedLogTailTime.ready.Store(true)
			c.receivedLogTailTime.tList[0].Store(f.read.Snapshot.Prev())
			c.subscribed.m[98] = &subEntry{dbID: 10, state: Subscribed}
			read := f.read
			switch scenario {
			case "pending":
				c.subscribed.m[98].pendingTo.Store(&read.Snapshot)
			case "waterline":
				c.receivedLogTailTime.tList[0].Store(timestamp.Timestamp{})
			case "not ready":
				c.receivedLogTailTime.ready.Store(false)
			case "unsubscribed":
				c.subscribed.m[98].state = Unsubscribing
			case "wrong database":
				c.subscribed.m[98].dbID++
			case "subscription contended":
				c.subscribed.rw.Lock()
				t.Cleanup(c.subscribed.rw.Unlock)
			case "engine contended":
				e.Lock()
				t.Cleanup(e.Unlock)
			case "historical":
				ps, release := part.MutateState()
				ps.UpdateDuration(types.BuildTS(10, 0), types.MaxTs())
				release()
			}
			_, _, ok := c.planningInputSnapshot(10, 98, read)
			require.False(t, ok)
			if scenario == "engine contended" {
				require.True(t, c.subscribed.rw.TryLock(), "failed engine acquisition must release the subscription lock")
				c.subscribed.rw.Unlock()
			}
		})
	}
}

func TestIndexBlockInputUsesRealCachedBlocks(t *testing.T) {
	f := newTemporalNativeFixture(t)
	e := f.base.origin.eng.(*Engine)
	fs, err := fileservice.NewMemoryFS(t.Name(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(f.ctx) })
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	data := batch.NewWithSize(2)
	data.Vecs[0] = vector.NewVec(types.New(types.T_uint64, 64, -1))
	data.Vecs[1] = vector.NewVec(types.New(types.T_datetime, 0, 6))
	defer data.Clean(mp)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, true, false, fs)
	date := types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0)
	for i := uint64(0); i < 2; i++ {
		require.NoError(t, vector.AppendFixed(data.Vecs[0], i, false, mp))
		require.NoError(t, vector.AppendFixed(data.Vecs[1], types.DatetimeFromClock(2026, 1, uint8(i+1), 0, 0, 0, 0), false, mp))
	}
	data.SetRowCount(2)
	_, err = writer.WriteBatch(data)
	require.NoError(t, err)
	data.CleanOnlyData()
	for i := uint64(2); i < 4; i++ {
		require.NoError(t, vector.AppendFixed(data.Vecs[0], i, false, mp))
		require.NoError(t, vector.AppendFixed(data.Vecs[1], types.DatetimeFromClock(2026, 1, uint8(i+1), 0, 0, 0, 0), false, mp))
	}
	data.SetRowCount(2)
	_, err = writer.WriteBatch(data)
	require.NoError(t, err)
	_, _, err = writer.Sync(f.ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	_, err = objectio.FastLoadObjectMeta(f.ctx, &location, false, fs)
	require.NoError(t, err)
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	e.pClient.eng = e
	e.pClient.subscriber = newLogTailSubscriber()
	e.pClient.subscriber.swapClient(&service.LogtailClient{})
	e.pClient.subscriber.setReady()
	e.pClient.subscribed.m = map[uint64]*subEntry{98: {dbID: 10, state: Subscribed}}
	e.pClient.receivedLogTailTime.ready.Store(true)
	e.pClient.receivedLogTailTime.tList = make([]atomic.Value, 1)
	e.pClient.receivedLogTailTime.tList[0].Store(f.read.Snapshot.Prev())
	part := e.GetOrCreateLatestPart(f.ctx, 7, 10, 98)
	putObject := func(t testing.TB, entry objectio.ObjectEntry) {
		t.Helper()
		ps, release := part.MutateState()
		defer release()
		require.NoError(t, ps.HandleObjectEntry(f.ctx, nil, entry, false))
	}
	func() {
		ps, done := part.MutateState()
		defer done()
		ps.UpdateDuration(types.TS{}, types.MaxTs())
		require.NoError(t, ps.HandleObjectEntry(f.ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}, false))
	}()
	f.full.Name2ColIndex = map[string]int32{"id": 0, "d": 1}
	f.full.Cols[1].Typ.Scale = 6
	f.full.Cols[0].Typ.Scale = -1
	f.stats.TableCnt, f.stats.BlockNumber = 4, 2
	observation, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.True(t, ok)
	bound := types.DatetimeFromClock(2026, 1, 2, 0, 0, 0, 0).ToTimestamp(time.UTC)
	req := engine.IndexBlockInputRequest{Column: "d", Observed: f.stats, Stats: observation, Bound: make([]byte, 8), BoundType: types.T_timestamp.ToType(), ComparisonScale: 6, Op: types.ComparisonGreater, Zone: time.UTC}
	copy(req.Bound[:], types.EncodeTimestamp(&bound))

	// The same persisted objects also carry a non-temporal column. Observe its
	// real block contents through the production API, with independent row oracles.
	f.stats.MinValMap["id"], f.stats.MaxValMap["id"], f.stats.NdvMap["id"] = 0, 3, 4
	f.stats.NullCntMap["id"], f.stats.DataTypeMap["id"] = 0, uint64(types.T_uint64)
	integerStats, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "id")
	require.NoError(t, err)
	require.True(t, ok)
	for _, tc := range []struct {
		op     types.ComparisonOp
		blocks uint64
	}{
		{types.ComparisonEqual, 1}, {types.ComparisonLess, 1}, {types.ComparisonLessEqual, 1}, {types.ComparisonGreater, 1}, {types.ComparisonGreaterEqual, 2},
	} {
		t.Run("integer "+tc.op.String(), func(t *testing.T) {
			value := uint64(1)
			request := engine.IndexBlockInputRequest{Column: "id", Observed: f.stats, Stats: integerStats, Bound: types.EncodeUint64(&value), BoundType: types.New(types.T_uint64, 64, -1), Op: tc.op}
			budget := engine.MaxIndexPlanningWork
			input, complete, err := f.base.ObserveIndexBlockInput(f.ctx, f.read, request, &budget)
			require.NoError(t, err)
			require.True(t, complete)
			require.Equal(t, engine.IndexBlockInput{Rows: 2 * tc.blocks, Blocks: tc.blocks}, input)
		})
	}
	var visibleWork int
	stopCause := errors.New("optional observation cancelled")
	for _, scenario := range []string{"complete", "two matching blocks", "missing overlap", "NaN overlap", "empty", "budget", "cancelled", "stale stats", "row population", "block population", "pending", "transport unavailable", "entry replaced", "unrelated subscription", "engine contended", "subscriber contended"} {
		t.Run(scenario, func(t *testing.T) {
			request := req
			request.Bound = append([]byte(nil), req.Bound...)
			ctx := f.ctx
			budget := engine.MaxIndexPlanningWork
			e.pClient.subscriber.swapClient(&service.LogtailClient{})
			e.pClient.subscribed.m[98].pendingTo.Store(nil)
			switch scenario {
			case "two matching blocks":
				bound := date.ToTimestamp(time.UTC)
				copy(request.Bound[:], types.EncodeTimestamp(&bound))
			case "missing overlap":
				original := f.stats.ShuffleRangeMap
				f.stats.ShuffleRangeMap = nil
				t.Cleanup(func() { f.stats.ShuffleRangeMap = original })
			case "NaN overlap":
				original := f.stats.ShuffleRangeMap["d"].Overlap
				f.stats.ShuffleRangeMap["d"].Overlap = math.NaN()
				t.Cleanup(func() { f.stats.ShuffleRangeMap["d"].Overlap = original })
			case "empty":
				request.Op = types.ComparisonLess
				empty := date.ToTimestamp(time.UTC)
				copy(request.Bound[:], types.EncodeTimestamp(&empty))
			case "budget":
				budget = 2
			case "cancelled":
				var cancel context.CancelCauseFunc
				ctx, cancel = context.WithCancelCause(ctx)
				cancel(stopCause)
			case "stale stats":
				request.Stats.Rows++
			case "row population":
				f.stats.TableCnt++
				request.Stats.Rows++
				t.Cleanup(func() { f.stats.TableCnt = 4 })
			case "block population":
				f.stats.BlockNumber++
				request.Stats.Blocks++
				t.Cleanup(func() { f.stats.BlockNumber = 2 })
			case "pending":
				e.pClient.subscribed.m[98].pendingTo.Store(&f.read.Snapshot)
			case "transport unavailable":
				e.pClient.subscriber.swapClient(nil)
			case "entry replaced":
				e.pClient.subscribed.m[98] = &subEntry{dbID: 11, state: Subscribed}
				t.Cleanup(func() { e.pClient.subscribed.m[98] = &subEntry{dbID: 10, state: Subscribed} })
			case "unrelated subscription":
				subscriber := e.pClient.subscriber
				previous := subscriber.sendSubscribe
				entered, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				subscriber.sendSubscribe = func(context.Context, api.TableID) error {
					close(entered)
					<-release
					return nil
				}
				go func() {
					_, _, err := e.pClient.isNotUnsubscribing(f.ctx, 10, 999)
					done <- err
				}()
				t.Cleanup(func() {
					close(release)
					require.NoError(t, <-done)
					subscriber.sendSubscribe = previous
					e.pClient.subscribed.rw.Lock()
					delete(e.pClient.subscribed.m, 999)
					e.pClient.subscribed.rw.Unlock()
				})
				<-entered
			case "engine contended":
				e.Lock()
				t.Cleanup(e.Unlock)
			case "subscriber contended":
				e.pClient.subscriber.mu.Lock()
				t.Cleanup(e.pClient.subscriber.mu.Unlock)
			}
			input, complete, err := f.base.ObserveIndexBlockInput(ctx, f.read, request, &budget)
			if scenario == "cancelled" {
				require.ErrorIs(t, err, stopCause)
			} else {
				require.NoError(t, err)
			}
			wantRows := scenario == "complete" || scenario == "two matching blocks" || scenario == "missing overlap" || scenario == "NaN overlap"
			require.Equal(t, wantRows || scenario == "empty", complete)
			if wantRows {
				want := engine.IndexBlockInput{Rows: 2, Blocks: 1}
				if scenario == "two matching blocks" {
					want = engine.IndexBlockInput{Rows: 4, Blocks: 2}
				}
				require.Equal(t, want, input)
				if scenario == "complete" {
					visibleWork = engine.MaxIndexPlanningWork - budget
				}
			} else {
				require.Zero(t, input)
			}
		})
	}
	for _, scenario := range []string{"cancel during scan", "pending during scan", "transport during scan", "stats during scan", "partition during scan", "entry during scan", "subscription contended", "engine contended", "subscriber contended", "cancel with subscription contended"} {
		t.Run(scenario, func(t *testing.T) {
			e.pClient.subscriber.swapClient(&service.LogtailClient{})
			e.pClient.subscribed.m[98].pendingTo.Store(nil)
			budget := engine.MaxIndexPlanningWork
			parent, cancel := context.WithCancelCause(context.WithoutCancel(f.ctx))
			defer cancel(nil)
			// With two matching blocks, skip the first checkpoint so a real
			// partial count exists before invalidation at the second block.
			ctx := &planningInputWorkContext{Context: parent, budget: &budget, below: budget - 4, skip: 1}
			request := req
			request.Bound = append([]byte(nil), req.Bound...)
			allBlocks := date.ToTimestamp(time.UTC)
			copy(request.Bound[:], types.EncodeTimestamp(&allBlocks))
			ctx.action = func() {
				switch scenario {
				case "cancel during scan":
					cancel(stopCause)
				case "pending during scan":
					e.pClient.subscribed.m[98].pendingTo.Store(&f.read.Snapshot)
				case "transport during scan":
					// A reconnect detaches the old client and publishes a fresh one.
					e.pClient.subscriber.swapClient(nil)
					e.pClient.subscriber.swapClient(&service.LogtailClient{})
				case "stats during scan":
					t.Cleanup(func() {
						require.NoError(t, f.gs.PatchStats(f.key, &PatchArgs{MinValMap: map[string]float64{"d": 100}}))
					})
					require.NoError(t, f.gs.PatchStats(f.key, &PatchArgs{MinValMap: map[string]float64{"d": 101}}))
				case "partition during scan":
					_, release := part.MutateState()
					release()
				case "entry during scan":
					e.pClient.subscribed.rw.Lock()
					e.pClient.subscribed.m[98] = &subEntry{dbID: 10, state: Subscribed}
					e.pClient.subscribed.rw.Unlock()
				case "subscription contended", "cancel with subscription contended":
					e.pClient.subscribed.rw.Lock()
					t.Cleanup(e.pClient.subscribed.rw.Unlock)
					if scenario == "cancel with subscription contended" {
						cancel(stopCause)
					}
				case "engine contended":
					e.Lock()
					t.Cleanup(e.Unlock)
				case "subscriber contended":
					e.pClient.subscriber.mu.Lock()
					t.Cleanup(e.pClient.subscriber.mu.Unlock)
				}
			}
			input, complete, err := f.base.ObserveIndexBlockInput(ctx, f.read, request, &budget)
			if scenario == "cancel during scan" || scenario == "cancel with subscription contended" {
				require.ErrorIs(t, err, stopCause)
			} else {
				require.NoError(t, err)
			}
			require.True(t, ctx.fired, "the observation must reach the controlled interleaving")
			require.False(t, complete)
			require.Zero(t, input, "late invalidation must discard every partial count")
		})
	}
	// Place a missing cache entry after the valid object in name order. This
	// must discard counts already obtained from the first surviving block.
	t.Run("later cache miss", func(t *testing.T) {
		e.pClient.subscriber.swapClient(&service.LogtailClient{})
		e.pClient.subscribed.m[98].pendingTo.Store(nil)
		missing := stats
		segment := stats.ObjectName().SegmentId()
		require.NoError(t, objectio.SetObjectStatsObjectName(&missing, objectio.BuildObjectName(&segment, stats.ObjectName().Num()+1)))
		putObject(t, objectio.ObjectEntry{ObjectStats: missing, CreateTime: types.BuildTS(1, 0)})
		f.stats.TableCnt, f.stats.BlockNumber = 8, 4
		request := req
		request.Bound = append([]byte(nil), req.Bound...)
		request.Stats.Rows, request.Stats.Blocks = 8, 4
		budget := engine.MaxIndexPlanningWork
		input, complete, err := f.base.ObserveIndexBlockInput(f.ctx, f.read, request, &budget)
		require.NoError(t, err)
		require.False(t, complete)
		require.Zero(t, input)
		f.stats.TableCnt, f.stats.BlockNumber = 4, 2
		putObject(t, objectio.ObjectEntry{ObjectStats: missing, CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(2, 0)})
		budget = engine.MaxIndexPlanningWork
		input, complete, err = f.base.ObserveIndexBlockInput(f.ctx, f.read, req, &budget)
		require.NoError(t, err)
		require.True(t, complete, "an invisible cache miss cannot affect the visible population")
		require.Equal(t, engine.IndexBlockInput{Rows: 2, Blocks: 1}, input)
		budget = visibleWork
		input, complete, err = f.base.ObserveIndexBlockInput(f.ctx, f.read, req, &budget)
		require.NoError(t, err)
		require.False(t, complete, "invisible tree entries still consume traversal budget")
		require.Zero(t, input)
	})
	t.Run("appendable", func(t *testing.T) {
		appendable := stats
		segment := stats.ObjectName().SegmentId()
		require.NoError(t, objectio.SetObjectStatsObjectName(&appendable, objectio.BuildObjectName(&segment, stats.ObjectName().Num()+2)))
		objectio.SetObjectStatsAppendable(&appendable, true)
		// Appendable objects have one block in the replay protocol.
		require.NoError(t, objectio.SetObjectStatsBlkCnt(&appendable, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(&appendable, 2))
		putObject(t, objectio.ObjectEntry{ObjectStats: appendable, CreateTime: types.BuildTS(1, 0)})
		f.stats.TableCnt, f.stats.BlockNumber = 6, 3
		request := req
		request.Bound = append([]byte(nil), req.Bound...)
		request.Stats.Rows, request.Stats.Blocks = 6, 3
		budget := engine.MaxIndexPlanningWork
		input, complete, err := f.base.ObserveIndexBlockInput(f.ctx, f.read, request, &budget)
		require.NoError(t, err)
		require.False(t, complete)
		require.Zero(t, input)
		f.stats.TableCnt, f.stats.BlockNumber = 4, 2
		putObject(t, objectio.ObjectEntry{ObjectStats: appendable, CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(2, 0)})
	})
	t.Run("excluded object with unknown block", func(t *testing.T) {
		badWriter := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, true, false, fs)
		block, err := badWriter.WriteBatch(data)
		require.NoError(t, err)
		unknown := index.NewZM(types.T_datetime, 6)
		for _, v := range []types.Datetime{-10, 10} {
			index.UpdateZM(unknown, types.EncodeDatetime(&v))
		}
		block.ColumnMeta(1).SetZoneMap(unknown)
		_, _, err = badWriter.Sync(f.ctx)
		require.NoError(t, err)
		bad := badWriter.GetObjectStats()
		loc := bad.ObjectLocation()
		_, err = objectio.FastLoadObjectMeta(f.ctx, &loc, false, fs)
		require.NoError(t, err)
		putObject(t, objectio.ObjectEntry{ObjectStats: bad, CreateTime: types.BuildTS(1, 0)})
		f.stats.TableCnt, f.stats.BlockNumber = 6, 3
		request := req
		request.Bound = append([]byte(nil), req.Bound...)
		request.Stats.Rows, request.Stats.Blocks = 6, 3
		future := types.DatetimeFromClock(2030, 1, 1, 0, 0, 0, 0).ToTimestamp(time.UTC)
		copy(request.Bound[:], types.EncodeTimestamp(&future))
		budget := engine.MaxIndexPlanningWork
		input, complete, err := f.base.ObserveIndexBlockInput(f.ctx, f.read, request, &budget)
		require.NoError(t, err)
		require.False(t, complete)
		require.Zero(t, input)
		f.stats.TableCnt, f.stats.BlockNumber = 4, 2
		putObject(t, objectio.ObjectEntry{ObjectStats: bad, CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(2, 0)})
	})
	for _, tombstone := range []bool{false, true} {
		name := "unpersisted rows"
		if tombstone {
			name = "unpersisted deletes"
		}
		t.Run(name, func(t *testing.T) {
			// A separate immutable replay state reuses the same real cached object.
			// Keeping each delta isolated prevents one decline guard masking another.
			fresh := logtailreplay.NewPartition("", nil, 7, 10, 98, nil)
			ps, publish := fresh.MutateState()
			func() {
				defer publish()
				ps.UpdateDuration(types.TS{}, types.MaxTs())
				require.NoError(t, ps.HandleObjectEntry(f.ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}, false))
				rows := batch.NewWithSize(4)
				defer rows.Clean(mp)
				rows.Vecs[0], rows.Vecs[1] = vector.NewVec(types.T_Rowid.ToType()), vector.NewVec(types.T_TS.ToType())
				rows.Vecs[2], rows.Vecs[3] = vector.NewVec(types.T_uint64.ToType()), vector.NewVec(types.T_Rowid.ToType())
				id := types.RandomRowid()
				require.NoError(t, vector.AppendFixed(rows.Vecs[0], id, false, mp))
				require.NoError(t, vector.AppendFixed(rows.Vecs[1], types.BuildTS(1, 0), false, mp))
				require.NoError(t, vector.AppendFixed(rows.Vecs[2], uint64(1), false, mp))
				require.NoError(t, vector.AppendFixed(rows.Vecs[3], id, false, mp))
				rows.SetRowCount(1)
				proto, err := batch.BatchToProtoBatch(rows)
				require.NoError(t, err)
				packer := types.NewPacker()
				defer packer.Close()
				if tombstone {
					ps.HandleRowsDelete(f.ctx, proto, packer, mp)
					require.Positive(t, ps.ApproxInMemTombstones())
				} else {
					ps.HandleRowsInsert(f.ctx, proto, 0, packer, mp)
					require.Positive(t, ps.ApproxInMemRows())
				}
			}()
			e.Lock()
			e.partitions[[2]uint64{10, 98}] = fresh
			e.Unlock()
			budget := engine.MaxIndexPlanningWork
			input, complete, err := f.base.ObserveIndexBlockInput(f.ctx, f.read, req, &budget)
			require.NoError(t, err)
			require.False(t, complete)
			require.Zero(t, input)
		})
	}
}
