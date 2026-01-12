// Copyright 2022 Matrix Origin
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
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	log "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

/*
func mockLogtail(table api.TableID, ts timestamp.Timestamp) logtail.TableLogtail {
	return logtail.TableLogtail{
		CkpLocation: "",
		Table:       &table,
		Ts:          &ts,
		Commands: []api.Entry{
			{
				DatabaseId:   table.DbId,
				TableId:      table.TbId,
				TableName:    table.TbName,
				DatabaseName: table.DbName,
			},
		},
	}
}

type logtailer struct {
	tables []api.TableID
}

func mockLocktailer(tables ...api.TableID) taelogtail.Logtailer {
	return &logtailer{
		tables: tables,
	}
}

func (m *logtailer) RangeLogtail(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]logtail.TableLogtail, []func(), error) {
	tails := make([]logtail.TableLogtail, 0, len(m.tables))
	for _, table := range m.tables {
		tails = append(tails, mockLogtail(table, to))
	}
	return tails, nil, nil
}

func (m *logtailer) RegisterCallback(cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error) {
}

func (m *logtailer) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, func(), error) {
	for _, t := range m.tables {
		if t.String() == table.String() {
			return mockLogtail(table, to), nil, nil
		}
	}
	table.DbName = "d1"
	table.TbName = "t2"
	return logtail.TableLogtail{
		CkpLocation: "",
		Table:       &table,
		Ts:          &to,
		Commands: []api.Entry{
			{
				DatabaseId:   table.DbId,
				TableId:      table.TbId,
				TableName:    table.TbName,
				DatabaseName: table.DbName,
			},
		},
	}, nil, nil
}

func (m *logtailer) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	panic("not implemented")
}

func startLogtailServer(
	t *testing.T, address string, rt runtime.Runtime, tables ...api.TableID,
) func() {
	logtailer := mockLocktailer(tables...)

	// construct logtail server
	logtailServer, err := service.NewLogtailServer(
		address, options.NewDefaultLogtailServerCfg(), logtailer, rt, nil,
		service.WithServerCollectInterval(20*time.Millisecond),
		service.WithServerSendTimeout(5*time.Second),
		service.WithServerEnableChecksum(true),
		service.WithServerMaxMessageSize(32+7),
	)
	require.NoError(t, err)

	// start logtail server
	err = logtailServer.Start()
	require.NoError(t, err)

	// generate incremental logtail
	go func() {
		from := timestamp.Timestamp{}

		for {
			now, _ := rt.Clock().Now()

			tails := make([]logtail.TableLogtail, 0, len(tables))
			for _, table := range tables {
				tails = append(tails, mockLogtail(table, now))
			}

			err := logtailServer.NotifyLogtail(from, now, nil, tails...)
			if err != nil {
				return
			}
			from = now

			time.Sleep(10 * time.Millisecond)
		}
	}()

	stop := func() {
		err := logtailServer.Close()
		require.NoError(t, err)
	}
	return stop
}

func runTestWithLogTailServer(t *testing.T, test func(ctx context.Context, e *Engine)) {
	defer leaktest.AfterTest(t)()
	tempDir := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sid := "s1"
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)

	serverAddr := fmt.Sprintf("unix://%s/%d.sock", tempDir, time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(serverAddr))
	clusterClient := &testHAKeeperClient{}
	clusterClient.addTN(log.NormalState, "tn1", serverAddr)

	cluster := clusterservice.NewMOCluster(
		sid,
		clusterClient,
		time.Hour,
	)
	defer cluster.Close()
	cluster.ForceRefresh(true)
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	lk := lockservice.NewLockService(lockservice.Config{
		ServiceID: sid,
	})
	defer lk.Close()
	rt.SetGlobalVariables(runtime.LockService, lk)
	mp, err := mpool.NewMPool(sid, 1024*1024, 0)
	catalog.SetupDefines(sid)
	assert.NoError(t, err)
	sender, err := rpc.NewSender(rpc.Config{}, rt)
	require.NoError(t, err)
	cli := client.NewTxnClient(sid, sender)
	defer cli.Close()
	e := New(
		ctx,
		sid,
		mp,
		nil,
		cli,
		nil,
		nil,
		4,
	)
	e.skipConsume = true
	defer e.Close()

	colexec.NewServer(nil)

	stop := startLogtailServer(t, serverAddr, rt)
	defer stop()

	tw := client.NewTimestampWaiter(runtime.ServiceRuntime(sid).Logger())
	defer tw.Close()
	assert.NoError(t, e.InitLogTailPushModel(ctx, tw))
	defer e.PushClient().Disconnect()
	test(ctx, e)
}
*/

func TestPushClient_LatestLogtailAppliedTime(t *testing.T) {
	var c PushClient
	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	c.receivedLogTailTime.initLogTailTimestamp(tw)
	ts := timestamp.Timestamp{
		PhysicalTime: 100,
		LogicalTime:  200,
	}
	for i := range c.receivedLogTailTime.tList {
		c.receivedLogTailTime.updateTimestamp(i, ts, time.Now())
	}
	assert.Equal(t, ts, c.LatestLogtailAppliedTime())
}

func TestPushClient_GetState(t *testing.T) {
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	c.receivedLogTailTime.initLogTailTimestamp(tw)
	ts := timestamp.Timestamp{
		PhysicalTime: 100,
		LogicalTime:  200,
	}
	for i := range c.receivedLogTailTime.tList {
		c.receivedLogTailTime.updateTimestamp(i, ts, time.Now())
	}
	c.subscribed.setTableSubscribed(1000, 2000)
	st := c.GetState()
	fmt.Println(st)
	assert.Equal(t, ts, st.LatestTS)
	assert.Equal(t, 1, len(st.SubTables))
	table, ok := st.SubTables[2000]
	assert.True(t, ok)
	assert.Equal(t, uint64(1000), table.DBID)
	assert.Equal(t, Subscribed, table.SubState)
}

// should ensure that subscribe and unsubscribe methods are effective.
func TestSubscribedTable(t *testing.T) {
	var subscribeRecord subscribedTable

	subscribeRecord.m = make(map[uint64]*subEntry)
	subscribeRecord.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	assert.Equal(t, 0, len(subscribeRecord.m))

	tbls := []struct {
		db uint64
		tb uint64
	}{
		{db: 0, tb: 1},
		{db: 0, tb: 2},
		{db: 0, tb: 3},
		{db: 0, tb: 4},
		{db: 0, tb: 1},
	}
	for _, tbl := range tbls {
		subscribeRecord.setTableSubscribed(tbl.db, tbl.tb)
		_ = subscribeRecord.eng.GetOrCreateLatestPart(context.Background(), 0, tbl.db, tbl.tb)
	}
	assert.Equal(t, 4, len(subscribeRecord.m))
	assert.Equal(t, true, subscribeRecord.isSubscribed(tbls[0].db, tbls[0].tb))
	for _, tbl := range tbls {
		subscribeRecord.setTableUnsubscribe(tbl.db, tbl.tb)
	}
	assert.Equal(t, 0, len(subscribeRecord.m))
}

func TestBlockInfoSlice(t *testing.T) {
	var data []byte
	s := string(data)
	cnt := len(s)
	assert.Equal(t, 0, cnt)

	data1 := data[:0]
	cnt = len(data1)
	assert.Equal(t, 0, cnt)

	blkSlice := objectio.BlockInfoSlice(data)
	assert.Equal(t, 0, len(blkSlice))
	cnt = blkSlice.Len()
	assert.Equal(t, 0, cnt)

	data = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	data1 = data[:0]
	assert.Equal(t, 0, len(data1))

}

func TestDca(t *testing.T) {
	pClient := &PushClient{}

	signalCnt := 0
	assert.True(t, pClient.dcaTryDelay(true, func() { signalCnt++ }))  // skip for sub response
	assert.True(t, pClient.dcaTryDelay(false, func() { signalCnt++ })) // delay
	pClient.dcaConfirmAndApply()
	assert.Equal(t, 1, signalCnt)
	assert.False(t, pClient.dcaTryDelay(false, func() {})) // skip for finished replay

}

type testHAKeeperClient struct {
	sync.RWMutex
	value log.ClusterDetails
}

func (c *testHAKeeperClient) addTN(state log.NodeState, serviceID, logtailAddr string) {
	c.Lock()
	defer c.Unlock()
	c.value.TNStores = append(c.value.TNStores, log.TNStore{
		UUID:                 serviceID,
		State:                state,
		LogtailServerAddress: logtailAddr,
	})
}

func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (log.ClusterDetails, error) {
	c.Lock()
	defer c.Unlock()
	return c.value, nil
}

func TestGetLogTailServiceAddr(t *testing.T) {
	e := &Engine{}

	t.Run("ok1", func(t *testing.T) {
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		clusterClient.addTN(log.NormalState, "tn1", "a")
		moc.ForceRefresh(true)
		assert.Equal(t, "a", e.getLogTailServiceAddr())
	})

	t.Run("ok2", func(t *testing.T) {
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		go func() {
			time.Sleep(time.Millisecond * 50)
			clusterClient.addTN(log.NormalState, "tn1", "a")
			moc.ForceRefresh(true)
		}()
		assert.Equal(t, "a", e.getLogTailServiceAddr())
	})

	t.Run("fail, empty addr", func(t *testing.T) {
		orig := defaultGetLogTailAddrTimeoutDuration
		defaultGetLogTailAddrTimeoutDuration = time.Millisecond * 50
		defer func() {
			defaultGetLogTailAddrTimeoutDuration = orig
		}()
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		clusterClient.addTN(log.NormalState, "tn1", "")
		moc.ForceRefresh(true)
		assert.Panics(t, func() {
			e.getLogTailServiceAddr()
		})
	})

	t.Run("fail, no tn", func(t *testing.T) {
		orig := defaultGetLogTailAddrTimeoutDuration
		defaultGetLogTailAddrTimeoutDuration = time.Millisecond * 50
		defer func() {
			defaultGetLogTailAddrTimeoutDuration = orig
		}()
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		assert.Panics(t, func() {
			e.getLogTailServiceAddr()
		})
	})
}

func TestWaitServerReady(t *testing.T) {
	temp := os.TempDir()
	origServerTimeout := defaultServerTimeout
	origDialServerInterval := defaultDialServerInterval
	origDialServerTimeout := defaultDialServerTimeout
	defer func() {
		defaultDialServerInterval = origDialServerInterval
		defaultDialServerTimeout = origDialServerTimeout
		defaultServerTimeout = origServerTimeout
	}()

	t.Run("ok", func(t *testing.T) {
		remoteAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
		assert.NoError(t, os.RemoveAll(remoteAddr))
		l, err := net.Listen("unix", remoteAddr)
		assert.NoError(t, err)
		assert.NotNil(t, l)
		defer func() {
			assert.NoError(t, l.Close())
		}()
		waitServerReady(remoteAddr)
	})

	t.Run("retry", func(t *testing.T) {
		defaultDialServerInterval = time.Millisecond * 30
		remoteAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
		assert.NoError(t, os.RemoveAll(remoteAddr))
		c := make(chan struct{})
		go func() {
			time.Sleep(time.Millisecond * 100)
			l, err := net.Listen("unix", remoteAddr)
			assert.NoError(t, err)
			assert.NotNil(t, l)
			defer func() {
				assert.NoError(t, l.Close())
			}()
			<-c
		}()
		waitServerReady(remoteAddr)
		c <- struct{}{}
	})

	t.Run("fail", func(t *testing.T) {
		defaultDialServerInterval = time.Millisecond * 30
		defaultServerTimeout = time.Millisecond * 500
		remoteAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
		assert.NoError(t, os.RemoveAll(remoteAddr))
		assert.Panics(t, func() {
			waitServerReady(remoteAddr)
		})
	})
}

/*
func TestPushClient_UnsubscribeTable(t *testing.T) {
	runTestWithLogTailServer(t, func(ctx context.Context, e *Engine) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pClient := e.PushClient()
		assert.NoError(t, pClient.subscriber.waitReady(ctx))
		e.PushClient().UnsubscribeTable(ctx, 1, 2)
		time.Sleep(time.Second * 2)
	})
}
*/

func TestPushClient_UnusedTableGCTicker(t *testing.T) {
	orig := unsubscribeProcessTicker
	unsubscribeProcessTicker = time.Millisecond
	defer func() {
		unsubscribeProcessTicker = orig
	}()
	t.Run("subscriber nil", func(t *testing.T) {
		var c PushClient
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go c.unusedTableGCTicker(ctx)
		time.Sleep(time.Millisecond * 10)
	})

	t.Run("context done", func(t *testing.T) {
		var c PushClient
		c.subscriber = &logTailSubscriber{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go c.unusedTableGCTicker(ctx)
		time.Sleep(time.Millisecond * 10)
	})
}

func TestPushClient_DoGCUnusedTable(t *testing.T) {
	initFn := func(ctx context.Context, c *PushClient) {
		c.eng = &Engine{}
		c.eng.globalStats = NewGlobalStats(ctx, c.eng, nil)
		c.subscriber = &logTailSubscriber{}
		c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
		c.subscriber.setReady()
		c.subscribed.m = make(map[uint64]*subEntry)
	}

	t.Run("unsubscribe - should not unsub", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c PushClient
		initFn(ctx, &c)
		ent := &subEntry{dbID: catalog.MO_CATALOG_ID, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-time.Hour * 2).UnixNano())
		c.subscribed.m[2] = ent
		c.doGCUnusedTable(ctx)
		assert.Equal(t, 1, len(c.subscribed.m))
		_, ok := c.subscribed.m[2]
		assert.True(t, ok)
	})

	t.Run("unsubscribe - not subscribed state", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c PushClient
		initFn(ctx, &c)
		ent := &subEntry{dbID: 1000, state: Subscribing}
		ent.lastTs.Store(time.Now().Add(-time.Hour * 2).UnixNano())
		c.subscribed.m[200] = ent
		c.doGCUnusedTable(ctx)
		assert.Equal(t, 1, len(c.subscribed.m))
		_, ok := c.subscribed.m[200]
		assert.True(t, ok)
	})

	t.Run("unsubscribe - ok", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c PushClient
		initFn(ctx, &c)
		var tid uint64 = 200
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-time.Hour * 2).UnixNano())
		c.subscribed.m[tid] = ent
		ch := make(chan struct{})
		c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
			go func() {
				c.subscribed.rw.Lock()
				defer c.subscribed.rw.Unlock()
				delete(c.subscribed.m, id.TbId)
				ch <- struct{}{}
			}()
			return nil
		}
		c.doGCUnusedTable(ctx)
		<-ch
		assert.Equal(t, 0, len(c.subscribed.m))
	})

	t.Run("unsubscribe - fail", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c PushClient
		initFn(ctx, &c)
		var tid uint64 = 200
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-time.Hour * 2).UnixNano())
		c.subscribed.m[tid] = ent
		ch := make(chan struct{})
		c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
			go func() {
				ch <- struct{}{}
			}()
			return io.EOF
		}
		c.doGCUnusedTable(ctx)
		<-ch
		assert.Equal(t, 1, len(c.subscribed.m))
		_, ok := c.subscribed.m[tid]
		assert.True(t, ok)
	})
}

func TestPushClient_PartitionStateGCTicker(t *testing.T) {
	orig := gcPartitionStateTicker
	gcPartitionStateTicker = time.Millisecond
	defer func() {
		gcPartitionStateTicker = orig
	}()

	startPStateGCTicker := func(c *PushClient, ctx context.Context) {
		var (
			wait sync.WaitGroup
		)

		wait.Add(1)
		go func() {
			wait.Done()
			c.partitionStateGCTicker(ctx, nil)
		}()

		wait.Wait()
	}

	t.Run("subscriber nil", func(t *testing.T) {
		var c PushClient
		ctx, cancel := context.WithCancel(context.Background())
		startPStateGCTicker(&c, ctx)
		time.Sleep(time.Millisecond * 10)
		cancel()
	})

	t.Run("context done", func(t *testing.T) {
		var c PushClient
		c.subscriber = &logTailSubscriber{}
		ctx, cancel := context.WithCancel(context.Background())

		startPStateGCTicker(&c, ctx)

		time.Sleep(time.Millisecond * 10)
		cancel()
	})
}

func TestPushClient_DoGCPartitionState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var c PushClient
	c.eng = &Engine{
		partitions: map[[2]uint64]*logtailreplay.Partition{},
	}
	c.eng.catalog.Store(cache.NewCatalog())
	ps := logtailreplay.NewPartition("", nil, 0, 0, 300, nil)
	state := ps.Snapshot()
	assert.NotNil(t, state)

	m := mpool.MustNew("test")
	packer := types.NewPacker()
	defer packer.Close()
	rowBatch := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil, m),
		},
		Attrs: []string{"varchar_column"},
	}
	rowBatch.SetRowCount(3)
	ibat, err := fillRandomRowidAndZeroTs(rowBatch, m)
	assert.NoError(t, err)
	state.HandleRowsInsert(ctx, ibat, 0, packer, m)
	assert.Equal(t, 3, state.ApproxInMemRows())
	state.UpdateDuration(types.BuildTS(10, 10), types.MaxTs())
	/*
		state.HandleDataObjectList(ctx, &api.Entry{
			EntryType:  api.Entry_Insert,
			TableName:  "_300_data_meta",
			DatabaseId: 200,
			TableId:    300,
			Bat:        &api.Batch{},
		}, nil, m)
	*/
	c.eng.partitions[[2]uint64{200, 300}] = ps
	c.doGCPartitionState(ctx, c.eng)
	assert.Equal(t, 3, state.ApproxInMemRows())
}

func TestLogTailConnect(t *testing.T) {
	// this case is tested by TestSpeedupAbortAllTxn
}

func TestPushClient_LoadAndConsumeLatestCkp(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		5*time.Minute,
		moerr.NewInternalErrorNoCtx("ut tester"))
	defer cancel()
	sid := "s1"
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)

	clusterClient := &testHAKeeperClient{}
	moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
	runtime.ServiceRuntime(sid).SetGlobalVariables(
		runtime.ClusterService,
		moc,
	)

	lk := lockservice.NewLockService(lockservice.Config{
		ServiceID: sid,
	})
	defer lk.Close()
	rt.SetGlobalVariables(runtime.LockService, lk)

	catalog.SetupDefines(sid)

	// Create Engine and PushClient for testing
	mp, err := mpool.NewMPool(sid, 1024*1024, mpool.NoFixed)
	assert.NoError(t, err)
	sender, err := rpc.NewSender(rpc.Config{}, rt)
	require.NoError(t, err)
	cli := client.NewTxnClient(sid, sender)
	defer cli.Close()

	// Create Engine
	e := New(
		ctx,
		sid,
		mp,
		nil,
		cli,
		nil,
		nil,
		4,
	)
	defer e.Close()

	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	c := &PushClient{
		eng:             e,
		serviceID:       sid,
		timestampWaiter: tw,
		subscriber:      &logTailSubscriber{},
		subscribed:      subscribedTable{m: make(map[uint64]*subEntry)},
	}
	c.receivedLogTailTime.initLogTailTimestamp(tw)

	// Test Case 1.1: Table is subscribed with SubRspReceived state, LazyLoadLatestCkp succeeds
	dbID1, tableID1 := uint64(1), uint64(1)
	c.SetSubscribeState(dbID1, tableID1, SubRspReceived)
	state, err := c.loadAndConsumeLatestCkp(ctx, 0, tableID1, "table1", dbID1, "db1")
	assert.NoError(t, err)
	assert.Equal(t, Subscribed, state)

	// Test Case 1.2: Table is subscribed with Subscribed state
	dbID12, tableID12 := uint64(12), uint64(12)
	c.SetSubscribeState(dbID12, tableID12, Subscribed)
	state, err = c.loadAndConsumeLatestCkp(ctx, 0, tableID12, "table12", dbID12, "db12")
	assert.NoError(t, err)
	assert.Equal(t, Subscribed, state)

	// Test Case 2.1: Table is not subscribed, subscriber is not ready
	dbID21, tableID21 := uint64(21), uint64(21)
	c.subscriber = &logTailSubscriber{} // not ready state
	state, err = c.loadAndConsumeLatestCkp(ctx, 0, tableID21, "table21", dbID21, "db21")
	assert.Error(t, err)
	assert.Equal(t, Unsubscribed, state)

	// Test Case 2.2: Table is not subscribed, subscriber is ready and subscribeTable succeeds
	dbID22, tableID22 := uint64(22), uint64(22)
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	c.subscriber.sendSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}
	state, err = c.loadAndConsumeLatestCkp(ctx, 0, tableID22, "table22", dbID22, "db22")
	assert.NoError(t, err)
	assert.Equal(t, Subscribing, state)

	// Test Case 2.3: Table is not subscribed, subscriber is ready but subscribeTable fails
	dbID23, tableID23 := uint64(23), uint64(23)
	c.subscriber.sendSubscribe = func(ctx context.Context, id api.TableID) error {
		return moerr.NewInternalErrorNoCtx("mock subscribe error")
	}
	state, err = c.loadAndConsumeLatestCkp(ctx, 0, tableID23, "table23", dbID23, "db23")
	assert.Error(t, err)
	assert.Equal(t, Unsubscribed, state)

	// Test Case 3: Table exists but has other state (Unsubscribing)
	dbID3, tableID3 := uint64(3), uint64(3)
	c.SetSubscribeState(dbID3, tableID3, Unsubscribing)
	state, err = c.loadAndConsumeLatestCkp(ctx, 0, tableID3, "table3", dbID3, "db3")
	assert.NoError(t, err)
	assert.Equal(t, Unsubscribing, state)
}

// TestRoutineControllerSendMethods verifies that routine controller send methods work correctly
func TestRoutineControllerSendMethods(t *testing.T) {
	t.Run("routineController initialization with pools", func(t *testing.T) {
		rc := &routineController{
			routineId:  1,
			closeChan:  make(chan bool, 1), // Buffered channel to avoid blocking
			signalChan: make(chan routineControlCmd, 5),
			cmdLogPool: sync.Pool{
				New: func() any {
					return &cmdToConsumeLog{}
				},
			},
			cmdTimePool: sync.Pool{
				New: func() any {
					return &cmdToUpdateTime{}
				},
			},
		}

		// Verify that existing pools are properly initialized
		assert.NotNil(t, rc.cmdLogPool.New)
		assert.NotNil(t, rc.cmdTimePool.New)

		// Ensure cleanup to avoid goroutine leaks
		defer func() {
			select {
			case rc.closeChan <- true:
			default:
			}
			// Drain signalChan to prevent blocking
			for len(rc.signalChan) > 0 {
				<-rc.signalChan
			}
		}()
	})

	t.Run("sendSubscribeResponse creates commands correctly", func(t *testing.T) {
		rc := &routineController{
			routineId:  1,
			closeChan:  make(chan bool, 1), // Buffered channel to avoid blocking
			signalChan: make(chan routineControlCmd, 5),
		}

		// Ensure cleanup
		defer func() {
			select {
			case rc.closeChan <- true:
			default:
			}
			// Drain signalChan to prevent blocking
			for len(rc.signalChan) > 0 {
				<-rc.signalChan
			}
		}()

		// Call sendSubscribeResponse multiple times
		ctx := context.Background()
		for i := 0; i < 3; i++ {
			rc.sendSubscribeResponse(ctx, &logtail.SubscribeResponse{}, time.Now())
		}

		// Verify that commands were added to the channel
		assert.Equal(t, 3, len(rc.signalChan))

		// Drain the channel and verify the commands
		for i := 0; i < 3; i++ {
			cmd := <-rc.signalChan
			assert.IsType(t, &cmdToConsumeSub{}, cmd)
		}
	})

	t.Run("sendUnSubscribeResponse creates commands correctly", func(t *testing.T) {
		rc := &routineController{
			routineId:  1,
			closeChan:  make(chan bool, 1), // Buffered channel to avoid blocking
			signalChan: make(chan routineControlCmd, 5),
		}

		// Ensure cleanup
		defer func() {
			select {
			case rc.closeChan <- true:
			default:
			}
			// Drain signalChan to prevent blocking
			for len(rc.signalChan) > 0 {
				<-rc.signalChan
			}
		}()

		// Call sendUnSubscribeResponse multiple times
		for i := 0; i < 3; i++ {
			rc.sendUnSubscribeResponse(&logtail.UnSubscribeResponse{}, time.Now())
		}

		// Verify that commands were added to the channel
		assert.Equal(t, 3, len(rc.signalChan))

		// Drain the channel and verify the commands
		for i := 0; i < 3; i++ {
			cmd := <-rc.signalChan
			assert.IsType(t, &cmdToConsumeUnSub{}, cmd)
		}
	})

	t.Run("sendTableLogTail creates commands correctly", func(t *testing.T) {
		rc := &routineController{
			routineId:  1,
			closeChan:  make(chan bool, 1), // Buffered channel to avoid blocking
			signalChan: make(chan routineControlCmd, 5),
			cmdLogPool: sync.Pool{
				New: func() any {
					return &cmdToConsumeLog{}
				},
			},
		}

		// Ensure cleanup
		defer func() {
			select {
			case rc.closeChan <- true:
			default:
			}
			// Drain signalChan to prevent blocking
			for len(rc.signalChan) > 0 {
				<-rc.signalChan
			}
		}()

		// Call sendTableLogTail multiple times
		for i := 0; i < 3; i++ {
			rc.sendTableLogTail(logtail.TableLogtail{}, time.Now())
		}

		// Verify that commands were added to the channel
		assert.Equal(t, 3, len(rc.signalChan))

		// Drain the channel and verify the commands
		for i := 0; i < 3; i++ {
			cmd := <-rc.signalChan
			assert.IsType(t, &cmdToConsumeLog{}, cmd)
		}
	})

	t.Run("updateTimeFromT creates commands correctly", func(t *testing.T) {
		rc := &routineController{
			routineId:  1,
			closeChan:  make(chan bool, 1), // Buffered channel to avoid blocking
			signalChan: make(chan routineControlCmd, 5),
			cmdTimePool: sync.Pool{
				New: func() any {
					return &cmdToUpdateTime{}
				},
			},
		}

		// Ensure cleanup
		defer func() {
			select {
			case rc.closeChan <- true:
			default:
			}
			// Drain signalChan to prevent blocking
			for len(rc.signalChan) > 0 {
				<-rc.signalChan
			}
		}()

		// Call updateTimeFromT multiple times
		for i := 0; i < 3; i++ {
			rc.updateTimeFromT(timestamp.Timestamp{
				PhysicalTime: 123456,
				LogicalTime:  1,
				NodeID:       1,
			}, time.Now())
		}

		// Verify that commands were added to the channel
		assert.Equal(t, 3, len(rc.signalChan))

		// Drain the channel and verify the commands
		for i := 0; i < 3; i++ {
			cmd := <-rc.signalChan
			assert.IsType(t, &cmdToUpdateTime{}, cmd)
		}
	})
}

// TestCommandActions verifies that command actions can be created and their basic functionality works
func TestCommandActions(t *testing.T) {
	t.Run("cmdToConsumeSub creation and basic properties", func(t *testing.T) {
		// Test that we can create a cmdToConsumeSub command
		log := &logtail.SubscribeResponse{
			Logtail: logtail.TableLogtail{
				// Set some basic properties for TableLogtail if needed
			},
		}
		receiveAt := time.Now()

		cmd := &cmdToConsumeSub{
			log:       log,
			receiveAt: receiveAt,
		}

		// Verify the command has the correct properties
		assert.Equal(t, log, cmd.log)
		assert.Equal(t, receiveAt, cmd.receiveAt)
	})

	t.Run("cmdToConsumeUnSub creation and basic properties", func(t *testing.T) {
		// Test that we can create a cmdToConsumeUnSub command
		log := &logtail.UnSubscribeResponse{
			Table: &api.TableID{
				DbId: 1,
				TbId: 2,
			},
		}
		receiveAt := time.Now()

		cmd := &cmdToConsumeUnSub{
			log:       log,
			receiveAt: receiveAt,
		}

		// Verify the command has the correct properties
		assert.Equal(t, log, cmd.log)
		assert.Equal(t, receiveAt, cmd.receiveAt)
	})

}

// =============================================================================
// Tests for subscribedTable with RWMutex and atomic timestamp (Solution E)
// =============================================================================

// TestSubEntry_AtomicTimestamp tests atomic timestamp operations
func TestSubEntry_AtomicTimestamp(t *testing.T) {
	t.Run("basic atomic operations", func(t *testing.T) {
		ent := &subEntry{
			dbID:  100,
			state: Subscribed,
		}
		now := time.Now().UnixNano()
		ent.lastTs.Store(now)

		assert.Equal(t, now, ent.lastTs.Load())
		assert.Equal(t, uint64(100), ent.dbID)
		assert.Equal(t, Subscribed, ent.state)
	})

	t.Run("concurrent atomic updates", func(t *testing.T) {
		ent := &subEntry{
			dbID:  100,
			state: Subscribed,
		}
		ent.lastTs.Store(time.Now().UnixNano())

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					ent.lastTs.Store(time.Now().UnixNano())
					_ = ent.lastTs.Load()
				}
			}()
		}
		wg.Wait()
		// No race condition should occur
	})
}

// TestSubscribedTable_RWMutex tests RWMutex behavior
func TestSubscribedTable_RWMutex(t *testing.T) {
	t.Run("concurrent reads", func(t *testing.T) {
		s := &subscribedTable{
			m: make(map[uint64]*subEntry),
		}
		// Add some entries
		for i := uint64(0); i < 10; i++ {
			ent := &subEntry{dbID: i, state: Subscribed}
			ent.lastTs.Store(time.Now().UnixNano())
			s.m[i] = ent
		}

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					s.rw.RLock()
					for _, ent := range s.m {
						_ = ent.state
						_ = ent.lastTs.Load()
					}
					s.rw.RUnlock()
				}
			}()
		}
		wg.Wait()
	})

	t.Run("read-write contention", func(t *testing.T) {
		s := &subscribedTable{
			m: make(map[uint64]*subEntry),
		}
		ent := &subEntry{dbID: 1, state: Subscribed}
		ent.lastTs.Store(time.Now().UnixNano())
		s.m[1] = ent

		var wg sync.WaitGroup
		var readCount atomic.Int32

		// Readers
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					s.rw.RLock()
					if e, ok := s.m[1]; ok {
						_ = e.state
						e.lastTs.Store(time.Now().UnixNano())
						readCount.Add(1)
					}
					s.rw.RUnlock()
				}
			}()
		}

		// Writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.rw.Lock()
				s.m[1].state = Subscribed
				s.rw.Unlock()
			}
		}()

		wg.Wait()
		// Verify readers ran
		assert.True(t, readCount.Load() > 0, "readers should have executed")
	})
}

// TestIsSubscribed_Concurrent tests concurrent isSubscribed calls
func TestIsSubscribed_Concurrent(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	// Setup subscribed table
	ent := &subEntry{dbID: 1, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, ok, state := c.isSubscribed(ctx, 0, 1, 100)
				assert.True(t, ok)
				assert.Equal(t, Subscribed, state)
			}
		}()
	}
	wg.Wait()
}

// TestIsSubscribed_StateChangeAfterPartitionCreate tests that isSubscribed correctly
// handles concurrent state changes and maintains consistency
func TestIsSubscribed_StateChangeAfterPartitionCreate(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	ent := &subEntry{dbID: 1, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup
	var mu sync.Mutex
	okCount, failCount := 0, 0

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, ok, state := c.isSubscribed(ctx, 0, 1, 100)
				mu.Lock()
				if ok {
					assert.Equal(t, Subscribed, state)
					okCount++
				} else {
					assert.Contains(t, []SubscribeState{Unsubscribing, Unsubscribed}, state)
					failCount++
				}
				mu.Unlock()
			}
		}()
	}

	// State changer - toggle between Subscribed and Unsubscribing
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			c.subscribed.rw.Lock()
			if c.subscribed.m[100].state == Subscribed {
				c.subscribed.m[100].state = Unsubscribing
			} else {
				c.subscribed.m[100].state = Subscribed
			}
			c.subscribed.rw.Unlock()
		}
	}()

	wg.Wait()

	t.Logf("Results: ok=%d, fail=%d", okCount, failCount)
	// Verify total count and that both ok and fail states are valid
	assert.Equal(t, 100, okCount+failCount, "should have 100 total results")
}

// TestIsSubscribed_TimestampSampling tests that timestamp is only updated when > 1 minute old
func TestIsSubscribed_TimestampSampling(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	t.Run("recent timestamp not updated", func(t *testing.T) {
		now := time.Now().UnixNano()
		ent := &subEntry{dbID: 1, state: Subscribed}
		ent.lastTs.Store(now)
		c.subscribed.m[100] = ent

		c.isSubscribed(ctx, 0, 1, 100)

		// Timestamp should not change (within 1 minute)
		assert.Equal(t, now, ent.lastTs.Load())
	})

	t.Run("old timestamp updated", func(t *testing.T) {
		oldTime := time.Now().Add(-2 * time.Minute).UnixNano()
		ent := &subEntry{dbID: 1, state: Subscribed}
		ent.lastTs.Store(oldTime)
		c.subscribed.m[101] = ent

		c.isSubscribed(ctx, 0, 1, 101)

		// Timestamp should be updated
		assert.True(t, ent.lastTs.Load() > oldTime)
	})
}

// TestDoGCUnusedTable_ConcurrentAccess tests GC with concurrent reads
func TestDoGCUnusedTable_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	// Track unsubscribe calls
	var unsubCalls sync.Map
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		unsubCalls.Store(id.TbId, true)
		return nil
	}

	// Add old entries that should be GC'd
	for i := uint64(200); i < 210; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		c.subscribed.m[i] = ent
	}

	// Add recent entries that should NOT be GC'd
	for i := uint64(300); i < 310; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().UnixNano())
		c.subscribed.m[i] = ent
	}

	var wg sync.WaitGroup

	// Concurrent readers
	stop := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c.subscribed.rw.RLock()
					for _, ent := range c.subscribed.m {
						_ = ent.state
						_ = ent.lastTs.Load()
					}
					c.subscribed.rw.RUnlock()
				}
			}
		}()
	}

	// Run GC
	c.doGCUnusedTable(ctx)

	close(stop)
	wg.Wait()

	// Verify old entries were marked for unsubscribe
	for i := uint64(200); i < 210; i++ {
		_, called := unsubCalls.Load(i)
		assert.True(t, called, "table %d should be unsubscribed", i)
	}

	// Verify recent entries were NOT unsubscribed
	for i := uint64(300); i < 310; i++ {
		_, called := unsubCalls.Load(i)
		assert.False(t, called, "table %d should NOT be unsubscribed", i)
	}
}

// TestDoGCUnusedTable_RaceWithIsSubscribed tests that GC Phase 2 re-check
// prevents unsubscribing recently accessed tables
func TestDoGCUnusedTable_RaceWithIsSubscribed(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var unsubCalled atomic.Int32
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		unsubCalled.Add(1)
		return nil
	}

	// Add entry with old timestamp
	ent := &subEntry{dbID: 1000, state: Subscribed}
	ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	c.subscribed.m[500] = ent

	var wg sync.WaitGroup

	// Concurrently update timestamp (simulating isSubscribed calls)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				ent.lastTs.Store(time.Now().UnixNano())
			}
		}()
	}

	// Run GC concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.doGCUnusedTable(ctx)
	}()

	wg.Wait()

	// Either the table was unsubscribed (GC won the race) or not (timestamp update won)
	// Both outcomes are valid - the test verifies no crash/panic occurs
	t.Logf("Unsubscribe called: %d times", unsubCalled.Load())
}

// TestSubscribedTable_StateTransitions tests state machine transitions
func TestSubscribedTable_StateTransitions(t *testing.T) {
	s := &subscribedTable{
		m:   make(map[uint64]*subEntry),
		eng: &Engine{partitions: make(map[[2]uint64]*logtailreplay.Partition), globalStats: &GlobalStats{}},
	}

	t.Run("subscribe flow", func(t *testing.T) {
		// Unsubscribed -> Subscribing -> SubRspReceived -> Subscribed
		s.m[1] = &subEntry{dbID: 100, state: Subscribing}

		s.setTableSubRspReceived(100, 1)
		assert.Equal(t, SubRspReceived, s.m[1].state)

		s.setTableSubscribed(100, 1)
		assert.Equal(t, Subscribed, s.m[1].state)
	})

	t.Run("unsubscribe flow", func(t *testing.T) {
		ent := &subEntry{dbID: 100, state: Subscribed}
		ent.lastTs.Store(time.Now().UnixNano())
		s.m[2] = ent

		// Mark as unsubscribing
		s.rw.Lock()
		s.m[2].state = Unsubscribing
		s.rw.Unlock()

		assert.Equal(t, Unsubscribing, s.m[2].state)

		// Complete unsubscribe
		s.setTableUnsubscribe(100, 2)
		_, exists := s.m[2]
		assert.False(t, exists)
	})

	t.Run("table not exist", func(t *testing.T) {
		s.setTableSubNotExist(100, 3)
		assert.Equal(t, SubRspTableNotExist, s.m[3].state)
	})
}

// TestGetState_Concurrent tests concurrent GetState calls
func TestGetState_Concurrent(t *testing.T) {
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	c.receivedLogTailTime.initLogTailTimestamp(tw)

	// Add entries
	for i := uint64(0); i < 100; i++ {
		ent := &subEntry{dbID: i, state: Subscribed}
		ent.lastTs.Store(time.Now().UnixNano())
		c.subscribed.m[i] = ent
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				state := c.GetState()
				assert.Equal(t, 100, len(state.SubTables))
			}
		}()
	}
	wg.Wait()
}

// TestIsSubscribed_NotFound tests isSubscribed with non-existent table
func TestIsSubscribed_NotFound(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	_, ok, state := c.isSubscribed(ctx, 0, 1, 999)
	assert.False(t, ok)
	assert.Equal(t, Unsubscribed, state)
}

// TestIsSubscribed_DifferentStates tests isSubscribed with different states
func TestIsSubscribed_DifferentStates(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	states := []SubscribeState{Subscribing, SubRspReceived, Unsubscribing, SubRspTableNotExist}
	for i, s := range states {
		ent := &subEntry{dbID: 1, state: s}
		ent.lastTs.Store(time.Now().UnixNano())
		c.subscribed.m[uint64(i)] = ent
	}

	for i, expectedState := range states {
		_, ok, state := c.isSubscribed(ctx, 0, 1, uint64(i))
		assert.False(t, ok)
		assert.Equal(t, expectedState, state)
	}
}

// TestConcurrentSubscribeUnsubscribe tests concurrent subscribe and unsubscribe operations
func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	s := &subscribedTable{
		m:   make(map[uint64]*subEntry),
		eng: &Engine{partitions: make(map[[2]uint64]*logtailreplay.Partition), globalStats: &GlobalStats{}},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		tblId := uint64(i)

		// Subscribe
		go func() {
			defer wg.Done()
			s.setTableSubscribed(100, tblId)
		}()

		// Unsubscribe (may or may not find the entry)
		go func() {
			defer wg.Done()
			time.Sleep(time.Microsecond * 10)
			s.setTableUnsubscribe(100, tblId)
		}()
	}
	wg.Wait()
}

// TestDoGCUnusedTable_ProtectedTables tests that system tables are not GC'd
func TestDoGCUnusedTable_ProtectedTables(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var unsubCalled atomic.Bool
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		unsubCalled.Store(true)
		return nil
	}

	// Add system table (should be protected)
	ent := &subEntry{dbID: catalog.MO_CATALOG_ID, state: Subscribed}
	ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	c.subscribed.m[catalog.MO_DATABASE_ID] = ent

	c.doGCUnusedTable(ctx)

	assert.False(t, unsubCalled.Load(), "system table should not be unsubscribed")
}

// TestDoGCUnusedTable_NonSubscribedState tests that non-Subscribed entries are skipped
func TestDoGCUnusedTable_NonSubscribedState(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var unsubCalled atomic.Bool
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		unsubCalled.Store(true)
		return nil
	}

	// Add entry with Subscribing state (should be skipped)
	ent := &subEntry{dbID: 1000, state: Subscribing}
	ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	c.subscribed.m[600] = ent

	c.doGCUnusedTable(ctx)

	assert.False(t, unsubCalled.Load(), "non-Subscribed entry should not be unsubscribed")
}

// =============================================================================
// Additional tests for edge cases and complex concurrent scenarios
// =============================================================================

// TestGC_ConcurrentWithSubscribe tests GC running while new subscriptions are being added
func TestGC_ConcurrentWithSubscribe(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var gcCount atomic.Int32
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		gcCount.Add(1)
		return nil
	}

	// Add old entries
	for i := uint64(100); i < 150; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		c.subscribed.m[i] = ent
	}

	var wg sync.WaitGroup

	// GC goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			c.doGCUnusedTable(ctx)
			time.Sleep(time.Microsecond * 100)
		}
	}()

	// Subscribe goroutine - adding new entries
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(200); i < 300; i++ {
			c.subscribed.setTableSubscribed(2000, i)
			time.Sleep(time.Microsecond * 10)
		}
	}()

	wg.Wait()

	// New subscriptions should not be affected by GC
	c.subscribed.rw.RLock()
	newCount := 0
	for id := range c.subscribed.m {
		if id >= 200 && id < 300 {
			newCount++
		}
	}
	c.subscribed.rw.RUnlock()
	assert.Equal(t, 100, newCount, "all new subscriptions should exist")
}

// TestGC_ConcurrentWithUnsubscribe tests GC running while unsubscriptions are happening
func TestGC_ConcurrentWithUnsubscribe(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	// Add entries
	for i := uint64(100); i < 200; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		c.subscribed.m[i] = ent
	}

	var wg sync.WaitGroup

	// GC goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			c.doGCUnusedTable(ctx)
			time.Sleep(time.Microsecond * 50)
		}
	}()

	// Manual unsubscribe goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(100); i < 150; i++ {
			c.subscribed.setTableUnsubscribe(1000, i)
			time.Sleep(time.Microsecond * 10)
		}
	}()

	wg.Wait()
	// No panic or race condition should occur
}

// TestTripleConcurrent_Subscribe_Unsubscribe_Read tests three-way concurrent operations
func TestTripleConcurrent_Subscribe_Unsubscribe_Read(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := uint64(0); j < 100; j++ {
						c.isSubscribed(ctx, 0, 1000, j)
					}
				}
			}
		}()
	}

	// Subscribe goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := uint64(0); j < 50; j++ {
						c.subscribed.setTableSubscribed(1000, j+uint64(offset*50))
					}
				}
			}
		}(i)
	}

	// Unsubscribe goroutines
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := uint64(0); j < 30; j++ {
						c.subscribed.setTableUnsubscribe(1000, j+uint64(offset*30))
					}
				}
			}
		}(i)
	}

	// Run for a short time
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	// No panic or race condition should occur
}

// TestGC_TimestampUpdateBetweenPhases tests the race between GC phases and isSubscribed
func TestGC_TimestampUpdateBetweenPhases(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var gcCalled atomic.Int32
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		gcCalled.Add(1)
		return nil
	}

	// Add entry with old timestamp
	ent := &subEntry{dbID: 1000, state: Subscribed}
	ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	c.subscribed.m[999] = ent

	// First GC - should try to unsubscribe (timestamp is old)
	c.doGCUnusedTable(ctx)
	firstGCCalls := gcCalled.Load()
	assert.Equal(t, int32(1), firstGCCalls, "first GC should call sendUnSubscribe")

	// Now update timestamp to simulate active access
	ent.lastTs.Store(time.Now().UnixNano())
	// Reset state to Subscribed (simulating the entry was re-subscribed or GC was reverted)
	ent.state = Subscribed

	// Second GC - should NOT try to unsubscribe (timestamp is recent)
	c.doGCUnusedTable(ctx)
	secondGCCalls := gcCalled.Load()
	assert.Equal(t, firstGCCalls, secondGCCalls, "second GC should not call sendUnSubscribe for recently accessed table")
}

// TestEmptyMap_Operations tests operations on empty map
func TestEmptyMap_Operations(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	// GC on empty map
	c.doGCUnusedTable(ctx)

	// isSubscribed on empty map
	_, ok, state := c.isSubscribed(ctx, 0, 1, 999)
	assert.False(t, ok)
	assert.Equal(t, Unsubscribed, state)

	// GetState on empty map
	st := c.GetState()
	assert.Equal(t, 0, len(st.SubTables))

	// IsSubscribed on empty map
	assert.False(t, c.IsSubscribed(999))
}

// TestHighConcurrency_StressTest tests under high concurrency load
func TestHighConcurrency_StressTest(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	var wg sync.WaitGroup
	const goroutines = 10
	const operations = 100

	// Mixed operations
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				tblId := uint64((id*operations + j) % 50)
				switch j % 5 {
				case 0:
					c.subscribed.setTableSubscribed(1000, tblId)
				case 1:
					c.isSubscribed(ctx, 0, 1000, tblId)
				case 2:
					c.IsSubscribed(tblId)
				case 3:
					c.GetState()
				case 4:
					c.subscribed.setTableUnsubscribe(1000, tblId)
				}
			}
		}(i)
	}

	wg.Wait()
	// No panic or race condition should occur
}

// TestGC_FailedUnsubscribe tests GC behavior when unsubscribe fails
func TestGC_FailedUnsubscribe(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var failCount atomic.Int32
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		failCount.Add(1)
		return moerr.NewInternalErrorNoCtx("network error")
	}

	// Add old entries
	for i := uint64(100); i < 110; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		c.subscribed.m[i] = ent
	}

	c.doGCUnusedTable(ctx)

	// All entries should have been attempted
	assert.Equal(t, int32(10), failCount.Load())

	// Entries should be reverted to Subscribed (for retry) since all failed
	c.subscribed.rw.RLock()
	for i := uint64(100); i < 110; i++ {
		assert.Equal(t, Subscribed, c.subscribed.m[i].state)
	}
	c.subscribed.rw.RUnlock()
}

// TestStateTransition_InvalidTransitions tests handling of invalid state transitions
func TestStateTransition_InvalidTransitions(t *testing.T) {
	s := &subscribedTable{
		m:   make(map[uint64]*subEntry),
		eng: &Engine{partitions: make(map[[2]uint64]*logtailreplay.Partition), globalStats: &GlobalStats{}},
	}

	// Set to Subscribing
	s.m[1] = &subEntry{dbID: 100, state: Subscribing}

	// Try to unsubscribe while Subscribing (should still work - just deletes)
	s.setTableUnsubscribe(100, 1)
	_, exists := s.m[1]
	assert.False(t, exists)

	// Set to Unsubscribing
	ent := &subEntry{dbID: 100, state: Unsubscribing}
	ent.lastTs.Store(time.Now().UnixNano())
	s.m[2] = ent

	// isSubscribed should return false for Unsubscribing state
	ok := s.isSubscribed(100, 2)
	assert.False(t, ok)
}

// TestConcurrent_GC_Subscribe_Unsubscribe_Read tests all four operations concurrently
func TestConcurrent_GC_Subscribe_Unsubscribe_Read(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	// Pre-populate with some old entries for GC
	for i := uint64(0); i < 50; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		c.subscribed.m[i] = ent
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// GC goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.doGCUnusedTable(ctx)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Subscribe goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(100); i < 200; i++ {
					c.subscribed.setTableSubscribed(2000, i)
				}
			}
		}
	}()

	// Unsubscribe goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(100); i < 150; i++ {
					c.subscribed.setTableUnsubscribe(2000, i)
				}
			}
		}
	}()

	// Read goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := uint64(0); j < 200; j++ {
						c.isSubscribed(ctx, 0, 1000, j)
						c.IsSubscribed(j)
					}
				}
			}
		}()
	}

	// GetState goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = c.GetState()
			}
		}
	}()

	// Run for a short time
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	// No panic or race condition should occur
}

// TestIsSubscribed_PartitionCreation tests that partition is created correctly
func TestIsSubscribed_PartitionCreation(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	// Add subscribed entry
	ent := &subEntry{dbID: 1, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	// Call isSubscribed
	ps, ok, state := c.isSubscribed(ctx, 0, 1, 100)
	assert.True(t, ok)
	assert.Equal(t, Subscribed, state)
	assert.NotNil(t, ps)

	// Verify partition was created
	c.eng.Lock()
	_, exists := c.eng.partitions[[2]uint64{1, 100}]
	c.eng.Unlock()
	assert.True(t, exists)
}

// =============================================================================
// Tests for complex write operations that require Lock (not RLock)
// These are the reasons why original implementation used Mutex instead of RWMutex
// =============================================================================

// TestToSubIfUnsubscribed_Concurrent tests concurrent toSubIfUnsubscribed calls
// This operation does read-then-write, requiring exclusive lock
func TestToSubIfUnsubscribed_Concurrent(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	// Mock sendSubscribe to avoid nil pointer
	c.subscriber.sendSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	var wg sync.WaitGroup
	var successCount atomic.Int32

	// Multiple goroutines trying to subscribe the same table
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, err := c.toSubIfUnsubscribed(ctx, 1000, 100)
			if err == nil && (state == Subscribing || state == Subscribed) {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// All should succeed (either got Subscribing or found already Subscribed)
	assert.Equal(t, int32(10), successCount.Load())

	// Table should exist in map
	c.subscribed.rw.RLock()
	_, exists := c.subscribed.m[100]
	c.subscribed.rw.RUnlock()
	assert.True(t, exists)
}

// TestIsNotSubscribing_Concurrent tests concurrent isNotSubscribing calls
func TestIsNotSubscribing_Concurrent(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	// Pre-set table to Subscribing state
	c.subscribed.m[100] = &subEntry{dbID: 1000, state: Subscribing}

	var wg sync.WaitGroup
	var subscribingCount atomic.Int32

	// Multiple goroutines checking if table is subscribing
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, state, _ := c.isNotSubscribing(ctx, 1000, 100)
			if !ok && state == Subscribing {
				subscribingCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// All should see Subscribing state
	assert.Equal(t, int32(20), subscribingCount.Load())
}

// TestIsNotUnsubscribing_Concurrent tests concurrent isNotUnsubscribing calls
func TestIsNotUnsubscribing_Concurrent(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	// Pre-set table to Unsubscribing state
	ent := &subEntry{dbID: 1000, state: Unsubscribing}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup
	var unsubscribingCount atomic.Int32

	// Multiple goroutines checking if table is unsubscribing
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, state, _ := c.isNotUnsubscribing(ctx, 1000, 100)
			if !ok && state == Unsubscribing {
				unsubscribingCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// All should see Unsubscribing state
	assert.Equal(t, int32(20), unsubscribingCount.Load())
}

// TestLoadAndConsumeLatestCkp_Concurrent tests concurrent loadAndConsumeLatestCkp calls
func TestLoadAndConsumeLatestCkp_Concurrent(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	// Pre-set table to SubRspReceived state
	ent := &subEntry{dbID: 1000, state: SubRspReceived}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup
	var successCount atomic.Int32

	// Multiple goroutines trying to consume checkpoint
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, err := c.loadAndConsumeLatestCkp(ctx, 0, 100, "test", 1000, "db")
			if err == nil && state == Subscribed {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// All should succeed
	assert.Equal(t, int32(10), successCount.Load())

	// State should be Subscribed
	c.subscribed.rw.RLock()
	assert.Equal(t, Subscribed, c.subscribed.m[100].state)
	c.subscribed.rw.RUnlock()
}

// TestConcurrent_ReadWrite_SameTable tests concurrent read and write on same table
// This is the core reason why we need careful locking
func TestConcurrent_ReadWrite_SameTable(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	// Pre-set table
	ent := &subEntry{dbID: 1000, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: isSubscribed (RLock)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.isSubscribed(ctx, 0, 1000, 100)
			}
		}
	}()

	// Reader: IsSubscribed (RLock)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.IsSubscribed(100)
			}
		}
	}()

	// Writer: state changes (Lock)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			select {
			case <-stop:
				return
			default:
				// Simulate state transitions
				c.subscribed.rw.Lock()
				if ent, ok := c.subscribed.m[100]; ok {
					if ent.state == Subscribed {
						ent.state = Unsubscribing
					} else {
						ent.state = Subscribed
					}
				}
				c.subscribed.rw.Unlock()
				time.Sleep(time.Microsecond * 100)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	// No race condition should occur
}

// TestLockUnlockRelock_Pattern tests the unlock-then-relock pattern in toSubIfUnsubscribed
// This is a tricky pattern that was one reason for using Mutex
func TestLockUnlockRelock_Pattern(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	// NOT setting ready - this will trigger the unlock-relock pattern
	c.subscriber.sendSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	var wg sync.WaitGroup

	// Multiple goroutines hitting the unlock-relock path
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// This will try to wait for subscriber ready, triggering unlock-relock
			c.toSubIfUnsubscribed(ctx, 1000, uint64(id))
		}(i)
	}

	// Another goroutine doing reads
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				c.subscribed.rw.RLock()
				_ = len(c.subscribed.m)
				c.subscribed.rw.RUnlock()
			}
		}
	}()

	// Set subscriber ready after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.subscriber.setReady()
	}()

	wg.Wait()
	// No deadlock should occur
}

// TestAllWriteOperations_Concurrent tests all write operations running concurrently
func TestAllWriteOperations_Concurrent(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		return nil
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// setTableSubscribed
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(0); i < 50; i++ {
					c.subscribed.setTableSubscribed(1000, i)
				}
			}
		}
	}()

	// setTableSubRspReceived
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(50); i < 100; i++ {
					c.subscribed.setTableSubRspReceived(1000, i)
				}
			}
		}
	}()

	// setTableUnsubscribe
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(0); i < 30; i++ {
					c.subscribed.setTableUnsubscribe(1000, i)
				}
			}
		}
	}()

	// clearTable
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(30); i < 50; i++ {
					c.subscribed.clearTable(1000, i)
				}
			}
		}
	}()

	// isSubscribed (read)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := uint64(0); i < 100; i++ {
					c.isSubscribed(ctx, 0, 1000, i)
				}
			}
		}
	}()

	// GetState (read)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = c.GetState()
			}
		}
	}()

	// doGCUnusedTable
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.doGCUnusedTable(ctx)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	// No race condition or deadlock should occur
}

// TestIsSubscribed_ShortCriticalSection tests that isSubscribed maintains atomicity
func TestIsSubscribed_Atomicity(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}
	c.subscribed.eng = c.eng

	// Add subscribed entry
	ent := &subEntry{dbID: 1, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup

	// Reader: call isSubscribed
	wg.Add(1)
	go func() {
		defer wg.Done()
		ps, ok, state := c.isSubscribed(ctx, 0, 1, 100)
		// Should get valid result
		assert.True(t, ok)
		assert.Equal(t, Subscribed, state)
		assert.NotNil(t, ps)
	}()

	wg.Wait()
}

// TestGC_FailureRetry tests that GC reverts state on failure for retry
func TestGC_FailureRetry(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	var callCount atomic.Int32
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		callCount.Add(1)
		return moerr.NewInternalErrorNoCtx("network error")
	}

	// Add old entry
	ent := &subEntry{dbID: 1000, state: Subscribed}
	ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	c.subscribed.m[100] = ent

	// First GC round - should fail and revert state with backoff
	c.doGCUnusedTable(ctx)
	assert.Equal(t, int32(1), callCount.Load())

	// State should be reverted to Subscribed with ~40min old timestamp (backoff for 20min)
	c.subscribed.rw.RLock()
	assert.Equal(t, Subscribed, c.subscribed.m[100].state)
	// Timestamp should be set to ~40min ago (unsubscribeTimer - unsubscribeProcessTicker = 60-20 = 40min)
	age := time.Now().UnixNano() - c.subscribed.m[100].lastTs.Load()
	assert.True(t, age > int64(30*time.Minute) && age < int64(50*time.Minute), "backoff timestamp should be ~40min old")
	c.subscribed.rw.RUnlock()

	// Second GC round - should skip due to backoff (not old enough)
	c.doGCUnusedTable(ctx)
	assert.Equal(t, int32(1), callCount.Load()) // no new call

	// Manually expire the timestamp to test retry
	c.subscribed.rw.Lock()
	c.subscribed.m[100].lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	c.subscribed.rw.Unlock()

	// Third GC round - should retry now
	c.doGCUnusedTable(ctx)
	assert.Equal(t, int32(2), callCount.Load())

	// State should still be Subscribed (failed again)
	c.subscribed.rw.RLock()
	assert.Equal(t, Subscribed, c.subscribed.m[100].state)
	c.subscribed.rw.RUnlock()
}

// TestGC_PartialFailure tests GC with some successes and some failures
func TestGC_PartialFailure(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: NewGlobalStats(ctx, nil, nil),
	}
	c.subscribed.eng = c.eng
	c.subscriber = &logTailSubscriber{}
	c.subscriber.mu.cond = sync.NewCond(&c.subscriber.mu)
	c.subscriber.setReady()

	// Fail for odd table IDs, succeed for even
	c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
		if id.TbId%2 == 1 {
			return moerr.NewInternalErrorNoCtx("network error")
		}
		return nil
	}

	// Add old entries
	for i := uint64(100); i < 110; i++ {
		ent := &subEntry{dbID: 1000, state: Subscribed}
		ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		c.subscribed.m[i] = ent
	}

	c.doGCUnusedTable(ctx)

	// Check states
	c.subscribed.rw.RLock()
	for i := uint64(100); i < 110; i++ {
		ent := c.subscribed.m[i]
		if i%2 == 0 {
			// Even: success, should be Unsubscribing (waiting for response)
			assert.Equal(t, Unsubscribing, ent.state, "table %d should be Unsubscribing", i)
		} else {
			// Odd: failed, should be reverted to Subscribed
			assert.Equal(t, Subscribed, ent.state, "table %d should be Subscribed (reverted)", i)
		}
	}
	c.subscribed.rw.RUnlock()
}

// TestIsSubscribed_StateChangeAfterCheck tests the TOCTOU scenario where
// state changes between RUnlock and GetOrCreateLatestPart call.
// This is acceptable behavior - the returned partition is still valid.
func TestIsSubscribed_StateChangeAfterCheck(t *testing.T) {
	ctx := context.Background()
	var c PushClient
	c.subscribed.m = make(map[uint64]*subEntry)
	c.eng = &Engine{
		partitions:  make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{},
	}

	ent := &subEntry{dbID: 1, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[100] = ent

	var wg sync.WaitGroup
	results := make(chan bool, 1000)

	// Make sure every reader observes the initial Subscribed state at least once
	// before we start flapping.
	var readersStarted sync.WaitGroup
	readersStarted.Add(10)
	startFlapping := make(chan struct{})
	const phases = 6
	phaseStart := make([]chan struct{}, phases)
	for i := 0; i < phases; i++ {
		phaseStart[i] = make(chan struct{})
	}
	var phaseDone [phases]sync.WaitGroup
	for i := 0; i < phases; i++ {
		phaseDone[i].Add(10)
	}

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// First read happens before flapping
			ps, ok, state := c.isSubscribed(ctx, 0, 1, 100)
			if ok {
				assert.NotNil(t, ps)
				results <- true
			} else {
				assert.Contains(t, []SubscribeState{Unsubscribed, Subscribing, Unsubscribing}, state)
				results <- false
			}
			readersStarted.Done()

			// Wait for state changes to begin
			<-startFlapping

			for p := 0; p < phases; p++ {
				<-phaseStart[p]
				ps, ok, state := c.isSubscribed(ctx, 0, 1, 100)
				if ok {
					assert.NotNil(t, ps)
					results <- true
				} else {
					assert.Contains(t, []SubscribeState{Unsubscribed, Subscribing, Unsubscribing}, state)
					results <- false
				}
				phaseDone[p].Done()
			}
		}()
	}

	// State changer goroutine - rapidly toggle state after all readers took
	// their initial read.
	wg.Add(1)
	go func() {
		defer wg.Done()
		readersStarted.Wait()
		close(startFlapping)
		for p := 0; p < phases; p++ {
			c.subscribed.rw.Lock()
			if p%2 == 0 {
				c.subscribed.m[100].state = Unsubscribing
			} else {
				c.subscribed.m[100].state = Subscribed
			}
			c.subscribed.rw.Unlock()
			close(phaseStart[p])
			phaseDone[p].Wait()
		}
	}()

	wg.Wait()
	close(results)

	// Count results - we should see a mix of true/false
	trueCount, falseCount := 0, 0
	for r := range results {
		if r {
			trueCount++
		} else {
			falseCount++
		}
	}
	t.Logf("Results: ok=%d, not_ok=%d", trueCount, falseCount)
	// Both should be non-zero in a proper concurrent test (initial read guarantees >=10)
	assert.True(t, trueCount >= 10, "should have successful reads")
	assert.True(t, falseCount > 0, "should observe non-subscribed states as well")
}
