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
	c.subscribed.m = make(map[uint64]SubTableStatus)
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

	subscribeRecord.m = make(map[uint64]SubTableStatus)
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
		_ = subscribeRecord.eng.GetOrCreateLatestPart(nil, 0, tbl.db, tbl.tb)
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
		c.subscribed.m = make(map[uint64]SubTableStatus)
	}

	t.Run("unsubscribe - should not unsub", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c PushClient
		initFn(ctx, &c)
		c.subscribed.m[2] = SubTableStatus{
			DBID: catalog.MO_CATALOG_ID,
		}
		c.doGCUnusedTable(ctx)
		assert.Equal(t, 1, len(c.subscribed.m))
		_, ok := c.subscribed.m[2]
		assert.True(t, ok)
	})

	t.Run("unsubscribe - not safe", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var c PushClient
		initFn(ctx, &c)
		c.subscribed.m[200] = SubTableStatus{
			DBID: 1000,
		}
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
		c.subscribed.m[tid] = SubTableStatus{
			DBID:       1000,
			SubState:   Subscribed,
			LatestTime: time.Now().Add(-time.Hour * 2),
		}
		ch := make(chan struct{})
		c.subscriber.sendUnSubscribe = func(ctx context.Context, id api.TableID) error {
			go func() {
				c.subscribed.mutex.Lock()
				defer c.subscribed.mutex.Unlock()
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
		c.subscribed.m[tid] = SubTableStatus{
			DBID:       1000,
			SubState:   Subscribed,
			LatestTime: time.Now().Add(-time.Hour * 2),
		}
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

	m := mpool.MustNewNoFixed("test")
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
		subscribed:      subscribedTable{m: make(map[uint64]SubTableStatus)},
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
