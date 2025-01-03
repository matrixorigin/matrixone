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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	log "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/assert"
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
		partitions: make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{
			logtailUpdate: newLogtailUpdate(),
			waitKeeper:    newWaitKeeper(),
		},
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
		_ = subscribeRecord.eng.GetOrCreateLatestPart(tbl.db, tbl.tb)
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
		c.eng.globalStats.waitKeeper.add(tid)
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
		c.eng.globalStats.waitKeeper.add(tid)
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
	t.Run("subscriber nil", func(t *testing.T) {
		var c PushClient
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go c.partitionStateGCTicker(ctx, nil)
		time.Sleep(time.Millisecond * 10)
	})

	t.Run("context done", func(t *testing.T) {
		var c PushClient
		c.subscriber = &logTailSubscriber{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go c.partitionStateGCTicker(ctx, nil)
		time.Sleep(time.Millisecond * 10)
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
	ps := logtailreplay.NewPartition("", 300)
	state := ps.Snapshot()
	assert.NotNil(t, state)

	m := mpool.MustNewNoFixed("test")
	packer := types.NewPacker()
	defer packer.Close()
	rowBatch := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil),
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
