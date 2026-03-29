// Copyright 2021 Matrix Origin
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

package service

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/tests"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	tableA := mockTable(1, 1, 1)
	tableB := mockTable(2, 2, 2)
	tableC := mockTable(3, 3, 3)

	addrs, err := tests.GetAddressBatch("127.0.0.1", 1)
	require.NoError(t, err)

	address := addrs[0]
	rt := mockRuntime()

	/* ---- construct logtail server ---- */
	stop := startLogtailServer(t, address, rt, tableA, tableB, tableC)
	defer stop()

	/* ---- construct logtail client ---- */
	codec := morpc.NewMessageCodec(
		"",
		func() morpc.Message { return &LogtailResponseSegment{} },
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(16*mpool.KB),
	)
	bf := morpc.NewGoettyBasedBackendFactory(codec)
	rpcClient, err := morpc.NewClient("", bf, morpc.WithClientMaxBackendPerHost(1))
	require.NoError(t, err)

	rpcStream, err := rpcClient.NewStream(context.Background(), address, false)
	require.NoError(t, err)

	logtailClient, err := NewLogtailClient(context.TODO(), rpcStream, WithClientRequestPerSecond(100))
	require.NoError(t, err)
	defer func() {
		err := logtailClient.Close()
		require.NoError(t, err)
	}()

	/* ---- send subscription request via logtail client ---- */
	{
		t.Log("===> send subscription request via logtail client")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := logtailClient.Subscribe(ctx, tableA)
		require.NoError(t, err)
	}

	/* ---- wait subscription response via logtail client ---- */
	{
		t.Log("===> wait subscription response via logtail client")
		resp, err := logtailClient.Receive(context.Background())
		require.NoError(t, err)
		require.NotNil(t, resp.GetSubscribeResponse())
		require.Equal(t, tableA.String(), resp.GetSubscribeResponse().Logtail.Table.String())
	}

	/* ---- wait update response via logtail client ---- */
	{
		t.Log("===> wait update response via logtail client")
		resp, err := logtailClient.Receive(context.Background())
		require.NoError(t, err)
		require.NotNil(t, resp.GetUpdateResponse())
		require.Equal(t, 1, len(resp.GetUpdateResponse().LogtailList))
		require.Equal(t, tableA.String(), resp.GetUpdateResponse().LogtailList[0].Table.String())
	}

	/* ---- send unsubscription request via logtail client ---- */
	{
		t.Log("===> send unsubscription request via logtail client")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := logtailClient.Unsubscribe(ctx, tableA)
		require.NoError(t, err)
	}

	/* ---- wait unsubscription response via logtail client ---- */
	{
		t.Log("===> wait unsubscription response via logtail client")
		for {
			resp, err := logtailClient.Receive(context.Background())
			require.NoError(t, err)
			if resp.GetUnsubscribeResponse() != nil {
				require.Equal(t, tableA.String(), resp.GetUnsubscribeResponse().Table.String())
				break
			}
		}
	}
}

type logtailer struct {
	tables []api.TableID
}

type tableLogtailer struct {
	tails map[uint64]logtail.TableLogtail
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
	return logtail.TableLogtail{CkpLocation: "checkpoint", Table: &table, Ts: &to}, nil, nil
}

func (m *logtailer) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	panic("not implemented")
}

func (m *tableLogtailer) RangeLogtail(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]logtail.TableLogtail, []func(), error) {
	return nil, nil, nil
}

func (m *tableLogtailer) RegisterCallback(cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error) {
}

func (m *tableLogtailer) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, func(), error) {
	if tail, ok := m.tails[table.TbId]; ok {
		tailCopy := tail
		if tailCopy.Table == nil {
			tailCopy.Table = &table
		}
		if tailCopy.Ts == nil {
			ts := to
			tailCopy.Ts = &ts
		}
		return tailCopy, func() {}, nil
	}
	return logtail.TableLogtail{Table: &table, Ts: &to}, func() {}, nil
}

func (m *tableLogtailer) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	panic("not implemented")
}

func mockRuntime() runtime.Runtime {
	return runtime.NewRuntime(
		metadata.ServiceType_TN,
		"",
		logutil.GetLogger(),
		runtime.WithClock(
			clock.NewHLCClock(
				func() int64 { return time.Now().UTC().UnixNano() },
				time.Duration(math.MaxInt64),
			),
		),
	)
}
func mockTable(dbID, tbID, ptID uint64) api.TableID {
	return api.TableID{
		DbId:        dbID,
		TbId:        tbID,
		PartitionId: ptID,
	}
}

func mockTimestamp(physical int64, logical uint32) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: physical,
		LogicalTime:  logical,
	}
}

func startLogtailServer(
	t *testing.T, address string, rt runtime.Runtime, tables ...api.TableID,
) func() {
	logtailer := mockLocktailer(tables...)

	/* ---- construct logtail server ---- */
	logtailServer, err := NewLogtailServer(
		address, options.NewDefaultLogtailServerCfg(), logtailer, rt, nil,
		WithServerCollectInterval(20*time.Millisecond),
		WithServerSendTimeout(5*time.Second),
		WithServerEnableChecksum(true),
		WithServerMaxMessageSize(32+7),
	)
	require.NoError(t, err)

	/* ---- start logtail server ---- */
	err = logtailServer.Start()
	require.NoError(t, err)

	/* ---- generate incremental logtail ---- */
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

func TestOnActivateAccountForCatalogQueuesWork(t *testing.T) {
	server, stream, session := newActivationQueueTestServer(t, 1)

	err := server.onActivateAccountForCatalog(context.Background(), stream, &logtail.ActivateAccountForCatalogRequest{
		AccountId: 10,
		Seq:       7,
	})
	require.NoError(t, err)

	select {
	case act := <-server.activationReqChan:
		require.Equal(t, uint32(10), act.accountID)
		require.Equal(t, uint64(7), act.seq)
		require.Same(t, session, act.session)
	case <-time.After(time.Second):
		t.Fatal("activation request was not queued")
	}
}

func TestOnActivateAccountForCatalogQueueFailureCleansSessionState(t *testing.T) {
	server, stream, session := newActivationQueueTestServer(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := server.onActivateAccountForCatalog(ctx, stream, &logtail.ActivateAccountForCatalogRequest{
		AccountId: 10,
		Seq:       7,
	})
	require.Error(t, err)
	_, ok := session.lazyCatalog.activatingSeqByAccount[10]
	require.False(t, ok)
}

func TestGetSubLogtailPhaseFiltersLazyCatalogRowsEarly(t *testing.T) {
	table := api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID}
	server := &LogtailServer{
		logger: mockMOLogger(),
		logtailer: &tableLogtailer{
			tails: map[uint64]logtail.TableLogtail{
				catalog.MO_COLUMNS_ID: {
					Table:       &table,
					Ts:          &timestamp.Timestamp{PhysicalTime: 10},
					CkpLocation: "ckp:phase1",
					Commands: []api.Entry{
						mustCatalogColumnInsertEntry(t, []uint32{0, 10, 20}),
					},
				},
			},
		},
	}

	session := newCatalogTestSession(t)
	require.NoError(t, session.configureLazyCatalogSubscription(&logtail.SubscribeRequest{
		Table:                 &table,
		LazyCatalog:           true,
		InitialActiveAccounts: []uint32{10},
	}))

	phase, err := server.getSubLogtailPhase(context.Background(), subscription{
		timeout: time.Second,
		tableID: MarshalTableID(&table),
		req: &logtail.SubscribeRequest{
			Table:       &table,
			LazyCatalog: true,
		},
		session: session,
	}, timestamp.Timestamp{}, timestamp.Timestamp{PhysicalTime: 10})
	require.NoError(t, err)
	t.Cleanup(func() {
		if phase.closeCB != nil {
			phase.closeCB()
		}
	})
	require.Equal(t, "ckp:phase1", phase.tail.CkpLocation)
	require.Len(t, phase.tail.Commands, 1)
	require.Equal(t, []uint32{10}, mustAccountIDsFromEntry(t, phase.tail.Commands[0]))
}

func TestPullActivationPhase1FiltersRowsBeforeEnqueue(t *testing.T) {
	table := api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID}
	server := &LogtailServer{
		logger: mockMOLogger(),
		logtailer: &tableLogtailer{tails: map[uint64]logtail.TableLogtail{
			catalog.MO_COLUMNS_ID: {
				Table:       &table,
				Ts:          &timestamp.Timestamp{PhysicalTime: 10},
				CkpLocation: "ckp:phase1",
				Commands: []api.Entry{
					mustCatalogColumnInsertEntry(t, []uint32{0, 10, 20}),
				},
			},
		}},
		waterline:          NewWaterliner(),
		activationTailChan: make(chan *catalogActivationPhase1, 1),
		pullWorkerPool:     make(chan struct{}, 1),
	}
	server.waterline.Advance(timestamp.Timestamp{PhysicalTime: 10})

	session := newCatalogTestSession(t)
	require.NoError(t, session.configureLazyCatalogSubscription(&logtail.SubscribeRequest{
		Table:                 &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
		LazyCatalog:           true,
		InitialActiveAccounts: []uint32{0},
	}))

	server.pullActivationPhase1(context.Background(), catalogActivation{
		timeout:   time.Second,
		accountID: 10,
		seq:       7,
		session:   session,
	})

	select {
	case phase := <-server.activationTailChan:
		t.Cleanup(phase.closeAll)
		require.Equal(t, "ckp:phase1", phase.tails[2].CkpLocation)
		require.Len(t, phase.tails[2].Commands, 1)
		require.Equal(t, []uint32{10}, mustAccountIDsFromEntry(t, phase.tails[2].Commands[0]))
	case <-time.After(time.Second):
		t.Fatal("activation phase1 was not enqueued")
	}
}

func newActivationQueueTestServer(t *testing.T, queueCap int) (*LogtailServer, morpcStream, *Session) {
	t.Helper()

	cfg := options.NewDefaultLogtailServerCfg()
	server := &LogtailServer{
		logger:            mockMOLogger(),
		cfg:               cfg,
		ssmgr:             NewSessionManager(),
		rootCtx:           context.Background(),
		activationReqChan: make(chan catalogActivation, queueCap),
	}
	server.pool.responses = NewLogtailResponsePool()

	stream := mockMorpcStream(&normalStream{}, 1, 1024)
	session := server.ssmgr.GetSession(
		server.rootCtx,
		server.logger,
		server.pool.responses,
		server,
		stream,
		server.cfg.ResponseSendTimeout,
		server.cfg.RPCStreamPoisonTime,
		server.cfg.LogtailCollectInterval,
	)
	require.NoError(t, session.configureLazyCatalogSubscription(&logtail.SubscribeRequest{
		Table:                 &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
		LazyCatalog:           true,
		InitialActiveAccounts: []uint32{0},
	}))
	return server, stream, session
}
