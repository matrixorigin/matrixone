// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"fmt"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingAsyncClient struct {
	Client
	threshold int64
	count     atomic.Int64
	reached   chan struct{}
	overflow  chan struct{}
	once      sync.Once
	overOnce  sync.Once
}

type peerIsolationClient struct {
	Client
	slowPeer    string
	healthyPeer string
	slowStarted chan struct{}
	healthySent chan struct{}
	slowOnce    sync.Once
	healthyOnce sync.Once
}

type globalBoundClient struct {
	Client
	active    atomic.Int64
	maxActive atomic.Int64
	submitted atomic.Int64
	reached   chan struct{}
	overflow  chan struct{}
	once      sync.Once
	overOnce  sync.Once
}

type refreshFairnessClient struct {
	Client
	mu        sync.Mutex
	refreshed map[uint64]int
}

type bindCursorClient struct {
	Client
	mu        sync.Mutex
	submitted map[uint64]int
	started   chan struct{}
}

type missingOwnerKeeperClient struct {
	bind             pb.LockTable
	missingService   string
	missingKeepCalls atomic.Int64
	otherKeepCalls   atomic.Int64
	getBindCalls     atomic.Int64
}

func (c *missingOwnerKeeperClient) AsyncSend(
	_ context.Context,
	req *pb.Request,
) (*morpc.Future, error) {
	defer releaseRequest(req)
	if req.Method != pb.Method_KeepRemoteLock {
		return nil, moerr.NewInternalErrorNoCtx("unexpected async request")
	}
	if req.LockTable.ServiceID == c.missingService {
		c.missingKeepCalls.Add(1)
		return nil, moerr.NewBackendCannotConnectNoCtx(
			"lockservice service " + req.LockTable.ServiceID + " is absent from cluster inventory",
		)
	}
	c.otherKeepCalls.Add(1)
	return nil, morpc.ErrBackendCreateTimeout
}

func (c *missingOwnerKeeperClient) Send(
	_ context.Context,
	req *pb.Request,
) (*pb.Response, error) {
	if req.Method != pb.Method_GetBind {
		return nil, moerr.NewInternalErrorNoCtx("unexpected request")
	}
	c.getBindCalls.Add(1)
	resp := acquireResponse()
	resp.GetBind.LockTable = c.bind
	resp.GetBind.AllocatorID = c.bind.AllocatorID
	resp.GetBind.AllocatorVersion = c.bind.Version
	return resp, nil
}

func (*missingOwnerKeeperClient) Close() error { return nil }

func (c *bindCursorClient) AsyncSend(
	ctx context.Context,
	req *pb.Request,
) (*morpc.Future, error) {
	c.mu.Lock()
	c.submitted[req.LockTable.Table]++
	c.mu.Unlock()
	if c.started != nil {
		c.started <- struct{}{}
	}
	<-ctx.Done()
	releaseRequest(req)
	return nil, ctx.Err()
}

func (c *bindCursorClient) Send(
	ctx context.Context,
	_ *pb.Request,
) (*pb.Response, error) {
	return nil, ctx.Err()
}

func runBlockedKeepRemoteLockRound(t *testing.T, keeper *lockTableKeeper, client *bindCursorClient) {
	t.Helper()
	activateAllKeeperBinds(keeper)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		keeper.doKeepRemoteLock(ctx, nil, nil)
		close(done)
	}()
	waitForDone := func(message string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal(message)
		}
	}

	for range keepRemoteLockBatchSize {
		select {
		case <-client.started:
		case <-time.After(time.Second):
			cancel()
			waitForDone("keeper did not stop after scheduling timed out")
			t.Fatal("keeper did not schedule a complete keep-remote-lock batch")
		}
	}
	cancel()
	waitForDone("keeper did not finish after the slow round was cancelled")
}

func activateAllKeeperBinds(keeper *lockTableKeeper) {
	var binds []pb.LockTable
	keeper.groupTables.iter(func(_ uint64, table lockTable) bool {
		bind := table.getBind()
		if bind.ServiceID != keeper.serviceID {
			binds = append(binds, bind)
		}
		return true
	})
	for _, bind := range binds {
		keeper.service.acquireRemoteBindRef(bind)
	}
}

func (c *refreshFairnessClient) AsyncSend(
	_ context.Context,
	req *pb.Request,
) (*morpc.Future, error) {
	releaseRequest(req)
	return nil, context.Canceled
}

func (c *refreshFairnessClient) Send(
	_ context.Context,
	req *pb.Request,
) (*pb.Response, error) {
	c.mu.Lock()
	c.refreshed[req.GetBind.Table]++
	c.mu.Unlock()
	return nil, context.Canceled
}

func (c *globalBoundClient) AsyncSend(
	ctx context.Context,
	req *pb.Request,
) (*morpc.Future, error) {
	submitted := c.submitted.Add(1)
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		maxActive := c.maxActive.Load()
		if active <= maxActive || c.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	if active == keepRemoteLockBatchSize {
		c.once.Do(func() { close(c.reached) })
	}
	if submitted > keepRemoteLockBatchSize {
		c.overOnce.Do(func() { close(c.overflow) })
	}
	<-ctx.Done()
	releaseRequest(req)
	return nil, ctx.Err()
}

func (c *globalBoundClient) Send(
	ctx context.Context,
	_ *pb.Request,
) (*pb.Response, error) {
	return nil, ctx.Err()
}

func (c *peerIsolationClient) AsyncSend(
	ctx context.Context,
	req *pb.Request,
) (*morpc.Future, error) {
	if req.LockTable.ServiceID == c.healthyPeer {
		select {
		case <-c.slowStarted:
		case <-ctx.Done():
			releaseRequest(req)
			return nil, ctx.Err()
		}
		c.healthyOnce.Do(func() { close(c.healthySent) })
		releaseRequest(req)
		return nil, context.Canceled
	}
	if req.LockTable.ServiceID == c.slowPeer {
		c.slowOnce.Do(func() { close(c.slowStarted) })
		<-ctx.Done()
		releaseRequest(req)
		return nil, ctx.Err()
	}
	releaseRequest(req)
	return nil, context.Canceled
}

func (c *peerIsolationClient) Send(
	ctx context.Context,
	_ *pb.Request,
) (*pb.Response, error) {
	return nil, ctx.Err()
}

func (c *countingAsyncClient) AsyncSend(
	ctx context.Context,
	req *pb.Request,
) (*morpc.Future, error) {
	n := c.count.Add(1)
	if n == c.threshold {
		c.once.Do(func() { close(c.reached) })
	} else if n > c.threshold {
		c.overOnce.Do(func() { close(c.overflow) })
	}
	return c.Client.AsyncSend(ctx, req)
}

func TestKeeper(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			n1 := atomic.Uint64{}
			n2 := atomic.Uint64{}
			c1 := make(chan struct{})
			c2 := make(chan struct{})
			s.RegisterMethodHandler(
				pb.Method_KeepLockTableBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {

					if n1.Add(1) == 10 {
						close(c1)
					}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			s.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {

					if n2.Add(1) == 10 {
						close(c2)
					}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				0,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s2"},
					c,
					func(lt pb.LockTable) {},
					getLogger(""),
				),
			)
			m.set(
				0,
				1,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s1"},
					c,
					func(lt pb.LockTable) {},
					getLogger(""),
				),
			)
			svc := &service{serviceID: "s1", logger: getLogger("")}
			svc.acquireRemoteBindRef(m.get(0, 0).getBind())
			k := NewLockTableKeeper(
				"s1",
				c,
				time.Millisecond*10,
				time.Millisecond*10,
				m,
				svc)
			defer func() {
				assert.NoError(t, k.Close())
			}()
			<-c1
			<-c2
		},
	)
}

func TestKeeperIntervalJitterIsBounded(t *testing.T) {
	const interval = time.Second
	const window = interval / keeperIntervalJitterFraction
	for i := 0; i < 1000; i++ {
		got := jitterKeeperInterval(interval)
		require.GreaterOrEqual(t, got, interval-window)
		require.LessOrEqual(t, got, interval+window)
	}
	require.Equal(t, time.Duration(0), jitterKeeperInterval(0))
}

func TestKeepRemoteLockReusesBindScratch(t *testing.T) {
	logger := getLogger("")
	client := &bindCursorClient{submitted: make(map[uint64]int)}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: make(map[uint32]*lockTableHolder),
	}
	const bindCount = 3
	for i := range bindCount {
		bind := pb.LockTable{
			Table:       uint64(i + 1),
			OriginTable: uint64(i + 1),
			ServiceID:   "peer",
			Version:     1,
			Valid:       true,
		}
		tables.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				"local",
				time.Second,
				bind,
				client,
				func(pb.LockTable) {},
				logger,
			),
		)
	}
	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}
	activateAllKeeperBinds(keeper)
	scratch := make([]pb.LockTable, 0, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, grown := keeper.doKeepRemoteLock(ctx, nil, scratch)
	cancel()
	require.GreaterOrEqual(t, cap(grown), bindCount)
	grownBacking := &grown[:cap(grown)][0]

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, reused := keeper.doKeepRemoteLock(ctx, nil, grown)
	cancel()
	require.Equal(t, cap(grown), cap(reused))
	require.Same(t, grownBacking, &reused[:cap(reused)][0])

	collectScratch := make([]pb.LockTable, 0, bindCount)
	var collected []pb.LockTable
	allocs := testing.AllocsPerRun(100, func() {
		collected = collectKeepRemoteLockBinds(keeper.service, collectScratch)
		goruntime.KeepAlive(collected)
	})
	require.Zero(t, allocs)
	require.Len(t, collected, bindCount)
}

func TestCollectKeepRemoteLockBindsUsesActiveRefsAndClearsStaleScratch(t *testing.T) {
	logger := getLogger("")
	svc := &service{serviceID: "local", logger: logger}
	bind := pb.LockTable{
		Table:       1,
		OriginTable: 1,
		ServiceID:   "peer",
		Version:     1,
		Valid:       true,
	}
	svc.acquireRemoteBindRef(bind)
	scratch := []pb.LockTable{
		{ServiceID: "old-1", AllocatorID: "allocator-1"},
		{
			ServiceID:        "old-2",
			AllocatorID:      "allocator-2",
			XXX_unrecognized: []byte("old-payload"),
		},
		{ServiceID: "old-3", AllocatorID: "allocator-3"},
	}

	binds := collectKeepRemoteLockBinds(svc, scratch)

	require.Len(t, binds, 1)
	for _, stale := range scratch[len(binds):] {
		require.Equal(t, pb.LockTable{}, stale)
	}

	svc.mu.Lock()
	svc.releaseRemoteBindRefLocked(bind)
	svc.mu.Unlock()
	binds = collectKeepRemoteLockBinds(svc, binds)
	require.Empty(t, binds)
	require.Equal(t, pb.LockTable{}, scratch[0])
}

func TestKeepRemoteLockIgnoresRouteCacheWithoutActiveRef(t *testing.T) {
	logger := getLogger("")
	client := &bindCursorClient{submitted: make(map[uint64]int)}
	bind := pb.LockTable{
		Table:       1,
		OriginTable: 1,
		ServiceID:   "peer",
		Version:     1,
		Valid:       true,
	}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: make(map[uint32]*lockTableHolder),
	}
	tables.set(
		bind.Group,
		bind.Table,
		newRemoteLockTable(
			"local",
			time.Second,
			bind,
			client,
			func(pb.LockTable) {},
			logger,
		),
	)
	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}

	_, binds := keeper.doKeepRemoteLock(context.Background(), nil, nil)

	require.Empty(t, binds)
	client.mu.Lock()
	require.Empty(t, client.submitted)
	client.mu.Unlock()
}

func TestPrepareKeepRemoteLockPeersDoesNotAllocatePerBind(t *testing.T) {
	const (
		bindCount = 10_000
		peerCount = 100
	)
	source := makeKeepRemoteLockBenchmarkBinds(bindCount, peerCount)
	binds := make([]pb.LockTable, bindCount)
	peers := make([]keepRemoteLockPeerWork, 0, peerCount)

	allocs := testing.AllocsPerRun(10, func() {
		copy(binds, source)
		peers = prepareKeepRemoteLockPeers(binds, peers[:0])
		goruntime.KeepAlive(peers)
	})

	require.Zero(t, allocs)
	require.Len(t, peers, peerCount)
}

func TestPrepareKeepRemoteLockPeersClearsStaleScratch(t *testing.T) {
	oldBinds := []pb.LockTable{
		{ServiceID: "peer-1"},
		{ServiceID: "peer-2"},
		{ServiceID: "peer-3"},
	}
	peers := prepareKeepRemoteLockPeers(
		oldBinds,
		make([]keepRemoteLockPeerWork, 0, len(oldBinds)),
	)
	require.Len(t, peers, len(oldBinds))

	peers = prepareKeepRemoteLockPeers(
		[]pb.LockTable{{ServiceID: "peer-1"}},
		peers,
	)

	require.Len(t, peers, 1)
	for _, stale := range peers[:cap(peers)][len(peers):] {
		require.Empty(t, stale.serviceID)
		require.Nil(t, stale.binds)
	}
	peers = prepareKeepRemoteLockPeers(nil, peers)
	require.Empty(t, peers)
	for _, stale := range peers[:cap(peers)] {
		require.Empty(t, stale.serviceID)
		require.Nil(t, stale.binds)
	}
}

func BenchmarkPrepareKeepRemoteLockPeers10K(b *testing.B) {
	const (
		bindCount = 10_000
		peerCount = 100
	)
	source := makeKeepRemoteLockBenchmarkBinds(bindCount, peerCount)
	binds := make([]pb.LockTable, bindCount)
	peers := make([]keepRemoteLockPeerWork, 0, peerCount)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		copy(binds, source)
		peers = prepareKeepRemoteLockPeers(binds, peers[:0])
	}
	goruntime.KeepAlive(peers)
}

func makeKeepRemoteLockBenchmarkBinds(
	bindCount int,
	peerCount int,
) []pb.LockTable {
	peerIDs := make([]string, peerCount)
	for i := range peerIDs {
		peerIDs[i] = fmt.Sprintf("peer-%03d", i)
	}
	binds := make([]pb.LockTable, bindCount)
	for i := range binds {
		reversed := bindCount - i - 1
		binds[i] = pb.LockTable{
			Group:     uint32(reversed % 8),
			Table:     uint64(reversed + 1),
			ServiceID: peerIDs[reversed%peerCount],
			Version:   1,
		}
	}
	return binds
}

func TestKeepRemoteLockHasBoundedInflight(t *testing.T) {
	runRPCTests(
		t,
		func(client Client, server Server) {
			firstEntered := make(chan struct{})
			releaseHandlers := make(chan struct{})
			var firstOnce sync.Once
			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession,
				) {
					firstOnce.Do(func() { close(firstEntered) })
					select {
					case <-releaseHandlers:
						writeResponse(getLogger(""), cancel, resp, nil, cs)
					case <-ctx.Done():
						writeResponse(getLogger(""), cancel, resp, ctx.Err(), cs)
					}
				},
			)

			tracked := &countingAsyncClient{
				Client:    client,
				threshold: keepRemoteLockBatchSize,
				reached:   make(chan struct{}),
				overflow:  make(chan struct{}),
			}
			logger := getLogger("")
			tables := &lockTableHolders{
				service: "s1",
				logger:  logger,
				holders: map[uint32]*lockTableHolder{},
			}
			for i := 0; i < keepRemoteLockBatchSize+1; i++ {
				bind := pb.LockTable{
					Group:       0,
					Table:       uint64(i + 1),
					OriginTable: uint64(i + 1),
					ServiceID:   "s2",
					Version:     1,
					Valid:       true,
				}
				tables.set(
					bind.Group,
					bind.Table,
					newRemoteLockTable(
						"s1",
						time.Second,
						bind,
						tracked,
						func(pb.LockTable) {},
						logger,
					),
				)
			}

			keeper := &lockTableKeeper{
				serviceID:   "s1",
				client:      tracked,
				groupTables: tables,
				service:     &service{serviceID: "s1", logger: logger},
			}
			activateAllKeeperBinds(keeper)
			done := make(chan struct{})
			go func() {
				keeper.doKeepRemoteLock(context.Background(), nil, nil)
				close(done)
			}()

			select {
			case <-tracked.reached:
			case <-time.After(time.Second):
				t.Fatal("keeper did not fill the first bounded batch")
			}
			select {
			case <-firstEntered:
			case <-time.After(time.Second):
				t.Fatal("first keep-remote request did not reach the server")
			}
			select {
			case <-tracked.overflow:
				t.Fatalf("keeper exceeded the in-flight limit of %d", keepRemoteLockBatchSize)
			case <-time.After(100 * time.Millisecond):
			}

			close(releaseHandlers)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("keeper did not finish after responses were released")
			}
			require.Equal(t, int64(keepRemoteLockBatchSize+1), tracked.count.Load())
		},
	)
}

func TestKeepRemoteLockSlowPeerDoesNotBlockHealthyPeer(t *testing.T) {
	logger := getLogger("")
	client := &peerIsolationClient{
		// Keep the slow peer first in the deterministic ServiceID ordering.
		// A scheduler that fills the entire window from one peer would then
		// consume all slots before reaching the healthy peer.
		slowPeer:    "a-slow",
		healthyPeer: "z-healthy",
		slowStarted: make(chan struct{}),
		healthySent: make(chan struct{}),
	}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	addRemote := func(table uint64, serviceID string) {
		bind := pb.LockTable{
			Table:       table,
			OriginTable: table,
			ServiceID:   serviceID,
			Version:     1,
			Valid:       true,
		}
		tables.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				"local",
				time.Second,
				bind,
				client,
				func(pb.LockTable) {},
				logger,
			),
		)
	}
	for i := 0; i < keepRemoteLockBatchSize; i++ {
		addRemote(uint64(i+1), client.slowPeer)
	}
	addRemote(1000, client.healthyPeer)

	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}
	activateAllKeeperBinds(keeper)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		keeper.doKeepRemoteLock(ctx, nil, nil)
		close(done)
	}()
	const hangGuard = 5 * time.Second
	t.Cleanup(func() {
		cancel()
		timer := time.NewTimer(hangGuard)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Errorf("keeper round did not stop during test cleanup")
		}
	})
	waitForSignal := func(ch <-chan struct{}, failure string) {
		t.Helper()
		timer := time.NewTimer(hangGuard)
		defer timer.Stop()
		select {
		case <-ch:
		case <-timer.C:
			t.Fatal(failure)
		}
	}

	waitForSignal(client.slowStarted, "slow peer keepalive did not start")
	waitForSignal(client.healthySent, "healthy peer keepalive was blocked by the slow peer")
	cancel()
	waitForSignal(done, "keeper round did not stop after cancellation")
}

func TestKeepRemoteLockHasGlobalInflightBoundAcrossPeers(t *testing.T) {
	logger := getLogger("")
	client := &globalBoundClient{
		reached:  make(chan struct{}),
		overflow: make(chan struct{}),
	}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	for i := 0; i < keepRemoteLockBatchSize*2; i++ {
		bind := pb.LockTable{
			Table:       uint64(i + 1),
			OriginTable: uint64(i + 1),
			ServiceID:   fmt.Sprintf("peer-%03d", i),
			Version:     1,
			Valid:       true,
		}
		tables.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				"local",
				time.Second,
				bind,
				client,
				func(pb.LockTable) {},
				logger,
			),
		)
	}
	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}
	activateAllKeeperBinds(keeper)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		keeper.doKeepRemoteLock(ctx, nil, nil)
		close(done)
	}()

	select {
	case <-client.reached:
	case <-time.After(time.Second):
		t.Fatal("keeper did not fill the global in-flight window")
	}
	select {
	case <-client.overflow:
		t.Fatalf("keeper exceeded the global in-flight limit of %d", keepRemoteLockBatchSize)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keeper did not stop after cancellation")
	}
	require.Equal(t, int64(keepRemoteLockBatchSize), client.maxActive.Load())
	require.Equal(t, int64(keepRemoteLockBatchSize), client.submitted.Load())
}

func TestKeepRemoteLockRefreshWindowIsFairAcrossRounds(t *testing.T) {
	logger := getLogger("")
	client := &refreshFairnessClient{refreshed: make(map[uint64]int)}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	const bindCount = maxKeepRemoteLockRefreshes * 2
	for i := 0; i < bindCount; i++ {
		bind := pb.LockTable{
			Table:       uint64(i + 1),
			OriginTable: uint64(i + 1),
			ServiceID:   "peer",
			Version:     1,
			Valid:       true,
		}
		tables.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				"local",
				time.Second,
				bind,
				client,
				func(pb.LockTable) {},
				logger,
			),
		)
	}
	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}
	activateAllKeeperBinds(keeper)

	keeper.doKeepRemoteLock(context.Background(), nil, nil)
	keeper.doKeepRemoteLock(context.Background(), nil, nil)

	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.refreshed, bindCount)
	for table := uint64(1); table <= bindCount; table++ {
		require.Equal(t, 1, client.refreshed[table])
	}
}

func TestKeepRemoteLockWorkCursorIsFairAcrossSlowRounds(t *testing.T) {
	logger := getLogger("")
	client := &bindCursorClient{
		submitted: make(map[uint64]int),
		started:   make(chan struct{}, keepRemoteLockBatchSize),
	}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	const bindCount = keepRemoteLockBatchSize * 2
	for i := 0; i < bindCount; i++ {
		bind := pb.LockTable{
			Table:       uint64(i + 1),
			OriginTable: uint64(i + 1),
			ServiceID:   "slow-peer",
			Version:     1,
			Valid:       true,
		}
		tables.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				"local",
				time.Second,
				bind,
				client,
				func(pb.LockTable) {},
				logger,
			),
		)
	}
	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}

	for range 2 {
		runBlockedKeepRemoteLockRound(t, keeper, client)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.submitted, bindCount)
	for table := uint64(1); table <= bindCount; table++ {
		require.Equal(t, 1, client.submitted[table])
	}
}

func TestKeepRemoteLockPeerCursorIsFairAcrossSlowRounds(t *testing.T) {
	logger := getLogger("")
	client := &bindCursorClient{
		submitted: make(map[uint64]int),
		started:   make(chan struct{}, keepRemoteLockBatchSize),
	}
	tables := &lockTableHolders{
		service: "local",
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	const peerCount = keepRemoteLockBatchSize * 2
	for i := range peerCount {
		bind := pb.LockTable{
			Table:       uint64(i + 1),
			OriginTable: uint64(i + 1),
			ServiceID:   fmt.Sprintf("peer-%03d", i),
			Version:     1,
			Valid:       true,
		}
		tables.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				"local",
				time.Second,
				bind,
				client,
				func(pb.LockTable) {},
				logger,
			),
		)
	}
	keeper := &lockTableKeeper{
		serviceID:   "local",
		client:      client,
		groupTables: tables,
		service:     &service{serviceID: "local", logger: logger},
	}

	runRound := func() { runBlockedKeepRemoteLockRound(t, keeper, client) }
	runRound()
	runRound()

	client.mu.Lock()
	require.Len(t, client.submitted, peerCount)
	for table := uint64(1); table <= peerCount; table++ {
		require.Equal(t, 1, client.submitted[table])
	}
	client.mu.Unlock()

	runRound()
	client.mu.Lock()
	defer client.mu.Unlock()
	for table := uint64(1); table <= peerCount; table++ {
		want := 1
		if table <= keepRemoteLockBatchSize {
			want = 2
		}
		require.Equal(t, want, client.submitted[table])
	}
}

func TestKeepBindFailedWillRemoveAllLocalLockTable(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			events := newWaiterEvents(1, nil, nil, time.Second, nil, getLogger(""))
			defer events.close()

			s.RegisterMethodHandler(
				pb.Method_KeepLockTableBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.KeepLockTableBind.OK = false
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			s.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				1,
				newLocalLockTable(
					pb.LockTable{ServiceID: "s1"},
					nil,
					events,
					runtime.DefaultRuntime().Clock(),
					nil,
					getLogger(""),
				),
			)
			m.set(
				0,
				2,
				newLocalLockTable(
					pb.LockTable{ServiceID: "s1"},
					nil,
					events,
					runtime.DefaultRuntime().Clock(),
					nil,
					getLogger(""),
				),
			)
			m.set(
				0,
				3,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s2"},
					c,
					func(lt pb.LockTable) {},
					getLogger(""),
				),
			)
			k := NewLockTableKeeper(
				"s1",
				c,
				time.Millisecond*10,
				time.Millisecond*10,
				m,
				&service{
					logger: getLogger(""),
				})
			defer func() {
				assert.NoError(t, k.Close())
			}()

			for {
				v := 0
				m.iter(func(key uint64, value lockTable) bool {
					v++
					return true
				})
				if v == 1 {
					v := m.get(0, 3)
					assert.NotNil(t, v)
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
	)
}

func TestKeepRemoteLockBindRefreshHonorsContext(t *testing.T) {
	client := &blockingBindRefreshClient{
		bindRefreshStarted: make(chan struct{}, 1),
	}
	logger := getLogger("")
	svc := &service{serviceID: "s1", logger: logger}
	bind := pb.LockTable{
		Group: 0, Table: 1, OriginTable: 1,
		ServiceID: "s2", Version: 1, Valid: true,
	}
	svc.tableGroups = &lockTableHolders{
		service: svc.serviceID,
		logger:  logger,
		holders: map[uint32]*lockTableHolder{},
	}
	svc.tableGroups.set(
		bind.Group,
		bind.Table,
		newRemoteLockTable(
			svc.serviceID,
			time.Second,
			bind,
			client,
			svc.handleBindChanged,
			logger,
		),
	)
	keeper := &lockTableKeeper{
		serviceID:   svc.serviceID,
		client:      client,
		groupTables: svc.tableGroups,
		service:     svc,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		keeper.maybeHandleRemoteBindChanged(ctx, bind, false)
		close(done)
	}()

	select {
	case <-client.bindRefreshStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "keeper bind refresh did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "keeper bind refresh ignored cancellation")
	}
	require.Equal(t, bind, svc.tableGroups.get(bind.Group, bind.Table).getBind())
}

func TestKeepRemoteLockBindChangedFencesActiveTxn(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldBind := pb.LockTable{Group: 0, Table: 1, OriginTable: 1, ServiceID: "s2", Version: 1, Valid: true}
			newBind := oldBind
			newBind.ServiceID = "s3"
			newBind.Version = 2

			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			logger := getLogger("")
			fsp := newFixedSlicePool(2)
			svc := &service{
				serviceID: "s1",
				fsp:       fsp,
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.activeTxnHolder = newMapBasedTxnHandler(
				svc.serviceID,
				logger,
				fsp,
				func(string) (bool, error) { return true, nil },
				func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
				func(pb.WaitTxn) (bool, error) { return true, nil },
			)
			svc.tableGroups.set(
				oldBind.Group,
				oldBind.Table,
				newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger),
			)

			txnID := []byte("txn1")
			txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
			txn.Lock()
			require.NoError(t, txn.lockAdded(oldBind.Group, oldBind, [][]byte{{1}}, pb.LockOptions{}, logger))
			txn.Unlock()
			svc.acquireRemoteBindRef(oldBind)

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)

			txn.Lock()
			require.True(t, txn.bindChanged)
			txn.Unlock()
			require.Equal(t, newBind, svc.tableGroups.get(newBind.Group, newBind.Table).getBind())

			require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
			txn.Lock()
			err := txn.close(txnID, timestamp.Timestamp{}, func(pb.LockTable) (lockTable, error) {
				return nil, nil
			}, logger)
			txn.Unlock()
			require.NoError(t, err)
		},
	)
}

func TestKeepRemoteLockBindChangedRefreshFailureInvalidatesOldBind(t *testing.T) {
	testCases := []struct {
		name        string
		withNewBind bool
		keepErr     error
	}{
		{name: "new-bind", withNewBind: true},
		{name: "bind-not-found", keepErr: ErrLockTableNotFound},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runRPCTests(
				t,
				func(c Client, server Server) {
					oldBind := pb.LockTable{
						Group: 0, Table: 1, OriginTable: 1,
						ServiceID: "s2", Version: 1, Valid: true,
					}
					responseBind := oldBind
					responseBind.ServiceID = "s3"
					responseBind.Version++
					unrelatedBind := pb.LockTable{
						Group: 0, Table: 2, OriginTable: 2,
						ServiceID: "s2", Version: 1, Valid: true,
					}

					server.RegisterMethodHandler(
						pb.Method_KeepRemoteLock,
						func(
							ctx context.Context,
							cancel context.CancelFunc,
							req *pb.Request,
							resp *pb.Response,
							cs morpc.ClientSession) {
							var err error
							if req.LockTable.Table == oldBind.Table && tc.withNewBind {
								resp.NewBind = &responseBind
							}
							if req.LockTable.Table == oldBind.Table {
								err = tc.keepErr
							}
							writeResponse(getLogger(""), cancel, resp, err, cs)
						})
					server.RegisterMethodHandler(
						pb.Method_GetBind,
						func(
							ctx context.Context,
							cancel context.CancelFunc,
							req *pb.Request,
							resp *pb.Response,
							cs morpc.ClientSession) {
							writeResponse(
								getLogger(""),
								cancel,
								resp,
								moerr.NewInternalErrorNoCtx("allocator unavailable"),
								cs,
							)
						})

					logger := getLogger("")
					fsp := newFixedSlicePool(4)
					svc := &service{serviceID: "s1", fsp: fsp, logger: logger}
					svc.remote.client = c
					svc.tableGroups = &lockTableHolders{
						service: svc.serviceID,
						logger:  logger,
						holders: map[uint32]*lockTableHolder{},
					}
					svc.activeTxnHolder = newMapBasedTxnHandler(
						svc.serviceID,
						logger,
						fsp,
						func(string) (bool, error) { return true, nil },
						func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) {
							return pb.CannotCommitResponse{}, nil
						},
						func(pb.WaitTxn) (bool, error) { return true, nil },
					)
					for _, bind := range []pb.LockTable{oldBind, unrelatedBind} {
						svc.tableGroups.set(
							bind.Group,
							bind.Table,
							newRemoteLockTable(
								svc.serviceID,
								time.Second,
								bind,
								c,
								svc.handleBindChanged,
								logger,
							),
						)
					}

					addTxn := func(txnID []byte, bind pb.LockTable) *activeTxn {
						txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
						txn.Lock()
						require.NoError(t, txn.lockAdded(bind.Group, bind, [][]byte{{1}}, pb.LockOptions{}, logger))
						txn.Unlock()
						svc.acquireRemoteBindRef(bind)
						return txn
					}
					oldTxnID := []byte("old-bind-txn")
					oldTxn := addTxn(oldTxnID, oldBind)
					unrelatedTxnID := []byte("unrelated-bind-txn")
					unrelatedTxn := addTxn(unrelatedTxnID, unrelatedBind)

					keeper := &lockTableKeeper{
						serviceID:   svc.serviceID,
						client:      c,
						groupTables: svc.tableGroups,
						service:     svc,
					}
					keeper.doKeepRemoteLock(context.Background(), nil, nil)

					oldTxn.Lock()
					require.True(t, oldTxn.bindChanged)
					oldTxn.Unlock()
					unrelatedTxn.Lock()
					require.False(t, unrelatedTxn.bindChanged)
					unrelatedTxn.Unlock()
					require.Nil(t, svc.tableGroups.get(oldBind.Group, oldBind.Table))
					require.Equal(
						t,
						unrelatedBind,
						svc.tableGroups.get(unrelatedBind.Group, unrelatedBind.Table).getBind(),
					)

					closeTxn := func(txnID []byte, txn *activeTxn) {
						require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
						txn.Lock()
						err := txn.close(
							txnID,
							timestamp.Timestamp{},
							func(pb.LockTable) (lockTable, error) { return nil, nil },
							logger,
						)
						txn.Unlock()
						require.NoError(t, err)
					}
					closeTxn(oldTxnID, oldTxn)
					closeTxn(unrelatedTxnID, unrelatedTxn)
				},
			)
		})
	}
}

func TestKeepRemoteLockMissingOwnerFencesActiveTxn(t *testing.T) {
	testCases := []struct {
		name              string
		removeOldRoute    bool
		replaceRoute      func(pb.LockTable) pb.LockTable
		addReplacementTxn bool
		addIntentTxn      bool
	}{
		{name: "route present"},
		{name: "route absent", removeOldRoute: true, addIntentTxn: true},
		{
			name:           "route replaced by new generation",
			removeOldRoute: true,
			replaceRoute: func(bind pb.LockTable) pb.LockTable {
				bind.ServiceID = "healthy-owner"
				bind.Version++
				return bind
			},
			addReplacementTxn: true,
		},
		{
			name:           "route replaced by Changed-compatible non-exact bind",
			removeOldRoute: true,
			replaceRoute: func(bind pb.LockTable) pb.LockTable {
				// Changed deliberately ignores routing metadata. Exact stale-bind
				// invalidation must still preserve this independently published route.
				bind.OriginTable++
				return bind
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			oldBind := pb.LockTable{
				Group:       0,
				Table:       1,
				OriginTable: 1,
				ServiceID:   "departed-owner",
				Version:     1,
				Valid:       true,
				AllocatorID: "allocator",
			}
			newBind := oldBind
			if test.replaceRoute != nil {
				newBind = test.replaceRoute(newBind)
			}
			client := &missingOwnerKeeperClient{
				bind:           oldBind,
				missingService: oldBind.ServiceID,
			}
			logger := getLogger("")
			fsp := newFixedSlicePool(4)
			svc := &service{
				serviceID: "source",
				fsp:       fsp,
				logger:    logger,
			}
			svc.remote.client = client
			svc.tableGroups = &lockTableHolders{
				service: svc.serviceID,
				logger:  logger,
				holders: map[uint32]*lockTableHolder{},
			}
			svc.activeTxnHolder = newMapBasedTxnHandler(
				svc.serviceID,
				logger,
				fsp,
				func(string) (bool, error) { return true, nil },
				func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) {
					return pb.CannotCommitResponse{}, nil
				},
				func(pb.WaitTxn) (bool, error) { return true, nil },
			)
			newTable := func(bind pb.LockTable) lockTable {
				return newRemoteLockTable(
					svc.serviceID,
					time.Second,
					bind,
					client,
					svc.handleBindChanged,
					logger,
				)
			}
			svc.tableGroups.set(oldBind.Group, oldBind.Table, newTable(oldBind))

			addTxn := func(txnID []byte, bind pb.LockTable) *activeTxn {
				txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
				txn.Lock()
				require.NoError(t, txn.lockAdded(
					bind.Group,
					bind,
					[][]byte{{1}},
					pb.LockOptions{},
					logger,
				))
				txn.Unlock()
				svc.acquireRemoteBindRef(bind)
				return txn
			}
			closeTxn := func(txnID []byte, txn *activeTxn, bind pb.LockTable) {
				require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
				txn.Lock()
				err := txn.close(
					txnID,
					timestamp.Timestamp{},
					func(pb.LockTable) (lockTable, error) { return nil, nil },
					logger,
				)
				txn.Unlock()
				require.NoError(t, err)
				svc.releaseTxnBindRefs([]pb.LockTable{bind})
			}

			oldTxnID := []byte("stale-owner-txn")
			oldTxn := addTxn(oldTxnID, oldBind)
			var intentTxnID []byte
			var intentTxn *activeTxn
			var intentCtx context.Context
			var finishIntent func()
			if test.addIntentTxn {
				intentTxnID = []byte("stale-owner-intent-txn")
				intentTxn = svc.activeTxnHolder.getActiveTxn(intentTxnID, true, "")
				intentTxn.Lock()
				require.True(t, intentTxn.lockTableBindTouched(oldBind))
				intentCtx, finishIntent = intentTxn.beginLockOpLocked(context.Background())
				intentTxn.Unlock()
				svc.acquireRemoteBindRef(oldBind)
			}
			if test.removeOldRoute {
				removed, err := svc.CloseRemoteLockTable(
					oldBind.Group, oldBind.Table, oldBind.Version)
				require.NoError(t, err)
				require.True(t, removed)
			}
			var newTxnID []byte
			var newTxn *activeTxn
			if test.replaceRoute != nil {
				client.bind = newBind
				svc.tableGroups.set(newBind.Group, newBind.Table, newTable(newBind))
			}
			if test.addReplacementTxn {
				newTxnID = []byte("new-generation-txn")
				newTxn = addTxn(newTxnID, newBind)
			}

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      client,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)

			require.Equal(t, int64(1), client.missingKeepCalls.Load())
			oldTxn.Lock()
			require.True(t, oldTxn.bindChanged,
				"an unusable owner must fence exact dependent transactions even after route removal")
			oldTxn.Unlock()
			if test.addIntentTxn {
				intentTxn.Lock()
				require.True(t, intentTxn.bindChanged,
					"an in-flight lock intent must be fenced before it can publish the stale bind")
				intentTxn.Unlock()
				select {
				case <-intentCtx.Done():
					require.ErrorIs(t, intentCtx.Err(), context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("in-flight stale-bind operation was not canceled")
				}
				finishIntent()
			}
			if test.addReplacementTxn {
				require.Equal(t, int64(1), client.otherKeepCalls.Load())
				require.Equal(t, int64(2), client.getBindCalls.Load())
				newTxn.Lock()
				require.False(t, newTxn.bindChanged,
					"invalidating an old bind must not fence a replacement generation")
				newTxn.Unlock()
			} else {
				require.Equal(t, int64(0), client.otherKeepCalls.Load())
				require.Equal(t, int64(1), client.getBindCalls.Load())
			}
			if test.replaceRoute != nil {
				current := svc.tableGroups.get(newBind.Group, newBind.Table)
				require.NotNil(t, current)
				require.Equal(t, newBind, current.getBind())
			} else {
				require.Nil(t, svc.tableGroups.get(oldBind.Group, oldBind.Table))
			}
			expectedBinds := []pb.LockTable{oldBind}
			if test.addReplacementTxn {
				expectedBinds = append(expectedBinds, newBind)
			}
			require.ElementsMatch(t, expectedBinds, svc.collectRemoteLockBinds(nil),
				"bind leases stay owned until their transactions clean up")

			closeTxn(oldTxnID, oldTxn, oldBind)
			if test.addIntentTxn {
				closeTxn(intentTxnID, intentTxn, oldBind)
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)
			require.Equal(t, int64(1), client.missingKeepCalls.Load(),
				"a cleaned stale bind must not be retried by later keeper rounds")
			if test.addReplacementTxn {
				require.Equal(t, int64(2), client.otherKeepCalls.Load())
				newTxn.Lock()
				require.False(t, newTxn.bindChanged)
				newTxn.Unlock()
				closeTxn(newTxnID, newTxn, newBind)
			}
			require.Empty(t, svc.collectRemoteLockBinds(nil))
		})
	}
}

func TestKeepRemoteLockFailureFetchesNewBindAndFencesActiveTxn(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldBind := pb.LockTable{Group: 0, Table: 1, OriginTable: 1, ServiceID: "s2", Version: 1, Valid: true}
			newBind := oldBind
			newBind.ServiceID = "s3"
			newBind.Version = 2

			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, ErrLockTableNotFound, cs)
				})

			logger := getLogger("")
			fsp := newFixedSlicePool(2)
			svc := &service{
				serviceID: "s1",
				fsp:       fsp,
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.activeTxnHolder = newMapBasedTxnHandler(
				svc.serviceID,
				logger,
				fsp,
				func(string) (bool, error) { return true, nil },
				func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
				func(pb.WaitTxn) (bool, error) { return true, nil },
			)
			svc.tableGroups.set(
				oldBind.Group,
				oldBind.Table,
				newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger),
			)

			txnID := []byte("txn1")
			txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
			txn.Lock()
			require.NoError(t, txn.lockAdded(oldBind.Group, oldBind, [][]byte{{1}}, pb.LockOptions{}, logger))
			txn.Unlock()
			svc.acquireRemoteBindRef(oldBind)

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)

			txn.Lock()
			require.True(t, txn.bindChanged)
			txn.Unlock()
			require.Equal(t, newBind, svc.tableGroups.get(newBind.Group, newBind.Table).getBind())

			require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
			txn.Lock()
			err := txn.close(txnID, timestamp.Timestamp{}, func(pb.LockTable) (lockTable, error) {
				return nil, nil
			}, logger)
			txn.Unlock()
			require.NoError(t, err)
		},
	)
}

func TestKeepRemoteLockRefreshRejectsSupersededAllocatorBind(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldAllocator := allocatorState{id: "old-keeper-refresh", version: 100}
			newAllocator := allocatorState{id: "new-keeper-refresh", version: 90}
			oldBind := pb.LockTable{
				Group:       0,
				Table:       1,
				OriginTable: 1,
				ServiceID:   "s2",
				Version:     oldAllocator.version,
				Valid:       true,
				AllocatorID: oldAllocator.id,
			}
			staleBind := oldBind
			staleBind.ServiceID = "s3"
			staleBind.Version++

			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = staleBind
					resp.GetBind.AllocatorID = oldAllocator.id
					resp.GetBind.AllocatorVersion = oldAllocator.version
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			logger := getLogger("")
			svc := &service{
				serviceID: "s1",
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.allocatorVersionMu.Lock()
			svc.lastAllocatorID = newAllocator.id
			svc.lastAllocatorVersion = newAllocator.version
			svc.addSupersededAllocatorIDLocked(oldAllocator.id)
			svc.allocatorVersionMu.Unlock()

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.maybeHandleRemoteBindChanged(context.Background(), oldBind, false)

			require.Nil(t, svc.tableGroups.get(oldBind.Group, oldBind.Table))
			require.Equal(t, newAllocator.id, svc.lastAllocatorID)
			require.Equal(t, newAllocator.version, svc.lastAllocatorVersion)
		},
	)
}

func TestKeepRemoteLockLateNewBindDoesNotRepublishSupersededAllocator(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldAllocator := allocatorState{id: "old-keeper-response", version: 100}
			newAllocator := allocatorState{id: "new-keeper-response", version: 90}
			oldBind := pb.LockTable{
				Group:       0,
				Table:       1,
				OriginTable: 1,
				ServiceID:   "s2",
				Version:     oldAllocator.version,
				Valid:       true,
				AllocatorID: oldAllocator.id,
			}
			lateBind := oldBind
			lateBind.ServiceID = "s3"
			lateBind.Version++
			currentBind := oldBind
			currentBind.ServiceID = "s4"
			currentBind.Version = newAllocator.version
			currentBind.AllocatorID = newAllocator.id

			keepEntered := make(chan struct{})
			releaseKeep := make(chan struct{})
			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					close(keepEntered)
					select {
					case <-releaseKeep:
						resp.NewBind = &lateBind
						writeResponse(getLogger(""), cancel, resp, nil, cs)
					case <-ctx.Done():
						writeResponse(getLogger(""), cancel, resp, ctx.Err(), cs)
					}
				})
			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = currentBind
					resp.GetBind.AllocatorID = newAllocator.id
					resp.GetBind.AllocatorVersion = newAllocator.version
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			logger := getLogger("")
			svc := &service{serviceID: "s1", logger: logger}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{
				service: svc.serviceID,
				logger:  logger,
				holders: map[uint32]*lockTableHolder{},
			}
			svc.lastAllocatorID = oldAllocator.id
			svc.lastAllocatorVersion = oldAllocator.version
			svc.tableGroups.set(
				oldBind.Group,
				oldBind.Table,
				newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger),
			)
			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			svc.acquireRemoteBindRef(oldBind)

			done := make(chan struct{})
			go func() {
				keeper.doKeepRemoteLock(context.Background(), nil, nil)
				close(done)
			}()
			select {
			case <-keepEntered:
			case <-time.After(time.Second):
				require.FailNow(t, "keep-remote request did not reach the old owner")
			}

			removed, accepted := svc.observeAllocatorStateWithHoldersFromSnapshot(
				"test-new-allocator",
				newAllocator,
				oldAllocator,
				true,
				svc.tableGroups)
			require.True(t, accepted)
			require.Equal(t, 1, removed)
			require.Nil(t, svc.tableGroups.get(oldBind.Group, oldBind.Table))

			close(releaseKeep)
			select {
			case <-done:
			case <-time.After(time.Second):
				require.FailNow(t, "keep-remote refresh did not finish")
			}

			got := svc.tableGroups.get(currentBind.Group, currentBind.Table)
			require.NotNil(t, got)
			require.Equal(t, currentBind, got.getBind())
			require.Equal(t, newAllocator.id, svc.lastAllocatorID)
			require.Equal(t, newAllocator.version, svc.lastAllocatorVersion)
		},
	)
}

func TestKeepRemoteLockIgnoresNonBindResponseErrors(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldBind := pb.LockTable{Group: 0, Table: 1, OriginTable: 1, ServiceID: "s2", Version: 1, Valid: true}
			getBindCalls := atomic.Uint64{}

			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					getBindCalls.Add(1)
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, moerr.NewInternalErrorNoCtx("boom"), cs)
				})

			logger := getLogger("")
			fsp := newFixedSlicePool(2)
			svc := &service{
				serviceID: "s1",
				fsp:       fsp,
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.activeTxnHolder = newMapBasedTxnHandler(
				svc.serviceID,
				logger,
				fsp,
				func(string) (bool, error) { return true, nil },
				func([]pb.OrphanTxn) (pb.CannotCommitResponse, error) { return pb.CannotCommitResponse{}, nil },
				func(pb.WaitTxn) (bool, error) { return true, nil },
			)
			svc.tableGroups.set(
				oldBind.Group,
				oldBind.Table,
				newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger),
			)

			txnID := []byte("txn1")
			txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
			txn.Lock()
			require.NoError(t, txn.lockAdded(oldBind.Group, oldBind, [][]byte{{1}}, pb.LockOptions{}, logger))
			txn.Unlock()
			svc.acquireRemoteBindRef(oldBind)

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)

			require.Equal(t, uint64(0), getBindCalls.Load())

			txn.Lock()
			require.False(t, txn.bindChanged)
			txn.Unlock()
			require.Equal(t, oldBind, svc.tableGroups.get(oldBind.Group, oldBind.Table).getBind())

			require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
			txn.Lock()
			err := txn.close(txnID, timestamp.Timestamp{}, func(pb.LockTable) (lockTable, error) {
				return nil, nil
			}, logger)
			txn.Unlock()
			require.NoError(t, err)
		},
	)
}
