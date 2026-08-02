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

package lockservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const rpcTestResponseTimeout = 10 * time.Second

type closeTrackingRPCClient struct {
	morpc.RPCClient
	closed      []string
	closeErrors map[string]error
	sent        []string
	closedC     chan string
	name        string
	closeOrder  *[]string
	closeErr    error
	closeCalls  int
}

type blockingCloseRPCClient struct {
	morpc.RPCClient
	started chan<- struct{}
	release <-chan struct{}
}

func (c *blockingCloseRPCClient) Close() error {
	c.started <- struct{}{}
	<-c.release
	return nil
}

func (c *blockingCloseRPCClient) CloseBackendFor(string) error {
	return nil
}

type refreshOnDemandCluster struct {
	clusterservice.MOCluster
	before             metadata.CNService
	after              metadata.CNService
	refreshed          bool
	synchronous        bool
	refreshing         chan struct{}
	refreshDone        chan struct{}
	refreshErr         error
	cancelAfterRefresh context.CancelFunc
}

type blockedClusterClient struct {
	started chan struct{}
	release chan struct{}
}

func (c *blockedClusterClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	select {
	case c.started <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
		return logpb.ClusterDetails{}, nil
	case <-ctx.Done():
		return logpb.ClusterDetails{}, ctx.Err()
	}
}

func (c *refreshOnDemandCluster) GetCNServiceWithoutWorkingState(
	_ clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	service := c.before
	if c.refreshed {
		service = c.after
	}
	apply(service)
}

func (c *refreshOnDemandCluster) ForceRefresh(sync bool) {
	c.synchronous = sync
	_ = c.Refresh(context.Background())
}

func (c *refreshOnDemandCluster) Refresh(ctx context.Context) error {
	c.synchronous = true
	if c.refreshing != nil {
		close(c.refreshing)
		select {
		case <-c.refreshDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if c.refreshErr != nil {
		return c.refreshErr
	}
	c.refreshed = true
	if c.cancelAfterRefresh != nil {
		c.cancelAfterRefresh()
	}
	return nil
}

func (c *closeTrackingRPCClient) Send(
	_ context.Context,
	remote string,
	_ morpc.Message,
) (*morpc.Future, error) {
	c.sent = append(c.sent, remote)
	return nil, nil
}

func (c *closeTrackingRPCClient) CloseBackendFor(remote string) error {
	c.closed = append(c.closed, remote)
	if c.closedC != nil {
		c.closedC <- remote
	}
	return c.closeErrors[remote]
}

func (c *closeTrackingRPCClient) Close() error {
	c.closeCalls++
	if c.closeOrder != nil {
		*c.closeOrder = append(*c.closeOrder, c.name)
	}
	return c.closeErr
}

type testClientSession struct {
	ctx         context.Context
	writeCtx    context.Context
	writeErr    error
	closeErr    error
	writeCalled bool
	asyncCalled bool
	closeCalled bool
	response    morpc.Message
}

type closeResultRPCServer struct {
	err   error
	calls int
}

type blockingBackendFactory struct {
	entered chan struct{}
	release chan struct{}
	started atomic.Bool
}

func (f *blockingBackendFactory) Create(
	string,
	...morpc.BackendOption,
) (morpc.Backend, error) {
	if f.started.CompareAndSwap(false, true) {
		close(f.entered)
	}
	<-f.release
	return timeoutPolicyTestBackend{}, nil
}

type timeoutPolicyTestBackend struct{}

var errTimeoutPolicyBackendReached = errors.New("backend reached")

func (timeoutPolicyTestBackend) Send(
	context.Context,
	morpc.Message,
) (*morpc.Future, error) {
	return nil, errTimeoutPolicyBackendReached
}

func (timeoutPolicyTestBackend) SendInternal(
	context.Context,
	morpc.Message,
) (*morpc.Future, error) {
	return nil, errTimeoutPolicyBackendReached
}

func (timeoutPolicyTestBackend) NewStream(bool) (morpc.Stream, error) {
	return nil, errTimeoutPolicyBackendReached
}

func (timeoutPolicyTestBackend) Close() {}

func (timeoutPolicyTestBackend) Busy() bool { return false }

func (timeoutPolicyTestBackend) LastActiveTime() time.Time { return time.Now() }

func (timeoutPolicyTestBackend) Lock() {}

func (timeoutPolicyTestBackend) Unlock() {}

func (timeoutPolicyTestBackend) Locked() bool { return false }

func (s *closeResultRPCServer) Start() error {
	return nil
}

func (s *closeResultRPCServer) Close() error {
	s.calls++
	return s.err
}

func (s *closeResultRPCServer) RegisterRequestHandler(
	func(context.Context, morpc.RPCMessage, uint64, morpc.ClientSession) error,
) {
}

func TestLockserviceRemoteRPCErrorType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"rpc timeout", moerr.NewRPCTimeoutNoCtx(), "rpc_timeout"},
		{"backend cannot connect", moerr.NewBackendCannotConnectNoCtx(), "backend_cannot_connect"},
		{"backend create timeout", morpc.ErrBackendCreateTimeout, "backend_create_timeout"},
		{"wrapped backend create timeout", fmt.Errorf("wrapped: %w", morpc.ErrBackendCreateTimeout), "backend_create_timeout"},
		{"backend closed", moerr.NewBackendClosedNoCtx(), "backend_closed"},
		{"unexpected eof", io.ErrUnexpectedEOF, "unexpected_eof"},
		{"caller context deadline ignored", context.DeadlineExceeded, ""},
		{"string timeout", moerr.NewInternalErrorNoCtx("read tcp 127.0.0.1:6003: i/o timeout"), "timeout"},
		{"business error ignored", moerr.NewInternalErrorNoCtx("lock conflict"), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, lockserviceRemoteRPCErrorType(tt.err))
		})
	}
}

func TestLockserviceBackendCreateTimeoutPolicy(t *testing.T) {
	normal := withBackendCreateQueueWaitTimeout(morpc.Config{
		ClientOptions: []morpc.ClientOption{
			morpc.WithClientEnableAutoCreateBackend(),
			morpc.WithClientDisableCircuitBreaker(),
		},
	})
	recovery := withBackendCreateWaitTimeout(normal)

	require.Len(t, normal.ClientOptions, 3,
		"normal lock traffic must not inherit the recovery factory deadline")
	require.Len(t, recovery.ClientOptions, 4)

	t.Run("normal traffic follows caller context", func(t *testing.T) {
		factory := &blockingBackendFactory{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := morpc.NewClient(
			t.Name(),
			factory,
			normal.ClientOptions...,
		)
		require.NoError(t, err)
		var released atomic.Bool
		release := func() {
			if released.CompareAndSwap(false, true) {
				close(factory.release)
			}
		}
		t.Cleanup(func() {
			release()
			require.NoError(t, client.Close())
		})

		result := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() {
			result <- client.Ping(ctx, "normal-backend")
		}()
		select {
		case <-factory.entered:
		case <-time.After(time.Second):
			t.Fatal("normal backend factory did not start")
		}
		select {
		case err := <-result:
			t.Fatalf("normal traffic stopped at the recovery deadline: %v", err)
		case <-time.After(recoveryBackendCreateWaitTimeout + 100*time.Millisecond):
		}
		release()
		select {
		case err := <-result:
			require.ErrorIs(t, err, errTimeoutPolicyBackendReached)
		case <-time.After(time.Second):
			t.Fatal("normal backend did not become usable after creation")
		}
	})

	t.Run("recovery traffic keeps fast failure", func(t *testing.T) {
		factory := &blockingBackendFactory{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := morpc.NewClient(
			t.Name(),
			factory,
			recovery.ClientOptions...,
		)
		require.NoError(t, err)
		defer func() {
			close(factory.release)
			require.NoError(t, client.Close())
		}()

		result := make(chan error, 1)
		go func() {
			result <- client.Ping(context.Background(), "recovery-backend")
		}()
		select {
		case <-factory.entered:
		case <-time.After(time.Second):
			t.Fatal("recovery backend factory did not start")
		}
		select {
		case err := <-result:
			require.ErrorIs(t, err, morpc.ErrBackendCreateTimeout)
		case <-time.After(2 * time.Second):
			t.Fatal("recovery traffic did not honor the backend-create deadline")
		}
	})

	t.Run("normal traffic remains caller cancellable", func(t *testing.T) {
		factory := &blockingBackendFactory{
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		client, err := morpc.NewClient(
			t.Name(),
			factory,
			normal.ClientOptions...,
		)
		require.NoError(t, err)
		defer func() {
			close(factory.release)
			require.NoError(t, client.Close())
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			result <- client.Ping(ctx, "cancelled-normal-backend")
		}()
		select {
		case <-factory.entered:
		case <-time.After(time.Second):
			t.Fatal("normal backend factory did not start")
		}
		cancel()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("normal traffic did not observe caller cancellation")
		}
	})
}

func TestCloseCreatedClientsClosesInReverseAndJoinsErrors(t *testing.T) {
	firstErr := errors.New("first close failed")
	thirdErr := errors.New("third close failed")
	var order []string
	first := &closeTrackingRPCClient{
		name:       "first",
		closeOrder: &order,
		closeErr:   firstErr,
	}
	second := &closeTrackingRPCClient{
		name:       "second",
		closeOrder: &order,
	}
	third := &closeTrackingRPCClient{
		name:       "third",
		closeOrder: &order,
		closeErr:   thirdErr,
	}

	err := closeCreatedClients([]io.Closer{first, second, third})
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, thirdErr)
	require.Equal(t, []string{"third", "second", "first"}, order)
	require.Equal(t, 1, first.closeCalls)
	require.Equal(t, 1, second.closeCalls)
	require.Equal(t, 1, third.closeCalls)
}

func TestClientCloseClosesEveryDistinctTransportAndJoinsErrors(t *testing.T) {
	normalErr := errors.New("normal close failed")
	activeTxnErr := errors.New("active txn close failed")
	validationErr := errors.New("validation close failed")
	keeperErr := errors.New("keeper close failed")
	controlErr := errors.New("control close failed")
	normal := &closeTrackingRPCClient{closeErr: normalErr}
	activeTxn := &closeTrackingRPCClient{closeErr: activeTxnErr}
	validation := &closeTrackingRPCClient{closeErr: validationErr}
	keeper := &closeTrackingRPCClient{closeErr: keeperErr}
	control := &closeTrackingRPCClient{closeErr: controlErr}
	c := &client{
		client:           normal,
		activeTxnClient:  activeTxn,
		validationClient: validation,
		keeperClient:     keeper,
		controlClient:    control,
	}

	err := c.Close()
	for _, expected := range []error{
		normalErr,
		activeTxnErr,
		validationErr,
		keeperErr,
		controlErr,
	} {
		require.ErrorIs(t, err, expected)
	}
	for _, transport := range []*closeTrackingRPCClient{
		normal,
		activeTxn,
		validation,
		keeper,
		control,
	} {
		require.Equal(t, 1, transport.closeCalls)
	}

	// Cleanup errors are sticky and concurrent/repeated callers join the same
	// ownership transition instead of closing transports again.
	err = c.Close()
	for _, expected := range []error{
		normalErr,
		activeTxnErr,
		validationErr,
		keeperErr,
		controlErr,
	} {
		require.ErrorIs(t, err, expected)
	}
	for _, transport := range []*closeTrackingRPCClient{
		normal,
		activeTxn,
		validation,
		keeper,
		control,
	} {
		require.Equal(t, 1, transport.closeCalls)
	}
}

func TestClientCloseClosesAliasedFallbackTransportOnce(t *testing.T) {
	closeErr := errors.New("close failed")
	shared := &closeTrackingRPCClient{closeErr: closeErr}
	c := &client{
		client:           shared,
		activeTxnClient:  shared,
		validationClient: shared,
		keeperClient:     shared,
		controlClient:    shared,
	}

	require.ErrorIs(t, c.Close(), closeErr)
	require.ErrorIs(t, c.Close(), closeErr)
	require.Equal(t, 1, shared.closeCalls)
}

func TestClientCloseClosesDistinctTransportsConcurrently(t *testing.T) {
	started := make(chan struct{}, 5)
	release := make(chan struct{})
	transports := make([]*blockingCloseRPCClient, 5)
	for idx := range transports {
		transports[idx] = &blockingCloseRPCClient{
			started: started,
			release: release,
		}
	}
	c := &client{
		client:           transports[0],
		activeTxnClient:  transports[1],
		validationClient: transports[2],
		keeperClient:     transports[3],
		controlClient:    transports[4],
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()
	for range transports {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("client Close serialized independent transport cleanup")
		}
	}
	close(release)
	require.NoError(t, <-closeDone)
}

func (s *testClientSession) Close() error {
	s.closeCalled = true
	return s.closeErr
}

func (s *testClientSession) SessionCtx() context.Context { return s.ctx }

func (s *testClientSession) Write(ctx context.Context, response morpc.Message) error {
	s.writeCtx = ctx
	s.writeCalled = true
	s.response = response
	return s.writeErr
}

func TestServerQueueAdmissionTimeoutIsBounded(t *testing.T) {
	requests := make(chan requestCtx, 1)
	requests <- requestCtx{}
	s := &server{
		logger:                getLogger(""),
		handlers:              map[lock.Method]RequestHandleFunc{lock.Method_Lock: func(context.Context, context.CancelFunc, *lock.Request, *lock.Response, morpc.ClientSession) {}},
		requests:              requests,
		getActiveTxnRequests:  make(chan requestCtx, 1),
		requestEnqueueTimeout: 10 * time.Millisecond,
	}
	req := acquireRequest()
	req.Method = lock.Method_Lock
	var canceled bool
	cs := &testClientSession{ctx: context.Background()}
	started := time.Now()
	err := s.onMessage(
		context.Background(),
		morpc.RPCMessage{
			Message: req,
			Cancel:  func() { canceled = true },
		},
		0,
		cs,
	)
	require.NoError(t, err)
	require.Less(t, time.Since(started), time.Second)
	require.True(t, canceled)
	require.True(t, cs.writeCalled)
	require.False(t, cs.closeCalled, "one saturated request must not close the shared session")
	resp := cs.response.(*lock.Response)
	require.ErrorContains(t, resp.UnwrapError(), "request queue full")
	releaseResponse(resp)
}

func TestServerQueueAdmissionStopsWithSession(t *testing.T) {
	requests := make(chan requestCtx, 1)
	requests <- requestCtx{}
	s := &server{
		logger:                getLogger(""),
		handlers:              map[lock.Method]RequestHandleFunc{lock.Method_Lock: func(context.Context, context.CancelFunc, *lock.Request, *lock.Response, morpc.ClientSession) {}},
		requests:              requests,
		getActiveTxnRequests:  make(chan requestCtx, 1),
		requestEnqueueTimeout: time.Second,
	}
	sessionCtx, closeSession := context.WithCancel(context.Background())
	closeSession()
	cs := &testClientSession{ctx: sessionCtx}
	req := acquireRequest()
	req.Method = lock.Method_Lock
	var canceled bool

	require.NoError(t, s.onMessage(
		context.Background(),
		morpc.RPCMessage{
			Message: req,
			Cancel:  func() { canceled = true },
		},
		0,
		cs,
	))
	require.True(t, canceled)
	require.False(t, cs.writeCalled)
	require.Len(t, requests, 1)
}

func TestReleaseQueuedRequestsCancelsAndDrains(t *testing.T) {
	requests := make(chan requestCtx, 2)
	var canceled int
	for range 2 {
		requests <- requestCtx{
			req:    acquireRequest(),
			cancel: func() { canceled++ },
		}
	}

	releaseQueuedRequests(requests)
	require.Equal(t, 2, canceled)
	require.Empty(t, requests)
}

func (s *testClientSession) AsyncWrite(response morpc.Message) error {
	s.asyncCalled = true
	return nil
}

func (s *testClientSession) CreateCache(ctx context.Context, cacheID uint64) (morpc.MessageCache, error) {
	return nil, nil
}

func (s *testClientSession) DeleteCache(cacheID uint64) {}

func (s *testClientSession) GetCache(cacheID uint64) (morpc.MessageCache, error) { return nil, nil }

func (s *testClientSession) RemoteAddress() string { return "" }

func TestWriteResponseWithDeadlineUsesSyncWrite(t *testing.T) {
	resp := acquireResponse()
	defer releaseResponse(resp)

	extraFieldsCalled := false
	cs := &testClientSession{ctx: context.Background()}
	err := writeResponseWithDeadline(getLogger(""), nil, resp, nil, cs, time.Second, func() []zap.Field {
		extraFieldsCalled = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, cs.writeCalled)
	require.False(t, cs.asyncCalled)
	require.False(t, cs.closeCalled)
	require.False(t, extraFieldsCalled)
	_, ok := cs.writeCtx.Deadline()
	require.True(t, ok)
}

func TestWriteResponseUsesSyncWrite(t *testing.T) {
	resp := acquireResponse()
	defer releaseResponse(resp)

	cs := &testClientSession{ctx: context.Background()}
	writeResponse(getLogger(""), nil, resp, nil, cs)
	require.True(t, cs.writeCalled)
	require.False(t, cs.asyncCalled)
	require.False(t, cs.closeCalled)
	_, ok := cs.writeCtx.Deadline()
	require.True(t, ok)
}

func TestWriteResponseWithDeadlineClosesSessionOnWriteError(t *testing.T) {
	resp := acquireResponse()
	defer releaseResponse(resp)
	resp.RequestID = 42
	resp.Method = lock.Method_Lock

	cs := &testClientSession{
		closeErr: moerr.NewInternalErrorNoCtx("close failed"),
		ctx:      context.Background(),
		writeErr: context.DeadlineExceeded,
	}
	extraFieldsCalled := false
	err := writeResponseWithDeadline(getLogger(""), nil, resp, nil, cs, time.Second, func() []zap.Field {
		extraFieldsCalled = true
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, cs.writeCalled)
	require.True(t, cs.closeCalled)
	require.True(t, extraFieldsCalled)
}

func TestServerCloseDrainsQueuesAfterRPCError(t *testing.T) {
	rpcErr := errors.New("rpc close failed")
	rpcServer := &closeResultRPCServer{err: rpcErr}
	requests := make(chan requestCtx, 1)
	getActiveTxnRequests := make(chan requestCtx, 1)
	canceled := false
	requests <- requestCtx{
		req: acquireRequest(),
		cancel: func() {
			canceled = true
		},
	}
	s := &server{
		rpc:                  rpcServer,
		stopper:              stopper.NewStopper("test-lock-rpc-server-close"),
		requests:             requests,
		getActiveTxnRequests: getActiveTxnRequests,
	}
	s.lifecycle.closingC = make(chan struct{})

	err := s.Close()
	require.ErrorIs(t, err, rpcErr)
	require.Equal(t, 1, rpcServer.calls)
	require.True(t, canceled)
	_, requestsOpen := <-requests
	require.False(t, requestsOpen)
	_, getActiveTxnRequestsOpen := <-getActiveTxnRequests
	require.False(t, getActiveTxnRequestsOpen)
	require.ErrorIs(t, s.stopper.RunTask(func(context.Context) {}), stopper.ErrUnavailable)

	// A session can outlive a failed listener close. Late messages must be
	// rejected by the lifecycle gate without touching the closed queues.
	lateRequest := acquireRequest()
	lateRequest.Method = lock.Method_Lock
	lateCanceled := false
	err = s.onMessage(
		context.Background(),
		morpc.RPCMessage{
			Message: lateRequest,
			Cancel:  func() { lateCanceled = true },
		},
		0,
		&testClientSession{},
	)
	require.Error(t, err)
	require.True(t, lateCanceled)

	// The first complete cleanup owns the sticky result. Retrying after an
	// underlying listener error must not close worker queues a second time.
	require.ErrorIs(t, s.Close(), rpcErr)
	require.Equal(t, 1, rpcServer.calls)
}

func TestServerConcurrentCloseJoinsSingleCleanup(t *testing.T) {
	rpcErr := errors.New("rpc close failed")
	rpcServer := &closeResultRPCServer{err: rpcErr}
	s := &server{
		rpc:                  rpcServer,
		stopper:              stopper.NewStopper("test-lock-rpc-server-concurrent-close"),
		requests:             make(chan requestCtx, 1),
		getActiveTxnRequests: make(chan requestCtx, 1),
	}
	s.lifecycle.closingC = make(chan struct{})

	const callers = 16
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- s.Close()
		}()
	}
	close(start)
	for range callers {
		require.ErrorIs(t, <-results, rpcErr)
	}
	require.Equal(t, 1, rpcServer.calls)
}

func TestServerCloseDoesNotWaitForBlockedFilter(t *testing.T) {
	filterStarted := make(chan struct{})
	releaseFilter := make(chan struct{})
	rpcServer := &closeResultRPCServer{}
	s := &server{
		logger:               getLogger(""),
		rpc:                  rpcServer,
		handlers:             map[lock.Method]RequestHandleFunc{lock.Method_Lock: func(context.Context, context.CancelFunc, *lock.Request, *lock.Response, morpc.ClientSession) {}},
		stopper:              stopper.NewStopper("test-lock-rpc-server-blocked-filter"),
		requests:             make(chan requestCtx, 1),
		getActiveTxnRequests: make(chan requestCtx, 1),
	}
	s.lifecycle.closingC = make(chan struct{})
	s.options.filter = func(*lock.Request) bool {
		close(filterStarted)
		<-releaseFilter
		return true
	}

	req := acquireRequest()
	req.Method = lock.Method_Lock
	onMessageDone := make(chan error, 1)
	go func() {
		onMessageDone <- s.onMessage(
			context.Background(),
			morpc.RPCMessage{Message: req},
			0,
			&testClientSession{},
		)
	}()
	<-filterStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.Close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server close waited for message filter")
	}

	close(releaseFilter)
	select {
	case err := <-onMessageDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("filtered request did not observe closed server")
	}
}

func TestServerCloseWakesSaturatedAdmission(t *testing.T) {
	requests := make(chan requestCtx, 1)
	requests <- requestCtx{req: acquireRequest()}
	s := &server{
		logger:                getLogger(""),
		rpc:                   &closeResultRPCServer{},
		handlers:              map[lock.Method]RequestHandleFunc{lock.Method_Lock: func(context.Context, context.CancelFunc, *lock.Request, *lock.Response, morpc.ClientSession) {}},
		stopper:               stopper.NewStopper("test-lock-rpc-server-saturated-close"),
		requests:              requests,
		getActiveTxnRequests:  make(chan requestCtx, 1),
		requestEnqueueTimeout: time.Hour,
	}
	s.lifecycle.closingC = make(chan struct{})

	req := acquireRequest()
	req.Method = lock.Method_Lock
	onMessageDone := make(chan error, 1)
	go func() {
		onMessageDone <- s.onMessage(
			context.Background(),
			morpc.RPCMessage{Message: req},
			0,
			&testClientSession{ctx: context.Background()},
		)
	}()

	// A failed writer TryLock proves onMessage is inside the admission read
	// section and waiting on the full channel, rather than merely unscheduled.
	require.Eventually(t, func() bool {
		if s.lifecycle.TryLock() {
			s.lifecycle.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.Close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server close waited for request enqueue timeout")
	}
	select {
	case err := <-onMessageDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("saturated admission did not observe server close")
	}
}

func TestRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			releaseResponse(resp)
		},
	)
}

func TestSetRestartServiceRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_SetRestartService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.SetRestartService.OK = true
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					SetRestartService: lock.SetRestartServiceRequest{ServiceID: "s1"},
					Method:            lock.Method_SetRestartService})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.True(t, resp.SetRestartService.OK)
			releaseResponse(resp)
		},
	)
}

func TestAbortRemoteDeadlockTxnFailed(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_AbortRemoteDeadlockTxn,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.AbortRemoteDeadlockTxn.OK = false
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					Method:                 lock.Method_AbortRemoteDeadlockTxn,
					AbortRemoteDeadlockTxn: lock.AbortRemoteDeadlockTxnRequest{Txn: lock.WaitTxn{WaiterAddress: "s1"}},
				})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.False(t, resp.SetRestartService.OK)
			releaseResponse(resp)
		},
	)
}

func TestCanRestartServiceRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_CanRestartService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.CanRestartService.OK = true
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					CanRestartService: lock.CanRestartServiceRequest{ServiceID: "s1"},
					Method:            lock.Method_CanRestartService})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.True(t, resp.CanRestartService.OK)
			releaseResponse(resp)
		},
	)
}

func TestRemainTxnServiceRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_RemainTxnInService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.RemainTxnInService.RemainTxn = -1
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx,
				&lock.Request{
					RemainTxnInService: lock.RemainTxnInServiceRequest{ServiceID: "s1"},
					Method:             lock.Method_RemainTxnInService})
			require.NoError(t, err)
			assert.NotNil(t, resp)
			require.Equal(t, int32(-1), resp.RemainTxnInService.RemainTxn)
			releaseResponse(resp)
		},
	)
}

func TestRPCSendErrBackendCannotConnect(t *testing.T) {
	runRPCServerNoCloseTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()
			err := s.Close()
			require.NoError(t, err)
			_, err = c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			if err != nil {
				t.Logf("Error: %v, Type: %T", err, err)
			}
			// A definitive dial failure must retain BackendCannotConnect so the
			// allocator can disable a dead bind instead of retrying it as an
			// ambiguous local-generation timeout.
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
		},
	)
}

func TestRPCSendWithNotSupport(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			_, err := c.Send(ctx,
				&lock.Request{
					LockTable: lock.LockTable{ServiceID: "s1"},
					Method:    lock.Method_Lock})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
				"expected ErrNotSupported, got %T: %v", err, err)
		},
	)
}

func TestMOErrorCanHandled(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, moerr.NewDeadLockDetectedNoCtx(), cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.Error(t, err)
			require.Nil(t, resp)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected))
		},
	)
}

func TestRequestCanBeFilter(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.Error(t, err)
			require.Nil(t, resp)
			require.Equal(t, err, ctx.Err())
		},
		WithServerMessageFilter(func(r *lock.Request) bool { return false }),
	)
}

func TestRetryValidateService(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_ValidateService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			_, err := validateService(time.Millisecond*100, "s1", c, getLogger(""))
			require.True(t, err != nil && isRetryError(err))
		},
		WithServerMessageFilter(func(r *lock.Request) bool { return false }),
	)
}

type scriptedValidationClient struct {
	Client
	results      []bool
	sendCalls    int
	resetCalls   int
	resetService string
	resetErr     error
}

func (c *scriptedValidationClient) Send(
	_ context.Context,
	_ *lock.Request,
) (*lock.Response, error) {
	result := c.results[c.sendCalls]
	c.sendCalls++
	resp := acquireResponse()
	resp.ValidateService.OK = result
	return resp, nil
}

func (c *scriptedValidationClient) ResetValidationBackend(
	_ context.Context,
	serviceID string,
) error {
	c.resetCalls++
	c.resetService = serviceID
	return c.resetErr
}

type sendOnlyValidationClient struct {
	Client
	sendCalls int
}

func (c *sendOnlyValidationClient) Send(
	_ context.Context,
	_ *lock.Request,
) (*lock.Response, error) {
	c.sendCalls++
	resp := acquireResponse()
	resp.ValidateService.OK = false
	return resp, nil
}

func TestValidateServiceNegativeWithoutResetterRemainsIndeterminate(t *testing.T) {
	client := &sendOnlyValidationClient{}

	valid, err := validateService(
		time.Second,
		"service-generation",
		client,
		getLogger(""),
	)
	require.False(t, valid)
	require.ErrorContains(t, err,
		"cannot confirm negative lockservice identity without validation backend reset")
	require.True(t, isRetryError(err),
		"missing fresh-transport capability must retain the allocator bind")
	require.Equal(t, 1, client.sendCalls,
		"an unrefreshed second negative would not be independent evidence")
}

func TestValidateServiceConfirmsNegativeOnFreshBackend(t *testing.T) {
	for _, test := range []struct {
		name        string
		results     []bool
		resetErr    error
		expectValid bool
		expectErr   error
		expectSends int
	}{
		{
			name:        "stale negative becomes valid",
			results:     []bool{false, true},
			expectValid: true,
			expectSends: 2,
		},
		{
			name:        "fresh negative is authoritative",
			results:     []bool{false, false},
			expectValid: false,
			expectSends: 2,
		},
		{
			name:        "reset failure remains indeterminate",
			results:     []bool{false},
			resetErr:    moerr.NewInternalErrorNoCtx("refresh failed"),
			expectValid: false,
			expectErr:   moerr.NewInternalErrorNoCtx("refresh failed"),
			expectSends: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &scriptedValidationClient{
				results:  test.results,
				resetErr: test.resetErr,
			}
			valid, err := validateService(time.Second, "service-generation", client, getLogger(""))
			require.Equal(t, test.expectValid, valid)
			if test.expectErr != nil {
				require.ErrorContains(t, err, test.expectErr.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.expectSends, client.sendCalls)
			require.Equal(t, 1, client.resetCalls)
			require.Equal(t, "service-generation", client.resetService)
		})
	}
}

func TestValidateService(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_ValidateService,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.ValidateService.OK = true
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			valid, err := validateService(time.Millisecond*100, "UNKNOWN", c, getLogger(""))
			require.False(t, err != nil && isRetryError(err))
			require.True(t, !valid)

			valid, err = validateService(rpcTestResponseTimeout, "s1", c, getLogger(""))
			require.False(t, err != nil && isRetryError(err))
			require.False(t, !valid)
		},
	)
}

func TestLockTableBindChanged(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			s.RegisterMethodHandler(
				lock.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *lock.Request,
					resp *lock.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &lock.LockTable{ServiceID: "s1"}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			ctx, cancel := context.WithTimeout(context.Background(), rpcTestResponseTimeout)
			defer cancel()
			resp, err := c.Send(ctx, &lock.Request{
				LockTable: lock.LockTable{ServiceID: "s1"},
				Method:    lock.Method_Lock})
			require.NoError(t, err)
			require.NotNil(t, resp.NewBind)
			assert.Equal(t, lock.LockTable{ServiceID: "s1"}, *resp.NewBind)
			releaseResponse(resp)
		},
	)
}

func TestNewClientWithMOCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testSocketDir, err := createTestSocketDir()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, removeTestSocketDir(testSocketDir))
	}()
	testSockets := testSocketAddress(testSocketDir, "rpc.sock")
	sid := "sid"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		sid,
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID:          "mock",
					LockServiceAddress: testSockets,
				},
			},
			[]metadata.TNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	defer cluster.Close()
	var newClientFailed bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				newClientFailed = true
			}
		}()
		_, err := NewClient(sid, morpc.Config{})
		if err != nil {
			newClientFailed = true
		}
	}()
	require.True(t, newClientFailed, "new LockService Client without a process-level cluster nor a custom cluster should fail")
	c, err := NewClient(sid, morpc.Config{}, WithMOCluster(cluster))
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()
}

func TestResetBackendPinsAndReplacesResolvedEndpoint(t *testing.T) {
	runtime.SetupServiceBasedRuntime("reset-backend-test", runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		"reset-backend-test",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{{
				ServiceID:          "cn-id",
				LockServiceAddress: "cn.example:18101",
			}},
			nil))
	defer cluster.Close()

	normalRPCClient := &closeTrackingRPCClient{}
	activeTxnRPCClient := &closeTrackingRPCClient{}
	validationRPCClient := &closeTrackingRPCClient{}
	keeperRPCClient := &closeTrackingRPCClient{}
	endpoint := "10.0.0.1:18101"
	c := &client{
		cluster:          cluster,
		client:           normalRPCClient,
		activeTxnClient:  activeTxnRPCClient,
		validationClient: validationRPCClient,
		keeperClient:     keeperRPCClient,
		recoveryBackends: make(map[string]recoveryBackend),
		resolveBackend: func(context.Context, string) (string, error) {
			return endpoint, nil
		},
	}
	serviceID := "0000000000000000000cn-id"

	require.NoError(t, c.ResetBackend(context.Background(), serviceID))
	require.Equal(t, "10.0.0.1:18101", c.activeTxnBackend("cn-id", "cn.example:18101"))
	require.Empty(t, normalRPCClient.closed)
	require.Equal(t, []string{
		"cn.example:18101",
		"cn.example:18101",
		"10.0.0.1:18101",
	}, activeTxnRPCClient.closed)
	_, err := c.AsyncSend(context.Background(), &lock.Request{
		Method: lock.Method_CheckActiveTxn,
		CheckActiveTxn: lock.CheckActiveTxnRequest{
			ServiceID: serviceID,
		},
	})
	require.NoError(t, err)
	require.Empty(t, normalRPCClient.sent)
	require.Equal(t, []string{"10.0.0.1:18101"}, activeTxnRPCClient.sent)

	_, err = c.AsyncSend(context.Background(), &lock.Request{
		Method: lock.Method_ValidateService,
		ValidateService: lock.ValidateServiceRequest{
			ServiceID: serviceID,
		},
	})
	require.NoError(t, err)
	require.Empty(t, normalRPCClient.sent)
	require.Equal(t, []string{"10.0.0.1:18101"}, activeTxnRPCClient.sent)
	require.Equal(t, []string{"cn.example:18101"}, validationRPCClient.sent,
		"validation must use the current discovery address, not a pinned recovery endpoint")

	_, err = c.AsyncSend(context.Background(), &lock.Request{
		Method:    lock.Method_Unlock,
		LockTable: lock.LockTable{ServiceID: serviceID},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"cn.example:18101"}, normalRPCClient.sent)

	_, err = c.AsyncSend(context.Background(), &lock.Request{
		Method:    lock.Method_KeepRemoteLock,
		LockTable: lock.LockTable{ServiceID: serviceID},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"cn.example:18101"}, keeperRPCClient.sent)
	require.Equal(t, []string{"cn.example:18101"}, normalRPCClient.sent)

	endpoint = "10.0.0.2:18101"
	require.NoError(t, c.ResetBackend(context.Background(), serviceID))
	require.Equal(t, "10.0.0.2:18101", c.activeTxnBackend("cn-id", "cn.example:18101"))
	require.Empty(t, normalRPCClient.closed)
	require.Empty(t, validationRPCClient.closed)
	require.Equal(t, []string{
		"cn.example:18101",
		"cn.example:18101",
		"10.0.0.1:18101",
		"cn.example:18101",
		"10.0.0.1:18101",
		"cn.example:18101",
		"10.0.0.1:18101",
		"10.0.0.2:18101",
	}, activeTxnRPCClient.closed)

	// A service-discovery address change invalidates the recovery override.
	require.Equal(t, "other.example:18101", c.activeTxnBackend("cn-id", "other.example:18101"))
	c.recoveryMu.RLock()
	_, ok := c.recoveryBackends["cn-id"]
	c.recoveryMu.RUnlock()
	require.False(t, ok)
}

func TestResetBackendRefreshesNonEmptyStaleAddress(t *testing.T) {
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "old.example:18101",
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "new.example:18101",
		},
	}
	activeTxnRPCClient := &closeTrackingRPCClient{}
	var resolvedAddress string
	c := &client{
		cluster:          cluster,
		activeTxnClient:  activeTxnRPCClient,
		recoveryBackends: make(map[string]recoveryBackend),
		resolveBackend: func(_ context.Context, address string) (string, error) {
			resolvedAddress = address
			return "10.0.0.2:18101", nil
		},
	}

	require.NoError(t, c.ResetBackend(context.Background(), "0000000000000000000cn-id"))
	require.Equal(t, "new.example:18101", resolvedAddress)
	require.True(t, cluster.refreshed)
	require.True(t, cluster.synchronous)
	require.Equal(t, []string{
		"old.example:18101",
		"old.example:18101",
		"new.example:18101",
		"10.0.0.2:18101",
	}, activeTxnRPCClient.closed)
	require.Equal(t,
		"10.0.0.2:18101",
		c.activeTxnBackend("cn-id", "new.example:18101"),
	)
}

func TestResetBackendRejectsFailedDiscoveryRefresh(t *testing.T) {
	refreshErr := moerr.NewInternalErrorNoCtx("injected refresh failure")
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "stale.example:18101",
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "new.example:18101",
		},
		refreshErr: refreshErr,
	}
	activeTxnRPCClient := &closeTrackingRPCClient{}
	c := &client{
		cluster:         cluster,
		activeTxnClient: activeTxnRPCClient,
		recoveryBackends: map[string]recoveryBackend{
			"cn-id": {
				discovered: "stale.example:18101",
				endpoint:   "10.0.0.1:18101",
			},
		},
		resolveBackend: func(context.Context, string) (string, error) {
			t.Fatal("resolver must not run after a failed authoritative refresh")
			return "", nil
		},
	}

	err := c.ResetBackend(context.Background(), "0000000000000000000cn-id")
	require.ErrorIs(t, err, refreshErr)
	require.Equal(t, []string{
		"stale.example:18101",
		"10.0.0.1:18101",
		"stale.example:18101",
		"10.0.0.1:18101",
	}, activeTxnRPCClient.closed)
	require.Equal(t,
		"stale.example:18101",
		c.activeTxnBackend("cn-id", "stale.example:18101"),
		"a failed refresh must preserve unknown state without a stale route override",
	)
}

func TestResetBackendWaitHonorsContext(t *testing.T) {
	c := &client{}
	require.NoError(t, c.acquireRecoveryReset(context.Background()))
	defer c.releaseRecoveryReset()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := c.ResetBackend(ctx, "0000000000000000000cn-id")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func TestResetBackendClusterStartupWaitHonorsContext(t *testing.T) {
	service := t.Name()
	runtime.SetupServiceBasedRuntime(service, runtime.DefaultRuntime())
	hakeeper := &blockedClusterClient{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	cluster := clusterservice.NewMOCluster(service, hakeeper, time.Hour)
	defer func() {
		close(hakeeper.release)
		cluster.Close()
	}()

	select {
	case <-hakeeper.started:
	case <-time.After(time.Second):
		t.Fatal("cluster refresh did not start")
	}

	activeTxnRPCClient := &closeTrackingRPCClient{}
	c := &client{
		cluster:         cluster,
		activeTxnClient: activeTxnRPCClient,
		recoveryBackends: map[string]recoveryBackend{
			"cn-id": {
				discovered: "stale.example:18101",
				endpoint:   "10.0.0.1:18101",
			},
		},
		resolveBackend: func(context.Context, string) (string, error) {
			t.Fatal("resolver must not run before the cluster snapshot is ready")
			return "", nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := c.ResetBackend(ctx, "0000000000000000000cn-id")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, []string{
		"stale.example:18101",
		"10.0.0.1:18101",
	}, activeTxnRPCClient.closed)
	require.Equal(t,
		"stale.example:18101",
		c.activeTxnBackend("cn-id", "stale.example:18101"),
	)

	gateCtx, gateCancel := context.WithTimeout(context.Background(), time.Second)
	defer gateCancel()
	require.NoError(t, c.acquireRecoveryReset(gateCtx), "failed reset must release the global slot")
	c.releaseRecoveryReset()
}

func TestKeeperAsyncSendClusterStartupWaitHonorsContext(t *testing.T) {
	service := t.Name()
	runtime.SetupServiceBasedRuntime(service, runtime.DefaultRuntime())
	hakeeper := &blockedClusterClient{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	cluster := clusterservice.NewMOCluster(service, hakeeper, time.Hour)
	defer func() {
		close(hakeeper.release)
		cluster.Close()
	}()

	select {
	case <-hakeeper.started:
	case <-time.After(time.Second):
		t.Fatal("cluster refresh did not start")
	}

	c := &client{
		service: service,
		cluster: cluster,
		logger:  getLogger(service),
	}
	req := acquireRequest()
	req.Method = lock.Method_KeepRemoteLock
	req.LockTable.ServiceID = "missing"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	f, err := c.AsyncSend(ctx, req)
	require.Nil(t, f)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func TestResetValidationBackendReclosesStableRouteAfterRefresh(t *testing.T) {
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "cn.example:18101",
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "cn.example:18101",
		},
	}
	normalRPCClient := &closeTrackingRPCClient{}
	activeTxnRPCClient := &closeTrackingRPCClient{}
	validationRPCClient := &closeTrackingRPCClient{}
	c := &client{
		cluster:          cluster,
		client:           normalRPCClient,
		activeTxnClient:  activeTxnRPCClient,
		validationClient: validationRPCClient,
	}

	require.NoError(t, c.ResetValidationBackend(
		context.Background(),
		"0000000000000000000cn-id",
	))
	require.True(t, cluster.refreshed)
	require.Equal(t,
		[]string{
			"cn.example:18101",
			"cn.example:18101",
		},
		validationRPCClient.closed,
	)
	require.Empty(t, activeTxnRPCClient.closed,
		"validation confirmation must not reset active-txn recovery futures")
	require.Empty(t, normalRPCClient.closed)
}

func TestResetBackendSlowRefreshDoesNotBlockActiveTxnRouteLookup(t *testing.T) {
	refreshing := make(chan struct{})
	refreshDone := make(chan struct{})
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "old.example:18101",
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "new.example:18101",
		},
		refreshing:  refreshing,
		refreshDone: refreshDone,
	}
	c := &client{
		cluster:          cluster,
		activeTxnClient:  &closeTrackingRPCClient{},
		recoveryBackends: make(map[string]recoveryBackend),
		resolveBackend: func(_ context.Context, address string) (string, error) {
			return address, nil
		},
	}
	c.recoveryBackends["cn-id"] = recoveryBackend{
		discovered: "old.example:18101",
		endpoint:   "10.0.0.1:18101",
	}

	resetDone := make(chan error, 1)
	go func() {
		resetDone <- c.ResetBackend(context.Background(), "0000000000000000000cn-id")
	}()
	select {
	case <-refreshing:
	case <-time.After(time.Second):
		close(refreshDone)
		t.Fatal("reset did not enter discovery refresh")
	}

	lookupDone := make(chan string, 1)
	go func() {
		lookupDone <- c.activeTxnBackend("cn-id", "old.example:18101")
	}()
	select {
	case endpoint := <-lookupDone:
		require.Equal(t, "old.example:18101", endpoint)
	case <-time.After(time.Second):
		close(refreshDone)
		t.Fatal("active-txn route lookup blocked on slow discovery refresh")
	}

	close(refreshDone)
	select {
	case err := <-resetDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reset did not finish after discovery refresh completed")
	}
}

func TestResetBackendRepeatsFinalBarrierForSameAddress(t *testing.T) {
	const address = "same.example:18101"
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: address,
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: address,
		},
	}
	activeTxnRPCClient := &closeTrackingRPCClient{}
	c := &client{
		cluster:          cluster,
		activeTxnClient:  activeTxnRPCClient,
		recoveryBackends: make(map[string]recoveryBackend),
		resolveBackend: func(_ context.Context, value string) (string, error) {
			return value, nil
		},
	}

	require.NoError(t,
		c.ResetBackend(context.Background(), "0000000000000000000cn-id"))
	require.Equal(t, []string{address, address}, activeTxnRPCClient.closed,
		"the post-refresh close must fence a generation recreated during refresh")
}

func TestResetBackendFinalBarrierRunsAfterResolver(t *testing.T) {
	const address = "same.example:18101"
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: address,
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: address,
		},
	}
	resolving := make(chan struct{})
	releaseResolver := make(chan struct{})
	activeTxnRPCClient := &closeTrackingRPCClient{
		closedC: make(chan string, 4),
	}
	c := &client{
		cluster:          cluster,
		activeTxnClient:  activeTxnRPCClient,
		recoveryBackends: make(map[string]recoveryBackend),
		resolveBackend: func(_ context.Context, value string) (string, error) {
			close(resolving)
			<-releaseResolver
			return value, nil
		},
	}

	resetDone := make(chan error, 1)
	go func() {
		resetDone <- c.ResetBackend(
			context.Background(),
			"0000000000000000000cn-id",
		)
	}()
	select {
	case <-resolving:
	case <-time.After(time.Second):
		close(releaseResolver)
		t.Fatal("reset did not enter resolver")
	}
	require.Equal(t, address, <-activeTxnRPCClient.closedC)
	select {
	case remote := <-activeTxnRPCClient.closedC:
		close(releaseResolver)
		t.Fatalf("final barrier ran before resolver completed: %s", remote)
	default:
	}

	close(releaseResolver)
	require.NoError(t, <-resetDone)
	require.Equal(t, address, <-activeTxnRPCClient.closedC,
		"same-address generation recreated during resolve must be closed")
	require.Equal(t, []string{address, address}, activeTxnRPCClient.closed)
}

func TestResetBackendFinalBarrierRunsOnRefreshExitErrors(t *testing.T) {
	const address = "same.example:18101"
	for _, test := range []struct {
		name               string
		refreshErr         error
		cancelAfterRefresh bool
	}{
		{name: "refresh-error", refreshErr: errors.New("refresh failed")},
		{name: "post-refresh-lookup-error", cancelAfterRefresh: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cluster := &refreshOnDemandCluster{
				before: metadata.CNService{
					ServiceID:          "cn-id",
					LockServiceAddress: address,
				},
				after: metadata.CNService{
					ServiceID:          "cn-id",
					LockServiceAddress: address,
				},
				refreshErr: test.refreshErr,
			}
			if test.cancelAfterRefresh {
				cluster.cancelAfterRefresh = cancel
			}
			activeTxnRPCClient := &closeTrackingRPCClient{}
			c := &client{
				cluster:         cluster,
				activeTxnClient: activeTxnRPCClient,
				recoveryBackends: map[string]recoveryBackend{
					"cn-id": {
						discovered: address,
						endpoint:   "10.0.0.1:18101",
					},
				},
				resolveBackend: func(_ context.Context, value string) (string, error) {
					return value, nil
				},
			}

			require.Error(t,
				c.ResetBackend(ctx, "0000000000000000000cn-id"))
			require.Equal(t, []string{
				address,
				"10.0.0.1:18101",
				address,
				"10.0.0.1:18101",
			}, activeTxnRPCClient.closed,
				"the post-refresh barrier must run before every error exit")
		})
	}
}

func TestResetBackendCloseFailureAttemptsAllRoutesAndClearsCache(t *testing.T) {
	cluster := &refreshOnDemandCluster{
		before: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "old.example:18101",
		},
		after: metadata.CNService{
			ServiceID:          "cn-id",
			LockServiceAddress: "new.example:18101",
		},
	}
	closeErr := moerr.NewInternalErrorNoCtx("injected close failure")
	activeTxnRPCClient := &closeTrackingRPCClient{
		closeErrors: map[string]error{"old.example:18101": closeErr},
	}
	c := &client{
		cluster:         cluster,
		activeTxnClient: activeTxnRPCClient,
		recoveryBackends: map[string]recoveryBackend{
			"cn-id": {
				discovered: "prior.example:18101",
				endpoint:   "10.0.0.1:18101",
			},
		},
		resolveBackend: func(_ context.Context, _ string) (string, error) {
			return "10.0.0.2:18101", nil
		},
	}

	err := c.ResetBackend(context.Background(), "0000000000000000000cn-id")
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, []string{
		"prior.example:18101",
		"10.0.0.1:18101",
		"old.example:18101",
		"prior.example:18101",
		"10.0.0.1:18101",
		"old.example:18101",
		"new.example:18101",
		"10.0.0.2:18101",
	}, activeTxnRPCClient.closed)
	c.recoveryMu.RLock()
	_, cached := c.recoveryBackends["cn-id"]
	c.recoveryMu.RUnlock()
	require.False(t, cached, "failed reset must not retain a stale recovery route")
}

func TestResolveTCP4EndpointRequiresOneValidIPv4(t *testing.T) {
	tests := []struct {
		name     string
		ips      []net.IP
		expected string
		wantErr  bool
	}{
		{name: "no address", wantErr: true},
		{
			name: "multiple addresses",
			ips: []net.IP{
				net.ParseIP("10.0.0.1"),
				net.ParseIP("10.0.0.2"),
			},
			wantErr: true,
		},
		{
			name:    "non IPv4 address",
			ips:     []net.IP{net.ParseIP("2001:db8::1")},
			wantErr: true,
		},
		{
			name:     "one IPv4 address",
			ips:      []net.IP{net.ParseIP("10.0.0.1")},
			expected: "10.0.0.1:18101",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := resolveTCP4EndpointWithLookup(
				context.Background(),
				"cn.example:18101",
				func(context.Context, string, string) ([]net.IP, error) {
					return test.ips, nil
				},
			)
			if test.wantErr {
				require.Equal(t, "cn.example:18101", endpoint)
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.expected, endpoint)
		})
	}
}

func TestRecoveryBackendCacheIsBounded(t *testing.T) {
	c := &client{recoveryBackends: make(map[string]recoveryBackend)}
	c.recoveryMu.Lock()
	for i := 0; i < maxRecoveryBackendEntries; i++ {
		c.recoveryBackends[fmt.Sprintf("cn-%d", i)] = recoveryBackend{
			discovered: "cn.example:18101",
			endpoint:   "10.0.0.1:18101",
		}
	}
	c.storeRecoveryBackendLocked("new-cn", recoveryBackend{
		discovered: "new.example:18101",
		endpoint:   "10.0.0.2:18101",
	})
	cacheSize := len(c.recoveryBackends)
	_, ok := c.recoveryBackends["new-cn"]
	c.recoveryMu.Unlock()
	require.LessOrEqual(t, cacheSize, maxRecoveryBackendEntries)
	require.True(t, ok)
}

func runRPCTests(
	t *testing.T,
	fn func(Client, Server),
	opts ...ServerOption) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("s1", rt)
			runtime.SetupServiceBasedRuntime("s2", rt)

			reuse.RunReuseTests(func() {
				defer leaktest.AfterTest(t)()
				testSocketDir, err := createTestSocketDir()
				require.NoError(t, err)
				defer func() {
					require.NoError(t, removeTestSocketDir(testSocketDir))
				}()
				testSockets := testSocketAddress(testSocketDir, "rpc.sock")

				cluster := clusterservice.NewMOCluster(
					sid,
					nil,
					0,
					clusterservice.WithDisableRefresh(),
					clusterservice.WithServices(
						[]metadata.CNService{
							{
								ServiceID:          "s1",
								LockServiceAddress: testSockets,
							},
							{
								ServiceID:          "s2",
								LockServiceAddress: testSockets,
							},
						},
						[]metadata.TNService{
							{
								LockServiceAddress: testSockets,
							},
						}))
				defer cluster.Close()
				runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)

				s, err := NewServer(sid, testSockets, morpc.Config{}, opts...)
				require.NoError(t, err)
				defer func() {
					assert.NoError(t, s.Close())
				}()
				require.NoError(t, s.Start())

				c, err := NewClient(sid, morpc.Config{})
				require.NoError(t, err)
				defer func() {
					assert.NoError(t, c.Close())
				}()

				fn(c, s)
			})
		},
	)
}

func runRPCServerNoCloseTests(
	t *testing.T,
	fn func(Client, Server),
	opts ...ServerOption) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			testSocketDir, err := createTestSocketDir()
			require.NoError(t, err)
			defer func() {
				require.NoError(t, removeTestSocketDir(testSocketDir))
			}()
			testSockets := testSocketAddress(testSocketDir, "rpc.sock")

			cluster := clusterservice.NewMOCluster(
				sid,
				nil,
				0,
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(
					[]metadata.CNService{
						{
							ServiceID:          "s1",
							LockServiceAddress: testSockets,
						},
						{
							ServiceID:          "s2",
							LockServiceAddress: testSockets,
						},
					},
					[]metadata.TNService{
						{
							LockServiceAddress: testSockets,
						},
					}))
			defer cluster.Close()
			runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)

			s, err := NewServer(sid, testSockets, morpc.Config{}, opts...)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, s.Close())
			}()
			require.NoError(t, s.Start())

			c, err := NewClient(sid, morpc.Config{})
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()

			fn(c, s)
		},
	)
}
