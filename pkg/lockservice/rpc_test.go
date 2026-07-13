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
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
}

type refreshOnDemandCluster struct {
	clusterservice.MOCluster
	before      metadata.CNService
	after       metadata.CNService
	refreshed   bool
	synchronous bool
	refreshing  chan struct{}
	refreshDone chan struct{}
	refreshErr  error
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
	return c.closeErrors[remote]
}

type testClientSession struct {
	ctx         context.Context
	writeCtx    context.Context
	writeErr    error
	closeErr    error
	writeCalled bool
	asyncCalled bool
	closeCalled bool
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

func (s *testClientSession) Close() error {
	s.closeCalled = true
	return s.closeErr
}

func (s *testClientSession) SessionCtx() context.Context { return s.ctx }

func (s *testClientSession) Write(ctx context.Context, response morpc.Message) error {
	s.writeCtx = ctx
	s.writeCalled = true
	return s.writeErr
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
			// After auto-create wait timeout (500ms), should return ErrBackendClosed
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendClosed))
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

			valid, err = validateService(time.Millisecond*100, "s1", c, getLogger(""))
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
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(testSockets[7:]))
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
	endpoint := "10.0.0.1:18101"
	c := &client{
		cluster:          cluster,
		client:           normalRPCClient,
		activeTxnClient:  activeTxnRPCClient,
		recoveryBackends: make(map[string]recoveryBackend),
		resolveBackend: func(context.Context, string) (string, error) {
			return endpoint, nil
		},
	}
	serviceID := "0000000000000000000cn-id"

	require.NoError(t, c.ResetBackend(context.Background(), serviceID))
	require.Equal(t, "10.0.0.1:18101", c.activeTxnBackend("cn-id", "cn.example:18101"))
	require.Empty(t, normalRPCClient.closed)
	require.Equal(t, []string{"cn.example:18101"}, activeTxnRPCClient.closed)
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
		Method:    lock.Method_Unlock,
		LockTable: lock.LockTable{ServiceID: serviceID},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"cn.example:18101"}, normalRPCClient.sent)

	endpoint = "10.0.0.2:18101"
	require.NoError(t, c.ResetBackend(context.Background(), serviceID))
	require.Equal(t, "10.0.0.2:18101", c.activeTxnBackend("cn-id", "cn.example:18101"))
	require.Empty(t, normalRPCClient.closed)
	require.Equal(t, []string{
		"cn.example:18101",
		"cn.example:18101",
		"10.0.0.1:18101",
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
		"new.example:18101",
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
	require.Empty(t, activeTxnRPCClient.closed)
	require.Equal(t,
		"10.0.0.1:18101",
		c.activeTxnBackend("cn-id", "stale.example:18101"),
		"a failed refresh must not publish a successful reset state",
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
	require.Empty(t, activeTxnRPCClient.closed)

	gateCtx, gateCancel := context.WithTimeout(context.Background(), time.Second)
	defer gateCancel()
	require.NoError(t, c.acquireRecoveryReset(gateCtx), "failed reset must release the global slot")
	c.releaseRecoveryReset()
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
		require.Equal(t, "10.0.0.1:18101", endpoint)
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
		"old.example:18101",
		"new.example:18101",
		"prior.example:18101",
		"10.0.0.1:18101",
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
				testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
				assert.NoError(t, os.RemoveAll(testSockets[7:]))

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
			testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
			assert.NoError(t, os.RemoveAll(testSockets[7:]))

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
