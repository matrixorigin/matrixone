// Copyright 2026 Matrix Origin
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

package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

type shutdownQueryClient struct {
	client.QueryClient
	closed atomic.Bool
}

func (q *shutdownQueryClient) Close() error {
	q.closed.Store(true)
	return nil
}

type shutdownCluster struct{ clusterservice.MOCluster }

func (*shutdownCluster) Close() {}

// Close must not release a connection's RPC dependencies while its final
// disconnect notification is still running.
func TestServerCloseWaitsForDisconnectCleanup(t *testing.T) {
	rt := runtime.DefaultRuntime()
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	query := &shutdownQueryClient{}
	h := &handler{
		logger:         rt.Logger(),
		counterSet:     newCounterSet(),
		moCluster:      &shutdownCluster{},
		queryClient:    query,
		haKeeperClient: &mockHAKeeperClient{},
	}
	notificationStarted := make(chan struct{})
	releaseNotification := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseNotification) })
	t.Cleanup(release)
	finished := make(chan struct{})
	tun := &tunnel{reqC: make(chan IEvent), connCacheEnabled: true}
	markTunnelInFlight(tun)
	tun.mu.sc = &killCurrentServerConn{cn: &CNServer{connID: 10}}
	cc := &mockClientConn{killFn: func(ServerConn) error {
		close(notificationStarted)
		<-releaseNotification
		return nil
	}}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	app, err := goetty.NewApplicationWithListeners([]net.Listener{listener}, nil,
		goetty.WithAppHandleSessionFunc(func(goetty.IOSession) error {
			defer close(finished)
			h.cleanupBackendOnClientDisconnect(io.EOF, cc, tun)
			return nil
		}))
	require.NoError(t, err)
	s := &Server{runtime: rt, app: app, listener: listener, handler: h}
	t.Cleanup(func() { release(); _ = s.Close() })
	require.NoError(t, s.Start())
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	select {
	case <-notificationStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("connection handler did not enter disconnect cleanup")
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	// Stop disconnects the real session before handler dependencies may close.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, readErr := conn.Read(make([]byte, 1))
	require.Error(t, readErr)
	select {
	case err = <-closed:
		t.Fatalf("Close returned before disconnect notification finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.False(t, query.closed.Load(), "query client must outlive the handler")
	require.Same(t, rt, runtime.ServiceRuntime(sid))
	release()
	select {
	case err = <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after the handler completed")
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before the handler's final cleanup")
	}
	require.True(t, query.closed.Load())
	require.NoError(t, s.Close())
}

type unavailableClusterClient struct {
	mockHAKeeperClient
	refreshStarted chan struct{}
	release        chan struct{}
	once           sync.Once
}

func (c *unavailableClusterClient) GetClusterDetails(ctx context.Context) (logservicepb.ClusterDetails, error) {
	c.once.Do(func() { close(c.refreshStarted) })
	select {
	case <-ctx.Done():
		return logservicepb.ClusterDetails{}, ctx.Err()
	case <-c.release:
		return logservicepb.ClusterDetails{}, nil
	}
}

// GetCNService has no context; shutting down the cluster must release its
// first-refresh waiters before waiting for the connection handler.
func TestServerCloseReleasesUnreadyCluster(t *testing.T) {
	rt := runtime.DefaultRuntime()
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	client := &unavailableClusterClient{refreshStarted: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(client.release) })
	t.Cleanup(release)
	cluster := clusterservice.NewMOCluster(sid, client, time.Hour)
	h := &handler{moCluster: cluster, haKeeperClient: client}
	entered := make(chan struct{})
	finished := make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	app, err := goetty.NewApplicationWithListeners([]net.Listener{listener}, nil,
		goetty.WithAppHandleSessionFunc(func(goetty.IOSession) error {
			defer close(finished)
			close(entered)
			cluster.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { return true })
			return nil
		}))
	require.NoError(t, err)
	s := &Server{runtime: rt, app: app, listener: listener, handler: h}
	t.Cleanup(func() { release(); _ = s.Close() })
	require.NoError(t, s.Start())
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	select {
	case <-client.refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("initial cluster refresh did not start")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not reach cluster lookup")
	}
	select {
	case <-finished:
		t.Fatal("lookup returned before the initial refresh or shutdown")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err = <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		release()
		<-closed
		t.Fatal("Close waited for unavailable HAKeeper instead of releasing lookup")
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before the lookup handler finished")
	}
}

func TestServerCloseCancelsClientHandshake(t *testing.T) {
	rt := runtime.DefaultRuntime()
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	s, err := NewServer(context.Background(), Config{UUID: sid, ListenAddress: "127.0.0.1:0"},
		WithRuntime(rt), WithHAKeeperClient(&mockHAKeeperClient{}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.Start())
	conn, err := net.DialTimeout("tcp", s.listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = conn.Read(make([]byte, 256)) // Receive the greeting; send no authentication response.
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err = <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not stop a client waiting in authentication")
	}
}

func TestShutdownCancelsInFlightTunnel(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pipeErr   error
		wantErr   error
		wantKills int
	}{
		{name: "no published error", wantErr: context.Canceled, wantKills: 1},
		{name: "pipe reports cancellation as server error", pipeErr: withCode(context.Canceled, codeServerDisconnect), wantErr: context.Canceled, wantKills: 1},
		{name: "already disconnected server", pipeErr: withCode(io.EOF, codeServerDisconnect)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			rt := runtime.DefaultRuntime()
			h := &handler{ctx: ctx, logger: rt.Logger(), counterSet: newCounterSet()}
			tun := &tunnel{reqC: make(chan IEvent), errC: make(chan error, 1), connCacheEnabled: true}
			markTunnelInFlight(tun)
			closed, killed := 0, 0
			tun.mu.sc = &killCurrentServerConn{cn: &CNServer{connID: 10}, closeFn: func() error {
				closed++
				return nil
			}}
			cc := &mockClientConn{killFn: func(ServerConn) error {
				killed++
				return nil
			}}
			if tc.pipeErr != nil {
				tun.errC <- tc.pipeErr
			}
			cancel()
			require.ErrorIs(t, h.waitForTunnel(cc, tun, 1, 1), tc.wantErr)
			require.Equal(t, tc.wantKills, closed)
			require.Equal(t, tc.wantKills, killed)
		})
	}
}

func TestTunnelCloseCancelsEventDelivery(t *testing.T) {
	for _, deliver := range []bool{false, true} {
		t.Run(map[bool]string{false: "not delivered", true: "already delivered"}[deliver], func(t *testing.T) {
			tun := newTunnel(context.Background(), runtime.DefaultRuntime().Logger(), newCounterSet(), withConnCacheEnabled(true))
			buf := &msgBuf{reqC: tun.reqC, connCacheEnabled: true}
			type result struct {
				handled bool
				err     error
			}
			done := make(chan result, 1)
			go func() {
				handled, err := buf.consumeClient(tun.ctx, makeQuitPacket())
				done <- result{handled, err}
			}()
			var event IEvent
			if deliver {
				select {
				case event = <-tun.reqC:
				case <-time.After(time.Second):
					t.Fatal("event was not delivered")
				}
			}
			require.NoError(t, tun.Close())
			if deliver {
				select {
				case <-done:
					t.Fatal("delivered event returned before acknowledgement")
				default:
				}
				event.notify()
			}
			select {
			case got := <-done:
				if deliver {
					require.NoError(t, got.err)
					require.True(t, got.handled)
				} else {
					require.ErrorIs(t, got.err, context.Canceled)
					require.False(t, got.handled)
				}
			case <-time.After(time.Second):
				t.Fatal("event delivery did not finish")
			}
		})
	}
}

func TestTunnelCloseWaitsForForwarder(t *testing.T) {
	client, clientPeer := net.Pipe()
	backend, backendPeer := net.Pipe()
	defer clientPeer.Close()
	defer backendPeer.Close()
	tun := newTunnel(context.Background(), runtime.DefaultRuntime().Logger(), newCounterSet())
	clientConn := newMySQLConn(connClientName, client, 0, tun.reqC, tun.respC, false, 1)
	serverConn := newMySQLConn(connServerName, backend, 0, tun.reqC, tun.respC, false, 2)
	csp := tun.newPipe(pipeClientToServer, clientConn, serverConn)
	scp := tun.newPipe(pipeServerToClient, serverConn, clientConn)
	tun.mu.clientConn, tun.mu.serverConn = clientConn, serverConn
	tun.mu.csp, tun.mu.scp = csp, scp
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	csp.testHelper.beforeSend = func() { close(entered); <-release }
	require.NoError(t, tun.kickoff())
	go func() { _, _ = clientPeer.Write(makeSimplePacket("select 1")) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("forwarder did not receive the command")
	}
	require.NoError(t, tun.Close())
	done := make(chan struct{})
	go func() { tun.workers.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("tunnel workers completed while a forwarder was active")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwarder did not stop after release")
	}
}

func TestTunnelStartsBothPipesBeforeQuit(t *testing.T) {
	client, clientPeer := net.Pipe()
	backend, backendPeer := net.Pipe()
	defer clientPeer.Close()
	defer backendPeer.Close()
	defer backend.Close()
	tun := newTunnel(context.Background(), runtime.DefaultRuntime().Logger(), newCounterSet(), withConnCacheEnabled(true))
	t.Cleanup(func() { _ = tun.Close() })
	clientConn := newMySQLConn(connClientName, client, 0, tun.reqC, tun.respC, true, 1)
	serverConn := newMySQLConn(connServerName, backend, 0, tun.reqC, tun.respC, true, 2)
	csp := tun.newPipe(pipeClientToServer, clientConn, serverConn)
	scp := tun.newPipe(pipeServerToClient, serverConn, clientConn)
	tun.mu.clientConn, tun.mu.serverConn = clientConn, serverConn
	tun.mu.csp, tun.mu.scp = csp, scp

	// Hold s2c before its start handshake while a client already has QUIT ready.
	scp.mu.Lock()
	unlock := sync.OnceFunc(scp.mu.Unlock)
	t.Cleanup(unlock)
	started := make(chan error, 1)
	go func() { started <- tun.kickoff() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cspReady := make(chan error, 1)
	go func() { cspReady <- csp.waitReady(ctx) }()
	select {
	case err := <-cspReady:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("client pipe did not start")
	}
	go func() { _, _ = clientPeer.Write(makeQuitPacket()) }()
	events := make(chan IEvent, 1)
	go func() {
		select {
		case event := <-tun.reqC:
			events <- event
		case <-ctx.Done():
		}
	}()
	select {
	case event := <-events:
		event.notify()
		t.Fatal("QUIT was delivered before the server pipe started")
	case <-time.After(100 * time.Millisecond):
	}
	unlock()
	select {
	case err := <-started:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("pipes did not start")
	}
	var event IEvent
	select {
	case event = <-events:
	case <-time.After(time.Second):
		t.Fatal("QUIT was not delivered")
	}
	// This is the pause used by QUIT before a backend enters the cache.
	require.NoError(t, scp.pause(ctx))
	event.notify()
	require.NoError(t, tun.Close())
	done := make(chan struct{})
	go func() { tun.workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cached backend kept a forwarding worker alive")
	}
}

type shutdownTransferClient struct {
	ClientConn
	started, canceled, release chan struct{}
}

func (c *shutdownTransferClient) BuildConnWithServer(ctx context.Context, _ string) (ServerConn, error) {
	close(c.started)
	<-ctx.Done()
	close(c.canceled)
	<-c.release
	return nil, ctx.Err()
}

func TestTunnelCloseWaitsForTransfer(t *testing.T) {
	client, clientPeer := net.Pipe()
	backend, backendPeer := net.Pipe()
	defer clientPeer.Close()
	defer backendPeer.Close()
	tun := newTunnel(context.Background(), runtime.DefaultRuntime().Logger(), newCounterSet())
	clientConn := newMySQLConn(connClientName, client, 0, tun.reqC, tun.respC, false, 1)
	serverConn := newMySQLConn(connServerName, backend, 0, tun.reqC, tun.respC, false, 2)
	tun.mu.clientConn, tun.mu.serverConn = clientConn, serverConn
	tun.mu.csp = tun.newPipe(pipeClientToServer, clientConn, serverConn)
	tun.mu.scp = tun.newPipe(pipeServerToClient, serverConn, clientConn)
	tun.mu.started = true
	transfer := &shutdownTransferClient{
		started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
	}
	unblock := sync.OnceFunc(func() { close(transfer.release) })
	t.Cleanup(unblock)
	tun.cc = transfer
	go func() { _ = tun.transfer(context.Background()) }()
	select {
	case <-transfer.started:
	case <-time.After(time.Second):
		t.Fatal("transfer did not start")
	}
	require.NoError(t, tun.Close())
	select {
	case <-transfer.canceled:
	case <-time.After(time.Second):
		t.Fatal("closing the tunnel did not cancel transfer")
	}
	done := make(chan struct{})
	go func() { tun.workers.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("tunnel workers completed while transfer was active")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("transfer did not stop after release")
	}
}

func TestCancelledEventResponseDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { sendRespContext(ctx, []byte("error"), make(chan []byte)); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled event response blocked")
	}
}
