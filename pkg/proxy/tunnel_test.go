// Copyright 2021 - 2023 Matrix Origin
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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func TestTunnelClientToServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	baseCtx := context.Background()
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	tu := newTunnel(baseCtx, logger, nil)
	defer func() { _ = tu.Close() }()

	clientProxy, client := net.Pipe()
	serverProxy, server := net.Pipe()

	cc := newMockClientConn(clientProxy, "t1", clientInfo{}, nil, tu)
	require.NotNil(t, cc)

	sc := newMockServerConn(serverProxy)
	require.NotNil(t, sc)

	err := tu.run(cc, sc)
	require.NoError(t, err)
	require.Nil(t, tu.ctx.Err())

	func() {
		tu.mu.Lock()
		defer tu.mu.Unlock()
		require.True(t, tu.mu.started)
	}()

	tu.mu.Lock()
	csp := tu.mu.csp
	scp := tu.mu.scp
	tu.mu.Unlock()

	barrierStart, barrierEnd := make(chan struct{}), make(chan struct{})
	csp.testHelper.beforeSend = func() {
		<-barrierStart
		<-barrierEnd
	}

	csp.mu.Lock()
	require.True(t, csp.mu.started)
	csp.mu.Unlock()

	scp.mu.Lock()
	require.True(t, scp.mu.started)
	scp.mu.Unlock()

	// Client writes some MySQL packets.
	sendEventCh := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	go func() {
		<-sendEventCh
		if _, err := client.Write(makeSimplePacket("select 1")); err != nil {
			errChan <- err
			return
		}

		<-sendEventCh
		if _, err := client.Write(makeSimplePacket("begin")); err != nil {
			errChan <- err
			return
		}

		<-sendEventCh
		if _, err := client.Write(makeSimplePacket("select 1")); err != nil {
			errChan <- err
			return
		}

		<-sendEventCh
		if _, err := client.Write(makeSimplePacket("commit")); err != nil {
			errChan <- err
			return
		}
	}()

	sendEventCh <- struct{}{}
	barrierStart <- struct{}{}
	scp.mu.Lock()
	require.Equal(t, true, scp.safeToTransferLocked())
	scp.mu.Unlock()
	barrierEnd <- struct{}{}

	ret := make([]byte, 30)
	n, err := server.Read(ret)
	require.NoError(t, err)
	require.Equal(t, 13, n)
	l, err := packetLen(ret)
	require.NoError(t, err)
	require.Equal(t, 9, int(l))
	require.Equal(t, comQuery, int(ret[4]))
	require.Equal(t, "select 1", string(ret[5:13]))

	sendEventCh <- struct{}{}
	barrierStart <- struct{}{}
	scp.mu.Lock()
	require.Equal(t, true, scp.safeToTransferLocked())
	scp.mu.Unlock()
	barrierEnd <- struct{}{}

	ret = make([]byte, 30)
	n, err = server.Read(ret)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	l, err = packetLen(ret)
	require.NoError(t, err)
	require.Equal(t, 6, int(l))
	require.Equal(t, comQuery, int(ret[4]))
	require.Equal(t, "begin", string(ret[5:10]))

	sendEventCh <- struct{}{}
	barrierStart <- struct{}{}
	scp.mu.Lock()
	// in txn
	require.Equal(t, true, scp.safeToTransferLocked())
	scp.mu.Unlock()
	barrierEnd <- struct{}{}

	ret = make([]byte, 30)
	n, err = server.Read(ret)
	require.NoError(t, err)
	require.Equal(t, 13, n)
	l, err = packetLen(ret)
	require.NoError(t, err)
	require.Equal(t, 9, int(l))
	require.Equal(t, comQuery, int(ret[4]))
	require.Equal(t, "select 1", string(ret[5:13]))

	sendEventCh <- struct{}{}
	barrierStart <- struct{}{}
	scp.mu.Lock()
	// out of txn
	require.Equal(t, true, scp.safeToTransferLocked())
	scp.mu.Unlock()
	barrierEnd <- struct{}{}

	ret = make([]byte, 30)
	n, err = server.Read(ret)
	require.NoError(t, err)
	require.Equal(t, 11, n)
	l, err = packetLen(ret)
	require.NoError(t, err)
	require.Equal(t, 7, int(l))
	require.Equal(t, comQuery, int(ret[4]))
	require.Equal(t, "commit", string(ret[5:11]))
	select {
	case err = <-errChan:
		t.Fatalf("require no error, but got %v", err)
	default:
	}
}

func TestWrapPipeSendError(t *testing.T) {
	err := wrapPipeSendError(pipeServerToClient, net.ErrClosed)
	require.Equal(t, codeClientDisconnect, getErrorCode(err))
	require.True(t, errors.Is(err, net.ErrClosed))
	require.True(t, isConnEndErr(err))

	err = wrapPipeSendError(pipeClientToServer, net.ErrClosed)
	require.Equal(t, codeNone, getErrorCode(err))
	require.True(t, errors.Is(err, net.ErrClosed))
}

func TestTunnelServerClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	baseCtx := context.Background()

	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	tu := newTunnel(baseCtx, logger, nil)
	defer func() { _ = tu.Close() }()

	clientProxy, client := net.Pipe()
	serverProxy, server := net.Pipe()

	cc := newMockClientConn(clientProxy, "t1", clientInfo{}, nil, tu)
	require.NotNil(t, cc)

	sc := newMockServerConn(serverProxy)
	require.NotNil(t, sc)

	err := tu.run(cc, sc)
	require.NoError(t, err)
	require.Nil(t, tu.ctx.Err())

	func() {
		tu.mu.Lock()
		defer tu.mu.Unlock()
		require.True(t, tu.mu.started)
	}()

	tu.mu.Lock()
	csp := tu.mu.csp
	scp := tu.mu.scp
	tu.mu.Unlock()

	csp.mu.Lock()
	require.True(t, csp.mu.started)
	csp.mu.Unlock()

	scp.mu.Lock()
	require.True(t, scp.mu.started)
	scp.mu.Unlock()

	// Client writes some MySQL packets.
	recvEventCh := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	go func() {
		<-recvEventCh
		if _, err := server.Write(makeSimplePacket("123456")); err != nil {
			errChan <- err
			return
		}
	}()

	recvEventCh <- struct{}{}
	ret := make([]byte, 30)
	n, err := client.Read(ret)
	require.NoError(t, err)
	require.Equal(t, 11, n)
	l, err := packetLen(ret)
	require.NoError(t, err)
	require.Equal(t, 7, int(l))
	require.Equal(t, comQuery, int(ret[4]))
	require.Equal(t, "123456", string(ret[5:11]))
	select {
	case err = <-errChan:
		t.Fatalf("require no error, but got %v", err)
	default:
	}
}

func TestTunnelClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rt := runtime.DefaultRuntime()
	for _, withRun := range []bool{true, false} {
		t.Run(fmt.Sprintf("withRun=%t", withRun), func(t *testing.T) {
			f := newTunnel(ctx, rt.Logger(), nil)
			defer f.Close()

			if withRun {
				p1, p2 := net.Pipe()

				cc := newMockClientConn(p1, "t1", clientInfo{}, nil, nil)
				require.NotNil(t, cc)

				sc := newMockServerConn(p2)
				require.NotNil(t, sc)

				err := f.run(cc, sc)
				require.NoError(t, err)
			}

			require.Nil(t, f.ctx.Err())
			f.Close()
			require.EqualError(t, f.ctx.Err(), context.Canceled.Error())
		})
	}
}

func TestTunnelReplaceConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	ctx := context.Background()
	clientProxy, client := net.Pipe()
	serverProxy, server := net.Pipe()

	tu := newTunnel(ctx, rt.Logger(), nil)
	defer tu.Close()

	cc := newMockClientConn(clientProxy, "t1", clientInfo{}, nil, tu)
	require.NotNil(t, cc)

	sc := newMockServerConn(serverProxy)
	require.NotNil(t, sc)

	err := tu.run(cc, sc)
	require.NoError(t, err)

	c, s := tu.getConns()
	require.Equal(t, clientProxy, c.Conn)
	require.Equal(t, serverProxy, s.Conn)
	require.NotNil(t, s.msgBuf.bufDst)
	require.Equal(t, proxyTunnelBufferSize,
		cap(c.msgBuf.buf)+cap(s.msgBuf.buf)+s.msgBuf.bufDst.Size(),
		"the budget constant must match concrete tunnel buffer capacities")
	oldWriter := s.msgBuf.bufDst

	csp, scp := tu.getPipes()
	require.NoError(t, csp.pause(ctx))
	require.NoError(t, scp.pause(ctx))

	newServerProxy, newServer := net.Pipe()
	newServerConn := newMySQLConn(
		"server",
		newServerProxy,
		0,
		nil,
		nil,
		false, 0,
	)
	tu.replaceServerConn(
		newServerConn,
		nil,
		false,
	)
	require.Same(t, oldWriter, newServerConn.msgBuf.bufDst,
		"non-sync migration must reuse the writer whose client destination is unchanged")
	require.NoError(t, tu.kickoff())

	go func() {
		_, _ = newServer.Write(makeSimplePacket("123456"))
	}()
	ret := make([]byte, 30)
	n, err := client.Read(ret)
	require.NoError(t, err)
	l, err := packetLen(ret)
	require.NoError(t, err)
	require.Equal(t, 11, n)
	require.Equal(t, 7, int(l))
	require.Equal(t, comQuery, int(ret[4]))
	require.Equal(t, "123456", string(ret[5:11]))

	_, err = server.Write([]byte("closed error"))
	require.Regexp(t, "closed pipe", err)
}

func TestPipeCancelError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	// cancel the context immediately
	cancel()

	clientProxy, serverProxy := net.Pipe()
	defer clientProxy.Close()
	defer serverProxy.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	tun := newTunnel(ctx, logger, newCounterSet())

	cc := newMySQLConn("client", clientProxy, 0, nil, nil, false, 0)
	sc := newMySQLConn("server", serverProxy, 0, nil, nil, false, 0)
	p := tun.newPipe(pipeClientToServer, cc, sc)
	err := p.kickoff(ctx, nil)
	require.EqualError(t, err, context.Canceled.Error())
	p.mu.Lock()
	require.True(t, p.mu.closed)
	p.mu.Unlock()

	p.mu.Lock()
	p.mu.started = true
	p.mu.Unlock()
	require.EqualError(t, p.pause(ctx), errPipeClosed.Error())
}

func TestPipeStart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientProxy, serverProxy := net.Pipe()
	defer clientProxy.Close()
	defer serverProxy.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	tun := newTunnel(ctx, logger, newCounterSet())

	cc := newMySQLConn("client", clientProxy, 0, nil, nil, false, 0)
	sc := newMySQLConn("server", serverProxy, 0, nil, nil, false, 0)
	p := tun.newPipe(pipeClientToServer, cc, sc)

	errCh := make(chan error)
	go func() {
		errCh <- p.waitReady(ctx)
	}()

	select {
	case <-errCh:
		t.Fatal("expected not started")
	default:
	}

	go func() { _ = p.kickoff(ctx, nil) }()

	var lastErr error
	require.Eventually(t, func() bool {
		select {
		case lastErr = <-errCh:
			return true
		default:
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, lastErr)
}

func TestPipePauseTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	clientProxy, serverProxy := net.Pipe()
	defer clientProxy.Close()
	defer serverProxy.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	tun := newTunnel(context.Background(), logger, newCounterSet())

	cc := newMySQLConn("client", clientProxy, 0, nil, nil, false, 0)
	sc := newMySQLConn("server", serverProxy, 0, nil, nil, false, 0)
	p := tun.newPipe(pipeClientToServer, cc, sc)

	p.mu.Lock()
	p.mu.started = true
	p.mu.Unlock()

	start := time.Now()
	err := p.pause(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
}

func TestPipeStartAndPause(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientProxy, serverProxy := net.Pipe()
	defer clientProxy.Close()
	defer serverProxy.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	tun := newTunnel(ctx, logger, newCounterSet())

	cc := newMySQLConn("client", clientProxy, 0, nil, nil, false, 0)
	sc := newMySQLConn("server", serverProxy, 0, nil, nil, false, 0)
	p := tun.newPipe(pipeClientToServer, cc, sc)

	errCh := make(chan error, 2)
	go func() { errCh <- p.kickoff(ctx, nil) }()
	go func() { errCh <- p.kickoff(ctx, nil) }()
	go func() { errCh <- p.kickoff(ctx, nil) }()
	err := <-errCh
	require.NoError(t, err)
	err = <-errCh
	require.NoError(t, err)

	err = p.waitReady(ctx)
	require.NoError(t, err)
	err = p.pause(ctx)
	require.NoError(t, err)

	err = <-errCh
	require.NoError(t, err)
	p.mu.Lock()
	require.False(t, p.mu.started)
	require.False(t, p.mu.inPreRecv)
	require.False(t, p.mu.paused)
	p.mu.Unlock()

	err = p.pause(ctx)
	require.NoError(t, err)

	p.mu.Lock()
	require.False(t, p.mu.started)
	require.False(t, p.mu.inPreRecv)
	require.False(t, p.mu.paused)
	p.mu.Unlock()
}

func TestCacheQuitDoesNotLeakPostQuitCommandIntoReusedBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientProxy, client := net.Pipe()
	defer clientProxy.Close()
	defer client.Close()
	backendProxy, backend := net.Pipe()
	defer backendProxy.Close()
	defer backend.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	tun := newTunnel(ctx, rt.Logger(), newCounterSet(), withConnCacheEnabled(true))
	clientMySQLConn := newMySQLConn(
		connClientName, clientProxy, 0, tun.reqC, tun.respC, true, 1,
	)
	serverConn := newMySQLConn(
		connServerName, backendProxy, 0, tun.reqC, tun.respC, true, 2,
	)
	csp := tun.newPipe(pipeClientToServer, clientMySQLConn, serverConn)
	scp := tun.newPipe(pipeServerToClient, serverConn, clientMySQLConn)
	tun.mu.Lock()
	tun.mu.clientConn = clientMySQLConn
	tun.mu.serverConn = serverConn
	tun.mu.csp = csp
	tun.mu.scp = scp
	tun.mu.Unlock()

	cached := &killCurrentServerConn{cn: &CNServer{connID: 2, uuid: "cn1"}}
	pushed := make(chan struct{})
	var pushOnce sync.Once
	var sealedAtPush atomic.Bool
	cache := &mockConnCache{
		pushFn: func(cacheKey, ServerConn) bool {
			csp.mu.Lock()
			sealedAtPush.Store(csp.mu.closed && csp.mu.paused)
			csp.mu.Unlock()
			pushOnce.Do(func() { close(pushed) })
			return true
		},
		popFn: func(cacheKey, uint32, []byte, []byte) ServerConn {
			return cached
		},
	}
	cc := &clientConn{
		log:       rt.Logger(),
		tun:       tun,
		sc:        cached,
		connCache: cache,
	}

	eventDone := make(chan error, 1)
	go func() {
		select {
		case event, ok := <-tun.reqC:
			if !ok {
				eventDone <- io.EOF
				return
			}
			eventDone <- cc.HandleEvent(ctx, event, tun.respC)
		case <-ctx.Done():
			eventDone <- context.Cause(ctx)
		}
	}()

	// Force the vulnerable ordering deterministically: if c2s attempts a
	// second send, it cannot forward it until the backend has been published.
	var sendCount atomic.Int32
	csp.testHelper.beforeSend = func() {
		if sendCount.Add(1) == 2 {
			<-pushed
		}
	}
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- csp.kickoff(ctx, scp) }()
	require.NoError(t, csp.waitReady(ctx))

	postQuitCommand := makeSimplePacket("post quit command")
	setConnectionID := makeSimplePacket("set connection id 3")
	staleResponse := makeErrPacket(8)
	freshResponse := makeOKPacket(8)
	backendDone := make(chan error, 1)
	go func() {
		receiver := newMySQLConn("backend", backend, 0, nil, nil, false, 0)
		first, err := receiver.receive()
		if err != nil {
			backendDone <- err
			return
		}
		if bytes.Equal(first, postQuitCommand) {
			staleWritten := make(chan error, 1)
			go func() {
				_, err := backend.Write(staleResponse)
				staleWritten <- err
			}()
			second, err := receiver.receive()
			if err != nil {
				backendDone <- err
				return
			}
			if !bytes.Equal(second, setConnectionID) {
				backendDone <- fmt.Errorf("unexpected command after stale response: %v", second)
				return
			}
			if err := <-staleWritten; err != nil {
				backendDone <- err
				return
			}
		} else if !bytes.Equal(first, setConnectionID) {
			backendDone <- fmt.Errorf("unexpected first backend command: %v", first)
			return
		}
		_, err = backend.Write(freshResponse)
		backendDone <- err
	}()

	combined := append(append([]byte{}, makeQuitPacket()...), postQuitCommand...)
	clientWriteDone := make(chan error, 1)
	go func() {
		// Split immediately before the COM_QUIT command byte. The second write
		// also coalesces the post-QUIT command, covering both fragmentation and
		// the cached-buffer generation race in one deterministic schedule.
		_, err := client.Write(combined[:mysqlHeadLen])
		if err == nil {
			_, err = client.Write(combined[mysqlHeadLen:])
		}
		clientWriteDone <- err
	}()
	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("backend was not published after COM_QUIT")
	}
	require.True(t, sealedAtPush.Load(),
		"the old client pipe must be terminal before cache publication")
	require.NoError(t, client.Close())
	select {
	case pipeErr := <-pipeDone:
		require.ErrorIs(t, pipeErr, io.EOF)
		require.Equal(t, codeClientDisconnect, getErrorCode(pipeErr))
	case <-time.After(time.Second):
		t.Fatal("old client pipe did not terminate")
	}
	require.NoError(t, <-clientWriteDone)
	require.NoError(t, <-eventDone)

	reused := cache.Pop("", 3, nil, nil, clientInfo{})
	require.Same(t, cached, reused)
	_, err := backendProxy.Write(setConnectionID)
	require.NoError(t, err)
	reusedConn := newMySQLConn("reused", backendProxy, 0, nil, nil, false, 0)
	response, err := reusedConn.receive()
	require.NoError(t, err)
	require.Equal(t, freshResponse, response,
		"the reused session must not read a response generated by the old client")
	require.NoError(t, <-backendDone)
}

func TestPipeMultipleStartAndPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientProxy, client := net.Pipe()
	defer clientProxy.Close()
	defer client.Close()
	serverProxy, server := net.Pipe()
	defer serverProxy.Close()
	defer server.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	tun := newTunnel(ctx, logger, newCounterSet())

	cc := newMySQLConn("client", clientProxy, 0, nil, nil, false, 0)
	sc := newMySQLConn("server", serverProxy, 0, nil, nil, false, 0)
	p := tun.newPipe(pipeClientToServer, cc, sc)

	const (
		queryCount  = 100
		concurrency = 200
	)

	buf := new(bytes.Buffer)
	pack := makeSimplePacket("select 1")

	for i := 0; i < queryCount; i++ {
		if i%2 == 0 {
			pack[12] = '1'
		} else {
			pack[12] = '2'
		}
		_, _ = buf.Write(pack)
	}
	go func() {
		_, _ = io.Copy(client, iotest.OneByteReader(buf))
	}()

	packetCh := make(chan []byte, queryCount)
	go func() {
		receiver := newMySQLConn("receiver", server, 0, nil, nil, false, 0)
		for {
			ret, err := receiver.receive()
			if err != nil {
				return
			}
			packetCh <- ret
		}
	}()

	errKickoffCh := make(chan error, concurrency)
	errPauseCh := make(chan error, concurrency)
	for i := 1; i <= concurrency; i++ {
		go func(p *pipe, i int) {
			time.Sleep(jitteredInterval(time.Duration((i*2)+500) * time.Millisecond))
			errKickoffCh <- p.kickoff(ctx, nil)
		}(p, i)
		go func(p *pipe, i int) {
			time.Sleep(jitteredInterval(time.Duration((i*2)+500) * time.Millisecond))
			errPauseCh <- p.pause(ctx)
		}(p, i)
	}

	for i := 0; i < concurrency-1; i++ {
		err := <-errKickoffCh
		require.NoError(t, err)
	}

	var lastErr error
	require.Eventually(t, func() bool {
		select {
		case lastErr = <-errKickoffCh:
			return true
		default:
			_ = p.pause(ctx)
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)
	if lastErr != nil {
		require.EqualError(t, lastErr, errPipeClosed.Error())
	}

	for i := 0; i < concurrency; i++ {
		err := <-errPauseCh
		require.NoError(t, err)
	}

	go func(p *pipe) { _ = p.kickoff(ctx, nil) }(p)

	err := p.waitReady(ctx)
	require.NoError(t, err)

	for i := 0; i < queryCount; i++ {
		p := <-packetCh

		expectedStr := "select 1"
		if i%2 == 1 {
			expectedStr = "select 2"
		}
		require.Equal(t, expectedStr, string(p[5:]))
	}

	err = p.pause(ctx)
	require.NoError(t, err)
}

func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.5 + 0.5*rand.Float64()))
}

func TestAdmitTransfer(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	t.Run("not_started", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		_, can := tu.admitTransfer(false)
		require.False(t, can)
	})

	t.Run("inTransfer", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.inTransfer = true
		_, can := tu.admitTransfer(false)
		require.False(t, can)
	})

	t.Run("inFlight", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.csp = &pipe{}
		tu.mu.scp = &pipe{}
		tu.mu.started = true
		tu.trackClientRequest(makeSimplePacket("select 1"))
		_, can := tu.admitTransfer(false)
		require.False(t, can)
	})

	t.Run("client_message_active", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		csp := &pipe{tun: tu}
		tu.mu.csp = csp
		tu.mu.scp = &pipe{}
		tu.mu.started = true

		_, claimed, err := csp.claimClientMessage(context.Background(), nil, false)
		require.NoError(t, err)
		require.True(t, claimed)
		_, can := tu.admitTransfer(true)
		require.False(t, can,
			"even an untracked protocol message must finish forwarding first")
		_, can = tu.admitTransfer(false)
		require.False(t, can)
		require.False(t, csp.tryArmSyncTransfer())

		csp.finishClientMessageClaim()
		require.True(t, csp.tryArmSyncTransfer())
		gate, can := tu.admitTransferWithGate(true, csp)
		require.True(t, can)
		defer gate.finishSyncTransfer()
		_, can = tu.admitTransfer(false)
		require.False(t, can, "transfer admissions must not overlap")
		gate.finishSyncTransfer()
	})

	t.Run("inTxn", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.scp = &pipe{}
		tu.mu.scp.src = newMySQLConn("", nil, 0, nil, nil, false, 0)
		tu.mu.scp.mu.inTxn = true
		_, can := tu.admitTransfer(false)
		require.False(t, can)
	})

	t.Run("paused", func(t *testing.T) {
		tu := &tunnel{logger: logger}
		tu.mu.csp = &pipe{}
		tu.mu.scp = &pipe{}
		tu.mu.csp.mu.paused = true
		tu.mu.started = true
		_, can := tu.admitTransfer(true)
		require.False(t, can)
	})

	t.Run("ok", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.csp = &pipe{}
		tu.mu.scp = &pipe{}
		tu.mu.scp.src = newMySQLConn("", nil, 0, nil, nil, false, 0)
		tu.mu.started = true
		_, can := tu.admitTransfer(false)
		require.True(t, can)
	})
}

func TestTransferCannotStartClearsInTransferForRetry(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	ctx := context.Background()
	tun := newTunnel(ctx, logger, newCounterSet())
	tun.cc = &clientConn{connID: 1}
	tun.mu.started = true
	tun.mu.csp = &pipe{}
	tun.mu.scp = &pipe{}
	tun.mu.scp.src = newMySQLConn("", nil, 0, nil, nil, false, 0)
	tun.mu.scp.mu.inTxn = true

	queue := make(chan *tunnel, 1)
	deliver := newTunnelDeliver(queue, logger)
	deliver.Deliver(tun, transferByScaling)
	require.Equal(t, 1, deliver.Count())

	queued := <-queue
	require.Same(t, tun, queued)
	require.Error(t, queued.transfer(ctx))
	require.True(t, tun.transferIntent.Load())
	defer tun.setTransferIntent(false)

	tun.mu.Lock()
	require.False(t, tun.mu.inTransfer)
	tun.mu.Unlock()

	deliver.Deliver(tun, transferByScaling)
	require.Equal(t, 1, deliver.Count())
}

func TestTransferSyncInTransferLatch(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	ctx := context.Background()

	t.Run("skip_when_async_transfer_in_progress", func(t *testing.T) {
		tun := newTunnel(ctx, logger, newCounterSet())
		tun.mu.inTransfer = true

		require.NoError(t, tun.transferSync(ctx))

		tun.mu.Lock()
		require.True(t, tun.mu.inTransfer)
		tun.mu.Unlock()
	})

	t.Run("release_when_cannot_start", func(t *testing.T) {
		tun := newTunnel(ctx, logger, newCounterSet())
		tun.mu.started = true
		tun.mu.csp = &pipe{}
		tun.mu.scp = &pipe{}
		tun.mu.scp.src = newMySQLConn("", nil, 0, nil, nil, false, 0)
		tun.mu.scp.mu.inTxn = true
		tun.setTransferIntent(true)
		defer tun.setTransferIntent(false)

		require.Error(t, tun.transferSync(ctx))
		require.True(t, tun.transferIntent.Load())

		tun.mu.Lock()
		require.False(t, tun.mu.inTransfer)
		tun.mu.Unlock()
	})
}

func TestSetTransferIntentUpdatesGaugeOnStateChange(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	tun := newTunnel(context.Background(), logger, newCounterSet())
	before := testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge)

	tun.setTransferIntent(true)
	require.Equal(t, before+1, testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge))

	tun.setTransferIntent(true)
	require.Equal(t, before+1, testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge))

	tun.setTransferIntent(false)
	require.Equal(t, before, testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge))

	tun.setTransferIntent(false)
	require.Equal(t, before, testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge))
}

func TestSetTransferIntentUsesAttemptedTransferType(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	tun := newTunnel(
		context.Background(),
		logger,
		newCounterSet(),
		withRebalancePolicy(RebalancePolicyPassive),
	)
	before := testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge)

	attemptedType := transferByScaling
	tun.setTransferType(transferByRebalance)
	tun.setTransferIntentForType(true, attemptedType)
	require.True(t, tun.transferIntent.Load())
	require.Equal(t, before+1, testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge))

	tun.setTransferType(transferByRebalance)
	tun.setTransferIntent(false)
	require.False(t, tun.transferIntent.Load())
	require.Equal(t, before, testutil.ToFloat64(v2.ProxyConnectionsTransferIntentGauge))
}

func TestReplaceServerConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	clientProxy, client := net.Pipe()
	serverProxy, _ := net.Pipe()

	rt := runtime.DefaultRuntime()
	tu := newTunnel(ctx, rt.Logger(), nil)
	defer func() {
		require.NoError(t, tu.Close())
	}()

	cc := newMockClientConn(clientProxy, "t1", clientInfo{}, nil, tu)
	require.NotNil(t, cc)

	sc := newMockServerConn(serverProxy)
	require.NotNil(t, sc)

	err := tu.run(cc, sc)
	require.NoError(t, err)

	mysqlCC, mysqlSC := tu.getConns()
	require.Equal(t, clientProxy, mysqlCC.src)
	require.Equal(t, serverProxy, mysqlSC.src)

	csp, scp := tu.getPipes()
	require.NoError(t, csp.pause(ctx))
	require.NoError(t, scp.pause(ctx))

	newServerProxy, newServer := net.Pipe()
	backendSC := newMockServerConn(newServerProxy)
	require.NotNil(t, backendSC)
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{headroomBytes: 1})
	lease, err := limiter.acquire(context.Background(), 1)
	require.NoError(t, err)
	newSC := &protocolMemoryServerConn{ServerConn: backendSC, lease: lease}
	newServerC := newMySQLConn("new-server", newSC.RawConn(), 0, nil, nil, false, 0)
	tu.replaceServerConn(newServerC, newSC, false)
	require.Zero(t, limiter.used.Load(), "replacement must become steady only after tunnel switch")
	_, newMysqlSC := tu.getConns()
	require.Equal(t, newServerC, newMysqlSC)
	require.NoError(t, tu.kickoff())

	go func() {
		_, err := client.Write(makeSimplePacket("select 1"))
		require.NoError(t, err)
	}()

	buf := make([]byte, 30)
	n, err := newServer.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "select 1", string(buf[5:n]))
}

type closeCountingServerConn struct {
	ServerConn
	closes atomic.Int64
}

func (c *closeCountingServerConn) Close() error {
	c.closes.Add(1)
	return c.ServerConn.Close()
}

func TestReplaceServerConnRejectsAfterTunnelClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	tu := newTunnel(context.Background(), rt.Logger(), nil)
	require.NoError(t, tu.Close())

	proxySide, peerSide := net.Pipe()
	defer peerSide.Close()
	backend := &closeCountingServerConn{ServerConn: newMockServerConn(proxySide)}
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{headroomBytes: 1})
	lease, err := limiter.acquire(context.Background(), 1)
	require.NoError(t, err)
	newSC := &protocolMemoryServerConn{ServerConn: backend, lease: lease}
	newServerConn := newMySQLConn(
		connServerName,
		newSC.RawConn(),
		0,
		nil,
		nil,
		false,
		0,
	)

	err = tu.replaceServerConn(newServerConn, newSC, false)
	require.ErrorIs(t, err, errPipeClosed)
	require.Equal(t, int64(1), backend.closes.Load(),
		"the unpublished backend must have exactly one terminal cleanup owner")
	require.Zero(t, limiter.used.Load(),
		"rejected publication must release transient protocol memory")
	require.Nil(t, tu.getServerConn())
	_, installed := tu.getConns()
	require.Nil(t, installed, "a closed generation must never publish the replacement wrapper")
}

func TestReplaceServerConnTransfersBufferedWriterWithoutBlockingFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clientProxy, clientPeer := net.Pipe()
	defer clientPeer.Close()
	oldProxy, oldPeer := net.Pipe()
	defer oldPeer.Close()
	newProxy, newPeer := net.Pipe()
	defer newPeer.Close()

	rt := runtime.DefaultRuntime()
	tu := newTunnel(context.Background(), rt.Logger(), nil)
	oldServerConn := newMySQLConn(connServerName, oldProxy, 0, nil, nil, false, 0)
	writer := bufio.NewWriterSize(clientProxy, 64)
	_, err := writer.Write([]byte("pending"))
	require.NoError(t, err)
	oldServerConn.msgBuf.bufDst = writer
	tu.mu.clientConn = newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 0)
	tu.mu.serverConn = oldServerConn
	tu.mu.sc = newMockServerConn(oldProxy)

	newSC := newMockServerConn(newProxy)
	newServerConn := newMySQLConn(connServerName, newProxy, 0, nil, nil, false, 0)
	result := make(chan error, 1)
	go func() {
		result <- tu.replaceServerConn(newServerConn, newSC, false)
	}()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		// Releasing the socket makes cleanup deterministic even if a future
		// regression reintroduces a blocking Flush under t.mu.
		_ = clientPeer.Close()
		<-result
		t.Fatal("replacement blocked flushing the client data path")
	}
	require.Same(t, writer, newServerConn.msgBuf.bufDst)
	require.Equal(t, len("pending"), writer.Buffered(),
		"ordered buffered bytes must move to the new backend wrapper intact")
	require.NoError(t, tu.Close())
}

func TestCheckTxnStatus(t *testing.T) {
	t.Run("mustOK false", func(t *testing.T) {
		inTxn, ok := checkTxnStatus(nil, false)
		require.True(t, ok)
		require.True(t, inTxn)

		inTxn, ok = checkTxnStatus(makeErrPacket(8), false)
		require.False(t, ok)
		require.True(t, inTxn)

		p1 := makeOKPacket(5)
		value := frontend.SERVER_QUERY_WAS_SLOW | frontend.SERVER_STATUS_NO_GOOD_INDEX_USED
		binary.LittleEndian.PutUint16(p1[7:], value)
		inTxn, ok = checkTxnStatus(p1, false)
		require.True(t, ok)
		require.False(t, inTxn)

		value |= frontend.SERVER_STATUS_IN_TRANS
		binary.LittleEndian.PutUint16(p1[7:], value)
		inTxn, ok = checkTxnStatus(p1, false)
		require.True(t, ok)
		require.True(t, inTxn)
	})

	t.Run("mustOK true", func(t *testing.T) {
		inTxn, ok := checkTxnStatus(nil, true)
		require.True(t, ok)
		require.True(t, inTxn)

		inTxn, ok = checkTxnStatus(makeErrPacket(8), true)
		require.False(t, ok)
		require.True(t, inTxn)

		p1 := makeOKPacket(5)
		value := frontend.SERVER_QUERY_WAS_SLOW | frontend.SERVER_STATUS_NO_GOOD_INDEX_USED
		binary.LittleEndian.PutUint16(p1[7:], value)
		inTxn, ok = checkTxnStatus(p1, true)
		require.True(t, ok)
		require.False(t, inTxn)

		value |= frontend.SERVER_STATUS_IN_TRANS
		binary.LittleEndian.PutUint16(p1[7:], value)
		inTxn, ok = checkTxnStatus(p1, true)
		require.True(t, ok)
		require.True(t, inTxn)

		value ^= frontend.SERVER_STATUS_IN_TRANS
		binary.LittleEndian.PutUint16(p1[7:], value)
		inTxn, ok = checkTxnStatus(p1, true)
		require.True(t, ok)
		require.False(t, inTxn)

		p1[3] = 4
		inTxn, ok = checkTxnStatus(p1, false)
		require.True(t, ok)
		require.True(t, inTxn)

		inTxn, ok = checkTxnStatus(p1, true)
		require.True(t, ok)
		require.False(t, inTxn)
	})
}

func makeLegacyEOFPacket(status uint16) []byte {
	msg := make([]byte, 9)
	msg[0] = 5
	msg[3] = 1
	msg[4] = 0xfe
	binary.LittleEndian.PutUint16(msg[7:9], status)
	return msg
}

func makeDeprecatedEOFPacket(status uint16) []byte {
	msg := make([]byte, 12)
	msg[0] = 8
	msg[3] = 1
	msg[4] = 0xfe
	// affected rows and last insert ID are both zero-length-encoded integers.
	binary.LittleEndian.PutUint16(msg[7:9], status)
	return msg
}

func makePrepareOKPacket(columns, params uint16) []byte {
	msg := make([]byte, 16)
	msg[0] = 12
	msg[3] = 1
	msg[4] = 0
	binary.LittleEndian.PutUint32(msg[5:9], 1)
	binary.LittleEndian.PutUint16(msg[9:11], columns)
	binary.LittleEndian.PutUint16(msg[11:13], params)
	return msg
}

func makeStmtCommandPacket(cmd frontend.CommandType, statementID uint32, tail ...byte) []byte {
	msg := make([]byte, mysqlHeadLen+1+4+len(tail))
	payloadLen := len(msg) - mysqlHeadLen
	msg[0] = byte(payloadLen)
	msg[1] = byte(payloadLen >> 8)
	msg[2] = byte(payloadLen >> 16)
	msg[4] = byte(cmd)
	binary.LittleEndian.PutUint32(msg[5:9], statementID)
	copy(msg[9:], tail)
	return msg
}

func TestTunnelRequestBoundaryTracker(t *testing.T) {
	t.Run("nil and quit packets", func(t *testing.T) {
		var nilTunnel *tunnel
		nilTunnel.trackClientRequest(nil)
		nilTunnel.trackServerResponse(nil)

		tun := &tunnel{}
		tun.trackClientRequest(nil)
		quit := makeSimplePacket("quit")
		quit[4] = byte(frontend.COM_QUIT)
		tun.trackClientRequest(quit)
		require.False(t, tun.hasUntransferableClientState())
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("orphan continuation stays conservative", func(t *testing.T) {
		tun := &tunnel{}
		nonCommand := makeSimplePacket("continuation")
		nonCommand[3] = 1
		tun.trackClientRequest(nonCommand)
		require.False(t, tun.hasInFlightClientRequest())
		require.True(t, tun.hasUnsafeClientState())
	})

	t.Run("prepared long data lifecycle", func(t *testing.T) {
		const statement1 uint32 = 41
		const statement2 uint32 = 42
		tun := &tunnel{}

		longData1 := makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, statement1, 0, 0, 'a')
		longData2 := makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, statement2, 0, 0, 'b')
		tun.trackClientRequest(longData1)
		tun.trackClientRequest(longData1)
		tun.trackClientRequest(longData2)
		require.False(t, tun.hasInFlightClientRequest())
		require.True(t, tun.hasUnsafeClientState())

		close1 := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, statement1))
		require.True(t, tun.hasUnsafeClientState(),
			"state must remain unsafe until CLOSE is forwarded")
		tun.commitClientRequest(close1)
		require.True(t, tun.hasUnsafeClientState(),
			"another statement still owns staged long data")

		close2 := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, statement2))
		tun.commitClientRequest(close2)
		require.False(t, tun.hasUntransferableClientState())
		require.True(t, tun.hasUnsafeClientState())
	})

	t.Run("later response fences long data for authoritative reconciliation", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, 41, 0, 0, 'x'))
		require.True(t, tun.hasUntransferableClientState())

		// A successful COM_QUERY response proves that the old CN processed the
		// no-response long-data command first. It does not prove whether SQL
		// DEALLOCATE/PREPARE removed or replaced that statement; MigrateConnFrom
		// remains the authoritative check.
		tun.trackClientRequest(makeSimplePacket("deallocate prepare __mo_stmt_id_41"))
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasUntransferableClientState())
		require.True(t, tun.hasUnsafeClientState())

		tun.rejectPendingLongDataReconciliation()
		require.True(t, tun.hasUntransferableClientState())
	})

	t.Run("locally consumed request is not a backend fence", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, 41, 0, 0, 'x'))
		tun.trackClientRequest(makeSimplePacket("kill query 123"))
		tun.finishLocallyConsumedRequest()

		require.True(t, tun.hasUntransferableClientState(),
			"local completion cannot prove that the CN received earlier long data")
		tun.requestBoundary.Lock()
		require.False(t, tun.requestBoundary.pendingLongData[41])
		tun.requestBoundary.Unlock()
	})

	t.Run("later response supersedes close tombstone", func(t *testing.T) {
		const statementID uint32 = 41
		tun := &tunnel{}
		commit := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, statementID))
		tun.commitClientRequest(commit)

		tun.trackClientRequest(makeSimplePacket(
			"prepare __mo_stmt_id_41 from select 1"))
		tun.trackServerResponse(makeOKPacket(8))

		stmt := &query.PrepareStmt{Name: frontend.GetPrepareStmtName(statementID)}
		filtered := tun.filterClosedStatementsForMigration([]*query.PrepareStmt{stmt})
		require.Equal(t, []*query.PrepareStmt{stmt}, filtered)
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("close is unsafe until forwarded without staged data", func(t *testing.T) {
		tun := &tunnel{}
		commit := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 41))
		require.True(t, tun.hasUnsafeClientState())
		tun.commitClientRequest(commit)
		require.False(t, tun.hasUntransferableClientState())
		require.True(t, tun.hasUnsafeClientState())
	})

	t.Run("close fence clears only completed close state", func(t *testing.T) {
		tun := &tunnel{}
		commit := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 41))
		tun.commitClientRequest(commit)
		require.True(t, tun.hasFenceableClosedStatementState())
		tun.completeClosedStatementFence()
		require.False(t, tun.hasUnsafeClientState())

		tun = &tunnel{}
		tun.trackClientRequest(makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, 41, 0, 0, 'x'))
		commit = tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 42))
		tun.commitClientRequest(commit)
		require.True(t, tun.hasUnsafeClientState())
		require.False(t, tun.hasFenceableClosedStatementState(),
			"staged long data must not be hidden by a close fence")
		tun.completeClosedStatementFence()
		require.True(t, tun.hasUnsafeClientState(),
			"completing the close fence must preserve staged long data")

		tun = &tunnel{}
		for i := range maxTrackedStatementIDs + 1 {
			commit = tun.trackClientRequest(makeStmtCommandPacket(
				frontend.COM_STMT_CLOSE, uint32(i)))
			tun.commitClientRequest(commit)
		}
		require.True(t, tun.hasFenceableClosedStatementState(),
			"an overflowed completed-close set is still fenceable")
		tun.completeClosedStatementFence()
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("fragmented long data closes after its final packet", func(t *testing.T) {
		tun := &tunnel{}
		first := makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, 41, 0, 0, 'x')
		first[0], first[1], first[2] = 0xff, 0xff, 0xff
		tun.trackClientRequest(first)
		tun.trackClientRequest([]byte{0, 0, 0, 1})

		commit := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 41))
		tun.commitClientRequest(commit)
		require.False(t, tun.hasUntransferableClientState())
		require.True(t, tun.hasUnsafeClientState())
	})

	t.Run("fragmented command tracks every continuation", func(t *testing.T) {
		tun := &tunnel{}
		first := makeSimplePacket("select 1")
		first[0], first[1], first[2] = 0xff, 0xff, 0xff
		tun.trackClientRequest(first)
		require.True(t, tun.hasUntransferableClientState())

		middle := []byte{0xff, 0xff, 0xff, 1}
		tun.trackClientRequest(middle)
		require.True(t, tun.hasUntransferableClientState())

		tun.trackClientRequest([]byte{0, 0, 0, 2})
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("fragmented close stays gated through its final packet", func(t *testing.T) {
		tun := &tunnel{}
		first := makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 41)
		first[0], first[1], first[2] = 0xff, 0xff, 0xff
		commit := tun.trackClientRequest(first)
		tun.commitClientRequest(commit)
		require.True(t, tun.hasUntransferableClientState())

		tun.trackClientRequest([]byte{0, 0, 0, 1})
		require.False(t, tun.hasUntransferableClientState())
		require.True(t, tun.hasUnsafeClientState(),
			"the close tombstone still prevents connection-cache reuse")
	})

	t.Run("prepare ID reuse supersedes an older close", func(t *testing.T) {
		tun := &tunnel{}
		commit := tun.trackClientRequest(
			makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 1))
		tun.commitClientRequest(commit)

		prepare := makeSimplePacket("select 1")
		prepare[4] = byte(frontend.COM_STMT_PREPARE)
		tun.trackClientRequest(prepare)
		tun.trackServerResponse(makePrepareOKPacket(0, 0))
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("execute consumes long data on success or error", func(t *testing.T) {
		for _, response := range [][]byte{makeOKPacket(8), makeErrPacket(8)} {
			tun := &tunnel{}
			tun.trackClientRequest(makeStmtCommandPacket(
				frontend.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 'x'))
			tun.trackClientRequest(makeStmtCommandPacket(frontend.COM_STMT_EXECUTE, 1))
			require.True(t, tun.hasInFlightClientRequest())
			tun.trackServerResponse(response)
			require.False(t, tun.hasUnsafeClientState())
		}
	})

	t.Run("reset clears long data only on success", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeStmtCommandPacket(
			frontend.COM_STMT_SEND_LONG_DATA, 1, 0, 0, 'x'))
		tun.trackClientRequest(makeStmtCommandPacket(frontend.COM_STMT_RESET, 1))
		tun.trackServerResponse(makeErrPacket(8))
		require.True(t, tun.hasUnsafeClientState())

		tun.trackClientRequest(makeStmtCommandPacket(frontend.COM_STMT_RESET, 1))
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("malformed and excessive long data stay bounded and recover", func(t *testing.T) {
		tun := &tunnel{}
		malformed := makeStmtCommandPacket(frontend.COM_STMT_SEND_LONG_DATA, 1)
		tun.trackClientRequest(malformed)
		require.True(t, tun.hasUnsafeClientState())

		tun = &tunnel{}
		for i := range maxTrackedStatementIDs + 1 {
			tun.trackClientRequest(makeStmtCommandPacket(
				frontend.COM_STMT_SEND_LONG_DATA, uint32(i), 0, 0, 'x'))
		}
		tun.requestBoundary.Lock()
		require.Len(t, tun.requestBoundary.pendingLongData, maxTrackedStatementIDs)
		require.False(t, tun.requestBoundary.ambiguous)
		require.True(t, tun.requestBoundary.pendingLongDataOverflow)
		require.False(t, tun.requestBoundary.pendingLongDataOverflowFenced)
		tun.requestBoundary.Unlock()
		require.True(t, tun.hasUntransferableClientState())

		// A terminal response fences every earlier no-response command. The
		// overflowed IDs are not known locally, so migration remains gated on the
		// exporting CN's authoritative staged-data check.
		tun.trackClientRequest(makeSimplePacket("select 1"))
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasUntransferableClientState())
		require.False(t, tun.acceptPendingLongDataSnapshot(false))
		require.True(t, tun.acceptPendingLongDataSnapshot(true))

		tun.rejectPendingLongDataReconciliation()
		require.True(t, tun.hasUntransferableClientState())
		tun.trackClientRequest(makeSimplePacket("select 2"))
		tun.trackServerResponse(makeOKPacket(8))
		tun.clearMigratedStatementState()
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("forwarded close tracking stays bounded and recovers at a response fence", func(t *testing.T) {
		tun := &tunnel{}
		for i := range maxTrackedStatementIDs + 1 {
			commit := tun.trackClientRequest(makeStmtCommandPacket(
				frontend.COM_STMT_CLOSE, uint32(i)))
			tun.commitClientRequest(commit)
		}
		tun.requestBoundary.Lock()
		require.Len(t, tun.requestBoundary.closedStatements, maxTrackedStatementIDs)
		require.False(t, tun.requestBoundary.ambiguous)
		require.True(t, tun.requestBoundary.closedStatementsOverflow)
		require.False(t, tun.requestBoundary.inFlight,
			"overflow must not strand request ownership")
		tun.requestBoundary.Unlock()
		require.True(t, tun.hasUntransferableClientState())

		tun.trackClientRequest(makeSimplePacket("select 1"))
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasUnsafeClientState())
	})

	t.Run("error response", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("select invalid"))
		tun.trackServerResponse(makeErrPacket(8))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("simple OK", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("insert into t values (1)"))
		require.True(t, tun.hasInFlightClientRequest())
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("multi-result OK", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("call p()"))
		intermediate := makeOKPacket(8)
		binary.LittleEndian.PutUint16(intermediate[7:9], frontend.SERVER_MORE_RESULTS_EXISTS)
		tun.trackServerResponse(intermediate)
		require.True(t, tun.hasInFlightClientRequest())
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("legacy result EOF", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("select 1"))
		tun.trackServerResponse(makeSimplePacket("column count"))
		tun.trackServerResponse(makeLegacyEOFPacket(0))
		require.True(t, tun.hasInFlightClientRequest(), "the column EOF is not terminal")
		tun.trackServerResponse(makeSimplePacket("row"))
		tun.trackServerResponse(makeLegacyEOFPacket(0))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("deprecated EOF result", func(t *testing.T) {
		tun := &tunnel{clientDeprecatesEOF: true}
		tun.trackClientRequest(makeSimplePacket("select 1"))
		tun.trackServerResponse(makeSimplePacket("column count"))
		tun.trackServerResponse(makeSimplePacket("row"))
		tun.trackServerResponse(makeDeprecatedEOFPacket(0))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("prepared statement metadata", func(t *testing.T) {
		tun := &tunnel{}
		prepare := makeSimplePacket("select ?")
		prepare[4] = byte(frontend.COM_STMT_PREPARE)
		tun.trackClientRequest(prepare)
		tun.trackServerResponse(makePrepareOKPacket(1, 1))
		for i := 0; i < 3; i++ {
			tun.trackServerResponse(makeSimplePacket("metadata"))
			require.True(t, tun.hasInFlightClientRequest())
		}
		tun.trackServerResponse(makeSimplePacket("metadata"))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("prepared statement without metadata", func(t *testing.T) {
		tun := &tunnel{}
		prepare := makeSimplePacket("do 1")
		prepare[4] = byte(frontend.COM_STMT_PREPARE)
		tun.trackClientRequest(prepare)
		tun.trackServerResponse(makePrepareOKPacket(0, 0))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("deprecated prepared metadata", func(t *testing.T) {
		tun := &tunnel{clientDeprecatesEOF: true}
		prepare := makeSimplePacket("select ?")
		prepare[4] = byte(frontend.COM_STMT_PREPARE)
		tun.trackClientRequest(prepare)
		tun.trackServerResponse(makePrepareOKPacket(1, 1))
		tun.trackServerResponse(makeSimplePacket("parameter"))
		require.True(t, tun.hasInFlightClientRequest())
		tun.trackServerResponse(makeSimplePacket("column"))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("local infile", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("load data local infile"))
		request := makeSimplePacket("file name")
		request[3] = 1
		request[4] = 0xfb
		tun.trackServerResponse(request)
		data := makeSimplePacket("file contents")
		data[3] = 2
		tun.trackClientRequest(data)
		tun.trackClientRequest([]byte{0, 0, 0, 3})
		tun.trackServerResponse(makeSimplePacket("not terminal"))
		require.True(t, tun.hasInFlightClientRequest())
		tun.trackServerResponse(makeOKPacket(8))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("command after local infile terminator stays non-transferable", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("load data local infile"))
		request := makeSimplePacket("file name")
		request[3] = 1
		request[4] = 0xfb
		tun.trackServerResponse(request)
		tun.trackClientRequest([]byte{0, 0, 0, 2})

		unexpected := makeSimplePacket("select 1")
		unexpected[3] = 3
		tun.trackClientRequest(unexpected)
		tun.trackServerResponse(makeOKPacket(8))
		require.True(t, tun.hasUntransferableClientState())
	})

	t.Run("single-packet and EOF commands", func(t *testing.T) {
		statistics := makeSimplePacket("statistics")
		statistics[4] = byte(frontend.COM_STATISTICS)
		tun := &tunnel{}
		tun.trackClientRequest(statistics)
		tun.trackServerResponse(makeSimplePacket("uptime"))
		require.False(t, tun.hasInFlightClientRequest())

		fieldList := makeSimplePacket("fields")
		fieldList[4] = byte(frontend.COM_FIELD_LIST)
		tun.trackClientRequest(fieldList)
		tun.trackServerResponse(makeLegacyEOFPacket(0))
		require.False(t, tun.hasInFlightClientRequest())

		tun.clientDeprecatesEOF = true
		tun.trackClientRequest(fieldList)
		tun.trackServerResponse(makeDeprecatedEOFPacket(0))
		require.False(t, tun.hasInFlightClientRequest())
	})

	t.Run("pipelined commands stay non-cacheable", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("select 1"))
		tun.trackClientRequest(makeSimplePacket("select 2"))
		tun.trackServerResponse(makeOKPacket(8))
		tun.trackServerResponse(makeOKPacket(8))
		require.True(t, tun.hasInFlightClientRequest())
	})

	t.Run("unexpected nonzero sequence stays non-transferable", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("select 1"))
		unexpected := makeSimplePacket("select 2")
		unexpected[3] = 1
		tun.trackClientRequest(unexpected)

		// The first response must not make migration eligible: the CN accepts
		// the second complete packet even though its sequence is malformed, so a
		// second response may still be outstanding on the old connection.
		tun.trackServerResponse(makeOKPacket(8))
		require.True(t, tun.hasUntransferableClientState())
		require.True(t, tun.hasUnsafeClientState())
	})

	t.Run("malformed OK stays conservative", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("insert"))
		tun.trackServerResponse(makeOKPacket(1))
		require.True(t, tun.hasInFlightClientRequest())
	})

	t.Run("coalesced packets cannot borrow framing", func(t *testing.T) {
		tun := &tunnel{}
		tun.trackClientRequest(makeSimplePacket("insert"))
		coalesced := append([]byte{0, 0, 0, 1}, makeErrPacket(8)...)
		tun.trackServerResponse(coalesced)
		require.True(t, tun.hasInFlightClientRequest(),
			"an empty first packet must not borrow the next packet's type byte")

		coalesced = append(makeOKPacket(8), makeErrPacket(8)...)
		tun.trackServerResponse(coalesced)
		require.False(t, tun.hasInFlightClientRequest())
	})
}

func TestTunnelIdentityChangeDisablesCachePublication(t *testing.T) {
	tun := &tunnel{}
	packet := make([]byte, mysqlHeadLen+1)
	packet[0] = 1
	packet[4] = byte(frontend.COM_CHANGE_USER)
	tun.trackClientRequest(packet)
	require.True(t, tun.hasCacheIdentityChanged())

	e, handled := makeEvent(makeSimplePacket("set role analyst"), nil)
	require.IsType(t, &identityChangeEvent{}, e)
	require.False(t, handled)
	cc := &clientConn{tun: tun}
	eventDone := make(chan error, 1)
	go func() { eventDone <- cc.HandleEvent(context.Background(), e, nil) }()
	e.wait()
	require.NoError(t, <-eventDone)
	require.True(t, tun.hasCacheIdentityChanged())
}

func TestTunnelTransferAfterStmtLongDataIsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientProxy, client := net.Pipe()
	defer client.Close()
	backendProxy, backend := net.Pipe()
	defer backend.Close()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tun := newTunnel(ctx, runtime.DefaultRuntime().Logger(), newCounterSet())
	clientConn := newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 1)
	backendConn := newMySQLConn(connServerName, backendProxy, 0, nil, nil, false, 2)
	csp := tun.newPipe(pipeClientToServer, clientConn, backendConn)
	tun.mu.csp = csp
	tun.mu.scp = &pipe{}
	tun.mu.started = true

	pipeDone := make(chan error, 1)
	go func() { pipeDone <- csp.kickoff(ctx, tun.mu.scp) }()
	require.NoError(t, csp.waitReady(ctx))

	forward := func(packet []byte) {
		t.Helper()
		writeDone := make(chan error, 1)
		go func() {
			_, err := client.Write(packet)
			writeDone <- err
		}()
		got := make([]byte, len(packet))
		_, err := io.ReadFull(backend, got)
		require.NoError(t, err)
		require.Equal(t, packet, got)
		require.NoError(t, <-writeDone)
	}

	const statementID uint32 = 42
	longData := makeStmtCommandPacket(
		frontend.COM_STMT_SEND_LONG_DATA, statementID, 0, 0, 'x')
	forward(longData)
	_, can := tun.admitTransfer(true)
	require.False(t, can,
		"staged long data cannot be reconstructed on a migrated CN")

	closeStmt := makeStmtCommandPacket(frontend.COM_STMT_CLOSE, statementID)
	forward(closeStmt)
	require.NoError(t, client.Close())
	select {
	case err := <-pipeDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("client-to-server pipe did not stop")
	}

	gate, can := tun.admitTransfer(true)
	require.True(t, can,
		"closing the statement discards its staged long data")
	defer gate.finishSyncTransfer()
	gate.finishSyncTransfer()
}

func TestSyncTransferSerializesClientRequestPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientProxy, client := net.Pipe()
	defer client.Close()
	oldBackendProxy, oldBackend := net.Pipe()
	defer oldBackend.Close()
	newBackendProxy, newBackend := net.Pipe()
	defer newBackend.Close()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tun := newTunnel(ctx, runtime.DefaultRuntime().Logger(), newCounterSet())
	clientConn := newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 1)
	oldBackendConn := newMySQLConn(connServerName, oldBackendProxy, 0, nil, nil, false, 2)
	csp := tun.newPipe(pipeClientToServer, clientConn, oldBackendConn)
	peer := &pipe{}
	tun.mu.started = true
	tun.mu.csp = csp
	tun.mu.scp = peer

	// Force the formerly missed ordering: c2s has returned from preRecv, but has
	// not published the request when synchronous transfer admission closes the
	// publication gate.
	beforeClaimRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(beforeClaimRelease) }) }
	defer release()
	beforeSend := make(chan struct{})
	var beforeSendOnce sync.Once
	csp.testHelper.beforeSend = func() {
		beforeSendOnce.Do(func() { close(beforeSend) })
		<-beforeClaimRelease
	}
	waitingOnTransfer := make(chan struct{})
	var waitingOnce sync.Once
	csp.testHelper.onSyncTransferWait = func() {
		waitingOnce.Do(func() { close(waitingOnTransfer) })
	}

	pipeDone := make(chan error, 1)
	go func() { pipeDone <- csp.kickoff(ctx, peer) }()
	require.NoError(t, csp.waitReady(ctx))

	const statementID uint32 = 42
	longData := makeStmtCommandPacket(
		frontend.COM_STMT_SEND_LONG_DATA, statementID, 0, 0, 'x')
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(longData)
		writeDone <- err
	}()

	select {
	case <-beforeSend:
	case <-time.After(time.Second):
		t.Fatal("client packet was not read before the migration barrier")
	}
	require.True(t, csp.tryArmSyncTransfer())
	gate, can := tun.admitTransferWithGate(true, csp)
	require.True(t, can)
	require.Same(t, csp, gate)
	defer gate.finishSyncTransfer()
	release()
	select {
	case <-waitingOnTransfer:
	case <-time.After(time.Second):
		t.Fatal("client request did not wait on synchronous transfer")
	}
	require.False(t, tun.hasUnsafeClientState(),
		"a post-snapshot packet must not be published before migration cleanup")
	tun.clearMigratedStatementState()
	csp.dst = newMySQLConn(connServerName, newBackendProxy, 0, nil, nil, false, 3)
	gate.finishSyncTransfer()

	got := make([]byte, len(longData))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(newBackend, got)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("post-snapshot request was not forwarded to the replacement backend")
	}
	require.Equal(t, longData, got)
	require.NoError(t, <-writeDone)
	require.True(t, tun.hasUntransferableClientState(),
		"the packet forwarded to the replacement CN must remain tracked")

	require.NoError(t, client.Close())
	select {
	case err := <-pipeDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("client-to-server pipe did not stop")
	}
}

func TestClientMessageClaimCancellation(t *testing.T) {
	csp := &pipe{}
	csp.mu.syncTransferDone = make(chan struct{})
	defer csp.finishSyncTransfer()
	waiting := make(chan struct{})
	var waitingOnce sync.Once
	csp.testHelper.onSyncTransferWait = func() {
		waitingOnce.Do(func() { close(waiting) })
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	wantErr := errors.New("stop waiting for migration")

	type claimResult struct {
		claimed bool
		err     error
	}
	done := make(chan claimResult, 1)
	go func() {
		_, claimed, err := csp.claimClientMessage(ctx, nil, false)
		done <- claimResult{claimed: claimed, err: err}
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("client publication did not reach the transfer gate")
	}
	cancel(wantErr)
	csp.finishSyncTransfer()
	var result claimResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("client publication did not stop after cancellation")
	}
	require.ErrorIs(t, result.err, wantErr)
	require.False(t, result.claimed)
	require.False(t, csp.clientMessageActive.Load())
}

func TestSyncTransferWaiterStopsWhenResponseForwardingFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backendProxy, backend := net.Pipe()
	defer backend.Close()
	clientProxy, client := net.Pipe()
	defer client.Close()
	require.NoError(t, clientProxy.Close())

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tun := newTunnel(ctx, runtime.DefaultRuntime().Logger(), newCounterSet())
	clientConn := newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 1)
	backendConn := newMySQLConn(connServerName, backendProxy, 0, nil, nil, false, 2)
	csp := tun.newPipe(pipeClientToServer, clientConn, backendConn)
	scp := tun.newPipe(pipeServerToClient, backendConn, clientConn)
	tun.mu.started = true
	tun.mu.csp = csp
	tun.mu.scp = scp
	tun.setTransferIntent(true)
	defer tun.setTransferIntent(false)

	pipeDone := make(chan error, 1)
	go func() { pipeDone <- scp.kickoff(ctx, csp) }()
	require.NoError(t, scp.waitReady(ctx))
	writeDone := make(chan error, 1)
	response := makeOKPacket(5)
	binary.LittleEndian.PutUint16(response[7:],
		frontend.SERVER_QUERY_WAS_SLOW|frontend.SERVER_STATUS_NO_GOOD_INDEX_USED)
	go func() {
		_, err := backend.Write(response)
		writeDone <- err
	}()

	select {
	case err := <-pipeDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("server-to-client pipe did not report the forwarding failure")
	}
	require.NoError(t, <-writeDone)

	// A failed response path must not reopen c2s and let a pipelined request
	// reach the old backend. Tunnel shutdown cancels the waiter instead.
	csp.mu.Lock()
	require.NotNil(t, csp.mu.syncTransferDone)
	csp.mu.Unlock()
	claimDone := make(chan error, 1)
	go func() {
		_, _, err := csp.claimClientMessage(ctx, nil, false)
		claimDone <- err
	}()
	cancel()
	select {
	case err := <-claimDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("client publication did not stop with tunnel cancellation")
	}
	csp.finishSyncTransfer()
}

func TestAsyncTransferSerializesClientRequestPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientProxy, client := net.Pipe()
	defer client.Close()
	oldBackendProxy, oldBackend := net.Pipe()
	defer oldBackend.Close()
	newBackendProxy, newBackend := net.Pipe()
	defer newBackend.Close()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tun := newTunnel(ctx, runtime.DefaultRuntime().Logger(), newCounterSet())
	clientConn := newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 1)
	oldBackendConn := newMySQLConn(connServerName, oldBackendProxy, 0, nil, nil, false, 2)
	csp := tun.newPipe(pipeClientToServer, clientConn, oldBackendConn)
	scp := tun.newPipe(pipeServerToClient, oldBackendConn, clientConn)
	tun.mu.started = true
	tun.mu.csp = csp
	tun.mu.scp = scp

	packetRead := make(chan struct{})
	releaseRead := make(chan struct{})
	var packetReadOnce sync.Once
	var releaseReadOnce sync.Once
	releasePacket := func() { releaseReadOnce.Do(func() { close(releaseRead) }) }
	defer releasePacket()
	csp.testHelper.beforeSend = func() {
		packetReadOnce.Do(func() { close(packetRead) })
		<-releaseRead
	}
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- csp.kickoff(ctx, scp) }()
	require.NoError(t, csp.waitReady(ctx))

	request := makeSimplePacket("select 1")
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(request)
		writeDone <- err
	}()
	select {
	case <-packetRead:
	case <-time.After(time.Second):
		t.Fatal("client request was not read")
	}

	// The async transfer wins while the request is read but unpublished. Its
	// paused flag and request publication use the same mutex, so the old c2s pipe
	// must stop without consuming the buffered packet.
	_, can := tun.admitTransfer(false)
	require.True(t, can)
	releasePacket()
	select {
	case err := <-pipeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("paused client-to-server pipe did not stop")
	}
	require.NoError(t, <-writeDone)
	require.False(t, tun.hasUnsafeClientState())

	// transfer() calls pause before installing its replacement pipes. Complete
	// that transition here, then prove the same buffered request is published
	// and forwarded exactly once by the replacement pipe.
	require.NoError(t, csp.pause(ctx))
	require.NoError(t, scp.pause(ctx))
	newBackendConn := newMySQLConn(connServerName, newBackendProxy, 0, nil, nil, false, 3)
	replacement := tun.newPipe(pipeClientToServer, clientConn, newBackendConn)
	replacementDone := make(chan error, 1)
	go func() { replacementDone <- replacement.kickoff(ctx, &pipe{}) }()
	require.NoError(t, replacement.waitReady(ctx))

	got := make([]byte, len(request))
	_, err := io.ReadFull(newBackend, got)
	require.NoError(t, err)
	require.Equal(t, request, got)
	require.True(t, tun.hasInFlightClientRequest())
	tun.trackServerResponse(makeOKPacket(8))
	require.False(t, tun.hasUnsafeClientState())

	require.NoError(t, client.Close())
	select {
	case err := <-replacementDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("replacement client-to-server pipe did not stop")
	}
}

func TestLocallyConsumedRequestClosesProxyBoundary(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		eventType eventType
	}{
		{name: "kill", statement: "kill query 123", eventType: TypeKill},
		{name: "upgrade", statement: "upgrade account all", eventType: TypeUpgrade},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clientProxy, client := net.Pipe()
			defer client.Close()
			backendProxy, backend := net.Pipe()
			defer backend.Close()

			runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
			tun := newTunnel(ctx, runtime.DefaultRuntime().Logger(), newCounterSet())
			clientConn := newMySQLConn(
				connClientName, clientProxy, 0, tun.reqC, tun.respC, false, 1)
			backendConn := newMySQLConn(connServerName, backendProxy, 0, nil, nil, false, 2)
			csp := tun.newPipe(pipeClientToServer, clientConn, backendConn)

			eventSeen := make(chan eventType, 1)
			go func() {
				var e IEvent
				select {
				case e = <-tun.reqC:
				case <-ctx.Done():
					return
				}
				switch e.(type) {
				case *killEvent:
					eventSeen <- TypeKill
				case *upgradeEvent:
					eventSeen <- TypeUpgrade
				default:
					eventSeen <- 0
				}
				e.notify()
			}()

			pipeDone := make(chan error, 1)
			go func() { pipeDone <- csp.kickoff(ctx, &pipe{}) }()
			require.NoError(t, csp.waitReady(ctx))

			localRequest := makeSimplePacket(test.statement)
			nextRequest := makeSimplePacket("select 1")
			writeDone := make(chan error, 1)
			go func() {
				if _, err := client.Write(localRequest); err != nil {
					writeDone <- err
					return
				}
				_, err := client.Write(nextRequest)
				writeDone <- err
			}()

			got := make([]byte, len(nextRequest))
			_, err := io.ReadFull(backend, got)
			require.NoError(t, err)
			require.Equal(t, nextRequest, got,
				"the locally consumed command must not reach the session CN")
			select {
			case gotType := <-eventSeen:
				require.Equal(t, test.eventType, gotType)
			case <-time.After(time.Second):
				t.Fatal("local event was not consumed")
			}
			require.NoError(t, <-writeDone)

			// The second request must own a normal, non-ambiguous boundary. Its
			// response can therefore restore a clean migration/cache boundary.
			tun.requestBoundary.Lock()
			require.True(t, tun.requestBoundary.inFlight)
			require.False(t, tun.requestBoundary.ambiguous)
			tun.requestBoundary.Unlock()
			tun.trackServerResponse(makeOKPacket(8))
			require.False(t, tun.hasUnsafeClientState())

			require.NoError(t, client.Close())
			select {
			case err := <-pipeDone:
				require.ErrorIs(t, err, io.EOF)
			case <-time.After(time.Second):
				t.Fatal("client-to-server pipe did not stop")
			}
		})
	}
}

func TestClientQuitBlocksTransferAdmission(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tun := newTunnel(context.Background(), runtime.DefaultRuntime().Logger(), newCounterSet())
	csp := &pipe{tun: tun}
	csp.mu.cond = sync.NewCond(&csp.mu)
	scp := &pipe{}
	scp.mu.cond = sync.NewCond(&scp.mu)
	tun.mu.started = true
	tun.mu.csp = csp
	tun.mu.scp = scp

	quit := makeSimplePacket("quit")
	quit[4] = byte(frontend.COM_QUIT)
	_, published, err := csp.claimClientMessage(context.Background(), quit, true)
	require.NoError(t, err)
	require.True(t, published)
	require.True(t, tun.hasExpectedClientQuit())
	_, can := tun.admitTransfer(false)
	require.False(t, can)
	csp.finishClientMessageClaim()
}

func TestPipeTracksFragmentedTerminalResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backendProxy, backend := net.Pipe()
	defer backendProxy.Close()
	defer backend.Close()
	clientProxy, client := net.Pipe()
	defer clientProxy.Close()
	defer client.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	tun := newTunnel(ctx, rt.Logger(), newCounterSet())
	backendConn := newMySQLConn(connServerName, backendProxy, 0, nil, nil, false, 1)
	clientConn := newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 2)
	scp := tun.newPipe(pipeServerToClient, backendConn, clientConn)
	csp := tun.newPipe(pipeClientToServer, clientConn, backendConn)
	tun.trackClientRequest(makeSimplePacket("insert"))

	pipeDone := make(chan error, 1)
	go func() { pipeDone <- scp.kickoff(ctx, csp) }()
	require.NoError(t, scp.waitReady(ctx))

	ok := makeOKPacket(8)
	writeDone := make(chan error, 1)
	go func() {
		if _, err := backend.Write(ok[:mysqlHeadLen]); err != nil {
			writeDone <- err
			return
		}
		_, err := backend.Write(ok[mysqlHeadLen:])
		writeDone <- err
	}()
	got := make([]byte, len(ok))
	_, err := io.ReadFull(client, got)
	require.NoError(t, err)
	require.Equal(t, ok, got)
	require.NoError(t, <-writeDone)
	require.False(t, tun.hasInFlightClientRequest(),
		"a response split immediately after its header must still close the request boundary")

	require.NoError(t, backend.Close())
	select {
	case err := <-pipeDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("server-to-client pipe did not stop")
	}
}

func Test_transfer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	// cancel the context immediately
	cancel()

	clientProxy, serverProxy := net.Pipe()
	defer clientProxy.Close()
	defer serverProxy.Close()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	tun := newTunnel(ctx, logger, newCounterSet())

	tun.mu.Lock()
	tun.mu.started = true
	tun.mu.Unlock()

	p1 := &pipe{}
	p1.src = &MySQLConn{
		Conn: clientProxy,
	}
	p1.mu.cond = sync.NewCond(&p1.mu)
	p1.mu.started = true

	tun.mu.Lock()
	tun.mu.scp = p1
	tun.mu.Unlock()

	p2 := &pipe{}
	p2.src = &MySQLConn{
		Conn: serverProxy,
	}
	p2.mu.cond = sync.NewCond(&p2.mu)
	p2.mu.started = true

	tun.mu.Lock()
	tun.mu.csp = p2
	tun.mu.Unlock()

	///test 1
	err := tun.transfer(ctx)
	assert.Error(t, err)

	///test 2
	p2.mu.Lock()
	p2.mu.started = false
	p2.mu.Unlock()
	err = tun.transfer(ctx)
	assert.Error(t, err)

	///test 3
	p2.mu.Lock()
	p2.mu.started = false
	p2.mu.Unlock()
	p1.mu.Lock()
	p1.mu.started = false
	p1.mu.Unlock()
	err = tun.transfer(ctx)
	assert.NoError(t, err)

	///test 4
	p2.mu.Lock()
	p2.mu.started = false
	p2.mu.Unlock()
	p1.mu.Lock()
	p1.mu.started = false
	p1.mu.Unlock()
	err = tun.transferSync(ctx)
	assert.Error(t, err)

}

func TestTransferSync_SkipCachePopOnMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)

	ctx := context.Background()
	tun := newTunnel(ctx, rt.Logger(), newCounterSet())
	cache := &popCountConnCache{}

	cc := &clientConn{
		ctx:        ctx,
		log:        rt.Logger(),
		router:     &routeErrRouter{errMsg: "route failed in transfer"},
		mysqlProto: &frontend.MysqlProtocolImpl{},
		connCache:  cache,
	}

	serverProxy, server := net.Pipe()
	defer serverProxy.Close()
	defer server.Close()

	tun.cc = cc
	tun.mu.started = true
	tun.mu.serverConn = newMySQLConn(
		connServerName,
		serverProxy,
		0,
		tun.reqC,
		tun.respC,
		false,
		0,
	)
	tun.mu.scp = &pipe{tun: tun}
	tun.mu.csp = &pipe{}
	require.True(t, tun.mu.csp.tryArmSyncTransfer())
	tun.mu.scp.syncTransferArmed = true
	err := tun.mu.scp.handleTransferIntent(ctx, tun.mu.csp)
	require.Error(t, err)
	require.Equal(t, 0, cache.popCount)
	require.False(t, tun.mu.scp.syncTransferArmed)
	tun.mu.csp.mu.Lock()
	require.Nil(t, tun.mu.csp.mu.syncTransferDone,
		"failed synchronous migration must release client publication")
	tun.mu.csp.mu.Unlock()
}
