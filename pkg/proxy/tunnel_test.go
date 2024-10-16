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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"
	"testing/iotest"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
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

	csp, scp := tu.getPipes()
	require.NoError(t, csp.pause(ctx))
	require.NoError(t, scp.pause(ctx))

	newServerProxy, newServer := net.Pipe()
	tu.replaceServerConn(
		newMySQLConn(
			"server",
			newServerProxy,
			0,
			nil,
			nil,
			false, 0,
		),
		nil,
		false,
	)
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

func TestCanStartTransfer(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	t.Run("not_started", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		can := tu.canStartTransfer(false)
		require.False(t, can)
	})

	t.Run("inTransfer", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.inTransfer = true
		can := tu.canStartTransfer(false)
		require.False(t, can)
	})

	t.Run("lastCmd", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.csp = &pipe{}
		tu.mu.scp = &pipe{}
		tu.mu.started = true
		csp, scp := tu.getPipes()
		now := time.Now()
		csp.mu.lastCmdTime = now.Add(time.Second)
		scp.mu.lastCmdTime = now
		can := tu.canStartTransfer(false)
		require.False(t, can)
	})

	t.Run("inTxn", func(t *testing.T) {
		tu := &tunnel{
			logger: logger,
		}
		tu.mu.scp = &pipe{}
		tu.mu.scp.src = newMySQLConn("", nil, 0, nil, nil, false, 0)
		tu.mu.scp.mu.inTxn = true
		can := tu.canStartTransfer(false)
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
		csp, scp := tu.getPipes()
		now := time.Now()
		csp.mu.lastCmdTime = now
		scp.mu.lastCmdTime = now.Add(time.Second)
		can := tu.canStartTransfer(false)
		require.True(t, can)
	})
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
	newSC := newMockServerConn(newServerProxy)
	require.NotNil(t, sc)
	newServerC := newMySQLConn("new-server", newSC.RawConn(), 0, nil, nil, false, 0)
	tu.replaceServerConn(newServerC, newSC, false)
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
