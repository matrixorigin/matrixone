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
	"context"
	"net"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/stretchr/testify/require"
)

func TestMakeEvent(t *testing.T) {
	e := makeEvent(nil)
	require.Nil(t, e)

	e = makeEvent(makeSimplePacket("kill quer8y 12"))
	require.Nil(t, e)

	e = makeEvent(makeSimplePacket("kill query 123"))
	require.NotNil(t, e)

	e = makeEvent(makeSimplePacket("kiLL Query 12"))
	require.NotNil(t, e)
}

func TestKillQueryEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tp := newTestProxyHandler(t)
	defer tp.closeFn()

	addr1 := "127.0.0.1:38001"
	cn1 := &CNServer{
		connID: 10,
		addr:   addr1,
		uuid:   "uuid1",
	}
	stopFn1 := startTestCNServer(t, tp.ctx, addr1)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := "127.0.0.1:38002"
	cn2 := &CNServer{
		connID: 20,
		addr:   addr2,
		uuid:   "uuid2",
	}
	stopFn2 := startTestCNServer(t, tp.ctx, addr2)
	defer func() {
		require.NoError(t, stopFn2())
	}()

	tu1 := newTunnel(tp.ctx, tp.logger)
	defer func() { _ = tu1.Close() }()
	tu2 := newTunnel(tp.ctx, tp.logger)
	defer func() { _ = tu2.Close() }()

	// Client2 will send "kill query 10", which will route to the server which
	// has connection ID 10. In this case, the connection is server1.
	clientProxy1, _ := net.Pipe()
	serverProxy1, _ := net.Pipe()

	cc1 := newMockClientConn(clientProxy1, "t1", labelInfo{}, tp.ru, tu1)
	require.NotNil(t, cc1)
	sc1 := newMockServerConn(serverProxy1)
	require.NotNil(t, sc1)

	clientProxy2, client2 := net.Pipe()
	serverProxy2, _ := net.Pipe()

	cc2 := newMockClientConn(clientProxy2, "t1", labelInfo{}, tp.ru, tu2)
	require.NotNil(t, cc2)
	sc2 := newMockServerConn(serverProxy2)
	require.NotNil(t, sc2)

	res := make(chan []byte)
	st := stopper.NewStopper("test-event", stopper.WithLogger(tp.logger.RawLogger()))
	defer st.Stop()
	err := st.RunNamedTask("test-event-handler", func(ctx context.Context) {
		for {
			select {
			case e := <-tu2.reqC:
				err := cc2.HandleEvent(ctx, e, tu2.respC)
				require.NoError(t, err)
			case r := <-tu2.respC:
				if len(r) > 0 {
					res <- r
				}
			case <-ctx.Done():
				return
			}
		}
	})
	require.NoError(t, err)

	// tunnel1 is on cn1, connection ID is 10.
	_, _, err = tp.ru.Connect(cn1, nil, tu1)
	require.NoError(t, err)

	// tunnel2 is on cn2, connection ID is 20.
	_, _, err = tp.ru.Connect(cn2, nil, tu2)
	require.NoError(t, err)

	err = tu1.run(cc1, sc1)
	require.NoError(t, err)
	require.Nil(t, tu1.ctx.Err())

	func() {
		tu1.mu.Lock()
		defer tu1.mu.Unlock()
		require.True(t, tu1.mu.started)
	}()

	err = tu2.run(cc2, sc2)
	require.NoError(t, err)
	require.Nil(t, tu2.ctx.Err())

	func() {
		tu2.mu.Lock()
		defer tu2.mu.Unlock()
		require.True(t, tu2.mu.started)
	}()

	tu1.mu.Lock()
	csp1 := tu1.mu.csp
	scp1 := tu1.mu.scp
	tu1.mu.Unlock()

	tu2.mu.Lock()
	csp2 := tu2.mu.csp
	scp2 := tu2.mu.scp
	tu2.mu.Unlock()

	barrierStart1, barrierEnd1 := make(chan struct{}), make(chan struct{})
	barrierStart2, barrierEnd2 := make(chan struct{}), make(chan struct{})
	csp1.testHelper.beforeSend = func() {
		<-barrierStart1
		<-barrierEnd1
	}
	csp2.testHelper.beforeSend = func() {
		<-barrierStart2
		<-barrierEnd2
	}

	csp1.mu.Lock()
	require.True(t, csp1.mu.started)
	csp1.mu.Unlock()

	scp1.mu.Lock()
	require.True(t, scp1.mu.started)
	scp1.mu.Unlock()

	csp2.mu.Lock()
	require.True(t, csp2.mu.started)
	csp2.mu.Unlock()

	scp2.mu.Lock()
	require.True(t, scp2.mu.started)
	scp2.mu.Unlock()

	// Client2 writes some MySQL packets.
	sendEventCh := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	go func() {
		<-sendEventCh
		// client2 send kill query 10, which is on server1.
		if _, err := client2.Write(makeSimplePacket("kill query 10")); err != nil {
			errChan <- err
			return
		}
	}()

	sendEventCh <- struct{}{}
	barrierStart2 <- struct{}{}
	barrierEnd2 <- struct{}{}

	addr := string(<-res)
	// This test case is mainly focus on if the query is route to the
	// right cn server, but not the result of the query. So we just
	// check the address which is handled is equal to cn1, but not cn2.
	require.Equal(t, cn1.addr, addr)

	select {
	case err = <-errChan:
		t.Fatalf("require no error, but got %v", err)
	default:
	}
}

func TestEventType_String(t *testing.T) {
	e1 := baseEvent{}
	require.Equal(t, "Unknown", e1.eventType().String())

	e2 := killQueryEvent{}
	require.Equal(t, "KillQuery", e2.eventType().String())
}
