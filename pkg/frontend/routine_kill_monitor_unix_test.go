// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin || linux

package frontend

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
)

type blockedKillProtocol struct {
	*testMysqlWriter
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (p *blockedKillProtocol) Close() {
	p.once.Do(func() { close(p.entered) })
	<-p.release
}

func (p *blockedKillProtocol) unblock() { p.releaseOnce.Do(func() { close(p.release) }) }

func TestKillCloseDoesNotBlockDisconnectMonitor(t *testing.T) {
	clients := make(map[*Conn]*Routine)
	protocols := make(map[*Conn]*blockedKillProtocol)
	contexts := make(map[*Conn]context.Context)
	for range 2 {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		protocol := &blockedKillProtocol{testMysqlWriter: &testMysqlWriter{}, entered: make(chan struct{}), release: make(chan struct{})}
		t.Cleanup(protocol.unblock)
		rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
		require.True(t, rt.mc.tryBeginRequest())
		require.True(t, rt.beginClientRequest())
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		rt.setCancelRequestFunc(cancel)
		conn := &Conn{conn: server, livenessProbe: newSocketLivenessProbe(server)}
		clients[conn], protocols[conn], contexts[conn] = rt, protocol, ctx
		require.NoError(t, client.(*net.TCPConn).SetLinger(0))
		require.NoError(t, client.Close())
		require.Eventually(t, func() bool {
			closed, err := connectionPeerClosed(conn)
			return err == nil && closed
		}, 3*time.Second, time.Millisecond)
	}
	now := time.Now()
	rm := &RoutineManager{clients: clients}
	selected := make(chan *Conn, 1)
	killStarted := make(chan struct{})
	killDone := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() {
		for _, protocol := range protocols {
			protocol.unblock()
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("monitor did not stop during cleanup")
		}
		select {
		case <-killStarted:
			select {
			case <-killDone:
			case <-time.After(5 * time.Second):
				t.Error("KILL did not stop during cleanup")
			}
		default:
		}
	})
	go func() {
		defer close(done)
		first := true
		rm.cancelDisconnectedRequests(now, 0, func(conn *Conn) (bool, error) {
			if first {
				first = false
				// The request ended after the monitor captured its snapshot.
				_ = clients[conn].endClientRequest(nil)
				clients[conn].mc.endRequest()
				close(killStarted)
				go func() { clients[conn].killConnection(false); close(killDone) }()
				<-protocols[conn].entered
				selected <- conn
			}
			return connectionPeerClosed(conn)
		})
	}()
	var first *Conn
	select {
	case first = <-selected:
	case <-time.After(5 * time.Second):
		t.Fatal("KILL did not reach protocol close")
	}
	var later *Conn
	for conn := range clients {
		if conn != first {
			later = conn
		}
	}
	select {
	case <-contexts[later].Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the monitor waited for an unrelated protocol close")
	}
	// The KILL call still owns the blocked close while the monitor progresses.
	select {
	case <-killDone:
		t.Fatal("protocol close unexpectedly finished before release")
	default:
	}
	protocols[first].unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("monitor did not finish")
	}
	select {
	case <-killDone:
	case <-time.After(3 * time.Second):
		t.Fatal("KILL did not finish after the protocol close returned")
	}
}
