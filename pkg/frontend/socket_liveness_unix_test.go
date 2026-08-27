// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin || linux

package frontend

import (
	"crypto/tls"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tcpConnectionPair(t testing.TB) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	client, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	select {
	case server := <-accepted:
		return server, client
	case err := <-acceptErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out accepting test TCP connection")
	}
	return nil, nil
}

func tcpConnectionPairs(t testing.TB, count int) ([]net.Conn, []net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	var servers, clients []net.Conn
	t.Cleanup(func() {
		for _, conn := range servers {
			_ = conn.Close()
		}
		for _, conn := range clients {
			_ = conn.Close()
		}
	})

	accepted := make(chan net.Conn, count)
	acceptErr := make(chan error, 1)
	go func() {
		for range count {
			conn, err := listener.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- conn
		}
	}()

	clients = make([]net.Conn, 0, count)
	for range count {
		client, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		clients = append(clients, client)
	}
	servers = make([]net.Conn, 0, count)
	for range count {
		select {
		case server := <-accepted:
			servers = append(servers, server)
		case err := <-acceptErr:
			require.NoError(t, err)
		}
	}
	return servers, clients
}

func TestConnectionPeerClosedDoesNotConsumeProtocolBytes(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})

	closed, err := rawConnectionPeerClosed(server)
	require.NoError(t, err)
	require.False(t, closed)

	_, err = client.Write([]byte{0x2a})
	require.NoError(t, err)
	closed, err = rawConnectionPeerClosed(server)
	require.NoError(t, err)
	require.False(t, closed)

	require.NoError(t, server.SetReadDeadline(time.Now().Add(time.Second)))
	var payload [1]byte
	_, err = server.Read(payload[:])
	require.NoError(t, err)
	require.Equal(t, byte(0x2a), payload[0], "socket probe consumed a protocol byte")
}

func TestConnectionPeerClosedDetectsDisconnect(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close() })

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		closed, err := rawConnectionPeerClosed(server)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}

func TestConnectionPeerClosedDetectsDisconnectBehindUnreadBytes(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close() })

	_, err := client.Write([]byte{0x2a})
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		closed, err := rawConnectionPeerClosed(server)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}

func TestConnectionPeerClosedUnwrapsTLS(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close() })
	tlsServer := tls.Server(server, &tls.Config{})

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		closed, err := rawConnectionPeerClosed(tlsServer)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}

func TestConnectionPeerClosedNilConnection(t *testing.T) {
	closed, err := rawConnectionPeerClosed(nil)
	require.NoError(t, err)
	require.True(t, closed)
}

func TestConnectionPeerClosedRefreshesProbeWhenConnChanges(t *testing.T) {
	firstServer, firstClient := tcpConnectionPair(t)
	secondServer, secondClient := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = firstServer.Close()
		_ = firstClient.Close()
		_ = secondServer.Close()
	})

	conn := &Conn{
		conn:          firstServer,
		livenessProbe: newSocketLivenessProbe(firstServer),
	}
	conn.UseConn(secondServer)
	require.NoError(t, secondClient.Close())
	require.Eventually(t, func() bool {
		closed, err := connectionPeerClosed(conn)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}

func BenchmarkRoutineManagerClientDisconnectProbe(b *testing.B) {
	server, client := tcpConnectionPair(b)
	b.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
	now := time.Now()

	for _, population := range []struct {
		name        string
		connections int
		distinctFDs bool
	}{
		{name: "10k-distinct-fds", connections: 10_000, distinctFDs: true},
		{name: "10k-shared-fd", connections: 10_000},
		{name: "100k-shared-fd", connections: 100_000},
	} {
		connections := population.connections
		probeConnections := []net.Conn{server}
		if population.distinctFDs {
			// Use distinct live TCP sockets for the realistic population. The
			// shared-fd cases isolate the bounded scan and exact syscall count; the
			// 100k upper bound cannot use one loopback destination because it does
			// not supply 100k distinct ephemeral client ports.
			probeConnections, _ = tcpConnectionPairs(b, connections)
		}
		for _, activePercent := range []int{0, 1, 10, 100} {
			b.Run(fmt.Sprintf("%s/active=%d%%", population.name, activePercent), func(b *testing.B) {
				active := connections * activePercent / 100
				rm := &RoutineManager{clients: make(map[*Conn]*Routine, connections)}
				for i := 0; i < connections; i++ {
					routine := &Routine{}
					if i < active {
						routine.requestStartedAt.Store(clientRequestClockValue(now))
					}
					rawConn := probeConnections[i%len(probeConnections)]
					rm.clients[&Conn{
						conn:          rawConn,
						livenessProbe: newSocketLivenessProbe(rawConn),
					}] = routine
				}

				b.ReportMetric(float64(connections), "connections")
				b.ReportMetric(float64(active), "active_connections")
				b.ReportAllocs()
				// Production reuses the manager-owned request snapshot after the
				// first tick. Keep setup growth outside the steady-state budget.
				rm.cancelDisconnectedRequests(now, clientDisconnectProbeGrace, connectionPeerClosed)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rm.cancelDisconnectedRequests(now, clientDisconnectProbeGrace, connectionPeerClosed)
				}
			})
		}
	}
}
