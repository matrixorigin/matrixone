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

package frontend

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// newBufferedTLSMonitor stages the real client's first TLS record in the
// frontend read-ahead buffer, as can happen during an SSLRequest read. Reading
// the exact combined write avoids depending on TCP packet boundaries.
func newBufferedTLSMonitor(t *testing.T) (*Conn, *tls.Conn, *tls.Conn, *net.TCPConn, *net.TCPConn, *connectionLivenessMonitor, *Routine, context.Context, time.Time) {
	t.Helper()
	serverRaw, clientRaw := tcpConnectionPair(t)
	t.Cleanup(func() { _ = serverRaw.Close(); _ = clientRaw.Close() })
	require.NoError(t, serverRaw.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, clientRaw.SetDeadline(time.Now().Add(10*time.Second)))

	sv, err := getSystemVariables("test/system_vars_config.toml")
	require.NoError(t, err)
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(serverRaw, config.NewParameterUnit(sv, nil, nil, nil), "", WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		require.True(t, allocator.CheckBalance())
	})

	certificate := newTestTLSCertificate(t)
	parsedCert, err := x509.ParseCertificate(certificate.Certificate[0])
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(parsedCert)
	sslRequest := make([]byte, 32)
	binary.LittleEndian.PutUint32(sslRequest, DefaultCapability|CLIENT_SSL)
	binary.LittleEndian.PutUint32(sslRequest[4:], uint32(MaxPayloadSize))
	sslRequest[8] = 33
	packet := makePacket(sslRequest, 1)
	written := make(chan firstWriteResult, 1)
	observed := &firstWriteConn{Conn: clientRaw, first: written}
	clientTLS := tls.Client(&prefixWriteConn{Conn: observed, prefix: packet}, &tls.Config{
		RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12,
	})
	clientHandshake := make(chan error, 1)
	handshakeDone := make(chan struct{})
	go func() {
		defer close(handshakeDone)
		clientHandshake <- clientTLS.Handshake()
	}()
	t.Cleanup(func() {
		_ = serverRaw.Close()
		_ = clientRaw.Close()
		select {
		case <-handshakeDone:
		case <-time.After(5 * time.Second):
			t.Error("TLS client handshake did not exit after closing sockets")
		}
	})

	pending := stageTLSReadAhead(t, serverRaw, conn, written, len(packet))
	payload, err := conn.Read()
	require.NoError(t, err)
	require.Equal(t, sslRequest, payload)
	require.Equal(t, pending, conn.fixBuf.AvailableDataLen())
	transport, ok := conn.connForTLS().(*bufferedNetConn)
	require.True(t, ok, "the TLS upgrade must retain the read-ahead ClientHello")
	require.Same(t, serverRaw, transport.Conn)
	serverTLS := tls.Server(transport, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	require.NoError(t, serverTLS.Handshake())
	select {
	case err = <-clientHandshake:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for TLS client handshake")
	}
	conn.UseConn(serverTLS)

	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.setInProcessRequest(true))
	now := time.Now()
	require.Less(t, clientRequestClockValue(now)-routine.activeRequestStartedAt(), clientDisconnectProbeGrace.Nanoseconds())
	monitor := &connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
	return conn, serverTLS, clientTLS, serverRaw.(*net.TCPConn), clientRaw.(*net.TCPConn), monitor, routine, ctx, now
}

func TestTerminalMonitorBufferedTLSReset(t *testing.T) {
	conn, serverTLS, clientTLS, serverRaw, clientRaw, monitor, routine, ctx, now := newBufferedTLSMonitor(t)
	writeDone := make(chan error, 1)
	go func() {
		_, err := clientTLS.Write([]byte("pending request"))
		writeDone <- err
	}()
	monitor.scan(now)
	require.NoError(t, <-writeDone)
	require.Eventually(t, func() bool { return terminalSocketHasEvent(serverRaw, unix.POLLIN) }, 3*time.Second, time.Millisecond)
	monitor.scan(now)
	require.NoError(t, ctx.Err())
	probe := monitor.probes[conn]
	monitor.scan(now)
	if probe != nil {
		require.Same(t, probe, monitor.probes[conn], "healthy scans should reuse the socket probe")
	}
	var request [15]byte
	_, err := io.ReadFull(serverTLS, request[:])
	require.NoError(t, err)
	require.Equal(t, "pending request", string(request[:]), "monitor must not consume encrypted data")

	require.NoError(t, clientRaw.SetLinger(0))
	require.NoError(t, clientRaw.Close())
	require.Eventually(t, func() bool { return terminalSocketHasEvent(serverRaw, unix.POLLERR) }, 3*time.Second, time.Millisecond)
	monitor.scan(now)
	require.ErrorIs(t, ctx.Err(), context.Canceled, "a reset must cancel the recent request before the legacy grace")
	require.NotNil(t, probe)
	require.True(t, routine.isTerminalRetired())
	require.False(t, routine.setInProcessRequest(true), "a retired connection cannot admit another request")
	monitor.scan(now)
	require.Empty(t, monitor.probes)
}

func TestTerminalMonitorBufferedTLSHalfClose(t *testing.T) {
	_, serverTLS, clientTLS, serverRaw, clientRaw, monitor, routine, ctx, now := newBufferedTLSMonitor(t)
	require.NoError(t, clientRaw.CloseWrite())
	require.Eventually(t, func() bool { return terminalSocketHasEvent(serverRaw, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
	monitor.scan(now)
	require.NoError(t, ctx.Err(), "a half-close must not be treated as a terminal reset")
	require.False(t, routine.isTerminalRetired())
	_, err := serverTLS.Write([]byte("reply"))
	require.NoError(t, err)
	var reply [5]byte
	_, err = io.ReadFull(clientTLS, reply[:])
	require.NoError(t, err)
	require.Equal(t, "reply", string(reply[:]))
	monitor.scan(now.Add(clientDisconnectProbeGrace + time.Second))
	require.ErrorIs(t, ctx.Err(), context.Canceled, "the legacy grace policy still applies to a buffered TLS FIN")
	require.False(t, routine.isTerminalRetired(), "a FIN uses the legacy path")
}

func TestTerminalTCPConnBufferedTLS(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	pipe, pipePeer := net.Pipe()
	t.Cleanup(func() { _ = pipe.Close(); _ = pipePeer.Close() })
	var nilTLS *tls.Conn
	var nilBuffered *bufferedNetConn
	var nilTCP *net.TCPConn
	buffered := &bufferedNetConn{Conn: server}
	cycle := &bufferedNetConn{}
	cycle.Conn = cycle
	for _, tc := range []struct {
		name string
		conn net.Conn
		want *net.TCPConn
	}{
		{"TCP", server, server.(*net.TCPConn)},
		{"TLS over TCP", tls.Server(server, nil), server.(*net.TCPConn)},
		{"buffered TCP", buffered, server.(*net.TCPConn)},
		{"TLS over buffered TCP", tls.Server(buffered, nil), server.(*net.TCPConn)},
		{"nil", nil, nil},
		{"nil TLS", nilTLS, nil},
		{"nil buffered", nilBuffered, nil},
		{"nil TCP", nilTCP, nil},
		{"TLS over nil buffered", tls.Server(nilBuffered, nil), nil},
		{"buffered nil TCP", &bufferedNetConn{Conn: nilTCP}, nil},
		{"unsupported", pipe, nil},
		{"unsupported wrapper", &prefixWriteConn{Conn: server}, nil},
		{"buffered self-cycle", cycle, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := terminalTCPConn(tc.conn)
			if tc.want == nil {
				require.Nil(t, got)
			} else {
				require.Same(t, tc.want, got)
			}
		})
	}
}
