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
	"bytes"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
)

func tcpConnectionPair(t *testing.T) (net.Conn, net.Conn) {
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

func TestConnectionPeerClosedDoesNotConsumeProtocolBytes(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})

	closed, err := connectionPeerClosed(server)
	require.NoError(t, err)
	require.False(t, closed)

	_, err = client.Write([]byte{0x2a})
	require.NoError(t, err)
	closed, err = connectionPeerClosed(server)
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
		closed, err := connectionPeerClosed(server)
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
		closed, err := connectionPeerClosed(server)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}

func TestConnectionPeerClosedUnwrapsTLS(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close() })
	tlsServer := tls.Server(server, &tls.Config{})

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		closed, err := connectionPeerClosed(tlsServer)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}

func TestConnectionPeerClosedNilConnection(t *testing.T) {
	closed, err := connectionPeerClosed(nil)
	require.NoError(t, err)
	require.True(t, closed)
}

func TestConnectionPeerClosedAfterBufferedTLSUpgrade(t *testing.T) {
	serverRaw, clientRaw := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = serverRaw.Close()
		_ = clientRaw.Close()
	})

	sv, err := getSystemVariables("test/system_vars_config.toml")
	require.NoError(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(
		serverRaw,
		pu,
		"",
		WithIOSessionAllocator(allocator),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		require.True(t, allocator.CheckBalance())
	})

	certificate := newTestTLSCertificate(t)
	clientTLS := tls.Client(&prefixWriteConn{
		Conn:   clientRaw,
		prefix: makePacket(bytes.Repeat([]byte{0x01}, 32), 1),
	}, &tls.Config{
		InsecureSkipVerify: true, // test certificate is intentionally self-signed
		MinVersion:         tls.VersionTLS12,
	})
	serverRaw.SetDeadline(time.Now().Add(5 * time.Second))
	clientRaw.SetDeadline(time.Now().Add(5 * time.Second))
	clientHandshake := make(chan error, 1)
	go func() {
		clientHandshake <- clientTLS.Handshake()
	}()

	payload, err := conn.Read()
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x01}, 32), payload)
	tlsConn := tls.Server(conn.connForTLS(), &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	})
	require.NotSame(t, serverRaw, tlsConn.NetConn())
	require.NoError(t, tlsConn.Handshake())
	require.NoError(t, <-clientHandshake)
	conn.UseConn(tlsConn)

	// Close the raw client socket instead of sending TLS close_notify. The
	// liveness probe must still reach the TCP socket through both TLS and the
	// buffered handshake wrapper and observe the peer FIN.
	require.NoError(t, clientRaw.Close())
	require.Eventually(t, func() bool {
		closed, err := connectionPeerClosed(tlsConn)
		return err == nil && closed
	}, time.Second, time.Millisecond)
}
