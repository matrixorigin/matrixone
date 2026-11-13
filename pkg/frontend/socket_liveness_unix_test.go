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
	"io"
	"net"
	"sync"
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

type firstWriteResult struct {
	n   int
	err error
}

type firstWriteConn struct {
	net.Conn
	once  sync.Once
	first chan firstWriteResult
}

func (c *firstWriteConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.once.Do(func() { c.first <- firstWriteResult{n: n, err: err} })
	return n, err
}

// stageTLSReadAhead reproduces one frontend read containing the SSLRequest and
// a real ClientHello, independently of how TCP segments the client write.
func stageTLSReadAhead(t *testing.T, server net.Conn, conn *Conn, written <-chan firstWriteResult, packetSize int) int {
	t.Helper()
	var first firstWriteResult
	select {
	case first = <-written:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for SSLRequest and TLS ClientHello")
	}
	require.NoError(t, first.err)
	require.Greater(t, first.n, packetSize)
	require.LessOrEqual(t, first.n, conn.fixBuf.AvailableSpaceLen())
	_, err := io.ReadFull(server, conn.fixBuf.AvailableSpace()[:first.n])
	require.NoError(t, err)
	conn.fixBuf.IncWriteIndex(first.n)
	return first.n - packetSize
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
	request := bytes.Repeat([]byte{0x01}, 32)
	packet := makePacket(request, 1)
	written := make(chan firstWriteResult, 1)
	clientTLS := tls.Client(&prefixWriteConn{
		Conn:   &firstWriteConn{Conn: clientRaw, first: written},
		prefix: packet,
	}, &tls.Config{
		InsecureSkipVerify: true, // test certificate is intentionally self-signed
		MinVersion:         tls.VersionTLS12,
	})
	require.NoError(t, serverRaw.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, clientRaw.SetDeadline(time.Now().Add(10*time.Second)))
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
	require.Equal(t, request, payload)
	require.Equal(t, pending, conn.fixBuf.AvailableDataLen())
	tlsConn := tls.Server(conn.connForTLS(), &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	})
	require.NotSame(t, serverRaw, tlsConn.NetConn())
	require.NoError(t, tlsConn.Handshake())
	select {
	case err = <-clientHandshake:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for TLS client handshake")
	}
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
