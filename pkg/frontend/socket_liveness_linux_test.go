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
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func livenessSocketHasEvent(socket syscall.Conn, event int16) bool {
	raw, err := socket.SyscallConn()
	if err != nil {
		return false
	}
	var events int16
	var pollErr error
	err = raw.Control(func(fd uintptr) {
		fds := [1]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLRDHUP}}
		_, pollErr = unix.Poll(fds[:], 0)
		events = fds[0].Revents
	})
	return err == nil && pollErr == nil && events&event != 0
}

func livenessActiveRequest(t *testing.T, conn *Conn) (*RoutineManager, context.Context) {
	t.Helper()
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.beginClientRequest())
	return &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}, ctx
}

func TestLivenessMonitorPreservesHalfClose(t *testing.T) {
	for _, unread := range []bool{false, true} {
		t.Run(map[bool]string{false: "request read", true: "request unread"}[unread], func(t *testing.T) {
			server, client := tcpConnectionPair(t)
			t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
			require.NoError(t, server.SetDeadline(time.Now().Add(10*time.Second)))
			require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
			_, err := client.Write([]byte("request"))
			require.NoError(t, err)
			var request [7]byte
			if !unread {
				_, err = io.ReadFull(server, request[:])
				require.NoError(t, err)
			}
			conn := &Conn{conn: server, livenessProbe: newSocketLivenessProbe(server)}
			rm, ctx := livenessActiveRequest(t, conn)
			require.NoError(t, client.(*net.TCPConn).CloseWrite())
			require.Eventually(t, func() bool {
				return livenessSocketHasEvent(server.(*net.TCPConn), unix.POLLRDHUP)
			}, 3*time.Second, time.Millisecond)
			for range 3 {
				rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
				require.NoError(t, ctx.Err())
			}
			if unread {
				_, err = io.ReadFull(server, request[:])
				require.NoError(t, err)
			}
			require.Equal(t, "request", string(request[:]))
			_, err = server.Write([]byte("reply"))
			require.NoError(t, err)
			var reply [5]byte
			_, err = io.ReadFull(client, reply[:])
			require.NoError(t, err)
			require.Equal(t, "reply", string(reply[:]))
		})
	}
}

func TestLivenessMonitorPreservesGracefulFullFIN(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	conn := &Conn{conn: server, livenessProbe: newSocketLivenessProbe(server)}
	rm, ctx := livenessActiveRequest(t, conn)
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		return livenessSocketHasEvent(server.(*net.TCPConn), unix.POLLRDHUP)
	}, 3*time.Second, time.Millisecond)
	rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
	require.NoError(t, ctx.Err(), "a graceful full close has the same FIN ambiguity")
}

func TestLivenessMonitorPreservesPendingResetAndInput(t *testing.T) {
	for _, unread := range []bool{false, true} {
		t.Run(map[bool]string{false: "no unread bytes", true: "unread bytes"}[unread], func(t *testing.T) {
			server, client := tcpConnectionPair(t)
			t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
			if unread {
				_, err := client.Write([]byte("xy"))
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					return livenessSocketHasEvent(server.(*net.TCPConn), unix.POLLIN)
				}, 3*time.Second, time.Millisecond)
			}
			conn := &Conn{conn: server, livenessProbe: newSocketLivenessProbe(server)}
			rm, ctx := livenessActiveRequest(t, conn)
			require.NoError(t, client.(*net.TCPConn).SetLinger(0))
			require.NoError(t, client.Close())
			require.Eventually(t, func() bool {
				return livenessSocketHasEvent(server.(*net.TCPConn), unix.POLLERR)
			}, 3*time.Second, time.Millisecond)
			rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if unread {
				var payload [2]byte
				_, err := io.ReadFull(server, payload[:])
				require.NoError(t, err)
				require.Equal(t, "xy", string(payload[:]))
			}
			raw, err := server.(*net.TCPConn).SyscallConn()
			require.NoError(t, err)
			var recvErr error
			require.NoError(t, raw.Control(func(fd uintptr) {
				var one [1]byte
				_, _, recvErr = unix.Recvfrom(int(fd), one[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
			}))
			require.ErrorIs(t, recvErr, syscall.ECONNRESET, "the connection owner must still observe the reset")
		})
	}
}

func TestLivenessMonitorIgnoresHealthySocketErrorQueue(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	tcp := server.(*net.TCPConn)
	raw, err := tcp.SyscallConn()
	require.NoError(t, err)
	var optionErr error
	require.NoError(t, raw.Control(func(fd uintptr) {
		optionErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPING, unix.SOF_TIMESTAMPING_TX_SOFTWARE|unix.SOF_TIMESTAMPING_SOFTWARE)
	}))
	require.NoError(t, optionErr)
	_, err = server.Write([]byte("a"))
	require.NoError(t, err)
	var received [1]byte
	_, err = io.ReadFull(client, received[:])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return livenessSocketHasEvent(tcp, unix.POLLERR) }, 3*time.Second, time.Millisecond)
	conn := &Conn{conn: server, livenessProbe: newSocketLivenessProbe(server)}
	rm, ctx := livenessActiveRequest(t, conn)
	rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
	require.NoError(t, ctx.Err())
	require.True(t, livenessSocketHasEvent(tcp, unix.POLLERR), "the error queue remains owned by the connection")
	_, err = server.Write([]byte("b"))
	require.NoError(t, err)
	_, err = io.ReadFull(client, received[:])
	require.NoError(t, err)
	require.Equal(t, byte('b'), received[0])
}

func TestLivenessMonitorPreservesBufferedTLSHalfClose(t *testing.T) {
	serverRaw, clientRaw := tcpConnectionPair(t)
	t.Cleanup(func() { _ = serverRaw.Close(); _ = clientRaw.Close() })
	require.NoError(t, serverRaw.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, clientRaw.SetDeadline(time.Now().Add(10*time.Second)))
	sv, err := getSystemVariables("test/system_vars_config.toml")
	require.NoError(t, err)
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(&readAheadTestConn{Conn: serverRaw, firstReadBytes: len(makePacket(bytes.Repeat([]byte{1}, 32), 1)) + 1}, config.NewParameterUnit(sv, nil, nil, nil), "", WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); require.True(t, allocator.CheckBalance()) })
	clientTLS := tls.Client(&prefixWriteConn{Conn: clientRaw, prefix: makePacket(bytes.Repeat([]byte{1}, 32), 1)}, &tls.Config{
		InsecureSkipVerify: true, // test-only certificate
		MinVersion:         tls.VersionTLS12,
	})
	clientHandshake := make(chan error, 1)
	handshakeDone := make(chan struct{})
	go func() { defer close(handshakeDone); clientHandshake <- clientTLS.Handshake() }()
	t.Cleanup(func() {
		_ = serverRaw.Close()
		_ = clientRaw.Close()
		select {
		case <-handshakeDone:
		case <-time.After(5 * time.Second):
			t.Error("TLS handshake did not stop during cleanup")
		}
	})
	payload, err := conn.Read()
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{1}, 32), payload)
	conn.UseConn(serverRaw) // retain the staged prefix while restoring the real TCP transport
	serverTLS := tls.Server(conn.connForTLS(), &tls.Config{Certificates: []tls.Certificate{newTestTLSCertificate(t)}, MinVersion: tls.VersionTLS12})
	_, buffered := serverTLS.NetConn().(*bufferedNetConn)
	require.True(t, buffered, "the real ClientHello must be read ahead")
	require.NoError(t, serverTLS.Handshake())
	require.NoError(t, <-clientHandshake)
	conn.UseConn(serverTLS)
	rm, ctx := livenessActiveRequest(t, conn)
	require.NoError(t, clientRaw.(*net.TCPConn).CloseWrite())
	require.Eventually(t, func() bool { return livenessSocketHasEvent(serverRaw.(*net.TCPConn), unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
	rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
	require.NoError(t, ctx.Err())
	_, err = serverTLS.Write([]byte("encrypted reply"))
	require.NoError(t, err)
	var reply [15]byte
	_, err = io.ReadFull(clientTLS, reply[:])
	require.NoError(t, err)
	require.Equal(t, "encrypted reply", string(reply[:]))
	require.NoError(t, clientRaw.(*net.TCPConn).SetLinger(0))
	require.NoError(t, clientRaw.Close())
	require.Eventually(t, func() bool {
		rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
		return ctx.Err() != nil
	}, 3*time.Second, time.Millisecond, "reset through buffered TLS must cancel")
}
