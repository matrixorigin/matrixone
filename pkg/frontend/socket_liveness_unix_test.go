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
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newDisconnectTestTCPPair(t testing.TB) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.AcceptTCP()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return server, client
}

func requireDisconnectDetected(t *testing.T, conn *Conn) {
	t.Helper()
	// TCP delivery is asynchronous; poll the transport condition, not a guessed
	// execution delay. The deadline only guards a missing termination event.
	require.Eventually(t, func() bool {
		closed, err := connectionPeerClosed(conn)
		return err == nil && closed
	}, 5*time.Second, time.Millisecond)
}

func TestConnectionPeerClosed(t *testing.T) {
	for _, mode := range []string{"fin", "reset", "tls"} {
		t.Run(mode, func(t *testing.T) {
			server, client := newDisconnectTestTCPPair(t)
			conn := &Conn{}
			var transport net.Conn = server
			if mode == "tls" {
				transport = tls.Server(server, &tls.Config{})
			}
			conn.UseConn(transport)
			closed, err := connectionPeerClosed(conn)
			require.NoError(t, err)
			require.False(t, closed)
			if mode == "reset" {
				require.NoError(t, client.SetLinger(0))
			}
			require.NoError(t, client.Close())
			requireDisconnectDetected(t, conn)
		})
	}
}

func TestConnectionProbePreservesBytesAndDeadline(t *testing.T) {
	server, client := newDisconnectTestTCPPair(t)
	conn := &Conn{}
	conn.UseConn(server)
	payload := []byte{1, 0, 0, 0, byte(COM_PING)}
	_, err := client.Write(payload)
	require.NoError(t, err)
	closed, err := connectionPeerClosed(conn)
	require.NoError(t, err)
	require.False(t, closed)
	require.NoError(t, server.SetReadDeadline(time.Now().Add(5*time.Second)))
	got := make([]byte, len(payload))
	_, err = io.ReadFull(server, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// A probe must neither fail because of nor reset the protocol's deadline.
	require.NoError(t, server.SetReadDeadline(time.Now().Add(-time.Second)))
	closed, err = connectionPeerClosed(conn)
	require.NoError(t, err)
	require.False(t, closed)
	_, err = server.Read(got)
	var timeoutErr net.Error
	require.ErrorAs(t, err, &timeoutErr)
	require.True(t, timeoutErr.Timeout())
}

func TestConnectionProbeRefreshAndUnsupported(t *testing.T) {
	server, client := newDisconnectTestTCPPair(t)
	conn := &Conn{}
	conn.UseConn(server)
	require.NoError(t, client.Close())
	requireDisconnectDetected(t, conn)
	replacement, _ := newDisconnectTestTCPPair(t)
	conn.UseConn(replacement)
	closed, err := connectionPeerClosed(conn)
	require.NoError(t, err)
	require.False(t, closed)
	// Cleanup can close the socket without racing the monitor over Conn.conn.
	require.NoError(t, replacement.Close())
	_, err = connectionPeerClosed(conn)
	require.Error(t, err)

	pipe, peer := net.Pipe()
	t.Cleanup(func() { _ = pipe.Close() })
	t.Cleanup(func() { _ = peer.Close() })
	conn.UseConn(pipe)
	closed, err = connectionPeerClosed(conn)
	require.NoError(t, err)
	require.False(t, closed, "unsupported transports must not be treated as disconnected")
	for _, c := range []*Conn{nil, {}} {
		closed, err = connectionPeerClosed(c)
		require.NoError(t, err)
		require.False(t, closed)
	}
}

type disconnectTestRawConn struct {
	err error
}

func (c disconnectTestRawConn) Control(fn func(uintptr)) error {
	if c.err != nil {
		return c.err
	}
	fn(0)
	return nil
}

func (disconnectTestRawConn) Read(func(uintptr) bool) error  { panic("unexpected Read") }
func (disconnectTestRawConn) Write(func(uintptr) bool) error { panic("unexpected Write") }

func TestConnectionProbeErrors(t *testing.T) {
	unknown := errors.New("unknown transport error")
	for _, tc := range []struct {
		err    error
		closed bool
		failed bool
	}{
		{err: unix.EAGAIN},
		{err: unix.EWOULDBLOCK},
		{err: unix.EINTR},
		{err: unix.ECONNRESET, closed: true},
		{err: unix.ENOTCONN, closed: true},
		{err: unix.EBADF, closed: true},
		{err: unknown, failed: true},
	} {
		probe := &socketLivenessProbe{rawConn: disconnectTestRawConn{}}
		probe.control = func(uintptr) { probe.probeErr = tc.err }
		closed, err := probe.connectionPeerClosed()
		require.Equal(t, tc.closed, closed)
		require.Equal(t, tc.failed, err != nil)
	}
	for _, probe := range []*socketLivenessProbe{
		{initErr: unknown},
		{rawConn: disconnectTestRawConn{err: unknown}},
	} {
		closed, err := probe.connectionPeerClosed()
		require.False(t, closed)
		require.ErrorIs(t, err, unknown)
	}
}

func TestSocketDisconnectCancelsActiveRequest(t *testing.T) {
	server, client := newDisconnectTestTCPPair(t)
	conn := &Conn{}
	conn.UseConn(server)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine := &Routine{}
	routine.setInProcessRequest(true)
	routine.setCancelRequestFunc(cancel)
	rm := &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}
	rm.cancelDisconnectedRequests(connectionPeerClosed)
	require.NoError(t, ctx.Err())
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		rm.cancelDisconnectedRequests(connectionPeerClosed)
		return ctx.Err() != nil
	}, 5*time.Second, time.Millisecond)
	require.True(t, routine.isCancelled())
}
