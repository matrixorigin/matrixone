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
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func livenessUnixConnectionPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	dir, err := os.MkdirTemp("", "mo32733-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	addr := &net.UnixAddr{Name: filepath.Join(dir, "s"), Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, listener.SetDeadline(time.Now().Add(5*time.Second)))
	client, err := net.DialUnix("unix", nil, addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.AcceptUnix()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, server.SetDeadline(time.Now().Add(10*time.Second)))
	return server, client
}

func TestLivenessMonitorUnixHalfCloseAndFullClose(t *testing.T) {
	for _, mode := range []string{"direct", "tls", "buffered-tls"} {
		t.Run(mode, func(t *testing.T) {
			server, client := livenessUnixConnectionPair(t)
			var transport net.Conn = server
			var buffered *bufferedNetConn
			if mode == "buffered-tls" {
				buffered = &bufferedNetConn{Conn: server, prefix: []byte("prefix")}
				transport = buffered
			}
			if mode != "direct" {
				transport = tls.Server(transport, &tls.Config{})
			}
			conn := &Conn{conn: transport, livenessProbe: newSocketLivenessProbe(transport)}
			rm, ctx := livenessActiveRequest(t, conn)

			_, err := client.Write([]byte("queued"))
			require.NoError(t, err)
			require.Eventually(t, func() bool { return livenessSocketHasEvent(server, unix.POLLIN) }, 3*time.Second, time.Millisecond)
			require.NoError(t, client.CloseWrite())
			require.Eventually(t, func() bool { return livenessSocketHasEvent(server, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
			rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
			require.NoError(t, ctx.Err(), "a Unix write-half-close must keep the response path usable")
			_, err = server.Write([]byte("reply"))
			require.NoError(t, err)
			var reply [5]byte
			_, err = io.ReadFull(client, reply[:])
			require.NoError(t, err)
			require.Equal(t, "reply", string(reply[:]))
			require.NoError(t, client.Close())
			require.Eventually(t, func() bool { return livenessSocketHasEvent(server, unix.POLLHUP) }, 3*time.Second, time.Millisecond)
			rm.cancelDisconnectedRequests(time.Now(), clientDisconnectProbeGrace, connectionPeerClosed)
			require.ErrorIs(t, ctx.Err(), context.Canceled, "a fully closed Unix connection must be retired")
			var payload [6]byte
			_, err = io.ReadFull(server, payload[:])
			require.NoError(t, err)
			require.Equal(t, "queued", string(payload[:]), "probing must leave queued bytes to the owner")

			if buffered != nil {
				require.Equal(t, "prefix", string(buffered.prefix))
			}
		})
	}
}
