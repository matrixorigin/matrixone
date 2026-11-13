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

	"github.com/matrixorigin/matrixone/pkg/config"
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

func TestTerminalConnectionMonitorUnixHalfCloseAndFullClose(t *testing.T) {
	for _, mode := range []string{"direct", "tls"} {
		t.Run(mode, func(t *testing.T) {
			server, client := livenessUnixConnectionPair(t)
			var transport net.Conn = server
			if mode == "tls" {
				transport = tls.Server(transport, &tls.Config{})
			}
			routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
			t.Cleanup(routine.cancelRoutineFunc)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			routine.setCancelRequestFunc(cancel)
			require.True(t, routine.setInProcessRequest(true))
			conn := &Conn{conn: transport}
			monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}

			_, err := client.Write([]byte("queued"))
			require.NoError(t, err)
			require.Eventually(t, func() bool { return terminalSocketHasEvent(server, unix.POLLIN) }, 3*time.Second, time.Millisecond)
			require.NoError(t, client.CloseWrite())
			require.Eventually(t, func() bool { return terminalSocketHasEvent(server, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
			monitor.scan(time.Now())
			require.NoError(t, ctx.Err(), "a Unix write-half-close must keep the response path usable")
			_, err = server.Write([]byte("reply"))
			require.NoError(t, err)
			var reply [5]byte
			_, err = io.ReadFull(client, reply[:])
			require.NoError(t, err)
			require.Equal(t, "reply", string(reply[:]))
			require.NoError(t, client.Close())
			require.Eventually(t, func() bool { return terminalSocketHasEvent(server, unix.POLLHUP) }, 3*time.Second, time.Millisecond)
			monitor.scan(time.Now())
			require.ErrorIs(t, ctx.Err(), context.Canceled, "a fully closed Unix connection must be retired")
			var payload [6]byte
			_, err = io.ReadFull(server, payload[:])
			require.NoError(t, err)
			require.Equal(t, "queued", string(payload[:]), "probing must leave queued bytes to the owner")

		})
	}
}
