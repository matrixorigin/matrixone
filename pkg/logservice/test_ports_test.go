// Copyright 2026 Matrix Origin
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

package logservice

import (
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestPortAllocation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		cause    error
	}{
		{"available", 0, nil},
		{"occupied", 3, syscall.EADDRINUSE},
		{"exhausted", maxPortAllocationAttempts, syscall.EADDRINUSE},
		{"fd-exhausted", 1, syscall.EMFILE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := allocatedPorts{ports: make(map[int]struct{})}
			calls := 0
			port, err := a.allocate(func(int) error {
				calls++
				if calls <= tc.failures {
					return &net.OpError{Op: "listen", Err: tc.cause}
				}
				return nil
			})
			switch tc.name {
			case "fd-exhausted":
				require.ErrorIs(t, err, syscall.EMFILE)
				require.Equal(t, 1, calls)
			case "exhausted":
				require.ErrorContains(t, err, "128 attempts")
				require.Equal(t, maxPortAllocationAttempts, calls)
			default:
				require.NoError(t, err)
				require.Equal(t, tc.failures+1, calls)
				require.Contains(t, a.ports, port)
			}
			if err != nil {
				require.Zero(t, port)
				require.Empty(t, a.ports)
			}
		})
	}
}

type probeCloseListener struct {
	net.Listener
	closed bool
}

func (l *probeCloseListener) Close() error {
	l.closed = true
	return nil
}

func TestProbeTestPortClosesTCPOnUDPFailure(t *testing.T) {
	listener := &probeCloseListener{}
	err := probeTestPortAddressWithListeners("127.0.0.1:1234",
		func(network, address string) (net.Listener, error) {
			require.Equal(t, "tcp4", network)
			return listener, nil
		},
		func(network, address string) (net.PacketConn, error) {
			require.Equal(t, "udp4", network)
			require.False(t, listener.closed)
			return nil, syscall.EADDRINUSE
		})
	require.ErrorIs(t, err, syscall.EADDRINUSE)
	require.True(t, listener.closed)
}

func TestProbeTestPortRejectsOccupiedSockets(t *testing.T) {
	t.Run("tcp", func(t *testing.T) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		require.ErrorIs(t, probeTestPort(listener.Addr().(*net.TCPAddr).Port), syscall.EADDRINUSE)
	})
	t.Run("udp", func(t *testing.T) {
		listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		port := listener.LocalAddr().(*net.UDPAddr).Port
		require.ErrorIs(t, probeTestPort(port), syscall.EADDRINUSE)
		// A UDP-owned port does not imply that its TCP counterpart is free.
		// Cleanup is checked separately without racing another process's bind.
	})
}
