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
	"errors"
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

func TestProbeTestPortRejectsOccupiedSockets(t *testing.T) {
	t.Run("tcp", func(t *testing.T) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		require.ErrorIs(t, probeTestPort(listener.Addr().(*net.TCPAddr).Port), syscall.EADDRINUSE)
	})
	t.Run("udp", func(t *testing.T) {
		listener, port := listenUDPOnTCPAvailablePort(t)
		defer listener.Close()
		require.ErrorIs(t, probeTestPort(port), syscall.EADDRINUSE)
		// The failed UDP probe must release its temporary TCP listener.
		tcp, err := net.Listen("tcp4", listener.LocalAddr().String())
		require.NoError(t, err)
		require.NoError(t, tcp.Close())
	})
}

func listenUDPOnTCPAvailablePort(t *testing.T) (net.PacketConn, int) {
	t.Helper()
	for range maxPortAllocationAttempts {
		// Reserve a TCP-selected ephemeral port while opening UDP on the same
		// address. Selecting the port with UDP alone is racy because the kernel's
		// TCP and UDP ephemeral-port allocators are independent.
		tcp, err := net.Listen("tcp4", "127.0.0.1:0")
		require.NoError(t, err)
		port := tcp.Addr().(*net.TCPAddr).Port
		udp, udpErr := net.ListenPacket("udp4", tcp.Addr().String())
		require.NoError(t, tcp.Close())
		if udpErr == nil {
			return udp, port
		}
		if !errors.Is(udpErr, syscall.EADDRINUSE) {
			require.NoError(t, udpErr)
		}
	}
	require.FailNow(t, "could not reserve a UDP port that is also available to TCP")
	return nil, 0
}
