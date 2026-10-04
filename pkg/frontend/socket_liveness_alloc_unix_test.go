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

//go:build (darwin || linux) && !race

package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The race runtime adds bookkeeping allocations to the syscall callback path.
// Functional live-socket behavior remains covered under -race by the tests in
// socket_liveness_unix_test.go.
func TestConnectionPeerClosedLivePathDoesNotAllocate(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
	conn := &Conn{
		conn:          server,
		livenessProbe: newSocketLivenessProbe(server),
	}

	var closed bool
	var err error
	allocs := testing.AllocsPerRun(100, func() {
		closed, err = connectionPeerClosed(conn)
	})
	require.NoError(t, err)
	require.False(t, closed)
	require.Zero(t, allocs)
}
