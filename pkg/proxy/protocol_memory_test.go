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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestCalculateProtocolMemoryBudget(t *testing.T) {
	t.Run("small login needs one extra backend buffer", func(t *testing.T) {
		transient := uint64(proxyIOSessionBufferSize)
		steady := uint64(10 * (2*proxyIOSessionBufferSize + 64))
		cfg := Config{
			MaxConnections:             10,
			ProtocolMemoryLimit:        toml.ByteSize(steady),
			ClientHandshakePacketLimit: 64,
		}
		_, err := calculateProtocolMemoryBudget(&cfg)
		require.Error(t, err)

		cfg.ProtocolMemoryLimit += toml.ByteSize(transient)
		budget, err := calculateProtocolMemoryBudget(&cfg)
		require.NoError(t, err)
		require.Equal(t, steady, budget.steadyBytes)
		require.Equal(t, transient, budget.transientBytes)
		require.Equal(t, transient, budget.headroomBytes)
		require.Equal(t, uint64(64), budget.initialBytes)
		require.Equal(t, uint64(proxyIOSessionBufferSize), budget.backendBytes)
	})

	t.Run("large login includes rounded backend dynamic write", func(t *testing.T) {
		const handshakeLimit = 64 << 10
		steady := uint64(2*proxyIOSessionBufferSize + handshakeLimit)
		transient := uint64(2 * handshakeLimit)
		cfg := Config{
			MaxConnections:             1,
			ProtocolMemoryLimit:        toml.ByteSize(steady + 2*transient),
			ClientHandshakePacketLimit: handshakeLimit,
		}
		budget, err := calculateProtocolMemoryBudget(&cfg)
		require.NoError(t, err)
		require.Equal(t, steady, budget.steadyBytes)
		require.Equal(t, transient, budget.transientBytes)
		require.Equal(t, 2*transient, budget.headroomBytes)
		require.Equal(t, uint64(2*handshakeLimit), budget.initialBytes)
		require.Equal(t, uint64(proxyIOSessionBufferSize+handshakeLimit), budget.backendBytes)
	})
}

func TestProtocolMemoryLimiterLifecycle(t *testing.T) {
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{headroomBytes: 100})
	first, err := limiter.acquire(context.Background(), 60)
	require.NoError(t, err)
	first.retain()

	timedOut, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = limiter.acquire(timedOut, 60)
	require.ErrorIs(t, err, errProxyConnectionLimit)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Releasing only the authentication owner must not make capacity reusable
	// while a pipelined prefix still owns the same transient peak.
	first.release()
	stillBlocked, cancelBlocked := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelBlocked()
	_, err = limiter.acquire(stillBlocked, 60)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	first.release()
	second, err := limiter.acquire(context.Background(), 60)
	require.NoError(t, err)
	second.release()
	require.Zero(t, limiter.used.Load())
}

func TestProtocolMemoryServerConnLifecycle(t *testing.T) {
	t.Run("failed connection closes lease", func(t *testing.T) {
		limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{headroomBytes: 1})
		lease, err := limiter.acquire(context.Background(), 1)
		require.NoError(t, err)
		conn := &protocolMemoryServerConn{
			ServerConn: &mockServerConn{},
			lease:      lease,
		}
		require.NoError(t, conn.Close())
		require.NoError(t, conn.Close())
		require.Zero(t, limiter.used.Load())
	})

	t.Run("promotion is idempotent", func(t *testing.T) {
		limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{headroomBytes: 1})
		lease, err := limiter.acquire(context.Background(), 1)
		require.NoError(t, err)
		conn := &protocolMemoryServerConn{
			ServerConn: &mockServerConn{},
			lease:      lease,
		}
		conn.promoteProtocolMemory()
		conn.promoteProtocolMemory()
		require.NoError(t, conn.Close())
		require.Zero(t, limiter.used.Load())
	})
}
