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
	"net"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestCalculateProtocolMemoryBudget(t *testing.T) {
	t.Run("small login reserves critical and background backend capacity", func(t *testing.T) {
		const connections = 10
		initial := uint64(64 + proxyBackendPacketLimit)
		backend := uint64(proxyIOSessionBufferSize + 2*proxyBackendPacketLimit)
		transient := backend + backend
		steady := uint64(connections * (2*proxyIOSessionBufferSize +
			64 + ProxyHeaderLength + int(defaultProxyProtocolBodyLimit) +
			proxyBackendRetainedResponseLimit))
		cfg := Config{
			MaxConnections:             connections,
			ProtocolMemoryLimit:        toml.ByteSize(steady + transient - 1),
			ClientHandshakePacketLimit: 64,
		}
		_, err := calculateProtocolMemoryBudget(&cfg)
		require.Error(t, err)

		cfg.ProtocolMemoryLimit++
		budget, err := calculateProtocolMemoryBudget(&cfg)
		require.NoError(t, err)
		require.Equal(t, steady, budget.steadyBytes)
		require.Equal(t, transient, budget.transientBytes)
		require.Equal(t, transient, budget.headroomBytes)
		require.Equal(t, backend, budget.backgroundBytes)
		require.Equal(t, initial, budget.initialBytes)
		require.Equal(t, backend, budget.backendBytes)
	})

	t.Run("large login includes rounded backend dynamic write", func(t *testing.T) {
		const handshakeLimit = 64 << 10
		steady := uint64(2*proxyIOSessionBufferSize + handshakeLimit +
			ProxyHeaderLength + int(defaultProxyProtocolBodyLimit) +
			proxyBackendRetainedResponseLimit)
		initial := uint64(3 * handshakeLimit)
		backend := uint64(proxyIOSessionBufferSize + 3*handshakeLimit)
		transient := backend + backend
		cfg := Config{
			MaxConnections:             1,
			ProtocolMemoryLimit:        toml.ByteSize(steady + transient),
			ClientHandshakePacketLimit: handshakeLimit,
		}
		budget, err := calculateProtocolMemoryBudget(&cfg)
		require.NoError(t, err)
		require.Equal(t, steady, budget.steadyBytes)
		require.Equal(t, transient, budget.transientBytes)
		require.Equal(t, transient, budget.headroomBytes)
		require.Equal(t, backend, budget.backgroundBytes)
		require.Equal(t, initial, budget.initialBytes)
		require.Equal(t, backend, budget.backendBytes)
	})
}

func TestProtocolMemoryLimiterSeparatesBackgroundAdmission(t *testing.T) {
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{
		headroomBytes:   2,
		backgroundBytes: 1,
		backendBytes:    1,
	})
	upgrade, err := limiter.acquireBackground(context.Background(), 1)
	require.NoError(t, err)

	critical, err := limiter.acquire(context.Background(), 1)
	require.NoError(t, err, "a stalled background operation must not starve login or migration")
	critical.release()

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = limiter.acquireBackground(waitCtx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	upgrade.release()
	reused, err := limiter.acquireBackground(context.Background(), 1)
	require.NoError(t, err)
	reused.release()
	require.Zero(t, limiter.used.Load())
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

func TestClientConnProtocolMemoryAdmissionUsesTransferContext(t *testing.T) {
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{
		headroomBytes: 1,
		backendBytes:  1,
	})
	occupied, err := limiter.acquire(context.Background(), 1)
	require.NoError(t, err)
	defer occupied.release()

	client := &clientConn{
		ctx:                   context.Background(),
		protocolMemoryLimiter: limiter,
	}
	transferCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.BuildConnWithServer(transferCtx, "old-backend")
	require.ErrorIs(t, err, errProxyConnectionLimit)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), limiter.used.Load())
}

func TestClientConnProtocolMemoryTransferCancellationAfterAdmission(t *testing.T) {
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{
		headroomBytes: 1,
		backendBytes:  1,
	})
	entered := make(chan struct{})
	client := &clientConn{
		ctx:                   context.Background(),
		log:                   runtime.DefaultRuntime().Logger(),
		protocolMemoryLimiter: limiter,
	}
	client.testHelper.connectToBackendContext = func(ctx context.Context) (ServerConn, error) {
		close(entered)
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}

	transferCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.BuildConnWithServer(transferCtx, "old-backend")
		result <- err
	}()
	<-entered
	require.Equal(t, int64(1), limiter.used.Load(), "transfer owns the admitted transient slot")
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("transfer cancellation did not terminate post-admission work")
	}
	require.Zero(t, limiter.used.Load(), "canceled transfer must release global headroom")
	reused, err := limiter.acquire(context.Background(), 1)
	require.NoError(t, err)
	reused.release()
}

func TestClientConnProtocolMemoryCancellationWinsConnectHandoff(t *testing.T) {
	limiter := newProtocolMemoryLimiterWithBudget(protocolMemoryBudget{
		headroomBytes: 1,
		backendBytes:  1,
	})
	local, remote := net.Pipe()
	defer remote.Close()
	backend := newMockServerConn(local)
	client := &clientConn{
		ctx:                   context.Background(),
		log:                   runtime.DefaultRuntime().Logger(),
		protocolMemoryLimiter: limiter,
	}
	transferCtx, cancel := context.WithCancel(context.Background())
	client.testHelper.connectToBackendContext = func(context.Context) (ServerConn, error) {
		cancel()
		return backend, nil
	}

	conn, err := client.BuildConnWithServer(transferCtx, "old-backend")
	require.Nil(t, conn)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, limiter.used.Load(), "canceled handoff must release global headroom")
	_ = remote.SetWriteDeadline(time.Now().Add(time.Second))
	_, err = remote.Write([]byte{1})
	require.Error(t, err, "canceled handoff must close the unowned backend")
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
