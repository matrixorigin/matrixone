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
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	goettybuf "github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
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
			frontend.PacketHeaderLength + 64 + ProxyHeaderLength +
			int(defaultProxyProtocolBodyLimit) + proxyBackendRetainedResponseLimit +
			proxyApplicationSessionPersistentBytes + proxyTunnelBufferSize))
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
		require.Equal(t, uint64(cfg.ProtocolMemoryLimit)-uint64(connections)*(proxyTunnelBufferSize+proxyApplicationSessionPersistentBytes),
			budget.managedBytes)
		require.Equal(t, transient, budget.transientBytes)
		require.Equal(t, transient, budget.headroomBytes)
		require.Equal(t, backend, budget.backgroundBytes)
		require.Equal(t, initial, budget.initialBytes)
		require.Equal(t, backend, budget.backendBytes)
	})

	t.Run("large login includes rounded backend dynamic write", func(t *testing.T) {
		const handshakeLimit = 64 << 10
		steady := uint64(2*proxyIOSessionBufferSize + frontend.PacketHeaderLength + handshakeLimit +
			ProxyHeaderLength + int(defaultProxyProtocolBodyLimit) +
			proxyBackendRetainedResponseLimit + proxyApplicationSessionPersistentBytes +
			proxyTunnelBufferSize)
		preAuth := uint64(frontend.PacketHeaderLength + handshakeLimit +
			ProxyHeaderLength + int(defaultProxyProtocolBodyLimit))
		applicationReadCapacity := uint64(2) * (preAuth +
			proxyApplicationReadChunkSize - 1 + proxyApplicationReadChunkSize)
		readGrowth := 2 * applicationReadCapacity
		readParse := uint64(2*handshakeLimit) + applicationReadCapacity
		initial := max(readGrowth, readParse) - preAuth
		backend := uint64(proxyIOSessionBufferSize + 3*handshakeLimit)
		transient := max(initial, backend) + backend
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

	t.Run("small wire limits still reserve the accepted session input", func(t *testing.T) {
		handshakeLimit := int(minimumClientHandshakePacketLimit)
		proxyBodyLimit := int(minimumProxyProtocolBodyLimit)
		steadyHandshake := max(
			frontend.PacketHeaderLength+handshakeLimit+ProxyHeaderLength+proxyBodyLimit,
			proxyApplicationReadBufferSize,
		)
		expectedSteady := uint64(2*proxyIOSessionBufferSize + steadyHandshake +
			proxyBackendRetainedResponseLimit +
			proxyApplicationSessionPersistentBytes + proxyTunnelBufferSize)
		cfg := Config{
			MaxConnections:             1,
			ProtocolMemoryLimit:        16 << 20,
			ClientHandshakePacketLimit: toml.ByteSize(handshakeLimit),
			ProxyProtocolBodyLimit:     toml.ByteSize(proxyBodyLimit),
		}
		budget, err := calculateProtocolMemoryBudget(&cfg)
		require.NoError(t, err)
		require.Equal(t, expectedSteady, budget.steadyBytes)
	})
}

func TestProxyApplicationReadCapacityUpperBound(t *testing.T) {
	type testCase struct {
		buffered uint64
		fragment int
	}
	cases := []testCase{
		{buffered: 1, fragment: 1},
		{buffered: 4 << 10, fragment: 1},
		{buffered: 72 << 10, fragment: 17},
		{buffered: 72 << 10, fragment: int(proxyApplicationReadChunkSize)},
		{buffered: 520 << 10, fragment: int(proxyApplicationReadChunkSize)},
	}
	for _, test := range cases {
		t.Run(fmt.Sprintf("bytes-%d-fragment-%d", test.buffered, test.fragment), func(t *testing.T) {
			managed := frontend.NewLeakCheckAllocator()
			input := goettybuf.NewByteBuf(
				int(proxyApplicationReadBufferSize),
				goettybuf.WithMemAllocator(newProxyApplicationAllocator(managed)),
			)
			require.True(t, managed.CheckBalance(),
				"bootstrap input must not consume bounded capacity before admission")
			reader := &fragmentReader{
				remaining: int(test.buffered),
				fragment:  test.fragment,
			}
			for reader.remaining > 0 {
				_, err := input.ReadFrom(reader)
				require.NoError(t, err)
			}

			capacity, ok := proxyApplicationReadCapacityUpperBound(test.buffered)
			require.True(t, ok)
			require.LessOrEqual(t, uint64(len(input.RawBuf())), capacity,
				"the budget must dominate the pinned Goetty ByteBuf growth contract")
			require.False(t, managed.CheckBalance(),
				"grown input must be owned by the shared allocator")
			input.Close()
			require.True(t, managed.CheckBalance(),
				"handshake handoff must release grown input immediately")
			input.Close()
			require.True(t, managed.CheckBalance(), "repeated Goetty close must be harmless")
		})
	}
}

func TestProxyApplicationSessionBufferContract(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	managed := frontend.NewLeakCheckAllocator()
	session := goetty.NewIOSession(
		goetty.WithSessionConn(1, local),
		goetty.WithSessionAllocator(newProxyApplicationAllocator(managed)),
		goetty.WithSessionRWBUfferSize(
			proxyApplicationReadBufferSize,
			proxyApplicationWriteBufferSize,
		),
	)
	require.Len(t, session.(goetty.BufferedIOSession).InBuf().RawBuf(),
		proxyApplicationReadBufferSize)
	require.Len(t, session.OutBuf().RawBuf(), proxyApplicationWriteBufferSize)

	// Goetty does not expose options for its per-session I/O-copy buffers. Pin
	// their concrete dependency contract here so an upgrade cannot silently
	// invalidate the steady-memory formula.
	implementation := reflect.ValueOf(session).Elem()
	require.Equal(t, proxyApplicationReadCopyBufferSize,
		implementation.FieldByName("readCopyBuf").Len())
	require.Equal(t, proxyApplicationWriteCopyBufferSize,
		implementation.FieldByName("writeCopyBuf").Len())
	require.True(t, managed.CheckBalance(),
		"all application-session bootstrap buffers must remain inline")
	require.NoError(t, session.Close())
	require.True(t, managed.CheckBalance())
}

func TestProxyApplicationAllocatorRejectsInvalidGrowth(t *testing.T) {
	allocator := newProxyApplicationAllocator(nil)
	bootstrap, err := allocator.Alloc(proxyApplicationReadBufferSize)
	require.NoError(t, err)
	require.Len(t, bootstrap, proxyApplicationReadBufferSize)
	allocator.Free(bootstrap)

	_, err = allocator.Alloc(proxyApplicationReadBufferSize + 1)
	require.Error(t, err)
	_, err = allocator.Alloc(-1)
	require.Error(t, err)
	require.NotPanics(t, func() {
		allocator.Free(make([]byte, proxyApplicationReadBufferSize+1))
	})

	input := goettybuf.NewByteBuf(
		proxyApplicationReadBufferSize,
		goettybuf.WithMemAllocator(allocator),
	)
	defer input.Close()
	_, err = input.ReadFrom(&fragmentReader{remaining: 1, fragment: 1})
	require.Error(t, err, "post-admission growth must fail normally instead of panicking")
	require.Len(t, input.RawBuf(), proxyApplicationReadBufferSize)
}

func TestProtocolMemoryBudgetCoversObservedOuterReadCapacity(t *testing.T) {
	const handshakeLimit = 512 << 10
	preAuth := uint64(ProxyHeaderLength) + uint64(defaultProxyProtocolBodyLimit) +
		frontend.PacketHeaderLength + handshakeLimit
	maxBuffered := preAuth + proxyApplicationReadChunkSize - 1
	input := goettybuf.NewByteBuf(int(proxyApplicationReadBufferSize))
	defer input.Close()
	reader := &fragmentReader{
		remaining: int(maxBuffered),
		fragment:  int(proxyApplicationReadChunkSize),
	}
	for reader.remaining > 0 {
		_, err := input.ReadFrom(reader)
		require.NoError(t, err)
	}
	require.Greater(t, uint64(len(input.RawBuf())), preAuth,
		"the regression requires real capacity to exceed logical wire bytes")

	cfg := Config{
		MaxConnections:             1,
		ProtocolMemoryLimit:        64 << 20,
		ClientHandshakePacketLimit: handshakeLimit,
	}
	budget, err := calculateProtocolMemoryBudget(&cfg)
	require.NoError(t, err)
	observedCapacity := uint64(len(input.RawBuf()))
	observedReadOverlap := max(
		2*observedCapacity,
		observedCapacity+2*handshakeLimit,
	) - max(preAuth, proxyApplicationReadBufferSize)
	require.GreaterOrEqual(t, budget.initialBytes, observedReadOverlap)
}

type fragmentReader struct {
	remaining int
	fragment  int
}

func (r *fragmentReader) Read(dst []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(dst), r.fragment, r.remaining)
	for i := range n {
		dst[i] = byte(i)
	}
	r.remaining -= n
	return n, nil
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
