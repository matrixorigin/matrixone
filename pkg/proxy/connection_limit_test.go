// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

type deadlineErrorConn struct {
	net.Conn
}

func (c *deadlineErrorConn) SetWriteDeadline(time.Time) error {
	return net.ErrClosed
}

func TestConnectionLimiter(t *testing.T) {
	t.Run("global limit and idempotent release", func(t *testing.T) {
		limiter := newConnectionLimiter(2, 2)
		first, ok := limiter.acquire()
		require.True(t, ok)
		second, ok := limiter.acquire()
		require.True(t, ok)
		_, ok = limiter.acquire()
		require.False(t, ok)

		first.release()
		first.release()
		third, ok := limiter.acquire()
		require.True(t, ok)
		second.release()
		third.release()
		require.Equal(t, 0, limiter.total)
	})

	t.Run("tenant rejection retains global lease until cleanup", func(t *testing.T) {
		limiter := newConnectionLimiter(3, 1)
		first, ok := limiter.acquire()
		require.True(t, ok)
		require.True(t, first.bindTenant("tenant-a"))
		require.True(t, first.bindTenant("TENANT-A"))
		require.False(t, first.bindTenant("tenant-b"))

		second, ok := limiter.acquire()
		require.True(t, ok)
		require.False(t, second.bindTenant("tenant-a"))
		require.Equal(t, 2, limiter.total)
		require.Equal(t, 1, limiter.byTenant[Tenant("tenant-a")])

		second.release()
		first.release()
		require.Equal(t, 0, limiter.total)
		require.Empty(t, limiter.byTenant)
	})

	t.Run("concurrent acquire never exceeds global bound", func(t *testing.T) {
		const limit = 8
		limiter := newConnectionLimiter(limit, limit)
		var active atomic.Int64
		var peak atomic.Int64
		var wg sync.WaitGroup
		var attempted sync.WaitGroup
		start := make(chan struct{})
		release := make(chan struct{})
		attempted.Add(128)
		for range 128 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				lease, ok := limiter.acquire()
				if !ok {
					attempted.Done()
					return
				}
				current := active.Add(1)
				for {
					old := peak.Load()
					if current <= old || peak.CompareAndSwap(old, current) {
						break
					}
				}
				attempted.Done()
				<-release
				active.Add(-1)
				lease.release()
			}()
		}
		close(start)
		attempted.Wait()
		close(release)
		wg.Wait()
		require.LessOrEqual(t, peak.Load(), int64(limit))
		require.Equal(t, 0, limiter.total)
	})

	t.Run("concurrent tenant binding never exceeds tenant bound", func(t *testing.T) {
		const (
			connections = 128
			tenantLimit = 8
		)
		limiter := newConnectionLimiter(connections, tenantLimit)
		leases := make([]*connectionLease, connections)
		for i := range leases {
			var ok bool
			leases[i], ok = limiter.acquire()
			require.True(t, ok)
		}

		start := make(chan struct{})
		var admitted atomic.Int64
		var wg sync.WaitGroup
		for _, lease := range leases {
			wg.Add(1)
			go func(lease *connectionLease) {
				defer wg.Done()
				<-start
				if lease.bindTenant("TENANT-A") {
					admitted.Add(1)
				}
			}(lease)
		}
		close(start)
		wg.Wait()

		require.Equal(t, int64(tenantLimit), admitted.Load())
		require.Equal(t, tenantLimit, limiter.byTenant[Tenant("tenant-a")])
		for _, lease := range leases {
			lease.release()
		}
		require.Equal(t, 0, limiter.total)
		require.Empty(t, limiter.byTenant)
	})

	t.Run("bind racing with release closes exactly once", func(t *testing.T) {
		limiter := newConnectionLimiter(1, 1)
		for range 256 {
			lease, ok := limiter.acquire()
			require.True(t, ok)
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_ = lease.bindTenant("tenant-a")
			}()
			go func() {
				defer wg.Done()
				<-start
				lease.release()
			}()
			close(start)
			wg.Wait()
			lease.release()
			require.Equal(t, 0, limiter.total)
			require.Empty(t, limiter.byTenant)
		}
	})
}

func TestRewriteProxyError(t *testing.T) {
	t.Run("connection limit", func(t *testing.T) {
		err := fmt.Errorf("wrapped: %w", errProxyConnectionLimit)
		code, state, message := rewriteProxyError(err)
		definition := moerr.MysqlErrorMsgRefer[moerr.ER_CON_COUNT_ERROR]
		require.Equal(t, definition.ErrorCode, code)
		require.Equal(t, definition.SqlStates[0], state)
		require.Equal(t, definition.ErrorMsgOrFormat, message)
		require.True(t, isProxyAdmissionError(err))
	})

	t.Run("packet too large", func(t *testing.T) {
		err := fmt.Errorf("wrapped: %w", frontend.ErrPacketTooLarge)
		code, state, message := rewriteProxyError(err)
		definition := moerr.MysqlErrorMsgRefer[moerr.ER_SERVER_NET_PACKET_TOO_LARGE]
		require.Equal(t, definition.ErrorCode, code)
		require.Equal(t, definition.SqlStates[0], state)
		require.Equal(t, definition.ErrorMsgOrFormat, message)
		require.True(t, isProxyAdmissionError(err))
	})

	require.False(t, isProxyAdmissionError(moerr.NewInternalErrorNoCtx("other")))
}

func TestWriteConnectionLimitError(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeConnectionLimitError(server)
		_ = server.Close()
	}()

	header := make([]byte, 4)
	_, err := io.ReadFull(client, header)
	require.NoError(t, err)
	payloadLength := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	require.Zero(t, header[3])
	payload := make([]byte, payloadLength)
	_, err = io.ReadFull(client, payload)
	require.NoError(t, err)
	require.Equal(t, byte(0xff), payload[0])
	require.Equal(t, moerr.ER_CON_COUNT_ERROR, binary.LittleEndian.Uint16(payload[1:3]))
	require.Equal(t, "#08004", string(payload[3:9]))
	require.Equal(t, "Too many connections", string(payload[9:]))
	<-done

	server, client = net.Pipe()
	defer server.Close()
	defer client.Close()
	writeConnectionLimitError(&deadlineErrorConn{Conn: server})
}
