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
	"io"
	"net"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

func TestHandshakeBufferedConnLifecycle(t *testing.T) {
	t.Run("read releases prefix immediately", func(t *testing.T) {
		local, remote := net.Pipe()
		defer remote.Close()
		allocator := frontend.NewLeakCheckAllocator()
		conn, err := newHandshakeBufferedConn(local, []byte("prefix"), allocator)
		require.NoError(t, err)
		require.False(t, allocator.CheckBalance())

		got := make([]byte, len("prefix"))
		_, err = io.ReadFull(conn, got)
		require.NoError(t, err)
		require.Equal(t, []byte("prefix"), got)
		require.True(t, allocator.CheckBalance())
		require.NoError(t, conn.Close())
	})

	t.Run("partial read and repeated close release once", func(t *testing.T) {
		local, remote := net.Pipe()
		defer remote.Close()
		allocator := frontend.NewLeakCheckAllocator()
		conn, err := newHandshakeBufferedConn(local, []byte("prefix"), allocator)
		require.NoError(t, err)

		got := make([]byte, 2)
		_, err = io.ReadFull(conn, got)
		require.NoError(t, err)
		require.Equal(t, []byte("pr"), got)
		require.False(t, allocator.CheckBalance())
		require.NoError(t, conn.Close())
		require.True(t, allocator.CheckBalance())
		_ = conn.Close()
		require.True(t, allocator.CheckBalance())
	})

	t.Run("concurrent read and close keep ownership balanced", func(t *testing.T) {
		for range 100 {
			local, remote := net.Pipe()
			allocator := frontend.NewLeakCheckAllocator()
			conn, err := newHandshakeBufferedConn(local, []byte("prefix"), allocator)
			require.NoError(t, err)

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, _ = conn.Read(make([]byte, len("prefix")))
			}()
			go func() {
				defer wg.Done()
				<-start
				_ = conn.Close()
			}()
			close(start)
			wg.Wait()
			require.True(t, allocator.CheckBalance())
			_ = remote.Close()
		}
	})
}
