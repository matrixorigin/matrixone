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
	"net"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

// handshakeBufferedConn transfers bytes read ahead by the handshake decoder to
// the raw tunnel. The prefix allocation is released as soon as it is consumed,
// or by Close when connection setup fails before the tunnel reads it.
//
// The mutex protects only the local prefix. It is deliberately released before
// reading the underlying connection so Close can still interrupt blocked I/O.
type handshakeBufferedConn struct {
	net.Conn

	mu         sync.Mutex
	prefix     []byte
	allocation []byte
	allocator  frontend.Allocator
	pending    atomic.Bool
}

func newHandshakeBufferedConn(
	conn net.Conn,
	source []byte,
	allocator frontend.Allocator,
) (*handshakeBufferedConn, error) {
	if conn == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil connection for handshake buffer handoff")
	}
	if len(source) == 0 {
		return &handshakeBufferedConn{Conn: conn}, nil
	}
	if allocator == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil allocator for handshake buffer handoff")
	}
	allocation, err := allocator.Alloc(len(source))
	if err != nil {
		return nil, err
	}
	if len(allocation) < len(source) {
		allocator.Free(allocation)
		return nil, moerr.NewInternalErrorNoCtx("short allocation for handshake buffer handoff")
	}
	prefix := allocation[:len(source)]
	copy(prefix, source)
	c := &handshakeBufferedConn{
		Conn:       conn,
		prefix:     prefix,
		allocation: allocation,
		allocator:  allocator,
	}
	c.pending.Store(true)
	return c, nil
}

func (c *handshakeBufferedConn) Read(dst []byte) (int, error) {
	if !c.pending.Load() {
		return c.Conn.Read(dst)
	}
	c.mu.Lock()
	if len(c.prefix) > 0 {
		n := copy(dst, c.prefix)
		c.prefix = c.prefix[n:]
		if len(c.prefix) == 0 {
			c.releasePrefixLocked()
		}
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()
	return c.Conn.Read(dst)
}

func (c *handshakeBufferedConn) Close() error {
	c.mu.Lock()
	c.releasePrefixLocked()
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *handshakeBufferedConn) releasePrefixLocked() {
	if c.allocation == nil {
		return
	}
	c.allocator.Free(c.allocation)
	c.prefix = nil
	c.allocation = nil
	c.allocator = nil
	c.pending.Store(false)
}
