// Copyright 2026 Matrix Origin
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

package morpc

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type transportBufferAllocator struct {
	freed atomic.Int32
}

func (*transportBufferAllocator) Alloc(size int) ([]byte, error) { return make([]byte, size), nil }
func (a *transportBufferAllocator) Free(data []byte) {
	if data != nil {
		a.freed.Add(1)
	}
}

func newWriterOwnerServer(t *testing.T, codec Codec) *server {
	t.Helper()
	s := &server{
		name: t.Name(), metrics: newServerMetrics(t.Name()), codec: codec,
		logger: zap.NewNop(), stopper: stopper.NewStopper(t.Name()), sessions: &sync.Map{},
	}
	s.adjust()
	s.pool.futures = &sync.Pool{New: func() any { return newFuture(s.releaseFuture) }}
	t.Cleanup(s.stopper.Stop)
	return s
}

// The initial Goetty reference belongs to the simulated application reader.
// getSession acquires the separate writer reference through the real path.
func newWriterOwnerTransport(t *testing.T, conn net.Conn, codec Codec, options ...goetty.Option) (
	goetty.IOSession, *transportBufferAllocator, func(),
) {
	t.Helper()
	a := &transportBufferAllocator{}
	options = append(options, goetty.WithSessionConn(1, conn), goetty.WithSessionCodec(codec),
		goetty.WithSessionAllocator(a), goetty.WithSessionRWBUfferSize(512, 512),
		goetty.WithSessionLogger(zap.NewNop()))
	rs := goetty.NewIOSession(options...)
	releaseReader := sync.OnceFunc(func() { require.NoError(t, rs.Close()) })
	t.Cleanup(releaseReader)
	return rs, a, releaseReader
}

type pausedBufferHeader struct {
	entered chan struct{}
	resume  chan struct{}
	encoded chan byte
	alloc   *transportBufferAllocator
}

func (h *pausedBufferHeader) Encode(_ *RPCMessage, out *buf.ByteBuf) (int, error) {
	index := out.GetWriteIndex()
	out.MustWriteByte(0xa5)
	close(h.entered)
	<-h.resume
	// Keep the failing baseline probe controlled: the old owner releases the
	// buffer here. The real trace codec would reach RawSlice on freed memory.
	if h.alloc.freed.Load() != 0 {
		return 0, io.ErrClosedPipe
	}
	h.encoded <- out.RawSlice(index, index+1)[0]
	return 1, nil
}

func (*pausedBufferHeader) Decode(_ *RPCMessage, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, io.ErrShortBuffer
	}
	return 1, nil
}

func TestClientSessionCloseRetainsWriterBuffers(t *testing.T) {
	codec := newTestCodec()
	s := newWriterOwnerServer(t, codec)
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	var released atomic.Int32
	rs, alloc, releaseReader := newWriterOwnerTransport(t, conn, codec,
		goetty.WithSessionReleaseMsgFunc(func(any) { released.Add(1) }))
	s.options.releaseMessageFunc = func(Message) { released.Add(1) }
	h := &pausedBufferHeader{entered: make(chan struct{}), resume: make(chan struct{}),
		encoded: make(chan byte, 1), alloc: alloc}
	codec.AddHeaderCodec(h)
	resume := sync.OnceFunc(func() { close(h.resume) })
	t.Cleanup(resume)
	cs, err := s.getSession(rs)
	require.NoError(t, err)
	require.NoError(t, cs.AsyncWrite(newTestMessage(1)))
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not enter header encoding")
	}

	closed := make(chan error, 1)
	go func() { closed <- cs.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		resume()
		t.Fatal("public Close waited for the active codec")
	}
	require.ErrorIs(t, cs.SessionCtx().Err(), context.Canceled)
	releaseReader()
	require.NoError(t, cs.Close())
	require.Zero(t, alloc.freed.Load(), "transport buffers were freed while Encode still owned them")

	resume()
	s.stopper.Stop()
	select {
	case encoded := <-h.encoded:
		require.Equal(t, byte(0xa5), encoded)
	default:
		t.Fatal("codec did not finish reading its retained buffer")
	}
	require.EqualValues(t, 2, alloc.freed.Load(), "both buffers must be released after writer exit")
	require.EqualValues(t, 1, released.Load(), "the accepted response must be released exactly once")
}

func TestGetSessionReleasesWriterReferenceOnStartFailure(t *testing.T) {
	codec := newTestCodec()
	s := newWriterOwnerServer(t, codec)
	s.stopper.Stop()
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	rs, alloc, releaseReader := newWriterOwnerTransport(t, conn, codec)
	cs, err := s.getSession(rs)
	require.Error(t, err)
	require.Nil(t, cs)
	_, exists := s.sessions.Load(rs.ID())
	require.False(t, exists)
	require.Zero(t, testutil.ToFloat64(s.metrics.sessionSizeGauge))
	require.Zero(t, alloc.freed.Load(), "failed admission must retain the reader's reference")
	releaseReader()
	require.EqualValues(t, 2, alloc.freed.Load(), "failed admission must not leak its writer reference")
}

type enteredWriteConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *enteredWriteConn) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(data)
}

func TestClientSessionCloseInterruptsTransportWrite(t *testing.T) {
	for _, payload := range []bool{false, true} {
		name := "flush"
		if payload {
			name = "payload encode"
		}
		t.Run(name, func(t *testing.T) {
			codec := newTestCodec()
			s := newWriterOwnerServer(t, codec)
			conn, peer := net.Pipe()
			t.Cleanup(func() { _ = peer.Close() })
			blocked := &enteredWriteConn{Conn: conn, entered: make(chan struct{})}
			var released atomic.Int32
			rs, alloc, releaseReader := newWriterOwnerTransport(t, blocked, codec,
				goetty.WithSessionReleaseMsgFunc(func(any) { released.Add(1) }))
			s.options.releaseMessageFunc = func(Message) { released.Add(1) }
			cs, err := s.getSession(rs)
			require.NoError(t, err)
			var message Message = &codecBodyMessage{id: 1}
			if payload {
				message = &codecBodyPayloadMessage{codecBodyMessage: codecBodyMessage{id: 1}, payload: []byte{1}}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f, err := cs.send(RPCMessage{Ctx: ctx, Message: message})
			require.NoError(t, err)
			t.Cleanup(f.Close)
			select {
			case <-blocked.entered:
			case <-ctx.Done():
				t.Fatal("writer did not enter socket output")
			}
			require.NoError(t, cs.Close())
			require.ErrorIs(t, f.waitSendCompleted(), io.ErrClosedPipe)
			require.NoError(t, ctx.Err(), "Close must interrupt output before the request deadline")
			releaseReader()
			s.stopper.Stop()
			require.EqualValues(t, 2, alloc.freed.Load())
			require.EqualValues(t, 1, released.Load())
		})
	}
}
