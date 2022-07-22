// Copyright 2021 - 2022 Matrix Origin
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
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var (
	testAddr     = "unix:///tmp/goetty.sock"
	testUnixFile = "/tmp/goetty.sock"
)

func TestSend(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
		WithBackendConnectWhenCreate())
}

func TestCloseWhileContinueSending(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			c := make(chan struct{})

			go func() {
				sendFunc := func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					req := newTestMessage(1)
					f, err := b.Send(ctx, req, SendOptions{})
					if err != nil {
						return
					}
					defer f.Close()

					resp, err := f.Get()
					assert.NoError(t, err)
					assert.Equal(t, req, resp)
					select {
					case c <- struct{}{}:
					default:
					}
				}

				for {
					sendFunc()
				}
			}()
			<-c
			b.Close()
		},
		WithBackendConnectWhenCreate())
}

func TestSendWithAlreadyContextDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)

	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {

			req := newTestMessage(1)
			f, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f.Close()
			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
		},
		WithBackendConnectWhenCreate(),
		WithBackendFilter(func(Message) bool {
			cancel()
			return true
		}))
}

func TestSendWithResetConnAndRetry(t *testing.T) {
	retry := 0
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := &testMessage{id: 1}
			f, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
			assert.True(t, retry > 0)
		},
		WithBackendFilter(func(Message) bool {
			retry++
			return true
		}))
}

func TestSendWithTimeout(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
			defer cancel()
			req := &testMessage{id: 1}
			f, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, err, ctx.Err())
		},
		WithBackendConnectWhenCreate())
}

func TestSendWithReconnect(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			for i := 0; i < 10; i++ {
				req := newTestMessage(1)
				f, err := b.Send(ctx, req, SendOptions{})
				assert.NoError(t, err)
				defer f.Close()

				resp, err := f.Get()
				assert.NoError(t, err)
				assert.Equal(t, req, resp)

				b.closeConn()
			}
		},
		WithBackendConnectWhenCreate())
}

func TestStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			st, err := b.NewStream()
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close())
				b.mu.RLock()
				assert.Equal(t, 0, len(b.mu.futures))
				b.mu.RUnlock()
			}()

			n := 1
			for i := 0; i < n; i++ {
				req := &testMessage{id: st.ID()}
				assert.NoError(t, st.Send(req, SendOptions{}))
			}

			rc, err := st.Receive()
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				v, ok := <-rc
				assert.True(t, ok)
				assert.Equal(t, &testMessage{id: st.ID()}, v)
			}
		},
		WithBackendConnectWhenCreate())
}

func TestStreamClosedByConnReset(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Close()
		},
		func(b *remoteBackend) {
			st, err := b.NewStream()
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close())
			}()
			c, err := st.Receive()
			assert.NoError(t, err)
			assert.NoError(t, st.Send(&testMessage{id: st.ID()}, SendOptions{}))

			v, ok := <-c
			assert.True(t, ok)
			assert.Nil(t, v)
		},
		WithBackendConnectWhenCreate())
}

func TestBusy(t *testing.T) {
	n := 0
	c := make(chan struct{})
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			assert.False(t, b.Busy())

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
			defer cancel()
			req := &testMessage{id: 1}
			f1, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f1.Close()

			f2, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f2.Close()

			assert.True(t, b.Busy())
			c <- struct{}{}
		},
		WithBackendConnectWhenCreate(),
		WithBackendFilter(func(Message) bool {
			if n == 0 {
				<-c
				n++
			}
			return false
		}),
		WithBackendBatchSendSize(1),
		WithBackendBufferSize(10),
		WithBackendBusyBufferSize(1))
}

func TestDoneWithClosedStreamCannotPanic(t *testing.T) {
	c := make(chan Message, 1)
	s := newStream(c,
		func(m backendSendMessage) error {
			return nil
		},
		func(s *stream) {},
		func() {})
	s.init(1)
	assert.NoError(t, s.Send(&testMessage{id: s.ID()}, SendOptions{}))
	assert.NoError(t, s.Close())
	assert.Nil(t, <-c)

	s.done(nil)
}

func TestGCStream(t *testing.T) {
	c := make(chan Message, 1)
	s := newStream(c,
		func(m backendSendMessage) error {
			return nil
		},
		func(s *stream) {},
		func() {})
	s.init(1)
	s = nil
	debug.FreeOSMemory()
	_, ok := <-c
	assert.False(t, ok)
}

func TestLastActiveWithNew(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			assert.NotEqual(t, time.Time{}, b.LastActiveTime())
		},
		WithBackendConnectWhenCreate())
}

func TestLastActiveWithSend(t *testing.T) {
	c := make(chan struct{})
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			<-c
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			t1 := b.LastActiveTime()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req, SendOptions{})
			assert.NoError(t, err)
			defer f.Close()

			t2 := b.LastActiveTime()
			assert.NotEqual(t, t1, t2)
			assert.True(t, t2.After(t1))
			c <- struct{}{}

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)

			t3 := b.LastActiveTime()
			assert.NotEqual(t, t2, t3)
			assert.True(t, t3.After(t2))

		},
		WithBackendConnectWhenCreate())
}

func TestLastActiveWithStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			t1 := b.LastActiveTime()

			st, err := b.NewStream()
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close())
			}()

			n := 1
			for i := 0; i < n; i++ {
				req := &testMessage{id: st.ID()}
				assert.NoError(t, st.Send(req, SendOptions{}))
				t2 := b.LastActiveTime()
				assert.NotEqual(t, t1, t2)
				assert.True(t, t2.After(t1))
			}
		},
		WithBackendConnectWhenCreate())
}

func testBackendSend(t *testing.T,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {
	app := newTestApp(t, handleFunc)
	assert.NoError(t, app.Start())
	defer func() {
		assert.NoError(t, app.Stop())
	}()

	options = append(options,
		WithBackendBufferSize(1),
		WithBackendLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel).With(zap.String("testcase", t.Name()))))
	rb, err := NewRemoteBackend(testAddr, newTestCodec(), options...)
	assert.NoError(t, err)

	b := rb.(*remoteBackend)
	defer func() {
		b.Close()
		assert.False(t, b.conn.Connected())
	}()
	testFunc(b)
}

func newTestApp(t *testing.T,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	opts ...goetty.AppOption) goetty.NetApplication {
	assert.NoError(t, os.RemoveAll(testUnixFile))
	codec := newTestCodec().(*messageCodec)
	opts = append(opts, goetty.WithAppSessionOptions(goetty.WithCodec(codec.encoder, codec.deocder)))
	app, err := goetty.NewApplication(testAddr, handleFunc, opts...)
	assert.NoError(t, err)

	return app
}

type testBackendFactory struct {
	sync.RWMutex
	id int
}

func newTestBackendFactory() *testBackendFactory {
	return &testBackendFactory{}
}

func (bf *testBackendFactory) Create(backend string) (Backend, error) {
	bf.Lock()
	defer bf.Unlock()
	b := &testBackend{id: bf.id}
	bf.id++
	return b, nil
}

type testBackend struct {
	sync.RWMutex
	id         int
	busy       bool
	activeTime time.Time
	closed     bool
}

func (b *testBackend) Send(ctx context.Context, request Message, opts SendOptions) (*Future, error) {
	b.active()
	f := newFuture(nil)
	f.init(request.GetID(), ctx)
	return f, nil
}

func (b *testBackend) NewStream() (Stream, error) {
	b.active()
	st := newStream(make(chan Message, 1),
		func(m backendSendMessage) error { return nil },
		func(s *stream) {},
		b.active)
	st.init(1)
	return st, nil
}

func (b *testBackend) Close() {
	b.Lock()
	defer b.Unlock()
	b.closed = true
}
func (b *testBackend) Busy() bool { return b.busy }
func (b *testBackend) LastActiveTime() time.Time {
	b.RLock()
	defer b.RUnlock()
	return b.activeTime
}

func (b *testBackend) active() {
	b.Lock()
	defer b.Unlock()
	b.activeTime = time.Now()
}

type testMessage struct {
	id      uint64
	payload []byte
}

func newTestMessage(id uint64) *testMessage {
	return &testMessage{id: id}
}

func (tm *testMessage) SetID(id uint64) {
	tm.id = id
}

func (tm *testMessage) GetID() uint64 {
	return tm.id
}

func (tm *testMessage) DebugString() string {
	return fmt.Sprintf("%d", tm.id)
}

func (tm *testMessage) Size() int {
	return 8
}

func (tm *testMessage) MarshalTo(data []byte) (int, error) {
	buf.Uint64ToBytesTo(tm.id, data)
	return 8, nil
}

func (tm *testMessage) Unmarshal(data []byte) error {
	tm.id = buf.Byte2Uint64(data)
	return nil
}

func (tm *testMessage) GetPayloadField() []byte {
	return tm.payload
}

func (tm *testMessage) SetPayloadField(data []byte) {
	tm.payload = data
}

func newTestCodec() Codec {
	return NewMessageCodec(func() Message { return messagePool.Get().(*testMessage) }, 1024)
}

var (
	messagePool = sync.Pool{
		New: func() any {
			return newTestMessage(0)
		},
	}
)
