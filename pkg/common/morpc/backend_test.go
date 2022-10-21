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
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	testProxyAddr = "unix:///tmp/proxy.sock"
	testAddr      = "unix:///tmp/goetty.sock"
	testUnixFile  = "/tmp/goetty.sock"
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
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
		WithBackendConnectWhenCreate())
}

func TestSendWithPayloadCannotTimeout(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			b.conn.RawConn().SetWriteDeadline(time.Now().Add(time.Millisecond))
			time.Sleep(time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			req := newTestMessage(1)
			req.payload = []byte("hello")
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
		WithBackendConnectWhenCreate())
}

func TestSendWithPayloadCannotBlockIfFutureRemoved(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			wg.Wait()
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			req := newTestMessage(1)
			req.payload = []byte("hello")
			f, err := b.Send(ctx, req)
			require.NoError(t, err)
			id := f.id
			f.Close()
			b.mu.RLock()
			_, ok := b.mu.futures[id]
			assert.True(t, ok)
			b.mu.RUnlock()
			wg.Done()
			time.Sleep(time.Second)
		},
		WithBackendConnectWhenCreate(),
		WithBackendHasPayloadResponse())
}

func TestCloseWhileContinueSending(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			c := make(chan struct{})
			stopC := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendFunc := func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					req := newTestMessage(1)
					f, err := b.Send(ctx, req)
					if err != nil {
						return
					}
					defer f.Close()

					resp, err := f.Get()
					if err == nil {
						assert.Equal(t, req, resp)
					}
					select {
					case c <- struct{}{}:
					default:
					}
				}

				for {
					select {
					case <-stopC:
						return
					default:
						sendFunc()
					}
				}
			}()
			<-c
			b.Close()
			close(stopC)
			wg.Wait()
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
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()
			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
		},
		WithBackendConnectWhenCreate(),
		WithBackendFilter(func(Message, string) bool {
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
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
			assert.True(t, retry > 0)
		},
		WithBackendFilter(func(Message, string) bool {
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
			f, err := b.Send(ctx, req)
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
	var rb *remoteBackend
	idx := 0
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			rb = b
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			for i := 0; i < 10; i++ {
				req := newTestMessage(1)
				f, err := b.Send(ctx, req)
				assert.NoError(t, err)
				defer f.Close()

				resp, err := f.Get()
				assert.NoError(t, err)
				assert.Equal(t, req, resp)
			}
		},
		WithBackendConnectWhenCreate(),
		WithBackendFilter(func(Message, string) bool {
			idx++
			if idx%2 == 0 {
				rb.closeConn(false)
				idx = 0
			}
			return true
		}))
}

func TestSendWithCannotConnectWillTimeout(t *testing.T) {
	var rb *remoteBackend
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			rb = b
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()
			req := &testMessage{id: 1}
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, err, ctx.Err())
		},
		WithBackendFilter(func(Message, string) bool {
			assert.NoError(t, rb.conn.Disconnect())
			rb.remote = ""
			return true
		}),
		WithBackendConnectTimeout(time.Millisecond*200),
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

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			n := 1
			for i := 0; i < n; i++ {
				req := &testMessage{id: st.ID()}
				assert.NoError(t, st.Send(ctx, req))
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

func TestStreamSendWillPanicIfDeadlineNotSet(t *testing.T) {
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

			defer func() {
				if err := recover(); err == nil {
					assert.Fail(t, "must panic")
				}
			}()

			req := &testMessage{id: st.ID()}
			assert.NoError(t, st.Send(context.TODO(), req))
		},
		WithBackendConnectWhenCreate())
}

func TestStreamClosedByConnReset(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Disconnect()
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			st, err := b.NewStream()
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close())
			}()
			c, err := st.Receive()
			assert.NoError(t, err)
			assert.NoError(t, st.Send(ctx, &testMessage{id: st.ID()}))

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
			f1, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f1.Close()

			f2, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f2.Close()

			assert.True(t, b.Busy())
			c <- struct{}{}
		},
		WithBackendConnectWhenCreate(),
		WithBackendFilter(func(Message, string) bool {
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
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	c := make(chan Message, 1)
	s := newStream(c,
		func(m backendSendMessage) error {
			return nil
		},
		func(s *stream) {},
		func() {})
	s.init(1)
	assert.NoError(t, s.Send(ctx, &testMessage{id: s.ID()}))
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
			f, err := b.Send(ctx, req)
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
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			t1 := b.LastActiveTime()

			st, err := b.NewStream()
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close())
			}()

			n := 1
			for i := 0; i < n; i++ {
				req := &testMessage{id: st.ID()}
				assert.NoError(t, st.Send(ctx, req))
				t2 := b.LastActiveTime()
				assert.NotEqual(t, t1, t2)
				assert.True(t, t2.After(t1))
			}
		},
		WithBackendConnectWhenCreate())
}

func TestBackendConnectTimeout(t *testing.T) {
	rb, err := NewRemoteBackend(testAddr, newTestCodec(),
		WithBackendConnectTimeout(time.Millisecond*200),
		WithBackendConnectWhenCreate())
	assert.Error(t, err)
	assert.Nil(t, rb)
}

func TestInactiveAfterCannotConnect(t *testing.T) {
	app := newTestApp(t, func(conn goetty.IOSession, msg interface{}, _ uint64) error {
		return conn.Write(msg, goetty.WriteOptions{Flush: true})
	})
	assert.NoError(t, app.Start())

	testBackendSendWithoutServer(t,
		testAddr,
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)

			assert.NoError(t, app.Stop())
			var v time.Time
			for {
				if b.LastActiveTime() == v {
					break
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
		WithBackendConnectWhenCreate(),
		WithBackendConnectTimeout(time.Millisecond*100))
}

func TestTCPProxyExample(t *testing.T) {
	assert.NoError(t, os.RemoveAll(testProxyAddr[7:]))
	p := goetty.NewProxy(testProxyAddr, nil)
	assert.NoError(t, p.Start())
	defer func() {
		assert.NoError(t, p.Stop())
	}()
	p.AddUpStream(testAddr, time.Second*10)

	testBackendSendWithAddr(t,
		testProxyAddr,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
		WithBackendConnectWhenCreate())
}

func testBackendSend(t *testing.T,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {
	testBackendSendWithAddr(t, testAddr, handleFunc, testFunc, options...)
}

func testBackendSendWithAddr(t *testing.T, addr string,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {
	app := newTestApp(t, handleFunc)
	assert.NoError(t, app.Start())
	defer func() {
		assert.NoError(t, app.Stop())
	}()

	testBackendSendWithoutServer(t, addr, testFunc, options...)
}

func testBackendSendWithoutServer(t *testing.T, addr string,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {

	options = append(options,
		WithBackendBufferSize(1),
		WithBackendLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel).With(zap.String("testcase", t.Name()))))
	rb, err := NewRemoteBackend(addr, newTestCodec(), options...)
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
	opts = append(opts, goetty.WithAppSessionOptions(goetty.WithSessionCodec(codec)))
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
	b.activeTime = time.Now()
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

func (b *testBackend) Send(ctx context.Context, request Message) (*Future, error) {
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

func newTestCodec(options ...CodecOption) Codec {
	options = append(options, WithCodecPayloadCopyBufferSize(1024))
	return NewMessageCodec(func() Message { return messagePool.Get().(*testMessage) }, options...)
}

var (
	messagePool = sync.Pool{
		New: func() any {
			return newTestMessage(0)
		},
	}
)
