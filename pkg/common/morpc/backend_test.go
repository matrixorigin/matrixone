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
	)
}

func TestSendWithSomeErrorInBatch(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*100000)
			defer cancel()

			n := 100
			largePayload := make([]byte, defaultMaxMessageSize)
			futures := make([]*Future, 0, n)
			requests := make([]Message, 0, n)
			for i := 0; i < n; i++ {
				req := newTestMessage(uint64(i))
				if i%2 == 0 {
					req.SetPayloadField(largePayload)
				}
				f, err := b.Send(ctx, req)
				assert.NoError(t, err)
				defer f.Close()
				futures = append(futures, f)
				requests = append(requests, req)
			}

			for i, f := range futures {
				resp, err := f.Get()
				if i%2 == 0 {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, requests[i], resp)
				}
			}
		},
	)
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
	)
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
			// keep future in the futures map
			f.ref()
			defer f.unRef()
			f.Close()
			b.mu.RLock()
			_, ok := b.mu.futures[id]
			assert.True(t, ok)
			b.mu.RUnlock()
			wg.Done()
			time.Sleep(time.Second)
		},
		WithBackendHasPayloadResponse())
}

func TestSendWithPayloadCannotBlockIfFutureClosed(t *testing.T) {
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
			f.mu.Lock()
			f.mu.closed = true
			f.releaseFunc = nil // make it nil to keep this future in b.mu.features
			f.mu.Unlock()
			b.mu.RLock()
			_, ok := b.mu.futures[id]
			b.mu.RUnlock()
			assert.True(t, ok)
			wg.Done()
			time.Sleep(time.Second)
		},
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
	)
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
		WithBackendFilter(func(Message, string) bool {
			cancel()
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
	)
}

func TestSendWithCannotConnect(t *testing.T) {
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
		},
		WithBackendFilter(func(Message, string) bool {
			assert.NoError(t, rb.conn.Disconnect())
			rb.remote = ""
			return true
		}),
		WithBackendConnectTimeout(time.Millisecond*200),
	)
}

func TestStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			st, err := b.NewStream(false)
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
	)
}

func TestStreamSendWillPanicIfDeadlineNotSet(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			st, err := b.NewStream(false)
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
	)
}

func TestStreamClosedByConnReset(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Disconnect()
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			st, err := b.NewStream(false)
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
	)
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
	s.init(1, false)
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
	s.init(1, false)
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
	)
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
	)
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

			st, err := b.NewStream(false)
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
	)
}

func TestBackendConnectTimeout(t *testing.T) {
	rb, err := NewRemoteBackend(testAddr, newTestCodec(),
		WithBackendConnectTimeout(time.Millisecond*200),
	)
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
	)
}

func TestLockedStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			assert.False(t, b.Locked())
			b.Lock()
			st, err := b.NewStream(true)
			assert.NoError(t, err)
			assert.True(t, b.Locked())
			assert.NoError(t, st.Close())
			assert.False(t, b.Locked())
		},
	)
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
	locked     bool
}

func (b *testBackend) Send(ctx context.Context, request Message) (*Future, error) {
	b.active()
	f := newFuture(nil)
	f.init(request.GetID(), ctx)
	return f, nil
}

func (b *testBackend) NewStream(unlockAfterClose bool) (Stream, error) {
	b.active()
	st := newStream(make(chan Message, 1),
		func(m backendSendMessage) error { return nil },
		func(s *stream) {
			if s.unlockAfterClose {
				b.Unlock()
			}
		},
		b.active)
	st.init(1, false)
	return st, nil
}

func (b *testBackend) Close() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	b.closed = true
}
func (b *testBackend) Busy() bool { return b.busy }
func (b *testBackend) LastActiveTime() time.Time {
	b.RLock()
	defer b.RUnlock()
	return b.activeTime
}

func (b *testBackend) Lock() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	if b.locked {
		panic("backend is already locked")
	}
	b.locked = true
}

func (b *testBackend) Unlock() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	if !b.locked {
		panic("backend is not locked")
	}
	b.locked = false
}

func (b *testBackend) Locked() bool {
	b.RLock()
	defer b.RUnlock()
	return b.locked
}

func (b *testBackend) active() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
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
	return fmt.Sprintf("%d:%d", tm.id, len(tm.payload))
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
	options = append(options,
		WithCodecPayloadCopyBufferSize(1024))
	return NewMessageCodec(func() Message { return messagePool.Get().(*testMessage) }, options...)
}

var (
	messagePool = sync.Pool{
		New: func() any {
			return newTestMessage(0)
		},
	}
)
