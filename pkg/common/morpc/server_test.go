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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCreateServerWithOptions(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		assert.Equal(t, 100, rs.options.batchSendSize)
		assert.Equal(t, 200, rs.options.bufferSize)
	}, WithServerBatchSendSize(100),
		WithServerSessionBufferSize(200))
}

func TestHandleServer(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
		defer cancel()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req, 0)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, req, resp)
	})
}

func TestHandleServerWithPayloadMessage(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		req := &testMessage{id: 1, payload: []byte("payload")}
		f, err := c.Send(ctx, testAddr, req, 0)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, req, resp)
	})
}

func TestHandleServerWriteWithClosedSession(t *testing.T) {
	wc := make(chan struct{}, 1)
	defer close(wc)

	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		c := newTestClient(t)
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			assert.NoError(t, c.Close())
			err := cs.Write(ctx, request.Message, request.GetTimeout())
			assert.Error(t, err)
			return err
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req, 0)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.Error(t, ctx.Err(), err)
		assert.Nil(t, resp)
	})
}

func TestHandleServerWriteWithClosedClientSession(t *testing.T) {
	wc := make(chan struct{}, 1)
	defer close(wc)

	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		c := newTestClient(t)
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			assert.NoError(t, cs.Close())
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req, 0)
		assert.NoError(t, err)

		defer f.Close()
		_, err = f.Get()
		assert.Error(t, err)
		assert.Equal(t, backendClosed, err)
	})
}

func TestStreamServer(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		wg := sync.WaitGroup{}
		wg.Add(1)
		n := 10
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					assert.NoError(t, cs.Write(ctx, request.Message, request.GetTimeout()))
				}
			}()
			return nil
		})

		st, err := c.NewStream(testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req, 0))

		rc, err := st.Receive()
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			assert.Equal(t, req, <-rc)
		}

		wg.Wait()
	})
}

func TestStreamServerWithCache(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(ctx context.Context, msg RPCMessage, seq uint64, cs ClientSession) error {
			request := msg.Message
			if seq == 1 {
				cache, err := cs.CreateCache(ctx, request.GetID(), msg.GetTimeout())
				if err != nil {
					return err
				}
				m := newTestMessage(request.GetID())
				return cache.Add(m)
			} else {
				cache, err := cs.GetCache(request.GetID())
				if err != nil {
					return err
				}
				m, _, err := cache.Pop()
				if err != nil {
					return err
				}
				if err := cs.Write(ctx, m, msg.GetTimeout()); err != nil {
					return err
				}
				if err := cs.Write(ctx, request, msg.GetTimeout()); err != nil {
					return err
				}
			}
			return nil
		})

		st, err := c.NewStream(testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		req1 := newTestMessage(st.ID())
		req1.payload = []byte{1}
		assert.NoError(t, st.Send(ctx, req1, 0))

		req2 := newTestMessage(st.ID())
		req2.payload = []byte{2}
		assert.NoError(t, st.Send(ctx, req2, 0))

		cc, err := st.Receive()
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			select {
			case <-ctx.Done():
				assert.Fail(t, "message failed")
			case <-cc:
			}
		}
	})
}

func TestServerTimeoutCacheWillRemoved(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		cc := make(chan struct{})
		rs.RegisterRequestHandler(func(ctx context.Context, msg RPCMessage, seq uint64, cs ClientSession) error {
			request := msg.Message
			cache, err := cs.CreateCache(ctx, request.GetID(), msg.GetTimeout())
			if err != nil {
				return err
			}
			close(cc)
			return cache.Add(request)
		})

		st, err := c.NewStream(testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		assert.NoError(t, st.Send(ctx, newTestMessage(1), 0))
		<-cc
		v, ok := rs.sessions.Load(uint64(1))
		if ok {
			cs := v.(*clientSession)
			for {
				cs.mu.RLock()
				if len(cs.mu.caches) == 0 {
					cs.mu.RUnlock()
					return
				}
				cs.mu.RUnlock()
			}
		}
	})
}

func TestStreamServerWithSequenceNotMatch(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		v, err := c.NewStream(testAddr, false)
		assert.NoError(t, err)
		st := v.(*stream)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		st.sequence = 2
		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req, 0))

		rc, err := st.Receive()
		assert.NoError(t, err)
		assert.NotNil(t, rc)
		resp := <-rc
		assert.Nil(t, resp)
	})
}

func TestStreamReadCannotBlockWrite(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		st, err := c.NewStream(testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		ch, err := st.Receive()
		require.NoError(t, err)

		cc := make(chan struct{})
		n := 1000
		go func() {
			defer close(cc)
			i := 0
			for {
				<-ch
				i++
				if i == n {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		for i := 0; i < n; i++ {
			require.NoError(t, st.Send(ctx, newTestMessage(st.ID()), 0))
		}
		<-cc
	})
}

func TestCannotGetClosedBackend(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t, WithClientMaxBackendPerHost(2))
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		st, err := c.NewStream(testAddr, true)
		require.NoError(t, err)
		require.NoError(t, st.Close(true))

		require.NoError(t, c.Ping(ctx, testAddr, 0))
	})
}

func TestPingError(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t, WithClientMaxBackendPerHost(2))
		defer func() {
			assert.NoError(t, c.Close())
		}()
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(context.Background(), request.Message, request.GetTimeout())
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		cancel()
		require.Error(t, c.Ping(ctx, testAddr, 0))
	})
}

func BenchmarkSend(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	testRPCServer(b, func(rs *server) {
		c := newTestClient(b,
			WithClientMaxBackendPerHost(1),
			WithClientInitBackends([]string{testAddr}, []int{1}))
		defer func() {
			assert.NoError(b, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message, request.GetTimeout())
		})

		req := newTestMessage(1)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := c.Send(ctx, testAddr, req, 0)
			if err == nil {
				_, err := f.Get()
				if err != nil {
					assert.Equal(b, ctx.Err(), err)
				}
				f.Close()
			}
		}
	}, WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(i interface{}) {
		msg := i.(RPCMessage)
		if !msg.InternalMessage() {
			messagePool.Put(msg.Message)
		}
	})))
}

func testRPCServer(t assert.TestingT, testFunc func(*server), options ...ServerOption) {
	assert.NoError(t, os.RemoveAll(testUnixFile))

	options = append(options,
		WithServerLogger(logutil.GetPanicLoggerWithLevel(zap.InfoLevel)))
	s, err := NewRPCServer("test", testAddr, newTestCodec(), options...)
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	testFunc(s.(*server))
}

func newTestClient(t assert.TestingT, options ...ClientOption) RPCClient {
	bf := NewGoettyBasedBackendFactory(newTestCodec())
	c, err := NewClient(
		"",
		bf,
		options...)
	assert.NoError(t, err)
	return c
}

func TestPing(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		assert.NoError(t, c.Ping(ctx, testAddr, 0))
	})
}
