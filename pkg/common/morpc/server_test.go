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

		rs.RegisterRequestHandler(func(request Message, sequence uint64, cs ClientSession) error {
			return cs.Write(request, SendOptions{})
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req, SendOptions{})
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

		rs.RegisterRequestHandler(func(request Message, sequence uint64, cs ClientSession) error {
			return cs.Write(request, SendOptions{})
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		req := &testMessage{id: 1, payload: []byte("payload")}
		f, err := c.Send(ctx, testAddr, req, SendOptions{})
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
		c := newTestClient(t)
		rs.RegisterRequestHandler(func(request Message, _ uint64, cs ClientSession) error {
			assert.NoError(t, c.Close())
			wc <- struct{}{}
			return cs.Write(request, SendOptions{})
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req, SendOptions{})
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.Error(t, ctx.Err(), err)
		assert.Nil(t, resp)
	}, WithServerWriteFilter(func(_ Message) bool {
		<-wc
		return true
	}))
}

func TestStreamServer(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		wg := sync.WaitGroup{}
		wg.Add(1)
		n := 10
		rs.RegisterRequestHandler(func(request Message, _ uint64, cs ClientSession) error {
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					assert.NoError(t, cs.Write(request, SendOptions{}))
				}
			}()
			return nil
		})

		st, err := c.NewStream(testAddr)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close())
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(req, SendOptions{}))

		rc, err := st.Receive()
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			assert.Equal(t, req, <-rc)
		}

		wg.Wait()
	})
}

func BenchmarkSend(b *testing.B) {
	testRPCServer(b, func(rs *server) {
		c := newTestClient(b,
			WithClientMaxBackendPerHost(1),
			WithClientDisableCreateTask(),
			WithClientInitBackends([]string{testAddr}, []int{1}))
		defer func() {
			c.(*client).logger.Info("55")
			assert.NoError(b, c.Close())
			c.(*client).logger.Info("66")
		}()

		rs.RegisterRequestHandler(func(request Message, sequence uint64, cs ClientSession) error {
			return cs.Write(request, SendOptions{})
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		req := newTestMessage(1)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := c.Send(ctx, testAddr, req, SendOptions{})
			if err == nil {
				_, err := f.Get()
				if err != nil {
					assert.Equal(b, ctx.Err(), err)
				}
				f.Close()
			}
		}
	}, WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(i interface{}) {
		messagePool.Put(i)
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
	bf := NewGoettyBasedBackendFactory(newTestCodec(),
		WithBackendConnectWhenCreate())
	c, err := NewClient(bf, options...)
	assert.NoError(t, err)
	return c
}
