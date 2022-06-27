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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestHandleServer(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(request Message, sequence uint64, cs ClientSession) error {
			return cs.Write(request)
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		req := &testMessage{id: []byte{1}}
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
			return cs.Write(request)
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		req := &testMessage{id: []byte{1}, payload: []byte("payload")}
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
		rs.RegisterRequestHandler(func(request Message, sequence uint64, cs ClientSession) error {
			assert.NoError(t, c.Close())
			wc <- struct{}{}
			return cs.Write(request)
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := &testMessage{id: []byte{1}}
		f, err := c.Send(ctx, testAddr, req, SendOptions{})
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.Error(t, ctx.Err(), err)
		assert.Nil(t, resp)
	}, WithServerWriteFilter(func(m []Message) []Message {
		<-wc
		return m
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
		rs.RegisterRequestHandler(func(request Message, sequence uint64, cs ClientSession) error {
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					assert.NoError(t, cs.Write(request))
				}
			}()
			return nil
		})

		st, err := c.NewStream(testAddr, 1)
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

func testRPCServer(t *testing.T, testFunc func(*server), options ...ServerOption) {
	assert.NoError(t, os.RemoveAll(testUnixFile))

	options = append(options,
		WithServerLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel)))
	s, err := NewRPCServer("test", testAddr, newTestCodec(), options...)
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	testFunc(s.(*server))
}

func newTestClient(t *testing.T) RPCClient {
	bf := NewGoettyBasedBackendFactory(newTestCodec(), WithBackendConnectWhenCreate())
	c, err := NewClient(bf)
	assert.NoError(t, err)
	return c
}
