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

var (
	testAddr     = "unix:///tmp/goetty.sock"
	testUnixFile = "/tmp/goetty.sock"
)

func TestSend(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := &testMessage{id: []byte("id1")}
			f := acquireFuture(ctx, req, SendOptions{})
			defer f.Close()
			assert.NoError(t, b.Send(f))

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
		WithBackendConnectWhenCreate())
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
			req := &testMessage{id: []byte("id1")}
			f := acquireFuture(ctx, req, SendOptions{})
			defer f.Close()
			assert.NoError(t, b.Send(f))

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
			assert.True(t, retry > 0)
		},
		WithBackendFilter(func(fs []*Future) []*Future {
			retry++
			return fs
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
			req := &testMessage{id: []byte("id1")}
			f := acquireFuture(ctx, req, SendOptions{})
			defer f.Close()
			assert.NoError(t, b.Send(f))

			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, err, ctx.Err())
		},
		WithBackendConnectWhenCreate())
}

func TestBusy(t *testing.T) {
	c := make(chan struct{})
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			assert.False(t, b.Busy())

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
			defer cancel()
			req := &testMessage{id: []byte("id1")}
			f1 := acquireFuture(ctx, req, SendOptions{})
			defer f1.Close()
			assert.NoError(t, b.Send(f1))

			f2 := acquireFuture(ctx, req, SendOptions{})
			defer f2.Close()
			assert.NoError(t, b.Send(f2))

			assert.True(t, b.Busy())
			c <- struct{}{}
		},
		WithBackendConnectWhenCreate(),
		WithBackendFilter(func(fs []*Future) []*Future {
			<-c
			return nil
		}),
		WithBackendBatchSendSize(1),
		WithBackendBufferSize(10),
		WithBackendBusyBufferSize(1))
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
	id   int
	busy bool
}

func (b *testBackend) Send(*Future) error { return nil }
func (b *testBackend) Close()             {}
func (b *testBackend) Busy() bool         { return b.busy }

type testMessage struct {
	id      []byte
	payload []byte
}

func newTestMessage(id []byte) *testMessage {
	return &testMessage{id: id}
}

func (tm *testMessage) ID() []byte {
	return tm.id
}

func (tm *testMessage) DebugString() string {
	return string(tm.id)
}

func (tm *testMessage) Size() int {
	return len(tm.id)
}

func (tm *testMessage) MarshalTo(data []byte) (int, error) {
	return copy(data, tm.id), nil
}

func (tm *testMessage) Unmarshal(data []byte) error {
	tm.id = make([]byte, len(data))
	copy(tm.id, data)
	return nil
}

func (tm *testMessage) GetPayloadField() []byte {
	return tm.payload
}

func (tm *testMessage) SetPayloadField(data []byte) {
	tm.payload = data
}

func newTestCodec() Codec {
	return NewMessageCodec(func() Message { return &testMessage{} }, 1024)
}
