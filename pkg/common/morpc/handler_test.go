// Copyright 2022 Matrix Origin
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
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(
			addr string,
			c RPCClient,
			h MessageHandler[*testMethodBasedMessage, *testMethodBasedMessage]) {
			fn := func(
				ctx context.Context,
				req, resp *testMethodBasedMessage) error {
				resp.payload = []byte{byte(req.method)}
				return nil
			}
			h.RegisterHandleFunc(
				1,
				fn,
				false)
			h.RegisterHandleFunc(
				2,
				fn,
				false)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			for i := uint32(0); i <= 2; i++ {
				f, err := c.Send(ctx, addr, &testMethodBasedMessage{method: i}, WriteOptions{})
				require.NoError(t, err)
				defer f.Close()
				v, err := f.Get()
				require.NoError(t, err)
				resp := v.(*testMethodBasedMessage)
				assert.Equal(t, i, resp.method)
				if i == 0 {
					assert.Error(t, resp.UnwrapError())
				} else {
					assert.Equal(t, []byte{byte(i)}, resp.payload)
				}
			}
		},
	)
}

func TestRequestCanBeFilter(t *testing.T) {
	runRPCTests(
		t,
		func(
			addr string,
			c RPCClient,
			h MessageHandler[*testMethodBasedMessage, *testMethodBasedMessage]) {
			fn := func(
				ctx context.Context,
				req, resp *testMethodBasedMessage) error {
				resp.payload = []byte{byte(req.method)}
				return nil
			}
			h.RegisterHandleFunc(
				1,
				fn,
				false)
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()

			f, err := c.Send(ctx, addr, &testMethodBasedMessage{method: 1}, WriteOptions{})
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			require.Error(t, err)
		},
		WithHandleMessageFilter[*testMethodBasedMessage, *testMethodBasedMessage](func(tmbm *testMethodBasedMessage) bool {
			return false
		}),
	)
}

func runRPCTests(
	t *testing.T,
	fn func(string, RPCClient, MessageHandler[*testMethodBasedMessage, *testMethodBasedMessage]),
	opts ...HandlerOption[*testMethodBasedMessage, *testMethodBasedMessage]) {
	defer leaktest.AfterTest(t)()
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(testSockets[7:]))
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())

	s, err := NewMessageHandler(
		"test",
		testSockets,
		Config{},
		NewMessagePool(
			func() *testMethodBasedMessage { return &testMethodBasedMessage{} },
			func() *testMethodBasedMessage { return &testMethodBasedMessage{} }),
		opts...)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	require.NoError(t, s.Start())

	cfg := Config{}
	c, err := cfg.NewClient("ctlservice",
		getLogger().RawLogger(),
		func() Message { return &testMethodBasedMessage{} })
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	fn(testSockets, c, s)
}

type testMethodBasedMessage struct {
	testMessage
	method uint32
	err    []byte
}

func (m *testMethodBasedMessage) Reset() {
	*m = testMethodBasedMessage{}
}

func (m *testMethodBasedMessage) Method() uint32 {
	return m.method
}

func (m *testMethodBasedMessage) SetMethod(v uint32) {
	m.method = v
}

func (m *testMethodBasedMessage) WrapError(err error) {
	me := moerr.ConvertGoError(context.TODO(), err).(*moerr.Error)
	data, e := me.MarshalBinary()
	if e != nil {
		panic(e)
	}
	m.err = data
}

func (m *testMethodBasedMessage) UnwrapError() error {
	if len(m.err) == 0 {
		return nil
	}

	err := &moerr.Error{}
	if e := err.UnmarshalBinary(m.err); e != nil {
		panic(e)
	}
	return err
}

func (m *testMethodBasedMessage) Size() int {
	return 12 + len(m.err) + len(m.payload)
}

func (m *testMethodBasedMessage) MarshalTo(data []byte) (int, error) {
	buf.Uint64ToBytesTo(m.id, data)
	buf.Uint32ToBytesTo(m.method, data[8:])
	if len(m.err) > 0 {
		copy(data[12:], m.err)
	}
	return 12 + len(m.err), nil
}

func (m *testMethodBasedMessage) Unmarshal(data []byte) error {
	m.id = buf.Byte2Uint64(data)
	m.method = buf.Byte2Uint32(data[8:])
	if len(data) > 12 {
		err := data[12:]
		m.err = make([]byte, len(err))
		copy(m.err, err)
	}
	return nil
}
