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
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMethodBasedClientSession struct {
	write func(context.Context, Message) error
}

type testMethodBasedRPCServer struct {
	RPCServer
	closeStarted chan struct{}
}

func (s *testMethodBasedRPCServer) Close() error {
	close(s.closeStarted)
	return s.RPCServer.Close()
}

func (s *testMethodBasedClientSession) Close() error {
	return nil
}

func (s *testMethodBasedClientSession) SessionCtx() context.Context {
	return context.Background()
}

func (s *testMethodBasedClientSession) Write(ctx context.Context, message Message) error {
	return s.write(ctx, message)
}

func (s *testMethodBasedClientSession) AsyncWrite(Message) error {
	panic("not implemented")
}

func (s *testMethodBasedClientSession) CreateCache(context.Context, uint64) (MessageCache, error) {
	panic("not implemented")
}
func (s *testMethodBasedClientSession) CreateCacheWithCancel(context.Context, uint64, context.CancelFunc) (MessageCache, error) {
	return nil, nil
}

func (s *testMethodBasedClientSession) DeleteCache(uint64) {
	panic("not implemented")
}

func (s *testMethodBasedClientSession) GetCache(uint64) (MessageCache, error) {
	panic("not implemented")
}

func (s *testMethodBasedClientSession) RemoteAddress() string {
	return ""
}

func TestMethodBasedServerCancelsRejectedRequest(t *testing.T) {
	writeErr := io.ErrClosedPipe
	for _, tc := range []struct {
		name     string
		writeErr error
	}{
		{name: "write succeeds"},
		{name: "write fails", writeErr: writeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := NewMessagePool(
				func() *testMethodBasedMessage { return &testMethodBasedMessage{} },
				func() *testMethodBasedMessage { return &testMethodBasedMessage{} },
			)
			s := &methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]{
				logger:   getLogger(""),
				pool:     pool,
				handlers: make(map[uint32]handleFuncCtx[*testMethodBasedMessage, *testMethodBasedMessage]),
			}

			cancelCalls := 0
			writeCalls := 0
			cs := &testMethodBasedClientSession{
				write: func(_ context.Context, message Message) error {
					writeCalls++
					require.Zero(t, cancelCalls)
					require.True(t, moerr.IsMoErrCode(
						message.(*testMethodBasedMessage).UnwrapError(),
						moerr.ErrNotSupported,
					))
					return tc.writeErr
				},
			}
			request := RPCMessage{
				Message: &testMethodBasedMessage{method: 100},
				Cancel: func() {
					cancelCalls++
				},
			}

			err := s.onMessage(t.Context(), request, 0, cs)
			if tc.writeErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.writeErr)
			}
			require.Equal(t, 1, writeCalls)
			require.Equal(t, 1, cancelCalls)
		})
	}
}

// newTestMethodServer owns binding/teardown; each test owns any admitted work.
func newTestMethodServer(t testing.TB, cfg Config) (*methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage], string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "method-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	pool := NewMessagePool(func() *testMethodBasedMessage { return &testMethodBasedMessage{} }, func() *testMethodBasedMessage { return &testMethodBasedMessage{} })
	addr := "unix://" + dir + "/s.sock"
	owner, err := NewMessageHandler("", "method-test", addr, cfg, pool)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	return owner.(*methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]), addr
}

func TestMethodBasedServerCloseJoinsHandlers(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		async, inline, mixed bool
	}{
		{name: "async", async: true}, {name: "mixed async", async: true, mixed: true},
		{name: "inline", async: true, inline: true}, {name: "mixed inline", async: true, inline: true, mixed: true}, {name: "sync"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, addr := newTestMethodServer(t, Config{})
			pool := s.pool
			rpc := &testMethodBasedRPCServer{RPCServer: s.rpc, closeStarted: make(chan struct{})}
			s.rpc = rpc
			started, atExit, allowExit := make(chan struct{}), make(chan struct{}), make(chan struct{})
			closed, secondClosed := make(chan struct{}), make(chan struct{})
			var handlerErr, closeErr, secondErr error
			var observedCtx context.Context
			var calls atomic.Int32
			finish := sync.OnceFunc(func() { close(allowExit) })
			closeServer := sync.OnceFunc(func() { go func() { closeErr = s.Close(); close(closed) }() })
			wait := func(event <-chan struct{}) {
				t.Helper()
				select {
				case <-event:
				case <-time.After(time.Second):
					t.Fatal("method server phase notification missing")
				}
			}
			t.Cleanup(func() {
				// Independent release also works if codec parenting or cancellation regresses.
				finish()
				closeServer()
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Error("method server failed to close")
				}
				joined := make(chan struct{})
				go func() { s.asyncWG.Wait(); close(joined) }()
				select {
				case <-joined:
				case <-time.After(time.Second):
					t.Error("test-owned async handler failed to finish")
				}
			})
			s.RegisterMethod(1, func(ctx context.Context, _ *testMethodBasedMessage, _ *testMethodBasedMessage, _ *Buffer) error {
				calls.Add(1)
				observedCtx = ctx
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) < 30*time.Minute {
					return fmt.Errorf("decoded hour deadline missing")
				}
				close(started)
				select {
				case <-ctx.Done():
					handlerErr = ctx.Err()
				case <-allowExit:
					return nil
				}
				close(atExit)
				<-allowExit
				return handlerErr
			}, tc.async)
			if tc.mixed {
				s.RegisterMethod(2, func(context.Context, *testMethodBasedMessage, *testMethodBasedMessage, *Buffer) error { return nil }, false)
			}
			if tc.inline {
				// Reject submission without closing or rebooting the shared pool.
				closedPool, err := ants.NewPool(1, ants.WithDisablePurge(true))
				require.NoError(t, err)
				t.Cleanup(closedPool.Release)
				require.NoError(t, closedPool.ReleaseTimeout(time.Second))
				require.ErrorIs(t, closedPool.Submit(func() {}), ants.ErrPoolClosed)
				s.rpc.RegisterRequestHandler(func(ctx context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
					return s.onMessageWithSubmit(ctx, request, sequence, cs, closedPool.Submit)
				})
			}
			require.NoError(t, s.Start())
			client, err := (Config{ClientOptions: []ClientOption{WithClientEnableAutoCreateBackend()}}).NewClient("", "close-test", func() Message { return &testMethodBasedMessage{} })
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			t.Cleanup(cancel)
			future, err := client.Send(ctx, addr, &testMethodBasedMessage{method: 1})
			require.NoError(t, err)
			t.Cleanup(future.Close)
			wait(started)
			closeServer()
			wait(rpc.closeStarted)
			if tc.async {
				wait(atExit)
				require.ErrorIs(t, handlerErr, context.Canceled)
			} else {
				require.NoError(t, observedCtx.Err(), "synchronous request remains independent of server cancellation")
			}
			go func() { secondErr = s.Close(); close(secondClosed) }()
			select {
			case <-closed:
				t.Fatal("Close returned before handler finished")
			case <-time.After(10 * time.Millisecond):
			}
			select {
			case <-secondClosed:
				t.Fatal("concurrent Close bypassed drain")
			default:
			}
			if tc.async {
				// Exercise the sealed admission gate separately from transport rejection.
				lateCtx, lateCancel := context.WithCancel(context.Background())
				defer lateCancel()
				late := &testMethodBasedMessage{method: 1}
				cs := &testMethodBasedClientSession{write: func(_ context.Context, resp Message) error {
					pool.ReleaseResponse(resp.(*testMethodBasedMessage))
					return nil
				}}
				require.NoError(t, s.onMessage(lateCtx, RPCMessage{Ctx: lateCtx, Message: late, Cancel: lateCancel}, 0, cs))
				require.ErrorIs(t, lateCtx.Err(), context.Canceled)
				require.Zero(t, late.method)
			}
			finish()
			wait(closed)
			wait(secondClosed)
			require.NoError(t, closeErr)
			require.NoError(t, secondErr)
			require.NoError(t, s.Close())
			require.Equal(t, int32(1), calls.Load())
			_, err = future.Get()
			require.Error(t, err)
		})
	}
}

func TestMethodBasedServerLifecycleIsolation(t *testing.T) {
	backing := make([]CodecOption, 2)
	marker := false
	backing[1] = func(*messageCodec) { marker = true }
	cfg := Config{CodecOptions: backing[:1]}
	cfg.CodecOptions[0] = WithCodecMaxBodySize(1024)
	first, _ := newTestMethodServer(t, cfg)
	second, _ := newTestMethodServer(t, cfg)
	pool := first.pool
	backing[1](newTestCodec().(*messageCodec))
	require.True(t, marker, "constructor overwrote caller's spare codec option")
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	first.RegisterMethod(1, func(ctx context.Context, _ *testMethodBasedMessage, _ *testMethodBasedMessage, _ *Buffer) error {
		return ctx.Err()
	}, false)
	require.NoError(t, first.Close())
	require.NoError(t, second.requestCtx.Err())
	response := first.Handle(caller, &testMethodBasedMessage{method: 1}, nil)
	require.NoError(t, response.UnwrapError())
	pool.ReleaseResponse(response)
	require.NoError(t, caller.Err(), "direct Handle context belongs to its caller")
	// The root must also retire when codec construction unwinds without a return.
	var partial *methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]
	cfg.CodecOptions = []CodecOption{func(*messageCodec) { panic("codec construction failed") }}
	require.PanicsWithValue(t, "codec construction failed", func() {
		_, _ = NewMessageHandler("", "failure", "unix:///tmp/unused-method-failure.sock", cfg, pool,
			func(s *methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]) { partial = s })
	})
	require.ErrorIs(t, partial.requestCtx.Err(), context.Canceled)
}

func TestMethodBasedServerAsyncContextProvenance(t *testing.T) {
	for _, name := range []string{"native", "mixed", "unmarked", "detached", "cross owner", "cutover"} {
		t.Run(name, func(t *testing.T) {
			s, _ := newTestMethodServer(t, Config{})
			pool := s.pool
			var foreign *methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]
			decode := func(source *methodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]) RPCMessage {
				t.Helper()
				codec := source.rpc.(*server).codec
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				defer cancel()
				out := buf.NewByteBuf(128)
				defer out.Close()
				require.NoError(t, codec.Encode(RPCMessage{Ctx: ctx, Message: &testMethodBasedMessage{method: 1}}, out, nil))
				v, ok, err := codec.Decode(out)
				require.NoError(t, err)
				require.True(t, ok)
				return v.(RPCMessage)
			}
			started, done, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var closeErr error
			closeServer := sync.OnceFunc(func() { go func() { closeErr = s.Close(); close(closed) }() })
			var request RPCMessage
			t.Cleanup(func() {
				if request.Cancel != nil {
					request.Cancel()
				}
				closeServer()
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Error("provenance cleanup failed")
				}
			})
			s.RegisterMethod(1, func(ctx context.Context, _ *testMethodBasedMessage, _ *testMethodBasedMessage, _ *Buffer) error {
				close(started)
				<-ctx.Done()
				close(done)
				return ctx.Err()
			}, true)
			if name == "mixed" {
				s.RegisterMethod(2, func(context.Context, *testMethodBasedMessage, *testMethodBasedMessage, *Buffer) error { return nil }, false)
			}
			source := s
			if name == "cross owner" {
				foreign, _ = newTestMethodServer(t, Config{})
				source = foreign
			}
			request = decode(source)
			if foreign != nil {
				received := request.Message.(*testMethodBasedMessage)
				target := pool.AcquireRequest()
				*target = *received
				foreign.pool.ReleaseRequest(received)
				request.Message = target
			}
			if name == "mixed" {
				require.Nil(t, request.nativeContextDone)
			} else {
				require.Equal(t, request.Ctx.Done(), request.nativeContextDone)
			}
			if name == "detached" || name == "unmarked" {
				original := request.Ctx
				request.Cancel() // retire the old native timeout before replacing it
				parent := context.Background()
				if name == "detached" {
					parent = context.WithoutCancel(original)
				}
				request.Ctx, request.Cancel = context.WithTimeout(parent, time.Hour)
			}
			if name == "cutover" {
				s.RegisterMethod(2, func(context.Context, *testMethodBasedMessage, *testMethodBasedMessage, *Buffer) error { return nil }, false)
				later := decode(s)
				require.Nil(t, later.nativeContextDone)
				later.Cancel()
				pool.ReleaseRequest(later.Message.(*testMethodBasedMessage))
				s.RegisterMethod(2, func(context.Context, *testMethodBasedMessage, *testMethodBasedMessage, *Buffer) error { return nil }, true)
				later = decode(s)
				require.Nil(t, later.nativeContextDone, "capability must not reset on replacement")
				later.Cancel()
				pool.ReleaseRequest(later.Message.(*testMethodBasedMessage))
			}
			cs := &testMethodBasedClientSession{write: func(_ context.Context, resp Message) error {
				pool.ReleaseResponse(resp.(*testMethodBasedMessage))
				return nil
			}}
			require.NoError(t, s.onMessage(request.Ctx, request, 0, cs))
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("handler did not start")
			}
			closeServer()
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("Close did not cancel the effective request context")
			}
			require.NoError(t, closeErr)
			select {
			case <-done:
			default:
				t.Fatal("Close did not join the borrower")
			}
			require.ErrorIs(t, request.Ctx.Err(), context.Canceled)
			if foreign != nil {
				require.NoError(t, foreign.requestCtx.Err(), "other owner's root must remain live")
			}
		})
	}
}

func TestRPCSend(t *testing.T) {
	runRPCTests(
		t,
		func(
			addr string,
			c RPCClient,
			h MethodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]) {
			fn := func(
				ctx context.Context,
				req, resp *testMethodBasedMessage,
				buf *Buffer,
			) error {
				resp.payload = []byte{byte(req.method)}
				return nil
			}
			h.RegisterMethod(
				1,
				fn,
				false)
			h.RegisterMethod(
				2,
				fn,
				false)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			for i := uint32(0); i <= 2; i++ {
				f, err := c.Send(ctx, addr, &testMethodBasedMessage{method: i})
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
	filtered := make(chan struct{}, 1)
	runRPCTests(
		t,
		func(
			addr string,
			c RPCClient,
			h MethodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]) {
			fn := func(
				ctx context.Context,
				req, resp *testMethodBasedMessage,
				buf *Buffer,
			) error {
				resp.payload = []byte{byte(req.method)}
				return nil
			}
			h.RegisterMethod(
				1,
				fn,
				false)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
			defer cancel()

			f, err := c.Send(ctx, addr, &testMethodBasedMessage{method: 1})
			require.NoError(t, err)
			defer f.Close()
			select {
			case <-filtered:
				cancel()
			case <-ctx.Done():
				require.FailNow(t, "request was not filtered", ctx.Err())
			}
			_, err = f.Get()
			require.Error(t, err)
		},
		WithHandleMessageFilter[*testMethodBasedMessage, *testMethodBasedMessage](func(tmbm *testMethodBasedMessage) bool {
			select {
			case filtered <- struct{}{}:
			default:
			}
			return false
		}),
	)
}

func runRPCTests(
	t *testing.T,
	fn func(string, RPCClient, MethodBasedServer[*testMethodBasedMessage, *testMethodBasedMessage]),
	opts ...HandlerOption[*testMethodBasedMessage, *testMethodBasedMessage]) {
	defer leaktest.AfterTest(t)()

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
			assert.NoError(t, os.RemoveAll(testSockets[7:]))

			s, err := NewMessageHandler(
				sid,
				"test",
				testSockets,
				Config{},
				NewMessagePool(
					func() *testMethodBasedMessage { return &testMethodBasedMessage{} },
					func() *testMethodBasedMessage { return &testMethodBasedMessage{} }),
				opts...,
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, s.Close())
			}()
			require.NoError(t, s.Start())

			cfg := Config{
				ClientOptions: []ClientOption{WithClientEnableAutoCreateBackend()},
			}
			c, err := cfg.NewClient(
				sid,
				"ctl-service",
				func() Message { return &testMethodBasedMessage{} },
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()

			fn(testSockets, c, s)
		},
	)
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

func (m *testMethodBasedMessage) ProtoSize() int {
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
