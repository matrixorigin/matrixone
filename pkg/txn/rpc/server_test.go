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

package rpc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHandleMessageWithSender(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			return nil
		})

		cli, err := NewSender(Config{EnableCompress: true}, runtime.ServiceRuntime(""))
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, cli.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		v, err := cli.Send(ctx, []txn.TxnRequest{{CNRequest: &txn.CNOpRequest{Target: metadata.TNShard{Address: testTN1Addr}}}})
		require.NoError(t, err)
		assert.Equal(t, 1, len(v.Responses))
	}, WithServerEnableCompress(true))
}

func TestHandleMessageEnableCompressWithSender(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			return nil
		})

		cli, err := NewSender(Config{}, newTestRuntime(newTestClock(), s.rt.Logger().RawLogger()))
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, cli.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		v, err := cli.Send(ctx, []txn.TxnRequest{{CNRequest: &txn.CNOpRequest{Target: metadata.TNShard{Address: testTN1Addr}}}})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(v.Responses))
	})
}

func TestHandleLargeMessageWithSender(t *testing.T) {
	size := 1024 * 1024 * 15
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			tr2.CNOpResponse = &txn.CNOpResponse{Payload: make([]byte, size)}
			return nil
		})

		cli, err := NewSender(Config{MaxMessageSize: toml.ByteSize(size + 1024)},
			newTestRuntime(newTestClock(), s.rt.Logger().RawLogger()))
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, cli.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		v, err := cli.Send(ctx, []txn.TxnRequest{{
			CNRequest: &txn.CNOpRequest{
				Target:  metadata.TNShard{Address: testTN1Addr},
				Payload: make([]byte, size),
			},
		}})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(v.Responses))
	}, WithServerMaxMessageSize(size+1024))
}

func TestHandleMessage(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			return nil
		})

		c := make(chan morpc.Message, 1)
		defer close(c)
		cs := newTestClientSession(c)

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		assert.NoError(t, s.onMessage(ctx, newMessage(&txn.TxnRequest{RequestID: 1}), 0, cs))
		v := <-c
		assert.Equal(t, uint64(1), v.GetID())
	})
}

func TestHandleMessageWithFilter(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		n := 0
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(_ context.Context, _ *txn.TxnRequest, _ *txn.TxnResponse) error {
			n++
			return nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		assert.NoError(t, s.onMessage(ctx, newMessage(&txn.TxnRequest{RequestID: 1}),
			0, nil))
		assert.Equal(t, 0, n)
	}, WithServerMessageFilter(func(tr *txn.TxnRequest) bool {
		return false
	}))
}

func TestHandleInvalidMessageWillPanic(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		assert.NoError(t, s.onMessage(ctx, newMessage(&txn.TxnResponse{}), 0, nil))
	})
}

func TestHandleNotRegisterWillReturnError(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		assert.Error(t, s.onMessage(context.Background(), newMessage(&txn.TxnRequest{}), 0, nil))
	})
}

func TestTimeoutRequestCannotHandled(t *testing.T) {
	runTestTxnServer(t, testTN1Addr, func(s *server) {
		n := 0
		s.RegisterMethodHandler(txn.TxnMethod_Read,
			func(_ context.Context, _ *txn.TxnRequest, _ *txn.TxnResponse) error {
				n++
				return nil
			})

		ctx, cancel := context.WithTimeout(context.Background(), 1)
		cancel()
		req := &txn.TxnRequest{Method: txn.TxnMethod_Read}
		assert.NoError(t, s.onMessage(ctx, newMessage(req), 0, nil))
		assert.Equal(t, 0, n)
	})
}

func runTestTxnServer(t *testing.T, addr string, testFunc func(s *server), opts ...ServerOption) {
	rt := newTestRuntime(newTestClock(), logutil.GetPanicLogger())
	runtime.SetupServiceBasedRuntime("", rt)

	assert.NoError(t, os.RemoveAll(addr[7:]))
	opts = append(opts,
		WithServerQueueBufferSize(100),
		WithServerQueueWorkers(2))
	s, err := NewTxnServer(addr,
		runtime.ServiceRuntime(""),
		opts...)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	assert.NoError(t, s.Start())

	testFunc(s.(*server))
}

type testClientSession struct {
	c chan morpc.Message
}

func newTestClientSession(c chan morpc.Message) *testClientSession {
	return &testClientSession{
		c: c,
	}
}

func (cs *testClientSession) RemoteAddress() string {
	return ""
}

func (cs *testClientSession) Close() error {
	return nil
}

func (cs *testClientSession) SessionCtx() context.Context {
	return nil
}

func (cs *testClientSession) Write(ctx context.Context, response morpc.Message) error {
	cs.c <- response
	return nil
}

func (cs *testClientSession) AsyncWrite(response morpc.Message) error {
	return nil
}

func (cs *testClientSession) CreateCache(
	ctx context.Context,
	cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

func (cs *testClientSession) DeleteCache(cacheID uint64) {
	panic("not implement")
}

func (cs *testClientSession) GetCache(cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

func newTestClock() clock.Clock {
	return clock.NewHLCClock(func() int64 { return 0 }, 0)
}

func newTestRuntime(clock clock.Clock, logger *zap.Logger) runtime.Runtime {
	return runtime.NewRuntime(metadata.ServiceType_CN, "", logutil.Adjust(logger), runtime.WithClock(clock))
}

func newMessage(req morpc.Message) morpc.RPCMessage {
	ctx, cancel := context.WithCancel(context.Background())
	return morpc.RPCMessage{
		Ctx:     ctx,
		Cancel:  cancel,
		Message: req,
	}
}

func TestTxnStateSwitch(t *testing.T) {
	runTestTxnServer(t, testTN5Addr, func(s *server) {
		curr, waitReady, handler := s.getTxnHandleState()
		require.Equal(t, TxnLocalHandle, curr)
		require.Nil(t, waitReady)
		require.Nil(t, handler)

		err := s.SwitchTxnHandleStateTo(TxnForwarding)
		require.NotNil(t, err)

		err = s.SwitchTxnHandleStateTo(TxnForwardWait)
		require.NotNil(t, err)

		err = s.SwitchTxnHandleStateTo(TxnForwardWait,
			WithForwardTarget(metadata.TNShard{
				Address: "test",
			}))
		require.NoError(t, err)

		curr, waitReady, handler = s.getTxnHandleState()
		require.Equal(t, TxnForwardWait, curr)
		require.NotNil(t, waitReady)
		require.NotNil(t, handler)

		err = s.SwitchTxnHandleStateTo(TxnForwarding)
		require.NoError(t, err)

		curr, waitReady, handler = s.getTxnHandleState()
		require.Equal(t, TxnForwarding, curr)
		require.Nil(t, waitReady)
		require.NotNil(t, handler)

		err = handler(context.Background(), nil, nil)
		require.NotNil(t, err)

		err = handler(context.Background(), &txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{},
		}, nil)
		require.NotNil(t, err)

		err = handler(context.Background(), &txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{},
		}, &txn.TxnResponse{})

		require.NotNil(t, err)

		fmt.Println(s.logTxnHandleState())
	})
}

func TestTNCanForwardingTxnWriteRequestsSimpleVersion(t *testing.T) {
	size := 10
	runTestTxnServer(t, testTN4Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read,
			func(ctx context.Context, req *txn.TxnRequest, resp *txn.TxnResponse) error {
				msg := "normal"
				resp.CNOpResponse = &txn.CNOpResponse{Payload: []byte(msg)}
				return nil
			})

		cli, err := NewSender(Config{MaxMessageSize: toml.ByteSize(size + 1024)},
			newTestRuntime(newTestClock(), s.rt.Logger().RawLogger()))
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, cli.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		var cnt = 1000
		var wg sync.WaitGroup
		var resps []*txn.CNOpResponse

		wg.Add(2)

		go func() {
			tickerA := time.NewTicker(time.Millisecond * 1)

			defer func() {
				fmt.Println("sender done")
				wg.Done()
			}()

			send := 0

			for {
				select {
				case <-ctx.Done():
					return

				case <-tickerA.C:
					if send >= cnt {
						return
					}

					send++
					resp, err := cli.Send(ctx, []txn.TxnRequest{{
						CNRequest: &txn.CNOpRequest{
							Target:  metadata.TNShard{Address: testTN4Addr},
							Payload: make([]byte, size),
						},
					}})

					require.NoError(t, err)
					resps = append(resps, resp.Responses[0].CNOpResponse)
					tickerA.Reset(time.Millisecond * 1)
				}
			}
		}()

		go func() {
			tickerB := time.NewTicker(time.Millisecond * 100)

			defer func() {
				wg.Done()
				require.NoError(t, s.SwitchTxnHandleStateTo(TxnLocalHandle))
				fmt.Println("state controller done")
			}()

			newCtx, cc := context.WithTimeout(context.Background(), time.Second*2)
			defer cc()

			currentState := TxnLocalHandle

			for {
				select {
				case <-newCtx.Done():
					return

				case <-tickerB.C:
					currentState = (currentState + 1) % 3

					err = s.SwitchTxnHandleStateTo(currentState,
						WithForwardTarget(metadata.TNShard{
							Address: "mocked forwarding addr",
						}),
						WithTxnForwardHandler(func(ctx context.Context, req *txn.TxnRequest, resp *txn.TxnResponse) error {
							msg := "forwarded"
							resp.CNOpResponse = &txn.CNOpResponse{Payload: []byte(msg)}
							return nil
						}))

					require.NoError(t, err)
					tickerB.Reset(time.Millisecond * 100)
				}
			}
		}()

		wg.Wait()

		var normal, forwarded, others int
		for i := range resps {
			msg := string(resps[i].Payload)
			if msg == "normal" {
				normal++
			} else if msg == "forwarded" {
				forwarded++
			} else {
				others++
			}
		}

		fmt.Println(normal, forwarded, others)

		require.Zero(t, others)
		require.NotZero(t, normal)
		require.NotZero(t, forwarded)
		require.Equal(t, cnt, normal+forwarded)

	}, WithServerMaxMessageSize(1024*10))

}
