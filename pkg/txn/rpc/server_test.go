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
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
)

func TestHandleMessageWithSender(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			return nil
		})

		cli, err := NewSender(newTestClock(), s.logger)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, cli.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		v, err := cli.Send(ctx, []txn.TxnRequest{{CNRequest: &txn.CNOpRequest{Target: metadata.DNShard{Address: testDN1Addr}}}})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(v.Responses))
	})
}

func TestHandleMessage(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			return nil
		})

		c := make(chan morpc.Message, 1)
		defer close(c)
		cs := newTestClientSession(c)

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		assert.NoError(t, s.onMessage(ctx, &txn.TxnRequest{RequestID: 1}, 0, cs))
		v := <-c
		assert.Equal(t, uint64(1), v.GetID())
	})
}

func TestHandleMessageWithFilter(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		n := 0
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(_ context.Context, _ *txn.TxnRequest, _ *txn.TxnResponse) error {
			n++
			return nil
		})
		s.SetFilter(func(tr *txn.TxnRequest) bool {
			return false
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		assert.NoError(t, s.onMessage(ctx, &txn.TxnRequest{RequestID: 1},
			0, nil))
		assert.Equal(t, 0, n)
	})
}

func TestHandleInvalidMessageWillPanic(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		assert.NoError(t, s.onMessage(ctx, &txn.TxnResponse{}, 0, nil))
	})
}

func TestHandleNotRegisterWillPanic(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()
		assert.NoError(t, s.onMessage(context.Background(), &txn.TxnRequest{}, 0, nil))
	})
}

func TestTimeoutRequestCannotHandled(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		n := 0
		s.RegisterMethodHandler(txn.TxnMethod_Read,
			func(_ context.Context, _ *txn.TxnRequest, _ *txn.TxnResponse) error {
				n++
				return nil
			})

		ctx, cancel := context.WithTimeout(context.Background(), 1)
		cancel()
		req := &txn.TxnRequest{Method: txn.TxnMethod_Read}
		assert.NoError(t, s.onMessage(ctx, req, 0, nil))
		assert.Equal(t, 0, n)
	})
}

func runTestTxnServer(t *testing.T, addr string, testFunc func(s *server)) {
	assert.NoError(t, os.RemoveAll(addr[7:]))
	s, err := NewTxnServer(addr,
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		logutil.GetPanicLogger())
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

func (cs *testClientSession) Write(ctx context.Context, response morpc.Message) error {
	cs.c <- response
	return nil
}

func newTestClock() clock.Clock {
	return clock.NewHLCClock(func() int64 { return 0 }, 0)
}
