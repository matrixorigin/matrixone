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
	"github.com/stretchr/testify/assert"
)

func TestHandleMessageWithSender(t *testing.T) {
	runTestTxnServer(t, testDN1Addr, func(s *server) {
		s.RegisterMethodHandler(txn.TxnMethod_Read, func(ctx context.Context, tr1 *txn.TxnRequest, tr2 *txn.TxnResponse) error {
			return nil
		})

		cli, err := NewSender(s.logger)
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

		assert.NoError(t, s.onMessage(&txn.TxnRequest{RequestID: 1, TimeoutAt: time.Now().Add(time.Hour).UnixNano()}, 0, cs))
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

		assert.NoError(t, s.onMessage(&txn.TxnRequest{RequestID: 1, TimeoutAt: time.Now().Add(time.Hour).UnixNano()},
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
		assert.NoError(t, s.onMessage(&txn.TxnResponse{}, 0, nil))
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
		assert.NoError(t, s.onMessage(&txn.TxnRequest{}, 0, nil))
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

		req := &txn.TxnRequest{Method: txn.TxnMethod_Read, TimeoutAt: time.Now().UnixNano() - 1}
		assert.NoError(t, s.onMessage(req, 0, nil))
		assert.Equal(t, 0, n)
	})
}

func runTestTxnServer(t *testing.T, addr string, testFunc func(s *server)) {
	assert.NoError(t, os.RemoveAll(addr[7:]))
	s, err := NewTxnServer(addr, logutil.GetPanicLogger())
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

func (cs *testClientSession) Write(response morpc.Message, opts morpc.SendOptions) error {
	cs.c <- response
	return nil
}
