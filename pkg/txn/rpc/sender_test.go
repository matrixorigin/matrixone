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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
)

var (
	testDN1Addr = "unix:///tmp/test-dn1.sock"
	testDN2Addr = "unix:///tmp/test-dn2.sock"
	testDN3Addr = "unix:///tmp/test-dn3.sock"
)

func TestSendWithSingleRequest(t *testing.T) {
	s := newTestTxnServer(t, testDN1Addr)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	s.RegisterRequestHandler(func(request morpc.Message, sequence uint64, cs morpc.ClientSession) error {
		return cs.Write(&txn.TxnResponse{
			RequestID: request.GetID(),
			Method:    txn.TxnMethod_Write,
		}, morpc.SendOptions{})
	})

	sd, err := NewSender(nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, sd.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	req := txn.TxnRequest{
		Method: txn.TxnMethod_Write,
		CNRequest: &txn.CNOpRequest{
			Target: metadata.DNShard{
				Address: testDN1Addr,
			},
		},
	}
	resps, err := sd.Send(ctx, []txn.TxnRequest{req})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resps))
	assert.Equal(t, txn.TxnMethod_Write, resps[0].Method)
}

func TestSendWithMultiDN(t *testing.T) {
	addrs := []string{testDN1Addr, testDN2Addr, testDN3Addr}
	for _, addr := range addrs {
		s := newTestTxnServer(t, addr)
		defer func() {
			assert.NoError(t, s.Close())
		}()

		s.RegisterRequestHandler(func(m morpc.Message, sequence uint64, cs morpc.ClientSession) error {
			request := m.(*txn.TxnRequest)
			return cs.Write(&txn.TxnResponse{
				RequestID:    request.GetID(),
				CNOpResponse: &txn.CNOpResponse{Payload: []byte(fmt.Sprintf("%s-%d", request.GetTargetDN().Address, sequence))},
			}, morpc.SendOptions{})
		})
	}

	sd, err := NewSender(nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, sd.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var requests []txn.TxnRequest
	n := 10
	for i := 0; i < n; i++ {
		requests = append(requests, txn.TxnRequest{
			Method: txn.TxnMethod_Read,
			CNRequest: &txn.CNOpRequest{
				Target: metadata.DNShard{
					Address: addrs[i%len(addrs)],
				},
			},
		})
	}

	resps, err := sd.Send(ctx, requests)
	assert.NoError(t, err)
	assert.Equal(t, n, len(resps))

	counts := make(map[string]int)
	for i := 0; i < n; i++ {
		addr := addrs[i%len(addrs)]
		seq := 1
		if v, ok := counts[addr]; ok {
			seq = v + 1
		}
		counts[addr] = seq
		assert.Equal(t, []byte(fmt.Sprintf("%s-%d", addr, seq)), resps[i].CNOpResponse.Payload)
	}
}

func TestNewExecutor(t *testing.T) {
	ts := newTestStream(1, nil)
	defer func() {
		assert.NoError(t, ts.Close())
	}()

	_, err := newExecutor(context.Background(), nil, ts)
	assert.NoError(t, err)
}

func TestNewExectorWithClosedStream(t *testing.T) {
	ts := newTestStream(1, nil)
	assert.NoError(t, ts.Close())
	_, err := newExecutor(context.Background(), nil, ts)
	assert.Error(t, err)
}

func TestExecute(t *testing.T) {
	var requests []morpc.Message
	ts := newTestStream(1, func(m morpc.Message) (morpc.Message, error) {
		requests = append(requests, m)
		return m, nil
	})
	defer func() {
		assert.NoError(t, ts.Close())
	}()

	exec, err := newExecutor(context.Background(), nil, ts)
	assert.NoError(t, err)

	n := 10
	for i := 0; i < n; i++ {
		assert.NoError(t, exec.execute(txn.NewTxnRequest(nil), i))
	}

	assert.Equal(t, n, len(requests))
	for i := 0; i < n; i++ {
		assert.Equal(t, ts.id, requests[i].GetID())
	}

	assert.Equal(t, n, len(exec.indexes))
	for i := 0; i < n; i++ {
		assert.Equal(t, i, exec.indexes[i])
	}
}

func TestExecuteWithClosedStream(t *testing.T) {
	ts := newTestStream(1, nil)

	exec, err := newExecutor(context.Background(), nil, ts)
	assert.NoError(t, err)

	assert.NoError(t, ts.Close())
	assert.Error(t, exec.execute(txn.NewTxnRequest(nil), 0))
}

func TestWaitCompleted(t *testing.T) {
	ts := newTestStream(1, func(m morpc.Message) (morpc.Message, error) {
		req := m.(*txn.TxnRequest)
		return &txn.TxnResponse{RequestID: m.GetID(), CNOpResponse: &txn.CNOpResponse{Payload: req.CNRequest.Payload}}, nil
	})
	defer func() {
		assert.NoError(t, ts.Close())
	}()

	n := 10
	exec, err := newExecutor(context.Background(), make([]txn.TxnResponse, n), ts)
	assert.NoError(t, err)

	for i := 0; i < n; i++ {
		assert.NoError(t, exec.execute(txn.NewTxnRequest(&txn.CNOpRequest{Payload: []byte{byte(n - i - 1)}}), n-i-1))
	}

	assert.NoError(t, exec.waitCompleted())
	for i := 0; i < n; i++ {
		assert.Equal(t, []byte{byte(i)}, exec.responses[i].CNOpResponse.Payload)
	}
}

func TestWaitCompletedWithContextDone(t *testing.T) {
	ts := newTestStream(1, func(m morpc.Message) (morpc.Message, error) {
		req := m.(*txn.TxnRequest)
		return &txn.TxnResponse{RequestID: m.GetID(), CNOpResponse: &txn.CNOpResponse{Payload: req.CNRequest.Payload}}, nil
	})
	defer func() {
		assert.NoError(t, ts.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1)
	exec, err := newExecutor(ctx, make([]txn.TxnResponse, 1), ts)
	assert.NoError(t, err)

	assert.NoError(t, exec.execute(txn.NewTxnRequest(&txn.CNOpRequest{Payload: []byte{byte(0)}}), 0))
	<-ts.c
	cancel()
	assert.Error(t, exec.waitCompleted())
}

func TestWaitCompletedWithStreamClosed(t *testing.T) {
	ts := newTestStream(1, func(m morpc.Message) (morpc.Message, error) {
		req := m.(*txn.TxnRequest)
		return &txn.TxnResponse{RequestID: m.GetID(), CNOpResponse: &txn.CNOpResponse{Payload: req.CNRequest.Payload}}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec, err := newExecutor(ctx, make([]txn.TxnResponse, 1), ts)
	assert.NoError(t, err)
	assert.NoError(t, exec.execute(txn.NewTxnRequest(&txn.CNOpRequest{Payload: []byte{byte(0)}}), 0))
	<-ts.c
	assert.NoError(t, ts.Close())
	assert.Error(t, exec.waitCompleted())
}

func TestNewSenderWithOptions(t *testing.T) {
	s, err := NewSender(nil, WithSenderPayloadBufferSize(100),
		WithSenderBackendOptions(morpc.WithBackendBusyBufferSize(1)),
		WithSenderClientOptions(morpc.WithClientDisableCreateTask()))
	assert.NoError(t, err)
	assert.Equal(t, 100, s.(*sender).options.payloadCopyBufferSize)
	assert.True(t, len(s.(*sender).options.backendCreateOptions) >= 3)
	assert.True(t, len(s.(*sender).options.clientOptions) >= 1)
}

type testStream struct {
	id     uint64
	c      chan morpc.Message
	handle func(morpc.Message) (morpc.Message, error)

	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newTestStream(id uint64, handle func(morpc.Message) (morpc.Message, error)) *testStream {
	return &testStream{
		id:     id,
		c:      make(chan morpc.Message, 1024),
		handle: handle,
	}
}

func (s *testStream) Send(request morpc.Message, opts morpc.SendOptions) error {
	if s.id != request.GetID() {
		panic("request.id != stream.id")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return errors.New("closed")
	}

	resp, err := s.handle(request)
	if err != nil {
		return err
	}
	s.c <- resp
	return nil
}

func (s *testStream) Receive() (chan morpc.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return nil, errors.New("closed")
	}
	return s.c, nil
}

func (s *testStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.closed {
		return nil
	}

	s.mu.closed = true
	close(s.c)
	return nil
}

func (s *testStream) ID() uint64 {
	return s.id
}

func newTestTxnServer(t *testing.T, addr string) morpc.RPCServer {
	assert.NoError(t, os.RemoveAll(addr[7:]))
	codec := morpc.NewMessageCodec(func() morpc.Message { return &txn.TxnRequest{} }, 0)
	s, err := morpc.NewRPCServer("test-txn-server", addr, codec)
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	return s
}
