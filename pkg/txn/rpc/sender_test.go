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
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	s.RegisterRequestHandler(func(ctx context.Context, request morpc.Message, sequence uint64, cs morpc.ClientSession) error {
		return cs.Write(ctx, &txn.TxnResponse{
			RequestID: request.GetID(),
			Method:    txn.TxnMethod_Write,
		})
	})

	sd, err := NewSender(newTestClock(), nil)
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
	result, err := sd.Send(ctx, []txn.TxnRequest{req})
	assert.NoError(t, err)
	defer result.Release()
	assert.Equal(t, 1, len(result.Responses))
	assert.Equal(t, txn.TxnMethod_Write, result.Responses[0].Method)
}

func TestSendWithMultiDN(t *testing.T) {
	addrs := []string{testDN1Addr, testDN2Addr, testDN3Addr}
	for _, addr := range addrs {
		s := newTestTxnServer(t, addr)
		defer func() {
			assert.NoError(t, s.Close())
		}()

		s.RegisterRequestHandler(func(ctx context.Context, m morpc.Message, sequence uint64, cs morpc.ClientSession) error {
			request := m.(*txn.TxnRequest)
			return cs.Write(ctx, &txn.TxnResponse{
				RequestID:    request.GetID(),
				CNOpResponse: &txn.CNOpResponse{Payload: []byte(fmt.Sprintf("%s-%d", request.GetTargetDN().Address, sequence))},
			})
		})
	}

	sd, err := NewSender(newTestClock(), nil)
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
					DNShardRecord: metadata.DNShardRecord{
						ShardID: uint64(i % len(addrs)),
					},
					Address: addrs[i%len(addrs)],
				},
			},
		})
	}

	result, err := sd.Send(ctx, requests)
	assert.NoError(t, err)
	defer result.Release()
	assert.Equal(t, n, len(result.Responses))

	counts := make(map[string]int)
	for i := 0; i < n; i++ {
		addr := addrs[i%len(addrs)]
		seq := 1
		if v, ok := counts[addr]; ok {
			seq = v + 1
		}
		counts[addr] = seq
		assert.Equal(t, []byte(fmt.Sprintf("%s-%d", addr, seq)), result.Responses[i].CNOpResponse.Payload)
	}
}

func TestSendWithMultiDNAndLocal(t *testing.T) {
	addrs := []string{testDN1Addr, testDN2Addr, testDN3Addr}
	for _, addr := range addrs[1:] {
		s := newTestTxnServer(t, addr)
		defer func() {
			assert.NoError(t, s.Close())
		}()

		s.RegisterRequestHandler(func(ctx context.Context, m morpc.Message, sequence uint64, cs morpc.ClientSession) error {
			request := m.(*txn.TxnRequest)
			return cs.Write(ctx, &txn.TxnResponse{
				RequestID:    request.GetID(),
				CNOpResponse: &txn.CNOpResponse{Payload: []byte(fmt.Sprintf("%s-%d", request.GetTargetDN().Address, sequence))},
			})
		})
	}

	sd, err := NewSender(
		newTestClock(),
		nil,
		WithSenderLocalDispatch(func(d metadata.DNShard) TxnRequestHandleFunc {
			if d.Address != testDN1Addr {
				return nil
			}
			sequence := uint64(0)
			return func(_ context.Context, req *txn.TxnRequest, resp *txn.TxnResponse) error {
				v := atomic.AddUint64(&sequence, 1)
				resp.RequestID = req.RequestID
				resp.CNOpResponse = &txn.CNOpResponse{Payload: []byte(fmt.Sprintf("%s-%d", req.GetTargetDN().Address, v))}
				return nil
			}
		}))
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
					DNShardRecord: metadata.DNShardRecord{
						ShardID: uint64(i % len(addrs)),
					},
					Address: addrs[i%len(addrs)],
				},
			},
		})
	}

	result, err := sd.Send(ctx, requests)
	assert.NoError(t, err)
	defer result.Release()
	assert.Equal(t, n, len(result.Responses))

	counts := make(map[string]int)
	for i := 0; i < n; i++ {
		addr := addrs[i%len(addrs)]
		seq := 1
		if v, ok := counts[addr]; ok {
			seq = v + 1
		}
		counts[addr] = seq
		assert.Equal(t, []byte(fmt.Sprintf("%s-%d", addr, seq)), result.Responses[i].CNOpResponse.Payload)
	}
}

func TestLocalStreamDestroy(t *testing.T) {
	ls := newLocalStream(func(ls *localStream) {}, func() *txn.TxnResponse { return &txn.TxnResponse{} })
	c := ls.in
	ls = nil
	debug.FreeOSMemory()
	_, ok := <-c
	assert.False(t, ok)
}

func BenchmarkLocalSend(b *testing.B) {
	sd, err := NewSender(
		newTestClock(),
		nil,
		WithSenderLocalDispatch(func(d metadata.DNShard) TxnRequestHandleFunc {
			return func(_ context.Context, req *txn.TxnRequest, resp *txn.TxnResponse) error {
				resp.RequestID = req.RequestID
				return nil
			}
		}))
	assert.NoError(b, err)
	defer func() {
		assert.NoError(b, sd.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var requests []txn.TxnRequest
	n := 10
	for i := 0; i < n; i++ {
		requests = append(requests, txn.TxnRequest{
			Method: txn.TxnMethod_Read,
			CNRequest: &txn.CNOpRequest{
				Target: metadata.DNShard{},
			},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := sd.Send(ctx, requests)
		assert.NoError(b, err)
		assert.Equal(b, n, len(result.Responses))
		result.Release()
	}
}

func TestNewSenderWithOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, err := NewSender(newTestClock(),
		nil,
		WithSenderPayloadBufferSize(100),
		WithSenderBackendOptions(morpc.WithBackendBusyBufferSize(1)))
	assert.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	assert.Equal(t, 100, s.(*sender).options.payloadCopyBufferSize)
	assert.True(t, len(s.(*sender).options.backendCreateOptions) >= 2)
	assert.True(t, len(s.(*sender).options.clientOptions) >= 1)
}

func TestCanSendWithLargeRequest(t *testing.T) {
	size := 1024 * 1024 * 20
	s := newTestTxnServer(t, testDN1Addr, morpc.WithCodecMaxBodySize(size+1024))
	defer func() {
		assert.NoError(t, s.Close())
	}()

	s.RegisterRequestHandler(func(ctx context.Context, request morpc.Message, sequence uint64, cs morpc.ClientSession) error {
		return cs.Write(ctx, &txn.TxnResponse{
			RequestID: request.GetID(),
			Method:    txn.TxnMethod_Write,
			CNOpResponse: &txn.CNOpResponse{
				Payload: make([]byte, size),
			},
		})
	})

	sd, err := NewSender(newTestClock(), nil, WithSenderMaxMessageSize(size+1024))
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
			Payload: make([]byte, size),
		},
	}
	result, err := sd.Send(ctx, []txn.TxnRequest{req})
	assert.NoError(t, err)
	defer result.Release()
	assert.Equal(t, 1, len(result.Responses))
	assert.Equal(t, txn.TxnMethod_Write, result.Responses[0].Method)
}

func newTestTxnServer(t assert.TestingT, addr string, opts ...morpc.CodecOption) morpc.RPCServer {
	assert.NoError(t, os.RemoveAll(addr[7:]))
	opts = append(opts,
		morpc.WithCodecIntegrationHLC(newTestClock()),
		morpc.WithCodecEnableChecksum())
	codec := morpc.NewMessageCodec(func() morpc.Message { return &txn.TxnRequest{} },
		opts...)
	s, err := morpc.NewRPCServer("test-txn-server", addr, codec)
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	return s
}
