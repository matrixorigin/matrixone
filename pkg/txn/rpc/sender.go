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
	"runtime"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// WithSenderLocalDispatch set options for dispatch request to local to avoid rpc call
func WithSenderLocalDispatch(localDispatch LocalDispatch) SenderOption {
	return func(s *sender) {
		s.options.localDispatch = localDispatch
	}
}

type sender struct {
	cfg    *Config
	rt     moruntime.Runtime
	client morpc.RPCClient

	options struct {
		localDispatch LocalDispatch
	}

	pool struct {
		resultPool      *sync.Pool
		responsePool    *sync.Pool
		localStreamPool *sync.Pool
	}
}

// NewSender create a txn sender
func NewSender(
	cfg Config,
	rt moruntime.Runtime,
	options ...SenderOption) (TxnSender, error) {
	s := &sender{rt: rt, cfg: &cfg}
	for _, opt := range options {
		opt(s)
	}
	s.adjust()

	s.pool.localStreamPool = &sync.Pool{
		New: func() any {
			return newLocalStream(s.releaseLocalStream, s.acquireResponse)
		},
	}
	s.pool.resultPool = &sync.Pool{
		New: func() any {
			rs := &SendResult{
				pool:    s.pool.resultPool,
				streams: make(map[uint64]morpc.Stream, 16),
			}
			return rs
		},
	}
	s.pool.responsePool = &sync.Pool{
		New: func() any {
			return &txn.TxnResponse{}
		},
	}

	s.cfg.BackendOptions = append(s.cfg.BackendOptions,
		morpc.WithBackendStreamBufferSize(10000))
	client, err := s.cfg.NewClient(
		"txn-client",
		s.rt.Logger().RawLogger(),
		func() morpc.Message { return s.acquireResponse() })
	if err != nil {
		return nil, err
	}
	s.client = client
	return s, nil
}

func (s *sender) adjust() {
	s.cfg.Adjust()
	s.cfg.CodecOptions = append(s.cfg.CodecOptions, morpc.WithCodecIntegrationHLC(s.rt.Clock()))
}

func (s *sender) Close() error {
	return s.client.Close()
}

func (s *sender) Send(
	ctx context.Context,
	requests []txn.TxnRequest,
) (*SendResult, error) {
	sr := s.acquireSendResult()
	if len(requests) == 1 {
		sr.reset(requests)
		resp, err := s.doSend(ctx, requests[0])
		if err != nil {
			sr.Release()
			return nil, err
		}
		sr.Responses[0] = resp
		return sr, nil
	}

	sr.reset(requests)
	for idx := range requests {
		tn := requests[idx].GetTargetTN()
		st := sr.getStream(tn.ShardID)
		if st == nil {
			v, err := s.createStream(ctx, tn, len(requests))
			if err != nil {
				sr.Release()
				return nil, err
			}
			st = v
			sr.setStream(tn.ShardID, v)
		}

		requests[idx].RequestID = st.ID()
		if err := st.Send(ctx, &requests[idx], 0); err != nil {
			sr.Release()
			return nil, err
		}
	}

	for idx := range requests {
		st := sr.getStream(requests[idx].GetTargetTN().ShardID)
		c, err := st.Receive()
		if err != nil {
			sr.Release()
			return nil, err
		}
		v, ok := <-c
		if !ok {
			return nil, moerr.NewStreamClosedNoCtx()
		}
		resp := v.(*txn.TxnResponse)
		sr.setResponse(resp, idx)
		s.releaseResponse(resp)
	}
	return sr, nil
}

func (s *sender) doSend(ctx context.Context, request txn.TxnRequest) (txn.TxnResponse, error) {
	ctx, span := trace.Debug(ctx, "sender.doSend")
	defer span.End()
	tn := request.GetTargetTN()
	if s.options.localDispatch != nil {
		if handle := s.options.localDispatch(tn); handle != nil {
			response := txn.TxnResponse{}
			err := handle(ctx, &request, &response)
			return response, err
		}
	}

	start := time.Now()
	f, err := s.client.Send(ctx, tn.Address, &request, 0)
	if err != nil {
		return txn.TxnResponse{}, err
	}
	v2.TxnCNSendCommitDurationHistogram.Observe(time.Since(start).Seconds())
	defer f.Close()

	v, err := f.Get()
	if err != nil {
		return txn.TxnResponse{}, err
	}
	return *(v.(*txn.TxnResponse)), nil
}

func (s *sender) createStream(ctx context.Context, tn metadata.TNShard, _ int) (morpc.Stream, error) {
	if s.options.localDispatch != nil {
		if h := s.options.localDispatch(tn); h != nil {
			ls := s.acquireLocalStream()
			ls.setup(ctx, h)
			return ls, nil
		}
	}
	return s.client.NewStream(tn.Address, false)
}

func (s *sender) acquireLocalStream() *localStream {
	return s.pool.localStreamPool.Get().(*localStream)
}

func (s *sender) releaseLocalStream(ls *localStream) {
	s.pool.localStreamPool.Put(ls)
}

func (s *sender) acquireResponse() *txn.TxnResponse {
	return s.pool.responsePool.Get().(*txn.TxnResponse)
}

func (s *sender) releaseResponse(response *txn.TxnResponse) {
	response.Reset()
	s.pool.responsePool.Put(response)
}

func (s *sender) acquireSendResult() *SendResult {
	return s.pool.resultPool.Get().(*SendResult)
}

type sendMessage struct {
	request         morpc.Message
	handleFunc      TxnRequestHandleFunc
	responseFactory func() *txn.TxnResponse
	ctx             context.Context
	timeout         time.Duration
}

type localStream struct {
	releaseFunc     func(ls *localStream)
	responseFactory func() *txn.TxnResponse
	in              chan sendMessage
	out             chan morpc.Message

	// reset fields
	closed     bool
	handleFunc TxnRequestHandleFunc
	ctx        context.Context
}

func newLocalStream(releaseFunc func(ls *localStream), responseFactory func() *txn.TxnResponse) *localStream {
	ls := &localStream{
		releaseFunc:     releaseFunc,
		responseFactory: responseFactory,
		in:              make(chan sendMessage, 32),
		out:             make(chan morpc.Message, 32),
	}
	ls.setFinalizer()
	ls.start()
	return ls
}

func (ls *localStream) setFinalizer() {
	runtime.SetFinalizer(ls, func(ls *localStream) {
		ls.destroy()
	})
}

func (ls *localStream) setup(ctx context.Context, handleFunc TxnRequestHandleFunc) {
	ls.handleFunc = handleFunc
	ls.ctx = ctx
	ls.closed = false
}

func (ls *localStream) ID() uint64 {
	return 0
}

func (ls *localStream) Send(
	ctx context.Context,
	request morpc.Message,
	timeout time.Duration,
) error {
	if ls.closed {
		panic("send after closed")
	}

	ls.in <- sendMessage{
		request:         request,
		handleFunc:      ls.handleFunc,
		responseFactory: ls.responseFactory,
		ctx:             ls.ctx,
		timeout:         timeout,
	}
	return nil
}

func (ls *localStream) Receive() (chan morpc.Message, error) {
	if ls.closed {
		panic("send after closed")
	}

	return ls.out, nil
}

func (ls *localStream) Close(closeConn bool) error {
	if ls.closed {
		return nil
	}
	ls.closed = true
	ls.ctx = nil
	ls.releaseFunc(ls)
	return nil
}

func (ls *localStream) destroy() {
	close(ls.in)
	close(ls.out)
}

func (ls *localStream) start() {
	go func(in chan sendMessage, out chan morpc.Message) {
		for {
			v, ok := <-in
			if !ok {
				return
			}

			response := v.responseFactory()
			err := v.handleFunc(v.ctx, v.request.(*txn.TxnRequest), response)
			if err != nil {
				response.TxnError = txn.WrapError(moerr.NewRpcErrorNoCtx(err.Error()), 0)
			}
			out <- response
		}
	}(ls.in, ls.out)
}

func (sr *SendResult) reset(requests []txn.TxnRequest) {
	size := len(requests)
	if size == len(sr.Responses) {
		for i := 0; i < size; i++ {
			sr.Responses[i] = txn.TxnResponse{}
		}
		return
	}

	for i := 0; i < size; i++ {
		sr.Responses = append(sr.Responses, txn.TxnResponse{})
	}
}

func (sr *SendResult) setStream(tn uint64, st morpc.Stream) {
	sr.streams[tn] = st
}

func (sr *SendResult) getStream(tn uint64) morpc.Stream {
	return sr.streams[tn]
}

func (sr *SendResult) setResponse(resp *txn.TxnResponse, index int) {
	sr.Responses[index] = *resp
}

// Release release send result
func (sr *SendResult) Release() {
	if sr.pool != nil {
		for k, st := range sr.streams {
			if st != nil {
				_ = st.Close(false)
			}
			delete(sr.streams, k)
		}
		sr.Responses = sr.Responses[:0]
		sr.pool.Put(sr)
	}
}
