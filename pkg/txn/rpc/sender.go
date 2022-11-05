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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"go.uber.org/zap"
)

// WithSenderPayloadBufferSize set buffer size for copy payload data to socket.
func WithSenderPayloadBufferSize(value int) SenderOption {
	return func(s *sender) {
		s.options.payloadCopyBufferSize = value
	}
}

// WithSenderBackendOptions set options for create backend connections
func WithSenderBackendOptions(options ...morpc.BackendOption) SenderOption {
	return func(s *sender) {
		s.options.backendCreateOptions = append(s.options.backendCreateOptions, options...)
	}
}

// WithSenderClientOptions set options for create client
func WithSenderClientOptions(options ...morpc.ClientOption) SenderOption {
	return func(s *sender) {
		s.options.clientOptions = options
	}
}

// WithSenderLocalDispatch set options for dispatch request to local to avoid rpc call
func WithSenderLocalDispatch(localDispatch LocalDispatch) SenderOption {
	return func(s *sender) {
		s.options.localDispatch = localDispatch
	}
}

// WithSenderMaxMessageSize set max rpc message size
func WithSenderMaxMessageSize(maxMessageSize int) SenderOption {
	return func(s *sender) {
		s.options.maxMessageSize = maxMessageSize
	}
}

type sender struct {
	logger *zap.Logger
	clock  clock.Clock
	client morpc.RPCClient

	options struct {
		localDispatch         LocalDispatch
		payloadCopyBufferSize int
		maxMessageSize        int
		backendCreateOptions  []morpc.BackendOption
		clientOptions         []morpc.ClientOption
	}

	pool struct {
		resultPool      *sync.Pool
		responsePool    *sync.Pool
		localStreamPool *sync.Pool
	}
}

// NewSenderWithConfig create a txn sender by config and options
func NewSenderWithConfig(cfg Config,
	clock clock.Clock,
	logger *zap.Logger,
	options ...SenderOption) (TxnSender, error) {
	cfg.adjust()
	options = append(options, WithSenderBackendOptions(cfg.getBackendOptions(logger)...))
	options = append(options, WithSenderClientOptions(cfg.getClientOptions(logger)...))
	options = append(options, WithSenderMaxMessageSize(int(cfg.MaxMessageSize)))
	return NewSender(clock, logger, options...)
}

// NewSender create a txn sender
func NewSender(clock clock.Clock,
	logger *zap.Logger,
	options ...SenderOption) (TxnSender, error) {
	logger = logutil.Adjust(logger)
	s := &sender{logger: logger, clock: clock}
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

	codec := morpc.NewMessageCodec(func() morpc.Message { return s.acquireResponse() },
		morpc.WithCodecIntegrationHLC(s.clock),
		morpc.WithCodecPayloadCopyBufferSize(s.options.payloadCopyBufferSize),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(s.options.maxMessageSize))
	bf := morpc.NewGoettyBasedBackendFactory(codec, s.options.backendCreateOptions...)
	client, err := morpc.NewClient(bf, s.options.clientOptions...)
	if err != nil {
		return nil, err
	}
	s.client = client
	return s, nil
}

func (s *sender) adjust() {
	if s.options.payloadCopyBufferSize == 0 {
		s.options.payloadCopyBufferSize = 16 * 1024
	}
	s.options.backendCreateOptions = append(s.options.backendCreateOptions,
		morpc.WithBackendConnectWhenCreate(),
		morpc.WithBackendLogger(s.logger))

	s.options.clientOptions = append(s.options.clientOptions, morpc.WithClientLogger(s.logger))
}

func (s *sender) Close() error {
	return s.client.Close()
}

func (s *sender) Send(ctx context.Context, requests []txn.TxnRequest) (*SendResult, error) {
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
		dn := requests[idx].GetTargetDN()
		st := sr.getStream(dn.ShardID)
		if st == nil {
			v, err := s.createStream(ctx, dn, len(requests))
			if err != nil {
				sr.Release()
				return nil, err
			}
			st = v
			sr.setStream(dn.ShardID, v)
		}

		requests[idx].RequestID = st.ID()
		if err := st.Send(ctx, &requests[idx]); err != nil {
			sr.Release()
			return nil, err
		}
	}

	for idx := range requests {
		st := sr.getStream(requests[idx].GetTargetDN().ShardID)
		c, err := st.Receive()
		if err != nil {
			sr.Release()
			return nil, err
		}
		v, ok := <-c
		if !ok {
			return nil, moerr.NewStreamClosed()
		}
		resp := v.(*txn.TxnResponse)
		sr.setResponse(resp, idx)
		s.releaseResponse(resp)
	}
	return sr, nil
}

func (s *sender) doSend(ctx context.Context, request txn.TxnRequest) (txn.TxnResponse, error) {
	dn := request.GetTargetDN()
	if s.options.localDispatch != nil {
		if handle := s.options.localDispatch(dn); handle != nil {
			response := txn.TxnResponse{}
			err := handle(ctx, &request, &response)
			return response, err
		}
	}

	f, err := s.client.Send(ctx, dn.Address, &request)
	if err != nil {
		return txn.TxnResponse{}, err
	}
	defer f.Close()

	v, err := f.Get()
	if err != nil {
		return txn.TxnResponse{}, err
	}
	return *(v.(*txn.TxnResponse)), nil
}

func (s *sender) createStream(ctx context.Context, dn metadata.DNShard, size int) (morpc.Stream, error) {
	if s.options.localDispatch != nil {
		if h := s.options.localDispatch(dn); h != nil {
			ls := s.acquireLocalStream()
			ls.setup(ctx, h)
			return ls, nil
		}
	}
	return s.client.NewStream(dn.Address)
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
	request morpc.Message
	// opts            morpc.SendOptions
	handleFunc      TxnRequestHandleFunc
	responseFactory func() *txn.TxnResponse
	ctx             context.Context
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

func (ls *localStream) Send(ctx context.Context, request morpc.Message) error {
	if ls.closed {
		panic("send after closed")
	}

	ls.in <- sendMessage{
		request:         request,
		handleFunc:      ls.handleFunc,
		responseFactory: ls.responseFactory,
		ctx:             ls.ctx,
	}
	return nil
}

func (ls *localStream) Receive() (chan morpc.Message, error) {
	if ls.closed {
		panic("send after closed")
	}

	return ls.out, nil
}

func (ls *localStream) Close() error {
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
				response.TxnError = txn.WrapError(moerr.NewRpcError(err.Error()), 0)
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

func (sr *SendResult) setStream(dn uint64, st morpc.Stream) {
	sr.streams[dn] = st
}

func (sr *SendResult) getStream(dn uint64) morpc.Stream {
	return sr.streams[dn]
}

func (sr *SendResult) setResponse(resp *txn.TxnResponse, index int) {
	sr.Responses[index] = *resp
}

// Release release send result
func (sr *SendResult) Release() {
	if sr.pool != nil {
		for k, st := range sr.streams {
			if st != nil {
				_ = st.Close()
			}
			delete(sr.streams, k)
		}
		sr.Responses = sr.Responses[:0]
		sr.pool.Put(sr)
	}
}
