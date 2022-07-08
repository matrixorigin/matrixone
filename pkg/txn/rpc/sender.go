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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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
		s.options.backendCreateOptions = options
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

type sender struct {
	logger          *zap.Logger
	client          morpc.RPCClient
	localStreamPool sync.Pool

	options struct {
		localDispatch         LocalDispatch
		payloadCopyBufferSize int
		backendCreateOptions  []morpc.BackendOption
		clientOptions         []morpc.ClientOption
	}
}

// NewSender create a txn sender
func NewSender(logger *zap.Logger, options ...SenderOption) (TxnSender, error) {
	logger = logutil.Adjust(logger)
	s := &sender{logger: logger}
	for _, opt := range options {
		opt(s)
	}
	s.adjust()

	codec := morpc.NewMessageCodec(func() morpc.Message { return &txn.TxnResponse{} }, s.options.payloadCopyBufferSize)
	bf := morpc.NewGoettyBasedBackendFactory(codec, s.options.backendCreateOptions...)
	client, err := morpc.NewClient(bf, s.options.clientOptions...)
	if err != nil {
		return nil, err
	}
	s.client = client
	s.localStreamPool.New = func() any {
		return newLocalStream(s.releaseLocalStream)
	}
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

func (s *sender) Send(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	s.mustSetupTimeoutAt(ctx, requests)

	if len(requests) == 1 {
		resp, err := s.doSend(ctx, requests[0])
		if err != nil {
			return nil, err
		}
		return []txn.TxnResponse{resp}, nil
	}

	responses := make([]txn.TxnResponse, len(requests))
	executors := make(map[string]*executor, len(requests))
	defer func() {
		for dn, st := range executors {
			if err := st.close(); err != nil {
				s.logger.Error("close stream failed",
					zap.String("dn", dn),
					zap.Error(err))
			}
		}
	}()

	for idx, req := range requests {
		dn := req.GetTargetDN()
		exec, ok := executors[dn.Address]
		if !ok {
			var st morpc.Stream
			var err error
			var local bool
			if s.options.localDispatch != nil {
				if h := s.options.localDispatch(dn); h != nil {
					local = true
					ls := s.acquireLocalStream()
					ls.setup(ctx, responses, h)
					local = true
					st = ls
				}
			}
			if err != nil {
				return nil, err
			}
			if st == nil {
				st, err = s.client.NewStream(dn.Address, len(requests))
				if err != nil {
					return nil, err
				}
			}

			exec, err = newExecutor(ctx, responses, st, local)
			if err != nil {
				return nil, err
			}
			executors[dn.Address] = exec
		}

		req.RequestID = exec.stream.ID()
		if err := exec.execute(req, idx); err != nil {
			return nil, err
		}
	}

	for _, se := range executors {
		if err := se.waitCompleted(); err != nil {
			return nil, err
		}
	}
	return responses, nil
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

	f, err := s.client.Send(ctx, dn.Address, &request, morpc.SendOptions{})
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

func (s *sender) mustSetupTimeoutAt(ctx context.Context, requests []txn.TxnRequest) {
	deadline, ok := ctx.Deadline()
	if !ok {
		s.logger.Fatal("context deadline not set")
	}
	for idx := range requests {
		requests[idx].TimeoutAt = deadline.UnixNano()
	}
}

func (s *sender) acquireLocalStream() *localStream {
	return s.localStreamPool.Get().(*localStream)
}

func (s *sender) releaseLocalStream(ls *localStream) {
	s.localStreamPool.Put(ls)
}

type executor struct {
	ctx       context.Context
	stream    morpc.Stream
	local     bool
	responses []txn.TxnResponse
	indexes   []int
	c         chan morpc.Message
}

func newExecutor(ctx context.Context, responses []txn.TxnResponse, stream morpc.Stream, local bool) (*executor, error) {
	exec := &executor{
		ctx:       ctx,
		local:     local,
		stream:    stream,
		responses: responses,
	}
	if !local {
		c, err := stream.Receive()
		if err != nil {
			return nil, err
		}
		exec.c = c
	}
	return exec, nil
}

func (se *executor) execute(req txn.TxnRequest, index int) error {
	se.indexes = append(se.indexes, index)
	req.RequestID = se.stream.ID()
	return se.stream.Send(&req, morpc.SendOptions{Arg: index})
}

func (se *executor) close() error {
	return se.stream.Close()
}

func (se *executor) waitCompleted() error {
	if se.local {
		return nil
	}
	for _, idx := range se.indexes {
		select {
		case <-se.ctx.Done():
			return se.ctx.Err()
		case v, ok := <-se.c:
			if !ok {
				return moerr.NewError(moerr.ErrStreamClosed, "stream closed")
			}
			se.responses[idx] = *(v.(*txn.TxnResponse))
		}
	}
	return nil
}

type localStream struct {
	releaseFunc func(ls *localStream)
	handleFunc  TxnRequestHandleFunc

	// reset fields
	closed    bool
	ctx       context.Context
	responses []txn.TxnResponse
}

func newLocalStream(releaseFunc func(ls *localStream)) *localStream {
	return &localStream{
		releaseFunc: releaseFunc,
	}
}

func (ls *localStream) setup(ctx context.Context, responses []txn.TxnResponse, handleFunc TxnRequestHandleFunc) {
	ls.handleFunc = handleFunc
	ls.ctx = ctx
	ls.responses = responses
	ls.closed = false
}

func (ls *localStream) ID() uint64 {
	return 0
}

func (ls *localStream) Send(request morpc.Message, opts morpc.SendOptions) error {
	if ls.closed {
		panic("send after closed")
	}

	response := &ls.responses[opts.Arg.(int)]
	err := ls.handleFunc(ls.ctx, request.(*txn.TxnRequest), response)
	if err != nil {
		return err
	}
	return nil
}

func (ls *localStream) Receive() (chan morpc.Message, error) {
	panic("not support")
}

func (ls *localStream) Close() error {
	if ls.closed {
		panic("closed")
	}
	ls.closed = true
	ls.ctx = nil
	ls.responses = nil
	if ls.releaseFunc != nil {
		ls.releaseFunc(ls)
	}
	return nil
}
