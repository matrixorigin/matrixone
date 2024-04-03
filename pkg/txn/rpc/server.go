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
	"encoding/hex"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var methodVersions = map[txn.TxnMethod]int64{
	txn.TxnMethod_Read:   defines.MORPCVersion1,
	txn.TxnMethod_Write:  defines.MORPCVersion1,
	txn.TxnMethod_Commit: defines.MORPCVersion1,

	txn.TxnMethod_Prepare:         defines.MORPCVersion1,
	txn.TxnMethod_CommitTNShard:   defines.MORPCVersion1,
	txn.TxnMethod_RollbackTNShard: defines.MORPCVersion1,
	txn.TxnMethod_GetStatus:       defines.MORPCVersion1,

	txn.TxnMethod_DEBUG: defines.MORPCVersion1,
}

// WithServerMaxMessageSize set max rpc message size
func WithServerMaxMessageSize(maxMessageSize int) ServerOption {
	return func(s *server) {
		s.options.maxMessageSize = maxMessageSize
	}
}

// WithServerEnableCompress enable compress
func WithServerEnableCompress(enable bool) ServerOption {
	return func(s *server) {
		s.options.enableCompress = enable
	}
}

// set filter func. Requests can be modified or filtered out by the filter
// before they are processed by the handler.
func WithServerMessageFilter(filter func(*txn.TxnRequest) bool) ServerOption {
	return func(s *server) {
		s.options.filter = filter
	}
}

// WithServerQueueBufferSize set queue buffer size
func WithServerQueueBufferSize(value int) ServerOption {
	return func(s *server) {
		s.options.maxChannelBufferSize = value
	}
}

// WithServerQueueWorkers set worker number
func WithServerQueueWorkers(value int) ServerOption {
	return func(s *server) {
		s.options.workers = value
	}
}

type server struct {
	rt       runtime.Runtime
	rpc      morpc.RPCServer
	handlers map[txn.TxnMethod]TxnRequestHandleFunc

	pool struct {
		requests  sync.Pool
		responses sync.Pool
	}

	options struct {
		filter               func(*txn.TxnRequest) bool
		maxMessageSize       int
		enableCompress       bool
		maxChannelBufferSize int
		workers              int
	}

	// in order not to block tcp, the data read from tcp will be put into this ringbuffer. This ringbuffer will
	// be consumed by many goroutines concurrently, and the number of goroutines will be set to the number of
	// cpu's number.
	queue   chan executor
	stopper *stopper.Stopper
}

// NewTxnServer create a txn server. One DNStore corresponds to one TxnServer
func NewTxnServer(
	address string,
	rt runtime.Runtime,
	opts ...ServerOption) (TxnServer, error) {
	s := &server{
		rt:       rt,
		handlers: make(map[txn.TxnMethod]TxnRequestHandleFunc),
		stopper: stopper.NewStopper("txn rpc server",
			stopper.WithLogger(rt.Logger().RawLogger())),
	}
	s.pool.requests = sync.Pool{
		New: func() any {
			return &txn.TxnRequest{}
		},
	}
	s.pool.responses = sync.Pool{
		New: func() any {
			return &txn.TxnResponse{}
		},
	}
	for _, opt := range opts {
		opt(s)
	}

	var codecOpts []morpc.CodecOption
	codecOpts = append(codecOpts,
		morpc.WithCodecIntegrationHLC(rt.Clock()),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecPayloadCopyBufferSize(16*1024),
		morpc.WithCodecMaxBodySize(s.options.maxMessageSize))
	if s.options.enableCompress {
		mp, err := mpool.NewMPool("txn-server", 0, mpool.NoFixed)
		if err != nil {
			return nil, err
		}
		codecOpts = append(codecOpts, morpc.WithCodecEnableCompress(mp))
	}
	rpc, err := morpc.NewRPCServer("txn-server", address,
		morpc.NewMessageCodec(s.acquireRequest, codecOpts...),
		morpc.WithServerLogger(s.rt.Logger().RawLogger()),
		morpc.WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
			m := v.(morpc.RPCMessage)
			if !m.InternalMessage() {
				s.releaseResponse(m.Message.(*txn.TxnResponse))
			}
		})))
	if err != nil {
		return nil, err
	}

	rpc.RegisterRequestHandler(s.onMessage)
	s.rpc = rpc
	return s, nil
}

func (s *server) Start() error {
	s.queue = make(chan executor, s.options.maxChannelBufferSize)
	s.startProcessors()
	return s.rpc.Start()
}

func (s *server) Close() error {
	s.stopper.Stop()
	return s.rpc.Close()
}

func (s *server) RegisterMethodHandler(m txn.TxnMethod, h TxnRequestHandleFunc) {
	s.handlers[m] = h
}

func (s *server) startProcessors() {
	for i := 0; i < s.options.workers; i++ {
		if err := s.stopper.RunTask(s.handleTxnRequest); err != nil {
			panic(err)
		}
	}
}

// onMessage a client connection has a separate read goroutine. The onMessage invoked in this read goroutine.
func (s *server) onMessage(
	ctx context.Context,
	msg morpc.RPCMessage,
	sequence uint64,
	cs morpc.ClientSession) error {
	ctx, span := trace.Debug(ctx, "server.onMessage")
	defer span.End()

	m, ok := msg.Message.(*txn.TxnRequest)
	if !ok {
		s.rt.Logger().Fatal("received invalid message", zap.Any("message", msg))
	}
	if s.options.filter != nil && !s.options.filter(m) {
		s.releaseRequest(m)
		return nil
	}
	if err := checkMethodVersion(ctx, m); err != nil {
		s.releaseRequest(m)
		return err
	}
	handler, ok := s.handlers[m.Method]
	if !ok {
		return moerr.NewNotSupported(ctx, "unknown txn request method: %s", m.Method.String())
	}

	select {
	case <-ctx.Done():
		s.releaseRequest(m)
		return nil
	default:
	}

	t := time.Now()
	s.queue <- executor{
		t:       t,
		ctx:     ctx,
		timeout: msg.GetTimeout(),
		req:     m,
		cs:      cs,
		handler: handler,
		s:       s,
	}
	n := len(s.queue)
	v2.TxnCommitQueueSizeGauge.Set(float64(n))
	if n > s.options.maxChannelBufferSize/2 {
		s.rt.Logger().Warn("txn request handle channel is busy",
			zap.Int("size", n),
			zap.Int("max", s.options.maxChannelBufferSize))
	}
	return nil
}

func (s *server) acquireResponse() *txn.TxnResponse {
	return s.pool.responses.Get().(*txn.TxnResponse)
}

func (s *server) releaseResponse(resp *txn.TxnResponse) {
	resp.Reset()
	s.pool.responses.Put(resp)
}

func (s *server) acquireRequest() morpc.Message {
	return s.pool.requests.Get().(*txn.TxnRequest)
}

func (s *server) releaseRequest(req *txn.TxnRequest) {
	req.Reset()
	s.pool.requests.Put(req)
}

func (s *server) handleTxnRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-s.queue:
			if txnID, err := req.exec(); err != nil {
				if s.rt.Logger().Enabled(zap.DebugLevel) {
					s.rt.Logger().Error("handle txn request failed",
						zap.String("txn-id", hex.EncodeToString(txnID)),
						zap.Error(err))
				}
			}
		}
	}
}

type executor struct {
	t       time.Time
	ctx     context.Context
	timeout time.Duration
	req     *txn.TxnRequest
	cs      morpc.ClientSession
	handler TxnRequestHandleFunc
	s       *server
}

func (r executor) exec() ([]byte, error) {
	defer r.s.releaseRequest(r.req)
	resp := r.s.acquireResponse()
	if err := r.handler(r.ctx, r.req, resp); err != nil {
		r.s.releaseResponse(resp)
		return r.req.Txn.ID, err
	}
	resp.RequestID = r.req.RequestID
	txnID := r.req.Txn.ID
	err := r.cs.Write(r.ctx, resp, r.timeout)
	return txnID, err
}

func checkMethodVersion(ctx context.Context, req *txn.TxnRequest) error {
	return runtime.CheckMethodVersion(ctx, methodVersions, req)
}
