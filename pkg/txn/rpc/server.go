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
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var methodVersions = map[txn.TxnMethod]int64{
	txn.TxnMethod_Read:     defines.MORPCVersion1,
	txn.TxnMethod_Write:    defines.MORPCVersion1,
	txn.TxnMethod_Commit:   defines.MORPCVersion1,
	txn.TxnMethod_Rollback: defines.MORPCVersion1,

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

func WithTxnSender(sender TxnSender) ServerOption {
	return func(s *server) {
		s.handleState.forward.sender = sender
	}
}

func WithTxnForwardHandler(
	handler func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error) ServerOption {
	return func(s *server) {
		s.handleState.forward.forwardFunc = handler
	}
}

func WithForwardTarget(tn metadata.TNShard) ServerOption {
	return func(s *server) {
		s.handleState.forward.target = tn
	}
}

const (
	TxnLocalHandle = iota
	TxnForwardWait
	TxnForwarding
)

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

	handleState struct {
		sync.RWMutex
		state int

		forward struct {
			target    metadata.TNShard
			sender    TxnSender
			waitReady chan struct{}

			forwardFunc func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error
		}
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
	opts ...ServerOption,
) (TxnServer, error) {
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
		codecOpts = append(codecOpts, morpc.WithCodecEnableCompress(malloc.GetDefault(nil)))
	}
	rpc, err := morpc.NewRPCServer("txn-server", address,
		morpc.NewMessageCodec(rt.ServiceUUID(), s.acquireRequest, codecOpts...),
		morpc.WithServerLogger(s.rt.Logger().RawLogger()),
		morpc.WithServerDisableAutoCancelContext(),
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
	cs morpc.ClientSession,
) error {
	ctx, span := trace.Debug(ctx, "server.onMessage")
	defer span.End()

	m, ok := msg.Message.(*txn.TxnRequest)
	if !ok {
		s.rt.Logger().Fatal("received invalid message", zap.Any("message", msg))
	}
	if s.options.filter != nil && !s.options.filter(m) {
		s.releaseRequest(m)
		msg.Cancel()
		return nil
	}
	if err := checkMethodVersion(ctx, s.rt, m); err != nil {
		s.releaseRequest(m)
		msg.Cancel()
		return err
	}
	handler, ok := s.handlers[m.Method]
	if !ok {
		return moerr.NewNotSupportedf(ctx, "unknown txn request method: %s", m.Method.String())
	}

	select {
	case <-ctx.Done():
		s.releaseRequest(m)
		msg.Cancel()
		return nil
	default:
	}

	t := time.Now()
	s.queue <- executor{
		t:       t,
		ctx:     ctx,
		cancel:  msg.Cancel,
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
			state, waitReady, newHandler := s.getTxnHandleState()
			switch state {
			case TxnForwardWait:
				wait := true
				for wait {
					select {
					case <-ctx.Done():
						wait = false
					case <-waitReady:
						wait = false
						req.handler = newHandler
					}
				}

			case TxnLocalHandle:

			case TxnForwarding:
				req.handler = newHandler
			}

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
	cancel  context.CancelFunc
	req     *txn.TxnRequest
	cs      morpc.ClientSession
	handler TxnRequestHandleFunc
	s       *server
}

func (r executor) exec() ([]byte, error) {
	defer r.cancel()
	defer r.s.releaseRequest(r.req)
	resp := r.s.acquireResponse()
	if err := r.handler(r.ctx, r.req, resp); err != nil {
		r.s.releaseResponse(resp)
		return r.req.Txn.ID, err
	}
	resp.RequestID = r.req.RequestID
	txnID := r.req.Txn.ID
	err := r.cs.Write(r.ctx, resp)
	return txnID, err
}

func checkMethodVersion(
	ctx context.Context,
	rt runtime.Runtime,
	req *txn.TxnRequest,
) error {
	return runtime.CheckMethodVersionWithRuntime(ctx, rt, methodVersions, req)
}

/////////////////////// forward txn request to another tn /////////////////////

// valid state switch path
//
// txnLocalHandle --> txnLocalHandle
//				  --> txnForwardWait --> txnLocalHandle
// 									 --> txnForwardWait
//									 --> txnForwarding --> txnLocalHandle
//													   --> txnForwarding
//													   --> txnForwardWait

func (s *server) SwitchTxnHandleStateTo(state int, opts ...ServerOption) error {
	currentState := s.handleState.state

	switch state {
	case TxnLocalHandle:
		return s.enterLocalHandleState()
	case TxnForwardWait:
		return s.enterForwardWaitState(opts...)
	case TxnForwarding:
		if currentState != TxnForwarding && currentState != TxnForwardWait {
			return moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("state %d -> state %d invalid", currentState, state))
		}
		return s.enterForwardingState()
	}

	return moerr.NewInternalErrorNoCtx("no state matched")
}

func (s *server) getTxnHandleState() (
	int, chan struct{},
	func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error) {

	s.handleState.RLock()
	defer s.handleState.RUnlock()

	return s.handleState.state,
		s.handleState.forward.waitReady,
		s.handleState.forward.forwardFunc
}

func (s *server) enterForwardWaitState(opts ...ServerOption) error {
	s.handleState.Lock()
	defer s.handleState.Unlock()

	if s.handleState.state == TxnForwardWait {
		return nil
	}

	for _, f := range opts {
		f(s)
	}

	if len(s.handleState.forward.target.Address) == 0 {
		return moerr.NewInternalErrorNoCtx("no target tn specified")
	}

	if s.handleState.forward.forwardFunc == nil {
		s.handleState.forward.forwardFunc = s.forwardingTxnRequest
	}

	s.handleState.state = TxnForwardWait
	s.handleState.forward.waitReady = make(chan struct{})

	return nil
}

func (s *server) enterForwardingState() error {
	s.handleState.Lock()
	defer s.handleState.Unlock()

	if s.handleState.state == TxnForwarding {
		return nil
	}

	close(s.handleState.forward.waitReady)
	s.handleState.forward.waitReady = nil
	s.handleState.state = TxnForwarding

	return nil
}

func (s *server) enterLocalHandleState() error {
	s.handleState.Lock()
	defer s.handleState.Unlock()

	if s.handleState.state == TxnLocalHandle {
		return nil
	}

	if s.handleState.forward.waitReady != nil {
		close(s.handleState.forward.waitReady)
		s.handleState.forward.waitReady = nil
	}

	s.handleState.state = TxnLocalHandle

	return nil
}

func (s *server) logTxnHandleState() string {
	stateName := []string{"local handle", "forwarding wait", "forwarding"}

	return fmt.Sprintf("STATE(%s)-TARGET(%s)-SENDER(nil=%v)",
		stateName[s.handleState.state],
		s.handleState.forward.target.Address,
		s.handleState.forward.sender == nil)
}

func (s *server) forwardingTxnRequest(
	ctx context.Context,
	req *txn.TxnRequest,
	resp *txn.TxnResponse) error {

	if req == nil {
		return moerr.NewInternalErrorNoCtx("req is nil")
	}

	req.ResetTargetTN(s.handleState.forward.target)

	if resp == nil {
		return moerr.NewInternalErrorNoCtx("resp is nil")
	}

	if s.handleState.forward.sender == nil {
		return moerr.NewInternalErrorNoCtx("txn sender is nil")
	}

	result, err := s.handleState.forward.sender.Send(ctx, []txn.TxnRequest{*req})
	if err != nil {
		return err
	}

	*resp = result.Responses[0]
	result.Release()

	return nil
}
