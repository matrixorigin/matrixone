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

package morpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// WithServerLogger set rpc server logger
func WithServerLogger(logger *zap.Logger) ServerOption {
	return func(rs *server) {
		rs.logger = logger
	}
}

// WithServerSessionBufferSize set the buffer size of the write response chan.
// Default is 16.
func WithServerSessionBufferSize(size int) ServerOption {
	return func(s *server) {
		s.options.bufferSize = size
	}
}

// WithServerWriteFilter set write filter func. Input ready to send Messages, output
// is really need to be send Messages.
func WithServerWriteFilter(filter func(Message) bool) ServerOption {
	return func(s *server) {
		s.options.filter = filter
	}
}

// WithServerGoettyOptions set write filter func. Input ready to send Messages, output
// is really need to be send Messages.
func WithServerGoettyOptions(options ...goetty.Option) ServerOption {
	return func(s *server) {
		s.options.goettyOptions = options
	}
}

// WithServerBatchSendSize set the maximum number of messages to be sent together
// at each batch. Default is 8.
func WithServerBatchSendSize(size int) ServerOption {
	return func(s *server) {
		s.options.batchSendSize = size
	}
}

// WithServerDisableAutoCancelContext disable automatic cancel messaging for the context.
// The server will receive RPC messages from the client, each message comes with a Context,
// and morpc will call the handler to process it, and when the handler returns, the Context
// will be auto cancel the context. But in some scenarios, the handler is asynchronous,
// so morpc can't directly cancel the context after the handler returns, otherwise many strange
// problems will occur.
func WithServerDisableAutoCancelContext() ServerOption {
	return func(s *server) {
		s.options.disableAutoCancelContext = true
	}
}

type server struct {
	name        string
	address     string
	logger      *zap.Logger
	application goetty.NetApplication
	stopper     *stopper.Stopper
	handler     func(ctx context.Context, request Message, sequence uint64, cs ClientSession) error
	sessions    *sync.Map // session-id => *clientSession
	options     struct {
		goettyOptions            []goetty.Option
		bufferSize               int
		batchSendSize            int
		filter                   func(Message) bool
		disableAutoCancelContext bool
	}
}

// NewRPCServer create rpc server with options. After the rpc server starts, one link corresponds to two
// goroutines, one read and one write. All messages to be written are first written to a buffer chan and
// sent to the client by the write goroutine.
func NewRPCServer(name, address string, codec Codec, options ...ServerOption) (RPCServer, error) {
	s := &server{
		name:     name,
		address:  address,
		stopper:  stopper.NewStopper(fmt.Sprintf("rpc-server-%s", name)),
		sessions: &sync.Map{},
	}
	for _, opt := range options {
		opt(s)
	}
	s.adjust()

	s.options.goettyOptions = append(s.options.goettyOptions,
		goetty.WithSessionCodec(codec),
		goetty.WithSessionLogger(s.logger))

	app, err := goetty.NewApplication(
		s.address,
		s.onMessage,
		goetty.WithAppLogger(s.logger),
		goetty.WithAppSessionOptions(s.options.goettyOptions...),
	)
	if err != nil {
		s.logger.Error("create rpc server failed",
			zap.Error(err))
		return nil, err
	}
	s.application = app
	return s, nil
}

func (s *server) Start() error {
	err := s.application.Start()
	if err != nil {
		s.logger.Fatal("start rpcserver failed",
			zap.Error(err))
		return err
	}
	return nil
}

func (s *server) Close() error {
	s.stopper.Stop()
	err := s.application.Stop()
	if err != nil {
		s.logger.Error("stop rpcserver failed",
			zap.Error(err))
	}

	return err
}

func (s *server) RegisterRequestHandler(handler func(ctx context.Context, request Message, sequence uint64, cs ClientSession) error) {
	s.handler = handler
}

func (s *server) adjust() {
	s.logger = logutil.Adjust(s.logger).With(zap.String("name", s.name))
	if s.options.batchSendSize == 0 {
		s.options.batchSendSize = 8
	}
	if s.options.bufferSize == 0 {
		s.options.bufferSize = 16
	}
	if s.options.filter == nil {
		s.options.filter = func(messages Message) bool {
			return true
		}
	}
}

func (s *server) onMessage(rs goetty.IOSession, value any, sequence uint64) error {
	cs, err := s.getSession(rs)
	if err != nil {
		return err
	}
	request := value.(RPCMessage)
	if ce := s.logger.Check(zap.DebugLevel, "received request"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Uint64("request-id", request.Message.GetID()),
			zap.String("request", request.Message.DebugString()))
	}

	// Can't be sure that the Context is properly consumed if disableAutoCancelContext is set to
	// true. So we use the pessimistic wait for the context to time out automatically be canceled
	// behavior here, which may cause some resources to be released more slowly.
	// FIXME: Use the CancelFunc pass to let the handler decide to cancel itself
	if !s.options.disableAutoCancelContext && request.cancel != nil {
		defer request.cancel()
	}
	// get requestID here to avoid data race, because the request maybe released in handler
	requestID := request.Message.GetID()
	if err := s.handler(request.Ctx, request.Message, sequence, cs); err != nil {
		s.logger.Error("handle request failed",
			zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Error(err))
		return err
	}

	if ce := s.logger.Check(zap.DebugLevel, "handle request completed"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Uint64("request-id", requestID))
	}
	return nil
}

func (s *server) startWriteLoop(cs *clientSession) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		defer s.closeClientSession(cs)

		responses := make([]RPCMessage, 0, s.options.batchSendSize)
		fetch := func() {
			for i := 0; i < len(responses); i++ {
				responses[i] = RPCMessage{}
			}
			responses = responses[:0]

			for i := 0; i < s.options.batchSendSize; i++ {
				if len(responses) == 0 {
					select {
					case <-ctx.Done():
						responses = nil
						return
					case resp := <-cs.c:
						responses = append(responses, resp)
					}
				} else {
					select {
					case <-ctx.Done():
						responses = nil
						return
					case resp := <-cs.c:
						responses = append(responses, resp)
					default:
						return
					}
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				fetch()

				if len(responses) > 0 {
					var fields []zap.Field
					ce := s.logger.Check(zap.DebugLevel, "write responses")
					if ce != nil {
						fields = append(fields, zap.String("client", cs.conn.RemoteAddress()))
					}

					written := 0
					sendResponses := responses[:0]
					for idx := range responses {
						if s.options.filter(responses[idx].Message) {
							sendResponses = append(sendResponses, responses[idx])
						}
					}
					timeout := time.Duration(0)
					for idx := range sendResponses {
						v, err := sendResponses[idx].GetTimeoutFromContext()
						if err != nil {
							continue
						}
						timeout += v

						// Record the information of some responses in advance, because after flush,
						// these responses will be released, thus avoiding causing data race.
						if ce != nil {
							fields = append(fields, zap.Uint64("request-id",
								sendResponses[idx].Message.GetID()))
							fields = append(fields, zap.String("response",
								sendResponses[idx].Message.DebugString()))
						}
						if err := cs.conn.Write(sendResponses[idx], goetty.WriteOptions{}); err != nil {
							s.logger.Error("write response failed",
								zap.Uint64("request-id", sendResponses[idx].Message.GetID()),
								zap.Error(err))
							return
						}
						written++
					}

					if written > 0 {
						if err := cs.conn.Flush(timeout); err != nil {
							if ce != nil {
								fields = append(fields, zap.Error(err))
							}
						}
						if ce != nil {
							ce.Write(fields...)
						}
					}
				}

			}
		}
	})
}

func (s *server) closeClientSession(cs *clientSession) {
	s.sessions.Delete(cs.conn.ID())
	if err := cs.close(); err != nil {
		s.logger.Error("close client session failed",
			zap.Error(err))
	}
}

func (s *server) getSession(rs goetty.IOSession) (*clientSession, error) {
	if v, ok := s.sessions.Load(rs.ID()); ok {
		return v.(*clientSession), nil
	}

	cs := newClientSession(rs)
	v, loaded := s.sessions.LoadOrStore(rs.ID(), cs)
	if loaded {
		close(cs.c)
		return v.(*clientSession), nil
	}

	rs.Ref()
	if err := s.startWriteLoop(cs); err != nil {
		s.closeClientSession(cs)
		return nil, err
	}
	return cs, nil
}

type clientSession struct {
	conn goetty.IOSession
	c    chan RPCMessage

	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newClientSession(conn goetty.IOSession) *clientSession {
	return &clientSession{
		c:    make(chan RPCMessage, 16),
		conn: conn,
	}
}

func (cs *clientSession) close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.mu.closed {
		return nil
	}

	close(cs.c)
	cs.mu.closed = true
	return cs.conn.Close()
}

func (cs *clientSession) Write(ctx context.Context, message Message) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.mu.closed {
		return moerr.NewClientClosed()
	}

	cs.c <- RPCMessage{Ctx: ctx, Message: message}
	return nil
}
