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

type server struct {
	name        string
	address     string
	logger      *zap.Logger
	application goetty.NetApplication
	stopper     *stopper.Stopper
	handler     func(request Message, sequence uint64, cs ClientSession) error
	sessions    *sync.Map // session-id => *clientSession
	options     struct {
		goettyOptions []goetty.Option
		bufferSize    int
		batchSendSize int
		filter        func(Message) bool
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
	err := s.application.Stop()
	if err != nil {
		s.logger.Error("stop rpcserver failed",
			zap.Error(err))
	}
	s.stopper.Stop()
	return err
}

func (s *server) RegisterRequestHandler(handler func(request Message, sequence uint64, cs ClientSession) error) {
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
	cs := s.getSession(rs)
	request := value.(Message)
	if ce := s.logger.Check(zap.DebugLevel, "received request"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Uint64("request-id", request.GetID()),
			zap.String("request", request.DebugString()))
	}

	err := s.handler(request, sequence, cs)
	if err != nil {
		s.logger.Error("handle request failed",
			zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Error(err))
		return err
	}

	if ce := s.logger.Check(zap.DebugLevel, "handle request completed"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Uint64("request-id", request.GetID()))
	}
	return nil
}

func (s *server) startWriteLoop(cs *clientSession) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		defer s.closeClientSession(cs)

		responses := make([]sendMessage, 0, s.options.batchSendSize)
		fetch := func() {
			for i := 0; i < len(responses); i++ {
				responses[i] = sendMessage{}
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
					written := 0
					sendResponses := responses[:0]
					for idx := range responses {
						if s.options.filter(responses[idx].message) {
							sendResponses = append(sendResponses, responses[idx])
						}
					}
					timeout := time.Duration(0)
					for idx := range sendResponses {
						timeout += sendResponses[idx].opts.Timeout
						if err := cs.conn.Write(sendResponses[idx].message, goetty.WriteOptions{}); err != nil {
							s.logger.Error("write response failed",
								zap.Uint64("request-id", sendResponses[idx].message.GetID()),
								zap.Error(err))
							return
						}
						written++
					}

					if written > 0 {
						if err := cs.conn.Flush(timeout); err != nil {
							for idx := range sendResponses {
								s.logger.Error("write response failed",
									zap.Uint64("request-id", sendResponses[idx].message.GetID()),
									zap.Error(err))
							}
							return
						}
						if ce := s.logger.Check(zap.DebugLevel, "write responses"); ce != nil {
							var fields []zap.Field
							fields = append(fields, zap.String("client", cs.conn.RemoteAddress()))
							for idx := range sendResponses {
								fields = append(fields, zap.Uint64("request-id",
									sendResponses[idx].message.GetID()))
								fields = append(fields, zap.String("response",
									sendResponses[idx].message.DebugString()))
							}

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

func (s *server) getSession(rs goetty.IOSession) *clientSession {
	if v, ok := s.sessions.Load(rs.ID()); ok {
		return v.(*clientSession)
	}

	cs := newClientSession(rs)
	v, loaded := s.sessions.LoadOrStore(rs.ID(), cs)
	if loaded {
		close(cs.c)
		return v.(*clientSession)
	}

	rs.Ref()
	if err := s.startWriteLoop(cs); err != nil {
		s.closeClientSession(cs)
	}
	return cs
}

type clientSession struct {
	conn goetty.IOSession
	c    chan sendMessage

	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newClientSession(conn goetty.IOSession) *clientSession {
	return &clientSession{
		c:    make(chan sendMessage, 16),
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

func (cs *clientSession) Write(message Message, opts SendOptions) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.mu.closed {
		return errClientClosed
	}

	cs.c <- sendMessage{message: message, opts: opts}
	return nil
}

type sendMessage struct {
	message Message
	opts    SendOptions
}
