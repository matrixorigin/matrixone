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
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/stop"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	attrClientSession = "cs"
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
func WithServerWriteFilter(filter func([]Message) []Message) ServerOption {
	return func(s *server) {
		s.options.filter = filter
	}
}

// WithServerBatchSendSize set the maximum number of messages to be sent together
// at each batch. Default is 8.
func WithServerBatchSendSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.batchSendSize = size
	}
}

type server struct {
	name        string
	address     string
	logger      *zap.Logger
	application goetty.NetApplication
	stopper     *stop.Stopper
	handler     func(request Message, sequence uint64, cs ClientSession) error

	options struct {
		bufferSize    int
		batchSendSize int
		filter        func([]Message) []Message
	}
}

// NewRPCServer create rpc server with options. After the rpc server starts, one link corresponds to two
// goroutines, one read and one write. All messages to be written are first written to a buffer chan and
// sent to the client by the write goroutine.
func NewRPCServer(name, address string, codec Codec, options ...ServerOption) (RPCServer, error) {
	s := &server{
		name:    name,
		address: address,
		stopper: stop.NewStopper(fmt.Sprintf("rpc-server-%s", name)),
	}
	for _, opt := range options {
		opt(s)
	}
	s.adjust()

	app, err := goetty.NewApplication(
		s.address,
		s.onMessage,
		goetty.WithAppLogger(s.logger),
		goetty.WithAppSessionOptions(
			goetty.WithCodec(codec, codec),
			goetty.WithLogger(s.logger),
		),
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
		s.options.filter = func(messages []Message) []Message {
			return messages
		}
	}
}

func (s *server) onMessage(rs goetty.IOSession, value interface{}, sequence uint64) error {
	var cs *clientSession
	if sequence == 1 {
		cs = newClientSession()
		rs.SetAttr(attrClientSession, cs)
		if err := s.startWriteLoop(rs, cs); err != nil {
			cs.close()
			return err
		}
	} else {
		cs = rs.GetAttr(attrClientSession).(*clientSession)
	}

	request := value.(Message)
	if ce := s.logger.Check(zap.DebugLevel, "received request"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddr()),
			zap.String("request-id", hex.EncodeToString(request.ID())),
			zap.String("request", request.DebugString()))
	}

	err := s.handler(request, sequence, cs)
	if err != nil {
		s.logger.Error("handle request failed",
			zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddr()),
			zap.Error(err))
		return err
	}

	if ce := s.logger.Check(zap.DebugLevel, "handle request completed"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddr()),
			zap.String("request-id", hex.EncodeToString(request.ID())))
	}
	return nil
}

func (s *server) startWriteLoop(rs goetty.IOSession, cs *clientSession) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		defer cs.close()

		responses := make([]Message, 0, s.options.batchSendSize)
		fetch := func() {
			for i := 0; i < len(responses); i++ {
				responses[i] = nil
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
					sendResponses := s.options.filter(responses)
					for _, resp := range sendResponses {
						if err := rs.Write(resp, goetty.WriteOptions{}); err != nil {
							s.logger.Error("write response failed",
								zap.String("request-id", hex.EncodeToString(resp.ID())),
								zap.Error(err))
							return
						}
						written++
					}

					if written > 0 {
						if err := rs.Flush(0); err != nil {
							for _, f := range sendResponses {
								s.logger.Error("write response failed",
									zap.String("request-id", hex.EncodeToString(f.ID())),
									zap.Error(err))
							}
							return
						}
						if ce := s.logger.Check(zap.DebugLevel, "write responses"); ce != nil {
							var fields []zap.Field
							fields = append(fields, zap.String("client", rs.RemoteAddr()))
							for _, r := range sendResponses {
								fields = append(fields, zap.String("request-id",
									hex.EncodeToString(r.ID())))
								fields = append(fields, zap.String("response",
									r.DebugString()))
							}

							ce.Write(fields...)
						}
					}
				}

			}
		}
	})
}

type clientSession struct {
	c chan Message

	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newClientSession() *clientSession {
	return &clientSession{
		c: make(chan Message, 16),
	}
}

func (cs *clientSession) close() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.mu.closed {
		return
	}

	cs.mu.closed = true
}

func (cs *clientSession) Write(msg Message) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.mu.closed {
		return errClientClosed
	}

	cs.c <- msg
	return nil
}
