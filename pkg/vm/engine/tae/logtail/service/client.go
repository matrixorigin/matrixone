// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"time"

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/multierr"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

type ClientOption func(*LogtailClient)

func WithClientRequestPerSecond(rps int) ClientOption {
	return func(c *LogtailClient) {
		c.options.rps = rps
	}
}

func WithClientResponsePool(p LogtailResponsePool) ClientOption {
	return func(c *LogtailClient) {
		c.pool.responses = p
	}
}

func WithClientSegmentPool(p LogtailResponseSegmentPool) ClientOption {
	return func(c *LogtailClient) {
		c.pool.segments = p
	}
}

// LogtailClient encapsulates morpc stream.
type LogtailClient struct {
	stream   morpc.Stream
	recvChan chan morpc.Message

	options struct {
		rps int
	}

	limiter ratelimit.Limiter

	pool struct {
		segments  LogtailResponseSegmentPool
		responses LogtailResponsePool
	}
}

// NewLogtailClient constructs LogtailClient.
func NewLogtailClient(
	stream morpc.Stream, opts ...ClientOption,
) (*LogtailClient, error) {
	client := &LogtailClient{
		stream: stream,
	}

	recvChan, err := stream.Receive()
	if err != nil {
		return nil, err
	}
	client.recvChan = recvChan

	client.options.rps = 200
	for _, opt := range opts {
		opt(client)
	}
	client.limiter = ratelimit.New(client.options.rps)

	if client.pool.segments == nil {
		client.pool.segments = NewLogtailResponseSegmentPool()
	}
	if client.pool.responses == nil {
		client.pool.responses = NewLogtailResponsePool()
	}

	return client, nil
}

// Close closes stream.
func (c *LogtailClient) Close() error {
	return c.stream.Close()
}

// Subscribe subscribes table.
func (c *LogtailClient) Subscribe(
	ctx context.Context, table api.TableID,
) error {
	c.limiter.Take()

	request := &LogtailRequest{}
	request.Request = &logtail.LogtailRequest_SubscribeTable{
		SubscribeTable: &logtail.SubscribeRequest{
			Table: &table,
		},
	}
	request.SetID(c.stream.ID())
	return c.stream.Send(ctx, request)
}

// Unsubscribe cancel subscription for table.
func (c *LogtailClient) Unsubscribe(
	ctx context.Context, table api.TableID,
) error {
	c.limiter.Take()

	request := &LogtailRequest{}
	request.Request = &logtail.LogtailRequest_UnsubscribeTable{
		UnsubscribeTable: &logtail.UnsubscribeRequest{
			Table: &table,
		},
	}
	request.SetID(c.stream.ID())
	return c.stream.Send(ctx, request)
}

// Receive fetches logtail response.
//
// 1. response for error: *LogtailResponse.GetError() != nil
// 2. response for subscription: *LogtailResponse.GetSubscribeResponse() != nil
// 3. response for unsubscription: *LogtailResponse.GetUnsubscribeResponse() != nil
// 3. response for incremental logtail: *LogtailResponse.GetUpdateResponse() != nil
func (c *LogtailClient) Receive() (*LogtailResponse, error) {
	recvFunc := func() (*LogtailResponseSegment, error) {
		message, ok := <-c.recvChan
		if !ok || message == nil {
			return nil, moerr.NewStreamClosedNoCtx()
		}
		return message.(*LogtailResponseSegment), nil
	}

	prev, err := recvFunc()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, prev.MessageSize)
	buf = AppendChunk(buf, prev.GetPayload())

	for prev.Sequence < prev.MaxSequence {
		segment, err := recvFunc()
		if err != nil {
			return nil, err
		}
		buf = AppendChunk(buf, segment.GetPayload())
		c.pool.segments.Release(prev)
		prev = segment
	}

	resp := c.pool.responses.Acquire()
	if err := resp.Unmarshal(buf); err != nil {
		return nil, err
	}
	return resp, nil
}

// LogtailStream encapsulates morpc stream.
//
// FIXME: replace LogtailClient with LogtailStream
type LogtailStream struct {
	address string
	rps     int

	pool struct {
		segments  LogtailResponseSegmentPool
		responses LogtailResponsePool
	}

	rpcClient morpc.RPCClient

	logtailClient *LogtailClient
}

// NewDefaultLogtailStream constructs default logtail stream.
func NewDefaultLogtailStream(
	address string,
	rps int,
	responses LogtailResponsePool,
	connectTimeout time.Duration,
	maxBackendPerHost int,
	readBufferSize int,
	writeBufferSize int,
	clientTag string,
	logger *zap.Logger,
) (*LogtailStream, error) {
	// construct morpc.CodecOption
	var codecOpts []morpc.CodecOption

	// construct morpc.BackendOption
	backendOpts := []morpc.BackendOption{
		morpc.WithBackendConnectTimeout(connectTimeout),
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(readBufferSize, writeBufferSize),
		),
		morpc.WithBackendLogger(logger),
	}

	// construct morpc.ClientOption
	clientOpts := []morpc.ClientOption{
		morpc.WithClientMaxBackendPerHost(maxBackendPerHost),
		morpc.WithClientTag(clientTag),
		morpc.WithClientLogger(logger),
	}

	return NewLogtailStream(
		address, rps, responses, codecOpts, backendOpts, clientOpts,
	)
}

// NewLogtailStream constructs logtail stream.
func NewLogtailStream(
	address string,
	rps int,
	responses LogtailResponsePool,
	codecOpts []morpc.CodecOption,
	backendOpts []morpc.BackendOption,
	clientOpts []morpc.ClientOption,
) (*LogtailStream, error) {
	ls := &LogtailStream{
		address: address,
		rps:     rps,
	}
	ls.pool.responses = responses
	ls.pool.segments = NewLogtailResponseSegmentPool()

	codec := morpc.NewMessageCodec(func() morpc.Message {
		return ls.pool.segments.Acquire()
	}, codecOpts...)
	bf := morpc.NewGoettyBasedBackendFactory(codec, backendOpts...)
	rpcClient, err := morpc.NewClient(bf, clientOpts...)
	if err != nil {
		return nil, err
	}
	ls.rpcClient = rpcClient

	rpcStream, err := rpcClient.NewStream(address, true)
	if err != nil {
		return nil, err
	}
	logtailClient, err := NewLogtailClient(rpcStream,
		WithClientRequestPerSecond(rps),
		WithClientResponsePool(ls.pool.responses),
		WithClientSegmentPool(ls.pool.segments),
	)
	if err != nil {
		return nil, err
	}
	ls.logtailClient = logtailClient

	return ls, nil
}

// Close closes logtail stream.
func (s *LogtailStream) Close() error {
	var err error
	if e := s.logtailClient.Close(); e != nil {
		err = multierr.Append(err, e)
	}
	if e := s.rpcClient.Close(); e != nil {
		err = multierr.Append(err, e)
	}
	return err
}

// Subscribe subscribes table.
func (s *LogtailStream) Subscribe(
	ctx context.Context, table api.TableID,
) error {
	return s.logtailClient.Subscribe(ctx, table)
}

// Unsubscribe cancel subscription for table.
func (s *LogtailStream) Unsubscribe(
	ctx context.Context, table api.TableID,
) error {
	return s.logtailClient.Unsubscribe(ctx, table)
}

// Receive fetches logtail response.
//
// 1. response for error: *LogtailResponse.GetError() != nil
// 2. response for subscription: *LogtailResponse.GetSubscribeResponse() != nil
// 3. response for unsubscription: *LogtailResponse.GetUnsubscribeResponse() != nil
// 3. response for incremental logtail: *LogtailResponse.GetUpdateResponse() != nil
func (s *LogtailStream) Receive() (*LogtailResponse, error) {
	return s.logtailClient.Receive()
}
