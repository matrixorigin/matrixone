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

	"go.uber.org/ratelimit"

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

// LogtailClient encapsulates morpc stream.
type LogtailClient struct {
	stream   morpc.Stream
	recvChan chan morpc.Message

	options struct {
		rps int
	}

	limiter ratelimit.Limiter
}

// NewLogtailClient constructs LogtailClient.
func NewLogtailClient(stream morpc.Stream, opts ...ClientOption) (*LogtailClient, error) {
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

	return client, nil
}

// Close closes stream.
func (c *LogtailClient) Close() error {
	return c.stream.Close(false)
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
		prev = segment
	}

	resp := &LogtailResponse{}
	if err := resp.Unmarshal(buf); err != nil {
		return nil, err
	}
	return resp, nil
}
