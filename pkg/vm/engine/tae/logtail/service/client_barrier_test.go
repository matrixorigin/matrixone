// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func newBarrierTestClient(
	t *testing.T,
	send func(*LogtailRequest) error,
) *LogtailClient {
	t.Helper()
	ctrl := gomock.NewController(t)
	stream := mock_morpc.NewMockStream(ctrl)
	stream.EXPECT().Receive().Return(make(chan morpc.Message), nil)
	stream.EXPECT().ID().AnyTimes().Return(uint64(1))
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, message morpc.Message) error {
			return send(message.(*LogtailRequest))
		},
	)
	stream.EXPECT().Close(true).AnyTimes().Return(nil)
	client, err := NewLogtailClient(t.Context(), stream)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func TestLogtailClientReadBarrierCorrelatesConcurrentResponses(t *testing.T) {
	requests := make(chan uint64, 2)
	client := newBarrierTestClient(t, func(request *LogtailRequest) error {
		requests <- request.GetReadBarrier().BarrierId
		return nil
	})

	type result struct {
		ts  timestamp.Timestamp
		err error
	}
	firstC := make(chan result, 1)
	go func() {
		ts, err := client.ReadBarrier(t.Context())
		firstC <- result{ts: ts, err: err}
	}()
	firstID := <-requests

	secondC := make(chan result, 1)
	go func() {
		ts, err := client.ReadBarrier(t.Context())
		secondC <- result{ts: ts, err: err}
	}()
	secondID := <-requests
	require.NotEqual(t, firstID, secondID)

	secondTS := timestamp.Timestamp{PhysicalTime: 22, LogicalTime: 2}
	client.completeReadBarrier(&logtail.ReadBarrierResponse{
		BarrierId: secondID,
		Timestamp: &secondTS,
	})
	second := <-secondC
	require.NoError(t, second.err)
	require.Equal(t, secondTS, second.ts)

	select {
	case <-firstC:
		t.Fatal("response for the second barrier completed the first request")
	default:
	}
	firstTS := timestamp.Timestamp{PhysicalTime: 11, LogicalTime: 1}
	client.completeReadBarrier(&logtail.ReadBarrierResponse{
		BarrierId: firstID,
		Timestamp: &firstTS,
	})
	first := <-firstC
	require.NoError(t, first.err)
	require.Equal(t, firstTS, first.ts)
}

func TestLogtailClientReceiveConsumesBarrierControlResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := mock_morpc.NewMockStream(ctrl)
	recvC := make(chan morpc.Message, 2)
	stream.EXPECT().Receive().Return(recvC, nil)
	stream.EXPECT().ID().Return(uint64(1))
	stream.EXPECT().Close(true).AnyTimes().Return(nil)

	frontier := timestamp.Timestamp{PhysicalTime: 33, LogicalTime: 3}
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message morpc.Message) error {
			request := message.(*LogtailRequest).GetReadBarrier()
			barrierSegment, err := encodeBarrierTestResponse(&LogtailResponse{
				LogtailResponse: logtail.LogtailResponse{
					Response: newReadBarrierResponse(request.BarrierId, frontier),
				},
			})
			if err != nil {
				return err
			}
			recvC <- barrierSegment
			updateTo := frontier.Next()
			updateSegment, err := encodeBarrierTestResponse(&LogtailResponse{
				LogtailResponse: logtail.LogtailResponse{
					Response: newUpdateResponse(frontier, updateTo),
				},
			})
			if err != nil {
				return err
			}
			recvC <- updateSegment
			return nil
		},
	)

	client, err := NewLogtailClient(t.Context(), stream)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	type receiveResult struct {
		response *LogtailResponse
		err      error
	}
	receiveC := make(chan receiveResult, 1)
	go func() {
		response, receiveErr := client.Receive(t.Context())
		receiveC <- receiveResult{response: response, err: receiveErr}
	}()

	got, err := client.ReadBarrier(t.Context())
	require.NoError(t, err)
	require.Equal(t, frontier, got)
	received := <-receiveC
	require.NoError(t, received.err)
	require.NotNil(t, received.response.GetUpdateResponse(),
		"barrier control responses must not escape to the logtail dispatcher")
}

func encodeBarrierTestResponse(
	response *LogtailResponse,
) (*LogtailResponseSegment, error) {
	payload, err := response.Marshal()
	if err != nil {
		return nil, err
	}
	return &LogtailResponseSegment{MessageSegment: logtail.MessageSegment{
		MessageSize: int32(len(payload)),
		Sequence:    1,
		MaxSequence: 1,
		Payload:     payload,
	}}, nil
}

func TestLogtailClientReadBarrierCancellationRemovesPendingRequest(t *testing.T) {
	requestSent := make(chan struct{}, 1)
	client := newBarrierTestClient(t, func(*LogtailRequest) error {
		requestSent <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := client.ReadBarrier(ctx)
		done <- err
	}()
	<-requestSent
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	client.barriers.Lock()
	pending := len(client.barriers.pending)
	client.barriers.Unlock()
	require.Zero(t, pending)
}

func TestLogtailClientReadBarrierCloseRemovesPendingRequest(t *testing.T) {
	requestSent := make(chan struct{}, 1)
	client := newBarrierTestClient(t, func(*LogtailRequest) error {
		requestSent <- struct{}{}
		return nil
	})

	done := make(chan error, 1)
	go func() {
		_, err := client.ReadBarrier(t.Context())
		done <- err
	}()
	<-requestSent
	require.NoError(t, client.Close())
	require.Error(t, <-done)

	client.barriers.Lock()
	pending := len(client.barriers.pending)
	client.barriers.Unlock()
	require.Zero(t, pending)
}

func TestLogtailClientReadBarrierRejectsMissingTimestamp(t *testing.T) {
	client := &LogtailClient{}
	client.barriers.pending = make(map[uint64]chan readBarrierResult)
	resultC := make(chan readBarrierResult, 1)
	client.barriers.pending[1] = resultC

	client.completeReadBarrier(&logtail.ReadBarrierResponse{BarrierId: 1})
	result := <-resultC
	require.ErrorContains(t, result.err, "has no timestamp")
	require.True(t, result.timestamp.IsEmpty())
}

func TestLogtailClientReadBarrierSendFailureBreaksStream(t *testing.T) {
	wantErr := errors.New("send failed")
	client := newBarrierTestClient(t, func(*LogtailRequest) error { return wantErr })

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.ReadBarrier(ctx)
	require.Error(t, err)
	require.True(t, client.streamBroken())
	require.Error(t, client.Subscribe(t.Context(), api.TableID{}),
		"requests after a broken stream must fail instead of entering an orphaned send queue")
}
