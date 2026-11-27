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
	"strings"
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

func TestLogtailClientReadBarrierBoundsPendingCorrelationState(t *testing.T) {
	requests := make(chan uint64, 2)
	client := newBarrierTestClient(t, func(request *LogtailRequest) error {
		requests <- request.GetReadBarrier().BarrierId
		return nil
	})
	client.readBarrierSlots = make(chan struct{}, 1)

	type result struct {
		ts  timestamp.Timestamp
		err error
	}
	firstDone := make(chan result, 1)
	go func() {
		ts, err := client.ReadBarrier(t.Context())
		firstDone <- result{ts: ts, err: err}
	}()
	firstID := <-requests

	blockedCtx, cancelBlocked := context.WithCancel(t.Context())
	blockedDone := make(chan error, 1)
	go func() {
		_, err := client.ReadBarrier(blockedCtx)
		blockedDone <- err
	}()
	cancelBlocked()
	require.ErrorIs(t, <-blockedDone, context.Canceled)
	select {
	case unexpected := <-requests:
		t.Fatalf("read-barrier correlation state exceeded its bound: %d", unexpected)
	default:
	}

	firstTS := timestamp.Timestamp{PhysicalTime: 1, LogicalTime: 1}
	client.completeReadBarrier(&logtail.ReadBarrierResponse{
		BarrierId: firstID,
		Timestamp: &firstTS,
	})
	first := <-firstDone
	require.NoError(t, first.err)
	require.Equal(t, firstTS, first.ts)

	lastDone := make(chan result, 1)
	go func() {
		ts, err := client.ReadBarrier(t.Context())
		lastDone <- result{ts: ts, err: err}
	}()
	lastID := <-requests
	lastTS := timestamp.Timestamp{PhysicalTime: 2, LogicalTime: 2}
	client.completeReadBarrier(&logtail.ReadBarrierResponse{
		BarrierId: lastID,
		Timestamp: &lastTS,
	})
	last := <-lastDone
	require.NoError(t, last.err)
	require.Equal(t, lastTS, last.ts)

	client.barriers.Lock()
	pending := len(client.barriers.pending)
	client.barriers.Unlock()
	require.Zero(t, pending)
	require.Empty(t, client.readBarrierSlots)
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
	require.Empty(t, client.readBarrierSlots)
	require.Error(t, client.Subscribe(t.Context(), api.TableID{}),
		"requests after a broken stream must fail instead of entering an orphaned send queue")
}

// encodeSegmentedTestResponse splits response's encoding into segments of
// at most limit bytes.
func encodeSegmentedTestResponse(
	t *testing.T, response *LogtailResponse, limit int,
) []*LogtailResponseSegment {
	payload, err := response.Marshal()
	require.NoError(t, err)
	chunks := Split(payload, limit)
	segments := make([]*LogtailResponseSegment, len(chunks))
	for i, chunk := range chunks {
		segments[i] = &LogtailResponseSegment{MessageSegment: logtail.MessageSegment{
			MessageSize: int32(len(payload)),
			Sequence:    int32(i + 1),
			MaxSequence: int32(len(chunks)),
			Payload:     chunk,
		}}
	}
	return segments
}

// Responses are reassembled into recycled receive buffers: their batches are
// views of the buffer until Release, a released buffer serves the next
// response, and a response still held gets a buffer of its own.
func TestLogtailClientReceiveReusesReleasedBuffers(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := mock_morpc.NewMockStream(ctrl)
	recvC := make(chan morpc.Message, 16)
	stream.EXPECT().Receive().Return(recvC, nil)
	stream.EXPECT().ID().Return(uint64(1)).AnyTimes()
	stream.EXPECT().Close(true).AnyTimes().Return(nil)
	client, err := NewLogtailClient(t.Context(), stream)
	require.NoError(t, err)

	push := func(batch string) {
		from := timestamp.Timestamp{PhysicalTime: 1}
		response := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
			Response: newUpdateResponse(from, from.Next(), logtail.TableLogtail{
				Table:    &api.TableID{TbId: 7},
				Commands: []api.Entry{{TableName: "t", Bat: api.NewEncodedBatch([]byte(batch))}},
			}),
		}}
		for _, segment := range encodeSegmentedTestResponse(t, response, 16) {
			recvC <- segment
		}
	}
	receive := func(want string) (*LogtailResponse, []byte) {
		response, err := client.Receive(t.Context())
		require.NoError(t, err)
		bat := response.GetUpdateResponse().LogtailList[0].Commands[0].Bat
		require.True(t, bat.IsView())
		require.Equal(t, []byte(want), bat.Bytes())
		return response, bat.Bytes()
	}
	batch := strings.Repeat("b", 100)

	push(batch)
	first, firstView := receive(batch)
	detached := first.GetUpdateResponse().LogtailList[0].Commands[0].Bat.Detach()
	first.Release()
	first.Release()

	allocated := logtailReceivePool.Stats().NumAlloc.Load()
	push(batch)
	second, secondView := receive(batch)
	require.True(t, &firstView[0] == &secondView[0], "the released buffer is reused")
	require.Equal(t, allocated, logtailReceivePool.Stats().NumAlloc.Load())
	require.Equal(t, []byte(batch), detached.Bytes(), "a detached batch survives the reuse")

	push(batch)
	third, thirdView := receive(batch)
	require.False(t, &secondView[0] == &thirdView[0], "a held response keeps its buffer")
	second.Release()

	before := logtailReceivePool.CurrNB()
	require.NoError(t, client.Close())
	third.Release()
	require.Less(t, logtailReceivePool.CurrNB(), before, "closing frees idle and later released buffers")
}
