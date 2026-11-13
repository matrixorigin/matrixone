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
	"fmt"
	"sync"
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

func TestLogtailClientReadTableTargetCarriesTableIdentity(t *testing.T) {
	requests := make(chan *logtail.ReadBarrierRequest, 1)
	client := newBarrierTestClient(t, func(request *LogtailRequest) error {
		requests <- request.GetReadBarrier()
		return nil
	})
	table := api.TableID{DbId: 1, TbId: 2, AccId: 3, DbName: "mo_catalog", TbName: "policy"}
	done := make(chan readBarrierResult, 1)
	go func() {
		ts, err := client.ReadTableTarget(t.Context(), table)
		done <- readBarrierResult{timestamp: ts, err: err}
	}()
	request := <-requests
	require.Equal(t, &table, request.GetTableReadTarget())
	want := timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 5}
	client.completeReadBarrier(&logtail.ReadBarrierResponse{BarrierId: request.BarrierId, Timestamp: &want})
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, want, got.timestamp)
}

func TestLogtailClientPeerTableReadTargetCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		t.Run(fmt.Sprintf("supported_%t", supported), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			recvC := make(chan morpc.Message, 1)
			stream.EXPECT().Receive().Return(recvC, nil)
			stream.EXPECT().Close(true).AnyTimes().Return(nil)
			peer, err := NewLogtailClient(t.Context(), stream)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, peer.Close()) })
			response := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
				Response: &logtail.LogtailResponse_SubscribeResponse{SubscribeResponse: &logtail.SubscribeResponse{
					SupportsReadBarrier: true, SupportsTableReadTarget: supported,
				}},
			}}
			segment, err := encodeBarrierTestResponse(response)
			require.NoError(t, err)
			recvC <- segment
			_, err = peer.Receive(t.Context())
			require.NoError(t, err)
			got, err := peer.PeerSupportsTableReadTarget(t.Context())
			require.NoError(t, err)
			require.Equal(t, supported, got)
		})
	}
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

type observedCapabilityContext struct {
	context.Context
	waiting chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *observedCapabilityContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.waiting)
		if c.release != nil {
			<-c.release
		}
	})
	return c.Context.Done()
}

func TestLogtailClientPeerReadBarrierCapability(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		advertised, explicitFalse bool
	}{
		{name: "true", advertised: true},
		{name: "absent"},
		{name: "explicit false", explicitFalse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			recvC := make(chan morpc.Message, 4)
			stream.EXPECT().Receive().Return(recvC, nil)
			stream.EXPECT().Close(true).AnyTimes().Return(nil)
			peer, err := NewLogtailClient(t.Context(), stream)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, peer.Close()) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type result struct {
				value bool
				err   error
			}
			results := make(chan result, 3)
			for range 3 {
				waiting := &observedCapabilityContext{Context: ctx, waiting: make(chan struct{})}
				go func() { value, e := peer.PeerSupportsReadBarrier(waiting); results <- result{value, e} }()
				select {
				case <-waiting.waiting:
				case <-ctx.Done():
					t.Fatal("capability wait did not start")
				}
			}
			select {
			case got := <-results:
				t.Fatalf("unknown capability returned: %+v", got)
			default:
			}
			sendSubscription := func(advertised, explicitFalse bool) {
				sub := &logtail.SubscribeResponse{SupportsReadBarrier: advertised}
				if explicitFalse {
					sub.XXX_unrecognized = []byte{0x10, 0x00}
				}
				resp := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{Response: &logtail.LogtailResponse_SubscribeResponse{SubscribeResponse: sub}}}
				segment, e := encodeBarrierTestResponse(resp)
				require.NoError(t, e)
				recvC <- segment
			}
			sendSubscription(tc.advertised, tc.explicitFalse)
			received := make(chan error, 1)
			ownerRelease := make(chan struct{})
			var releaseOnce sync.Once
			releaseOwner := func() { releaseOnce.Do(func() { close(ownerRelease) }) }
			defer releaseOwner()
			go func() { _, e := peer.Receive(ctx); received <- e; <-ownerRelease }()
			for range 3 {
				select {
				case got := <-results:
					require.NoError(t, got.err)
					require.Equal(t, tc.advertised, got.value)
				case <-ctx.Done():
					t.Fatal("capability wait did not finish")
				}
			}
			require.NoError(t, <-received)
			// The owner has not dispatched the response, but the decoded capability
			// is already published to all waiters.
			releaseOwner()
			sendSubscription(tc.advertised, tc.explicitFalse)
			_, err = peer.Receive(ctx)
			require.NoError(t, err)
			sendSubscription(!tc.advertised, false)
			_, err = peer.Receive(ctx)
			require.ErrorContains(t, err, "inconsistent")
			_, err = peer.PeerSupportsReadBarrier(ctx)
			require.Error(t, err, "a broken peer must not select a fallback")
		})
	}
}

func TestLogtailClientPeerCapabilityWaitStops(t *testing.T) {
	for _, how := range []string{"caller canceled", "peer closed", "peer broken", "malformed response", "error response"} {
		t.Run(how, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			recvC := make(chan morpc.Message, 1)
			stream.EXPECT().Receive().Return(recvC, nil)
			stream.EXPECT().Close(true).AnyTimes().Return(nil)
			peer, err := NewLogtailClient(t.Context(), stream)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, peer.Close()) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			waiting := &observedCapabilityContext{Context: ctx, waiting: make(chan struct{})}
			done := make(chan error, 1)
			go func() { _, e := peer.PeerSupportsReadBarrier(waiting); done <- e }()
			select {
			case <-waiting.waiting:
			case <-ctx.Done():
				t.Fatal("capability wait did not start")
			}
			switch how {
			case "caller canceled":
				cancel()
			case "peer closed":
				require.NoError(t, peer.Close())
			case "peer broken":
				peer.markStreamBroken()
			case "malformed response":
				recvC <- &LogtailResponseSegment{MessageSegment: logtail.MessageSegment{MessageSize: 1, Sequence: 1, MaxSequence: 1, Payload: []byte{0xff}}}
				_, err = peer.Receive(ctx)
				require.Error(t, err)
			case "error response":
				segment, e := encodeBarrierTestResponse(&LogtailResponse{LogtailResponse: logtail.LogtailResponse{
					Response: &logtail.LogtailResponse_Error{Error: &logtail.ErrorResponse{}},
				}})
				require.NoError(t, e)
				recvC <- segment
				response, e := peer.Receive(ctx)
				require.NoError(t, e)
				require.NotNil(t, response.GetError())
			}
			if how == "malformed response" || how == "error response" {
				select {
				case <-peer.capabilityReady:
					t.Fatalf("%s published a capability", how)
				default:
				}
				select {
				case err := <-done:
					t.Fatalf("%s resolved capability: %v", how, err)
				default:
				}
				cancel()
			}
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("capability wait did not stop")
			}
		})
	}
}

func TestLogtailClientPeerCapabilityStopWinsReady(t *testing.T) {
	for _, how := range []string{"caller canceled", "peer closed", "peer broken"} {
		t.Run(how, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			recvC := make(chan morpc.Message, 1)
			stream.EXPECT().Receive().Return(recvC, nil)
			stream.EXPECT().Close(true).AnyTimes().Return(nil)
			peer, err := NewLogtailClient(t.Context(), stream)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, peer.Close()) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			waiting := &observedCapabilityContext{Context: ctx, waiting: make(chan struct{}), release: release}
			done := make(chan error, 1)
			go func() { _, e := peer.PeerSupportsReadBarrier(waiting); done <- e }()
			select {
			case <-waiting.waiting:
			case <-ctx.Done():
				t.Fatal("capability wait did not start")
			}
			segment, err := encodeBarrierTestResponse(&LogtailResponse{LogtailResponse: logtail.LogtailResponse{
				Response: &logtail.LogtailResponse_SubscribeResponse{SubscribeResponse: &logtail.SubscribeResponse{}},
			}})
			require.NoError(t, err)
			recvC <- segment
			_, err = peer.Receive(ctx)
			require.NoError(t, err)
			switch how {
			case "caller canceled":
				cancel()
			case "peer closed":
				require.NoError(t, peer.Close())
			case "peer broken":
				peer.markStreamBroken()
			}
			unblock()
			select {
			case err := <-done:
				require.Error(t, err, "a stopped peer must not select legacy fallback")
			case <-time.After(5 * time.Second):
				t.Fatal("capability wait did not stop")
			}
		})
	}
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
