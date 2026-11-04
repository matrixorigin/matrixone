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

package disttae

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/stretchr/testify/require"
)

type barrierReconnectStream struct {
	*closeRaceStream
	requests chan *service.LogtailRequest
}

func newBarrierReconnectStream() *barrierReconnectStream {
	return &barrierReconnectStream{
		closeRaceStream: newCloseRaceStream(),
		requests:        make(chan *service.LogtailRequest, 1),
	}
}

func (s *barrierReconnectStream) Send(ctx context.Context, message morpc.Message) error {
	select {
	case s.requests <- message.(*service.LogtailRequest):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type barrierReconnectResult struct {
	frontier timestamp.Timestamp
	err      error
}

func TestLogtailReadBarrierReconnectKeepsCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &PushClient{subscriber: newLogTailSubscriber()}
		owner := c.start(t.Context())
		oldStream := newBarrierReconnectStream()
		oldClient, err := service.NewLogtailClient(owner, oldStream)
		require.NoError(t, err)
		c.subscriber.swapClient(oldClient)
		c.subscriber.setReady()
		frontier := timestamp.Timestamp{PhysicalTime: 37, LogicalTime: 4}
		applyCalls := 0
		c.timestampWaiter = &barrierTimestampWaiter{get: func(ctx context.Context, target timestamp.Timestamp) (timestamp.Timestamp, error) {
			applyCalls++
			require.NoError(t, ctx.Err())
			require.Equal(t, frontier, target)
			return target.Next(), nil
		}}
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		result := make(chan barrierReconnectResult, 1)
		go func() {
			ts, err := c.AcquireLogtailReadBarrier(t.Context())
			result <- barrierReconnectResult{ts, err}
		}()
		request := <-oldStream.requests
		require.NotNil(t, request.GetReadBarrier())
		// Match init's generation handoff, before publishing the replacement.
		c.subscriber.setNotReady()
		require.Same(t, oldClient, c.subscriber.swapClient(nil))
		require.NoError(t, oldClient.Close())
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("live caller abandoned during reconnect: %v", got.err)
		default:
		}
		require.Zero(t, applyCalls, "retired generation must not publish an apply frontier")

		newStream := newBarrierReconnectStream()
		newClient, err := service.NewLogtailClient(owner, newStream)
		require.NoError(t, err)
		c.subscriber.swapClient(newClient)
		c.subscriber.setReady()
		receiveDone := make(chan struct{})
		go func() {
			_, _ = newClient.Receive(owner)
			close(receiveDone)
		}()
		request = <-newStream.requests
		response := &service.LogtailResponse{LogtailResponse: logtail.LogtailResponse{
			Response: &logtail.LogtailResponse_ReadBarrierResponse{ReadBarrierResponse: &logtail.ReadBarrierResponse{
				BarrierId: request.GetReadBarrier().BarrierId, Timestamp: &frontier,
			}},
		}}
		payload, err := response.Marshal()
		require.NoError(t, err)
		newStream.recv <- &service.LogtailResponseSegment{MessageSegment: logtail.MessageSegment{
			MessageSize: int32(len(payload)), Sequence: 1, MaxSequence: 1, Payload: payload,
		}}
		got := <-result
		require.NoError(t, got.err)
		require.Equal(t, frontier, got.frontier)
		require.Equal(t, 1, applyCalls)
		require.NoError(t, c.Close())
		<-receiveDone
	})
}

func TestLogtailReadBarrierReconnectCancellation(t *testing.T) {
	for _, mode := range []string{"caller", "owner", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := &PushClient{subscriber: newLogTailSubscriber()}
				owner := c.start(t.Context())
				stream := newBarrierReconnectStream()
				old, err := service.NewLogtailClient(owner, stream)
				require.NoError(t, err)
				c.subscriber.swapClient(old)
				c.subscriber.setReady()
				var applied atomic.Int32
				c.timestampWaiter = &barrierTimestampWaiter{get: func(_ context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) {
					applied.Add(1)
					return ts, nil
				}}
				t.Cleanup(func() { require.NoError(t, c.Close()) })
				ctx, cancel := context.WithCancel(t.Context())
				if mode == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), time.Second)
				}
				defer cancel()
				result := make(chan error, 1)
				go func() { _, err := c.AcquireLogtailReadBarrier(ctx); result <- err }()
				<-stream.requests
				c.subscriber.setNotReady()
				c.subscriber.swapClient(nil)
				require.NoError(t, old.Close())
				synctest.Wait()
				select {
				case err := <-result:
					t.Fatalf("request exited before %s cancellation: %v", mode, err)
				default:
				}
				if mode == "owner" {
					require.NoError(t, c.Close())
				} else if mode == "caller" {
					cancel()
				}
				want := context.Canceled
				if mode == "deadline" {
					want = context.DeadlineExceeded
				}
				require.ErrorIs(t, <-result, want)
				require.Zero(t, applied.Load())
			})
		})
	}
}

func TestLogtailReadBarrierDoesNotRetryUnreplacedClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &PushClient{subscriber: newLogTailSubscriber()}
		owner := c.start(t.Context())
		stream := newBarrierReconnectStream()
		transport, err := service.NewLogtailClient(owner, stream)
		require.NoError(t, err)
		c.subscriber.swapClient(transport)
		c.subscriber.setReady()
		c.timestampWaiter = &barrierTimestampWaiter{get: func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
			t.Error("failed transport must not reach the apply wait")
			return timestamp.Timestamp{}, nil
		}}
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		result := make(chan error, 1)
		go func() { _, err := c.AcquireLogtailReadBarrier(t.Context()); result <- err }()
		<-stream.requests
		require.NoError(t, transport.Close())
		require.ErrorIs(t, <-result, context.Canceled)
		require.NoError(t, owner.Err())
		require.Empty(t, stream.requests)
	})
}

func TestLogtailReadBarrierConcurrentReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers = 8
		c := &PushClient{subscriber: newLogTailSubscriber()}
		owner := c.start(t.Context())
		oldStream := newBarrierReconnectStream()
		old, err := service.NewLogtailClient(owner, oldStream)
		require.NoError(t, err)
		c.subscriber.swapClient(old)
		c.subscriber.setReady()
		frontier := timestamp.Timestamp{PhysicalTime: 83}
		var applied atomic.Int32
		c.timestampWaiter = &barrierTimestampWaiter{get: func(_ context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) {
			applied.Add(1)
			if !ts.Equal(frontier) {
				t.Errorf("wrong generation frontier: %v", ts)
			}
			return ts, nil
		}}
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		result := make(chan barrierReconnectResult, callers)
		for range callers {
			go func() {
				ts, err := c.AcquireLogtailReadBarrier(t.Context())
				result <- barrierReconnectResult{ts, err}
			}()
		}
		for range callers {
			<-oldStream.requests
		}
		c.subscriber.setNotReady()
		c.subscriber.swapClient(nil)
		require.NoError(t, old.Close())
		synctest.Wait()
		require.Empty(t, result)
		require.Zero(t, applied.Load())
		newStream := newBarrierReconnectStream()
		transport, err := service.NewLogtailClient(owner, newStream)
		require.NoError(t, err)
		c.subscriber.swapClient(transport)
		c.subscriber.setReady()
		receiveDone := make(chan struct{})
		go func() { _, _ = transport.Receive(owner); close(receiveDone) }()
		for range callers {
			request := <-newStream.requests
			response := &service.LogtailResponse{LogtailResponse: logtail.LogtailResponse{
				Response: &logtail.LogtailResponse_ReadBarrierResponse{ReadBarrierResponse: &logtail.ReadBarrierResponse{
					BarrierId: request.GetReadBarrier().BarrierId, Timestamp: &frontier,
				}},
			}}
			payload, err := response.Marshal()
			require.NoError(t, err)
			newStream.recv <- &service.LogtailResponseSegment{MessageSegment: logtail.MessageSegment{
				MessageSize: int32(len(payload)), Sequence: 1, MaxSequence: 1, Payload: payload,
			}}
		}
		for range callers {
			got := <-result
			require.NoError(t, got.err)
			require.Equal(t, frontier, got.frontier)
		}
		require.EqualValues(t, callers, applied.Load())
		require.NoError(t, c.Close())
		<-receiveDone
	})
}
