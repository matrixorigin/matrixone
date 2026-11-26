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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// withTestReceivePool swaps the clients' receive pool for a fresh one, and
// checks at the end of the test that every receive buffer was freed.
func withTestReceivePool(t *testing.T) *mpool.MPool {
	mp := mpool.MustNewZero()
	old := logtailReceivePool
	logtailReceivePool = mp
	t.Cleanup(func() {
		logtailReceivePool = old
		require.Zero(t, mp.CurrNB(), "a receive buffer leaked")
	})
	return mp
}

func newReceiveTestClient(t *testing.T) (*LogtailClient, chan morpc.Message) {
	ctrl := gomock.NewController(t)
	stream := mock_morpc.NewMockStream(ctrl)
	recvC := make(chan morpc.Message, 64)
	stream.EXPECT().Receive().Return(recvC, nil)
	stream.EXPECT().ID().Return(uint64(1)).AnyTimes()
	stream.EXPECT().Close(true).AnyTimes().Return(nil)
	client, err := NewLogtailClient(t.Context(), stream)
	require.NoError(t, err)
	return client, recvC
}

func updateWithBatch(batch string) *LogtailResponse {
	from := timestamp.Timestamp{PhysicalTime: 1}
	return &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
		Response: newUpdateResponse(from, from.Next(), logtail.TableLogtail{
			Table:    &api.TableID{TbId: 7},
			Commands: []api.Entry{{TableName: "t", Bat: api.NewEncodedBatch([]byte(batch))}},
		}),
	}}
}

func receivedBatch(t *testing.T, resp *LogtailResponse) []byte {
	return resp.GetUpdateResponse().LogtailList[0].Commands[0].Bat.Bytes()
}

// A receive that fails part way through a response — cancelled, or on a
// broken stream — returns its buffer.
func TestLogtailClientReceiveFailureReturnsBuffer(t *testing.T) {
	for name, fail := range map[string]func(cancel context.CancelFunc, recvC chan morpc.Message){
		"cancelled":     func(cancel context.CancelFunc, _ chan morpc.Message) { cancel() },
		"stream-broken": func(_ context.CancelFunc, recvC chan morpc.Message) { close(recvC) },
	} {
		t.Run(name, func(t *testing.T) {
			mp := withTestReceivePool(t)
			client, recvC := newReceiveTestClient(t)
			segments := encodeSegmentedTestResponse(t, updateWithBatch(strings.Repeat("x", 200)), 32)
			require.Greater(t, len(segments), 2)
			recvC <- segments[0]
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				_, err := client.Receive(ctx)
				done <- err
			}()
			// Fail only once the response is part way through reassembly,
			// in its buffer.
			require.Eventually(t, func() bool { return mp.CurrNB() > 0 },
				10*time.Second, time.Millisecond)
			fail(cancel, recvC)
			require.Error(t, <-done)
			cancel()
			require.Len(t, client.recvBufs.free, 1, "the partial response's buffer is idle again")
			require.NoError(t, client.Close())
			require.Zero(t, mp.CurrNB(), "and freed by Close")
		})
	}
}

// An undecodable response returns its buffer, and the next response is
// received normally.
func TestLogtailClientReceiveCorruptResponse(t *testing.T) {
	withTestReceivePool(t)
	client, recvC := newReceiveTestClient(t)
	recvC <- &LogtailResponseSegment{MessageSegment: logtail.MessageSegment{
		MessageSize: 4, Sequence: 1, MaxSequence: 1, Payload: []byte{0xff, 0xff, 0xff, 0xff},
	}}
	_, err := client.Receive(t.Context())
	require.Error(t, err)

	for _, segment := range encodeSegmentedTestResponse(t, updateWithBatch("ok"), 1024) {
		recvC <- segment
	}
	resp, err := client.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), receivedBatch(t, resp))
	resp.Release()
	require.NoError(t, client.Close())
}

// A segment's MessageSize is only a hint: a wrong one still reassembles the
// response.
func TestLogtailClientReceiveIgnoresBadSizeHint(t *testing.T) {
	withTestReceivePool(t)
	client, recvC := newReceiveTestClient(t)
	for _, hint := range []int32{-1, 0, 3, 1 << 30} {
		for _, segment := range encodeSegmentedTestResponse(t, updateWithBatch(strings.Repeat("h", 100)), 40) {
			segment.MessageSize = hint
			recvC <- segment
		}
		resp, err := client.Receive(t.Context())
		require.NoError(t, err)
		require.Equal(t, []byte(strings.Repeat("h", 100)), receivedBatch(t, resp))
		resp.Release()
	}
	require.NoError(t, client.Close())
}

// A read-barrier control response is consumed inside Receive and returns
// its buffer to the client.
func TestLogtailClientBarrierResponseReturnsBuffer(t *testing.T) {
	mp := withTestReceivePool(t)
	client, recvC := newReceiveTestClient(t)
	barrier := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
		Response: newReadBarrierResponse(12345, timestamp.Timestamp{PhysicalTime: 3}),
	}}
	for _, segment := range encodeSegmentedTestResponse(t, barrier, 1024) {
		recvC <- segment
	}
	for _, segment := range encodeSegmentedTestResponse(t, updateWithBatch("after"), 1024) {
		recvC <- segment
	}
	resp, err := client.Receive(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("after"), receivedBatch(t, resp))
	require.Empty(t, client.recvBufs.free, "the barrier's buffer was reused for the update")
	resp.Release()
	require.Len(t, client.recvBufs.free, 1)
	require.NoError(t, client.Close())
	require.Zero(t, mp.CurrNB())
}

// Release is idempotent and safe on responses not from Receive; a response
// released after Close frees its buffer.
func TestLogtailResponseReleaseAfterClose(t *testing.T) {
	mp := withTestReceivePool(t)
	client, recvC := newReceiveTestClient(t)
	var held []*LogtailResponse
	for i := 0; i < 4; i++ {
		for _, segment := range encodeSegmentedTestResponse(t, updateWithBatch(strings.Repeat("r", 50+i)), 1024) {
			recvC <- segment
		}
		resp, err := client.Receive(t.Context())
		require.NoError(t, err)
		held = append(held, resp)
	}
	held[0].Release()
	require.NoError(t, client.Close())
	require.NotZero(t, mp.CurrNB(), "held responses keep their buffers past Close")
	for _, resp := range held {
		resp.Release()
		resp.Release()
	}
	(*LogtailResponse)(nil).Release()
	(&LogtailResponse{}).Release()
	require.Zero(t, mp.CurrNB())
}

// Responses released concurrently with Close neither leak nor race.
func TestLogtailResponseReleaseRacesClose(t *testing.T) {
	withTestReceivePool(t)
	client, recvC := newReceiveTestClient(t)
	var held []*LogtailResponse
	for i := 0; i < 16; i++ {
		for _, segment := range encodeSegmentedTestResponse(t, updateWithBatch(strings.Repeat("c", 100)), 64) {
			recvC <- segment
		}
		resp, err := client.Receive(t.Context())
		require.NoError(t, err)
		held = append(held, resp)
	}
	var wg sync.WaitGroup
	for _, resp := range held {
		wg.Add(1)
		go func(resp *LogtailResponse) {
			defer wg.Done()
			resp.Release()
		}(resp)
	}
	require.NoError(t, client.Close())
	wg.Wait()
}
