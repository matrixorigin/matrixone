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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/stretchr/testify/require"
)

// withTestSendPool swaps the sessions' send pool for a fresh one, and checks
// at the end of the test that every session freed its buffer.
func withTestSendPool(t *testing.T) *mpool.MPool {
	mp := mpool.MustNewZero()
	old := logtailSendPool
	logtailSendPool = mp
	t.Cleanup(func() {
		logtailSendPool = old
		require.Zero(t, mp.CurrNB(), "a session's send buffer leaked")
	})
	return mp
}

// writeSession is a capture transport whose writes can be made to fail.
type writeSession struct {
	*captureSession
	fail error
}

func (s *writeSession) Write(ctx context.Context, message morpc.Message) error {
	if s.fail != nil {
		return s.fail
	}
	return s.captureSession.Write(ctx, message)
}

func newBufferTestSession(t *testing.T, cs morpc.ClientSession) *Session {
	logger := mockMOLogger()
	ss := NewSession(
		context.Background(), logger, NewLogtailResponsePool(),
		mockSessionErrorNotifier(logger.RawLogger()), mockMorpcStream(cs, 9, 1024),
		5*time.Second, time.Minute, time.Hour, time.Hour,
	)
	return ss
}

// drainSegments consumes the segments of n responses written to cs and
// returns each response's payload.
func drainSegments(t *testing.T, cs *captureSession, stream morpcStream, n int) [][]byte {
	var payloads [][]byte
	for len(payloads) < n {
		var payload []byte
		for {
			seg := (<-cs.writes).(*LogtailResponseSegment)
			payload = append(payload, seg.Payload...)
			last := seg.Sequence == seg.MaxSequence
			stream.segments.Release(seg)
			if last {
				break
			}
		}
		payloads = append(payloads, payload)
	}
	return payloads
}

// A session reuses one send buffer for all its responses, and frees it when
// the session is cleaned.
func TestSessionSendBufferReusedAndFreedOnClean(t *testing.T) {
	mp := withTestSendPool(t)
	cs := newCaptureSession()
	ss := newBufferTestSession(t, cs)
	table := mockTable(1, 2, 3)

	message := strings.Repeat("m", 3000) // several segments
	require.NoError(t, ss.SendErrorResponse(context.Background(), table, moerr.ErrInternal, message))
	drainSegments(t, cs, ss.stream, 1)
	allocated := mp.Stats().NumAlloc.Load()
	for i := 0; i < 5; i++ {
		require.NoError(t, ss.SendErrorResponse(context.Background(), table, moerr.ErrInternal, message[:1000*(i%3+1)]))
	}
	for _, payload := range drainSegments(t, cs, ss.stream, 5) {
		var resp logtail.LogtailResponse
		require.NoError(t, resp.Unmarshal(payload))
		require.NotNil(t, resp.GetError())
	}
	require.Equal(t, allocated, mp.Stats().NumAlloc.Load(), "the buffer is reused")
	require.NotZero(t, mp.CurrNB(), "and kept between responses")

	ss.PostClean()
	require.Zero(t, mp.CurrNB())
}

// The sender frees its buffer on every exit: a failed send, a closed
// transport, and a cancelled session.
func TestSessionSendBufferFreedOnEverySenderExit(t *testing.T) {
	table := mockTable(1, 2, 3)
	cases := map[string]func(*testing.T, *Session, *writeSession){
		"send-fails": func(t *testing.T, ss *Session, cs *writeSession) {
			cs.fail = errors.New("broken transport")
			// The failed send ends the sender; the notifier cleans the session.
			_ = ss.SendErrorResponse(context.Background(), table, moerr.ErrInternal, strings.Repeat("x", 2000))
		},
		"transport-closed": func(t *testing.T, ss *Session, cs *writeSession) {
			require.NoError(t, cs.Close())
		},
		"session-cleaned": func(t *testing.T, ss *Session, cs *writeSession) {},
	}
	for name, end := range cases {
		t.Run(name, func(t *testing.T) {
			mp := withTestSendPool(t)
			cs := &writeSession{captureSession: newCaptureSession()}
			ss := newBufferTestSession(t, cs)
			// Warm the buffer first, so each exit has memory to free.
			require.NoError(t, ss.SendErrorResponse(context.Background(), table, moerr.ErrInternal, "warm"))
			drainSegments(t, cs.captureSession, ss.stream, 1)
			require.NotZero(t, mp.CurrNB())

			end(t, ss, cs)
			ss.PostClean()
			require.Zero(t, mp.CurrNB())
		})
	}
}

// write frees a buffer that grew past its retained bound, and keeps a
// smaller one.
func TestStreamWriteRetainsOnlyBoundedBuffer(t *testing.T) {
	cs := newCaptureSession()
	t.Cleanup(func() { require.NoError(t, cs.Close()) })
	stream := mockMorpcStream(cs, 17, 1024)
	mp := mpool.MustNewZero()
	buf := mpool.NewMessageBuffer(mp, 2048)
	send := func(n int) {
		response := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
			Response: &logtail.LogtailResponse_Error{Error: &logtail.ErrorResponse{
				Status: logtail.Status{Message: strings.Repeat("x", n)},
			}},
		}}
		require.NoError(t, stream.write(context.Background(), response, &buf))
		drainSegments(t, cs, stream, 1)
	}
	send(500)
	require.NotZero(t, mp.CurrNB())
	send(5000)
	require.Zero(t, mp.CurrNB(), "a buffer past its bound is freed after the send")
	send(10)
	buf.Free()
	require.Zero(t, mp.CurrNB())
}

// failingEncoder makes an Entry's marshal fail.
type failingEncoder struct{}

func (failingEncoder) EncodeTo([]byte) error { return errors.New("batch changed") }

// A response that fails to marshal is not sent, and leaves the buffer
// reusable and leak-free.
func TestStreamWriteMarshalFailure(t *testing.T) {
	cs := newCaptureSession()
	t.Cleanup(func() { require.NoError(t, cs.Close()) })
	stream := mockMorpcStream(cs, 17, 1024)
	mp := mpool.MustNewZero()
	buf := mpool.NewMessageBuffer(mp, 1<<20)
	table := mockTable(1, 2, 3)
	bad := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
		Response: newUpdateResponse(mockTimestamp(1, 0), mockTimestamp(2, 0), logtail.TableLogtail{
			Table:    &table,
			Commands: []api.Entry{{TableName: "t", Bat: api.NewBorrowedEncodedBatch(failingEncoder{}, 64)}},
		}),
	}}
	require.Error(t, stream.write(context.Background(), bad, &buf))
	require.Empty(t, cs.writes, "nothing is sent")

	good := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
		Response: newUpdateResponse(mockTimestamp(1, 0), mockTimestamp(2, 0)),
	}}
	require.NoError(t, stream.write(context.Background(), good, &buf))
	drainSegments(t, cs, stream, 1)
	buf.Free()
	require.Zero(t, mp.CurrNB())
}

// Segments copy their chunks: a response already handed to the transport
// is intact after the send buffer is reused and even freed (and poisoned).
func TestStreamSegmentsOutliveSendBuffer(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	defer mpool.DisableDebugPoisonOnFree()
	cs := newCaptureSession()
	t.Cleanup(func() { require.NoError(t, cs.Close()) })
	stream := mockMorpcStream(cs, 17, 1024)
	mp := mpool.MustNewZero()
	buf := mpool.NewMessageBuffer(mp, 1<<20)
	message := strings.Repeat("a", 3000)
	first := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
		Response: &logtail.LogtailResponse_Error{Error: &logtail.ErrorResponse{Status: logtail.Status{Message: message}}},
	}}
	require.NoError(t, stream.write(context.Background(), first, &buf))
	var held []*LogtailResponseSegment
	for len(held) == 0 || held[len(held)-1].Sequence != held[len(held)-1].MaxSequence {
		held = append(held, (<-cs.writes).(*LogtailResponseSegment))
	}
	second := &LogtailResponse{LogtailResponse: logtail.LogtailResponse{
		Response: &logtail.LogtailResponse_Error{Error: &logtail.ErrorResponse{Status: logtail.Status{Message: strings.Repeat("b", 3000)}}},
	}}
	require.NoError(t, stream.write(context.Background(), second, &buf))
	drainSegments(t, cs, stream, 1)
	buf.Free()
	require.Zero(t, mp.CurrNB())

	var payload []byte
	for _, seg := range held {
		payload = append(payload, seg.Payload...)
		stream.segments.Release(seg)
	}
	var got logtail.LogtailResponse
	require.NoError(t, got.Unmarshal(payload))
	require.Equal(t, message, got.GetError().Status.Message)
}
