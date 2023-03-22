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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestMain(m *testing.M) {
	// make responseBufferSize small enough temporarily
	// Larger buffer size would have a negative effect on CI.
	original := responseBufferSize
	responseBufferSize = 1024
	ret := m.Run()
	responseBufferSize = original
	os.Exit(ret)
}

func TestSessionManger(t *testing.T) {
	sm := NewSessionManager()

	ctx := context.Background()

	// constructs mocker
	logger := mockMOLogger()
	pooler := NewLogtailResponsePool()
	notifier := mockSessionErrorNotifier(logger.RawLogger())
	sendTimeout := 5 * time.Second
	poisionTime := 10 * time.Millisecond
	heartbeatInterval := 50 * time.Millisecond
	chunkSize := 1024

	/* ---- 1. register sessioin A ---- */
	csA := mockNormalClientSession(logger.RawLogger())
	streamA := mockMorpcStream(csA, 10, chunkSize)
	sessionA := sm.GetSession(
		ctx, logger, pooler, notifier, streamA,
		sendTimeout, poisionTime, heartbeatInterval,
	)
	require.NotNil(t, sessionA)
	require.Equal(t, 1, len(sm.ListSession()))

	/* ---- 2. register sessioin B ---- */
	csB := mockNormalClientSession(logger.RawLogger())
	streamB := mockMorpcStream(csB, 11, chunkSize)
	sessionB := sm.GetSession(
		ctx, logger, pooler, notifier, streamB,
		sendTimeout, poisionTime, heartbeatInterval,
	)
	require.NotNil(t, sessionB)
	require.Equal(t, 2, len(sm.ListSession()))

	/* ---- 3. delete sessioin ---- */
	sm.DeleteSession(streamA)
	require.Equal(t, 1, len(sm.ListSession()))
	sm.DeleteSession(streamB)
	require.Equal(t, 0, len(sm.ListSession()))
}

func TestSessionError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// constructs mocker
	logger := mockMOLogger()
	pooler := NewLogtailResponsePool()
	notifier := mockSessionErrorNotifier(logger.RawLogger())
	cs := mockBrokenClientSession()
	stream := mockMorpcStream(cs, 10, 1024)
	sendTimeout := 5 * time.Second
	poisionTime := 10 * time.Millisecond
	heartbeatInterval := 50 * time.Millisecond

	tableA := mockTable(1, 2, 3)
	ss := NewSession(
		ctx, logger, pooler, notifier, stream,
		sendTimeout, poisionTime, heartbeatInterval,
	)

	/* ---- 1. send subscription response ---- */
	err := ss.SendSubscriptionResponse(
		context.Background(),
		logtail.TableLogtail{
			Table: &tableA,
		},
	)
	require.NoError(t, err)

	// wait session cleaned
	<-ss.sessionCtx.Done()

	/* ---- 2. send subscription response ---- */
	err = ss.SendSubscriptionResponse(
		context.Background(),
		logtail.TableLogtail{
			Table: &tableA,
		},
	)
	require.Error(t, err)
}

func TestPoisionSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// constructs mocker
	logger := mockMOLogger()
	pooler := NewLogtailResponsePool()
	notifier := mockSessionErrorNotifier(logger.RawLogger())
	cs := mockBlockStream()
	stream := mockMorpcStream(cs, 10, 1024)
	sendTimeout := 5 * time.Second
	poisionTime := 10 * time.Millisecond
	heartbeatInterval := 50 * time.Millisecond

	tableA := mockTable(1, 2, 3)
	ss := NewSession(
		ctx, logger, pooler, notifier, stream,
		sendTimeout, poisionTime, heartbeatInterval,
	)

	/* ---- 1. send response repeatedly ---- */
	for i := 0; i < cap(ss.sendChan)+2; i++ {
		err := ss.SendUpdateResponse(
			context.Background(),
			mockTimestamp(int64(i), 0),
			mockTimestamp(int64(i+1), 0),
			logtail.TableLogtail{
				Table: &tableA,
			},
		)
		if err != nil {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrStreamClosed))
			break
		}
	}
}

func TestSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// constructs mocker
	logger := mockMOLogger()
	pooler := NewLogtailResponsePool()
	notifier := mockSessionErrorNotifier(logger.RawLogger())
	cs := mockNormalClientSession(logger.RawLogger())
	stream := mockMorpcStream(cs, 10, 1024)
	sendTimeout := 5 * time.Second
	poisionTime := 10 * time.Millisecond
	heartbeatInterval := 50 * time.Millisecond

	// constructs tables
	tableA := mockTable(1, 2, 3)
	idA := MarshalTableID(&tableA)
	tableB := mockTable(1, 4, 3)
	idB := MarshalTableID(&tableB)

	ss := NewSession(
		ctx, logger, pooler, notifier, stream,
		sendTimeout, poisionTime, heartbeatInterval,
	)
	defer ss.PostClean()

	// no table resigered now
	require.Equal(t, 0, len(ss.ListSubscribedTable()))

	/* ---- 1. register table ---- */
	require.False(t, ss.Register(idA, tableA))
	require.True(t, ss.Register(idA, tableA))

	/* ---- 2. unregister table ---- */
	require.Equal(t, TableOnSubscription, ss.Unregister(idA))
	require.Equal(t, TableNotFound, ss.Unregister(idA))

	/* ---- 3. register more table ---- */
	require.False(t, ss.Register(idA, tableA))
	require.False(t, ss.Register(idB, tableB))
	require.Equal(t, 0, len(ss.ListSubscribedTable()))

	/* ---- 4. filter logtail ---- */
	// promote state for table A
	ss.AdvanceState(idA)
	require.Equal(t, 1, len(ss.ListSubscribedTable()))
	// promote state for non-exist table
	ss.AdvanceState(TableID("non-exist"))
	require.Equal(t, 1, len(ss.ListSubscribedTable()))
	// filter logtail for subscribed table
	qualified := ss.FilterLogtail(
		mockWrapLogtail(tableA),
		mockWrapLogtail(tableB),
	)
	require.Equal(t, 1, len(qualified))
	require.Equal(t, tableA.String(), qualified[0].Table.String())

	// promote state for table B
	ss.AdvanceState(idB)
	require.Equal(t, 2, len(ss.ListSubscribedTable()))
	// filter logtail for subscribed table
	qualified = ss.FilterLogtail(
		mockWrapLogtail(tableA),
		mockWrapLogtail(tableB),
	)
	require.Equal(t, 2, len(qualified))

	/* ---- 5. send error response ---- */
	err := ss.SendErrorResponse(
		context.Background(),
		tableA,
		moerr.ErrInternal,
		"interval error",
	)
	require.NoError(t, err)

	/* ---- 6. send subscription response ---- */
	err = ss.SendSubscriptionResponse(
		context.Background(),
		logtail.TableLogtail{
			Table: &tableA,
		},
	)
	require.NoError(t, err)

	/* ---- 7. send unsubscription response ---- */
	err = ss.SendUnsubscriptionResponse(
		context.Background(),
		tableA,
	)
	require.NoError(t, err)

	/* ---- 8. send update response ---- */
	{
		from := mockTimestamp(1, 0)
		to := mockTimestamp(2, 0)
		err = ss.SendUpdateResponse(
			context.Background(),
			from,
			to,
			mockLogtail(tableA, to),
			mockLogtail(tableB, to),
		)
		require.NoError(t, err)
	}

	/* ---- 9. publish update response ---- */
	err = ss.Publish(
		context.Background(),
		mockTimestamp(2, 0),
		mockTimestamp(3, 0),
		mockWrapLogtail(tableA),
		mockWrapLogtail(tableB),
	)
	require.NoError(t, err)
}

type blockStream struct {
	once sync.Once
	ch   chan bool
}

func mockBlockStream() morpc.ClientSession {
	return &blockStream{
		ch: make(chan bool),
	}
}

func (m *blockStream) Write(ctx context.Context, message morpc.Message) error {
	<-m.ch
	return moerr.NewStreamClosedNoCtx()
}

func (m *blockStream) Close() error {
	m.once.Do(func() {
		close(m.ch)
	})
	return nil
}

func (m *blockStream) CreateCache(
	ctx context.Context,
	cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

func (m *blockStream) DeleteCache(cacheID uint64) {
	panic("not implement")
}

func (m *blockStream) GetCache(cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

type brokenStream struct{}

func mockBrokenClientSession() morpc.ClientSession {
	return &brokenStream{}
}

func (m *brokenStream) Write(ctx context.Context, message morpc.Message) error {
	return moerr.NewStreamClosedNoCtx()
}

func (m *brokenStream) Close() error {
	return nil
}

func (m *brokenStream) CreateCache(
	ctx context.Context,
	cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

func (m *brokenStream) DeleteCache(cacheID uint64) {
	panic("not implement")
}

func (m *brokenStream) GetCache(cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

type normalStream struct {
	logger *zap.Logger
}

func mockNormalClientSession(logger *zap.Logger) morpc.ClientSession {
	return &normalStream{
		logger: logger,
	}
}

func (m *normalStream) Write(ctx context.Context, message morpc.Message) error {
	response := message.(*LogtailResponseSegment)
	m.logger.Info("write response segment:", zap.String("segment", response.String()))
	return nil
}

func (m *normalStream) Close() error {
	return nil
}

func (m *normalStream) CreateCache(
	ctx context.Context,
	cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

func (m *normalStream) DeleteCache(cacheID uint64) {
	panic("not implement")
}

func (m *normalStream) GetCache(cacheID uint64) (morpc.MessageCache, error) {
	panic("not implement")
}

type notifySessionError struct {
	logger *zap.Logger
}

func mockSessionErrorNotifier(logger *zap.Logger) SessionErrorNotifier {
	return &notifySessionError{
		logger: logger,
	}
}

func (m *notifySessionError) NotifySessionError(ss *Session, err error) {
	if err != nil {
		m.logger.Error("receive session error", zap.Error(err))
		ss.PostClean()
	}
}

func mockWrapLogtail(table api.TableID) wrapLogtail {
	return wrapLogtail{
		id: MarshalTableID(&table),
		tail: logtail.TableLogtail{
			Table: &table,
		},
	}
}

func mockLogtail(table api.TableID, ts timestamp.Timestamp) logtail.TableLogtail {
	return logtail.TableLogtail{
		CkpLocation: "checkpoint",
		Table:       &table,
		Ts:          &ts,
	}
}

func mockMorpcStream(
	cs morpc.ClientSession, id uint64, maxMessageSize int,
) morpcStream {
	segments := NewLogtailServerSegmentPool(maxMessageSize)

	return morpcStream{
		streamID: id,
		limit:    segments.LeastEffectiveCapacity(),
		logger:   mockMOLogger(),
		cs:       cs,
		segments: segments,
	}
}

func mockMOLogger() *log.MOLogger {
	return log.GetServiceLogger(
		logutil.GetGlobalLogger().Named(LogtailServiceRPCName),
		metadata.ServiceType_DN,
		"uuid",
	)
}
