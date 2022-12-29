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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

func TestSessionManger(t *testing.T) {
	sm := NewSessionManager()

	ctx := context.Background()

	// constructs mocker
	logger := logutil.GetGlobalLogger().Named(LogtailServiceRPCName)
	pooler := mockResponsePooler()
	notifier := mockSessionErrorNotifier(logger)
	timeout := 5 * time.Second

	/* ---- 1. register sessioin A ---- */
	csA := mockNormalClientSession(logger)
	sessionA := sm.GetSession(ctx, logger, timeout, pooler, notifier, csA)
	require.NotNil(t, sessionA)
	require.Equal(t, 1, len(sm.ListSession()))

	/* ---- 2. register sessioin B ---- */
	csB := mockNormalClientSession(logger)
	sessionB := sm.GetSession(ctx, logger, timeout, pooler, notifier, csB)
	require.NotNil(t, sessionB)
	require.Equal(t, 2, len(sm.ListSession()))

	/* ---- 3. delete sessioin ---- */
	sm.DeleteSession(csA)
	require.Equal(t, 1, len(sm.ListSession()))
	sm.DeleteSession(csB)
	require.Equal(t, 0, len(sm.ListSession()))
}

func TestSessionError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// constructs mocker
	logger := logutil.GetGlobalLogger().Named(LogtailServiceRPCName)
	pooler := mockResponsePooler()
	notifier := mockSessionErrorNotifier(logger)
	cs := mockBrokenSession()
	timeout := 5 * time.Second

	tableA := mockTable(1, 2, 3)
	ss := NewSession(ctx, logger, timeout, pooler, notifier, cs)

	/* ---- 1. send subscription response ---- */
	err := ss.SendSubscriptionResponse(
		context.Background(),
		&logtail.TableLogtail{
			Table: &tableA,
		},
	)
	require.NoError(t, err)

	/* ---- 2. send subscription response ---- */
	err = ss.SendSubscriptionResponse(
		context.Background(),
		&logtail.TableLogtail{
			Table: &tableA,
		},
	)
	require.Error(t, err)
}

func TestSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// constructs mocker
	logger := logutil.GetGlobalLogger().Named(LogtailServiceRPCName)
	pooler := mockResponsePooler()
	notifier := mockSessionErrorNotifier(logger)
	cs := mockNormalClientSession(logger)
	timeout := 5 * time.Second

	// constructs tables
	tableA := mockTable(1, 2, 3)
	idA := TableID(tableA.String())
	tableB := mockTable(1, 4, 3)
	idB := TableID(tableB.String())

	ss := NewSession(ctx, logger, timeout, pooler, notifier, cs)
	defer ss.Drop()

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
		mockTableLogtail(tableA),
		mockTableLogtail(tableB),
	)
	require.Equal(t, 1, len(qualified))
	require.Equal(t, tableA.String(), qualified[0].Table.String())

	// promote state for table B
	ss.AdvanceState(idB)
	require.Equal(t, 2, len(ss.ListSubscribedTable()))
	// filter logtail for subscribed table
	qualified = ss.FilterLogtail(
		mockTableLogtail(tableA),
		mockTableLogtail(tableB),
	)
	require.Equal(t, 2, len(qualified))

	/* ---- 5. send error response ---- */
	err := ss.SendErrorResponse(
		context.Background(),
		tableA,
		moerr.ErrDuplicate,
		"duplicated subscription",
	)
	require.NoError(t, err)

	/* ---- 6. send subscription response ---- */
	err = ss.SendSubscriptionResponse(
		context.Background(),
		&logtail.TableLogtail{
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
	err = ss.SendUpdateResponse(
		context.Background(),
		mockLogtail(tableA),
		mockLogtail(tableB),
	)
	require.NoError(t, err)

	/* ---- 9. publish update response ---- */
	err = ss.Publish(
		context.Background(),
		mockTableLogtail(tableA),
		mockTableLogtail(tableB),
	)
	require.NoError(t, err)
}

type brokenSession struct{}

func mockBrokenSession() morpc.ClientSession {
	return &brokenSession{}
}

func (m *brokenSession) Write(ctx context.Context, message morpc.Message) error {
	return moerr.NewStreamClosedNoCtx()
}

type normalSession struct {
	logger *zap.Logger
}

func mockNormalClientSession(logger *zap.Logger) morpc.ClientSession {
	return &normalSession{
		logger: logger,
	}
}

func (m *normalSession) Write(ctx context.Context, message morpc.Message) error {
	response := message.(*LogtailResponse)
	if resp := response.GetError(); resp != nil {
		m.logger.Info(
			"receive error response",
			zap.String("content", resp.String()),
		)
	} else if resp := response.GetUpdateResponse(); resp != nil {
		m.logger.Info(
			"receive update response",
			zap.String("content", resp.String()),
		)
	} else if resp := response.GetSubscribeResponse(); resp != nil {
		m.logger.Info(
			"receive subscription response",
			zap.String("content", resp.String()),
		)
	} else if resp := response.GetUnsubscribeResponse(); resp != nil {
		m.logger.Info(
			"receive unsubscription response",
			zap.String("content", resp.String()),
		)
	}
	return nil
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
		ss.Drop()
	}
}

type respPooler struct {
	pool *sync.Pool
}

func mockResponsePooler() ResponsePooler {
	return &respPooler{
		pool: &sync.Pool{
			New: func() any {
				return &LogtailResponse{}
			},
		},
	}
}

func (m *respPooler) AcquireResponse() *LogtailResponse {
	return m.pool.Get().(*LogtailResponse)
}

func (m *respPooler) ReleaseResponse(resp *LogtailResponse) {
	resp.Reset()
	m.pool.Put(resp)
}

func mockTableLogtail(table api.TableID) tableLogtail {
	return tableLogtail{
		id: TableID(table.String()),
		tail: &logtail.TableLogtail{
			Table: &table,
		},
	}
}

func mockLogtail(table api.TableID) *logtail.TableLogtail {
	return &logtail.TableLogtail{
		Table: &table,
	}
}
