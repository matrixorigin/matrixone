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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"go.uber.org/zap"
)

type TableState int

const (
	TableOnSubscription TableState = iota
	TableSubscribed
	TableNotFound

	// it's estimated the average size of logtail response would be 64 bytes.
	responseBufferSize = 1024 * 1024
)

// SessionManager manages all client sessions.
type SessionManager struct {
	sync.RWMutex
	clients map[morpcStream]*Session
}

// NewSessionManager constructs a session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		clients: make(map[morpcStream]*Session),
	}
}

// GetSession constructs a session for new morpc.ClientSession.
func (sm *SessionManager) GetSession(
	rootCtx context.Context,
	logger *log.MOLogger,
	sendTimeout time.Duration,
	responses ResponsePool,
	notifier SessionErrorNotifier,
	stream morpcStream,
	poisionTime time.Duration,
) *Session {
	sm.Lock()
	defer sm.Unlock()

	if _, ok := sm.clients[stream]; !ok {
		sm.clients[stream] = NewSession(
			rootCtx, logger, sendTimeout, responses, notifier, stream, poisionTime,
		)
	}
	return sm.clients[stream]
}

// DeleteSession deletes session from manager.
func (sm *SessionManager) DeleteSession(stream morpcStream) {
	sm.Lock()
	defer sm.Unlock()
	delete(sm.clients, stream)
}

// ListSession takes a snapshot of all sessions.
func (sm *SessionManager) ListSession() []*Session {
	sm.RLock()
	defer sm.RUnlock()

	sessions := make([]*Session, 0, len(sm.clients))
	for _, ss := range sm.clients {
		sessions = append(sessions, ss)
	}
	return sessions
}

// message describes response to be sent.
type message struct {
	timeout  time.Duration
	response *LogtailResponse
}

// morpcStream describes morpc stream.
type morpcStream struct {
	streamID uint64
	limit    int
	logger   *log.MOLogger
	cs       morpc.ClientSession
	segments SegmentPool
}

// Close closes morpc client session.
func (s *morpcStream) Close() error {
	return s.cs.Close()
}

// write sends response by segment.
func (s *morpcStream) write(
	ctx context.Context, response *LogtailResponse,
) error {
	size := response.ProtoSize()
	buf := make([]byte, size)
	n, err := response.MarshalToSizedBuffer(buf[:size])
	if err != nil {
		return err
	}
	chunks := Split(buf[:n], s.limit)

	s.logger.Debug("start to send response by segment",
		zap.Int("chunk-number", len(chunks)),
		zap.Int("chunk-limit", s.limit),
		zap.Int("message-size", size),
	)

	for index, chunk := range chunks {
		seg := s.segments.Acquire()
		seg.SetID(s.streamID)
		seg.MessageSize = int32(size)
		seg.Sequence = int32(index + 1)
		seg.MaxSequence = int32(len(chunks))
		n := copy(seg.Payload, chunk)
		seg.Payload = seg.Payload[:n]

		s.logger.Debug("real segment proto size", zap.Int("ProtoSize", seg.ProtoSize()))

		if err := s.cs.Write(ctx, seg); err != nil {
			s.segments.Release(seg)
			return err
		}
	}

	return nil
}

// Session manages subscription for logtail client.
type Session struct {
	sessionCtx context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	logger      *log.MOLogger
	sendTimeout time.Duration
	responses   ResponsePool
	notifier    SessionErrorNotifier

	stream      morpcStream
	poisionTime time.Duration
	sendChan    chan message

	mu     sync.RWMutex
	tables map[TableID]TableState
}

type SessionErrorNotifier interface {
	NotifySessionError(*Session, error)
}

// NewSession constructs a session for logtail client.
func NewSession(
	rootCtx context.Context,
	logger *log.MOLogger,
	sendTimeout time.Duration,
	responses ResponsePool,
	notifier SessionErrorNotifier,
	stream morpcStream,
	poisionTime time.Duration,
) *Session {
	ctx, cancel := context.WithCancel(rootCtx)
	ss := &Session{
		sessionCtx:  ctx,
		cancelFunc:  cancel,
		logger:      logger,
		sendTimeout: sendTimeout,
		responses:   responses,
		notifier:    notifier,
		stream:      stream,
		poisionTime: poisionTime,
		sendChan:    make(chan message, responseBufferSize), // buffer response for morpc client session
		tables:      make(map[TableID]TableState),
	}

	sender := func() {
		defer ss.wg.Done()

		for {
			select {
			case <-ss.sessionCtx.Done():
				ss.logger.Error("stop session sender", zap.Error(ss.sessionCtx.Err()))
				return

			case msg, ok := <-ss.sendChan:
				if !ok {
					ss.logger.Info("session sender channel closed")
					return
				}

				sendFunc := func() error {
					defer ss.responses.Release(msg.response)

					ctx, cancel := context.WithTimeout(ss.sessionCtx, msg.timeout)
					defer cancel()
					return ss.stream.write(ctx, msg.response)
				}

				if err := sendFunc(); err != nil {
					ss.logger.Error("fail to send logtail response", zap.Error(err))
					ss.notifier.NotifySessionError(ss, err)
					return
				}
			}
		}
	}

	ss.wg.Add(1)
	go sender()

	return ss
}

// Drop closes sender goroutine.
func (ss *Session) PostClean() {
	ss.cancelFunc()
	ss.wg.Wait()
}

// Register registers table for client.
//
// The returned true value indicates repeated subscription.
func (ss *Session) Register(id TableID, table api.TableID) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, ok := ss.tables[id]; ok {
		return true
	}
	ss.tables[id] = TableOnSubscription
	return false
}

// Unsubscribe unsubscribes table.
func (ss *Session) Unregister(id TableID) TableState {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	state, ok := ss.tables[id]
	if !ok {
		return TableNotFound
	}
	delete(ss.tables, id)
	return state
}

// ListTable takes a snapshot of all
func (ss *Session) ListSubscribedTable() []TableID {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	ids := make([]TableID, 0, len(ss.tables))
	for id, state := range ss.tables {
		if state == TableSubscribed {
			ids = append(ids, id)
		}
	}
	return ids
}

// FilterLogtail selects logtail for expected tables.
func (ss *Session) FilterLogtail(tails ...wrapLogtail) []logtail.TableLogtail {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	qualified := make([]logtail.TableLogtail, 0, len(ss.tables))
	for _, t := range tails {
		if state, ok := ss.tables[t.id]; ok && state == TableSubscribed {
			qualified = append(qualified, t.tail)
		}
	}
	return qualified
}

// Publish publishes additional logtail.
func (ss *Session) Publish(
	ctx context.Context, from, to timestamp.Timestamp, wraps ...wrapLogtail,
) error {
	sendCtx, cancel := context.WithTimeout(ctx, ss.sendTimeout)
	defer cancel()

	qualified := ss.FilterLogtail(wraps...)
	return ss.SendUpdateResponse(sendCtx, from, to, qualified...)
}

// TransitionState marks table as subscribed.
func (ss *Session) AdvanceState(id TableID) {
	ss.logger.Debug("mark table as subscribed", zap.String("table-id", string(id)))

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, ok := ss.tables[id]; !ok {
		return
	}
	ss.tables[id] = TableSubscribed
}

// SendErrorResponse sends error response to logtail client.
func (ss *Session) SendErrorResponse(
	sendCtx context.Context, table api.TableID, code uint16, message string,
) error {
	resp := ss.responses.Acquire()
	resp.Response = newErrorResponse(table, code, message)
	return ss.SendResponse(sendCtx, resp)
}

// SendSubscriptionResponse sends subscription response.
func (ss *Session) SendSubscriptionResponse(
	sendCtx context.Context, tail logtail.TableLogtail,
) error {
	resp := ss.responses.Acquire()
	resp.Response = newSubscritpionResponse(tail)
	return ss.SendResponse(sendCtx, resp)
}

// SendUnsubscriptionResponse sends unsubscription response.
func (ss *Session) SendUnsubscriptionResponse(
	sendCtx context.Context, table api.TableID,
) error {
	resp := ss.responses.Acquire()
	resp.Response = newUnsubscriptionResponse(table)
	return ss.SendResponse(sendCtx, resp)
}

// SendUpdateResponse sends publishment response.
func (ss *Session) SendUpdateResponse(
	sendCtx context.Context, from, to timestamp.Timestamp, tails ...logtail.TableLogtail,
) error {
	resp := ss.responses.Acquire()
	resp.Response = newUpdateResponse(from, to, tails...)
	return ss.SendResponse(sendCtx, resp)
}

// SendResponse sends response.
//
// If the sender of Session finished, it would block until
// sendCtx/sessionCtx cancelled or timeout.
func (ss *Session) SendResponse(
	sendCtx context.Context, response *LogtailResponse,
) error {
	select {
	case <-ss.sessionCtx.Done():
		ss.logger.Error("session context done", zap.Error(ss.sessionCtx.Err()))
		ss.responses.Release(response)
		return ss.sessionCtx.Err()
	case <-sendCtx.Done():
		ss.logger.Error("send context done", zap.Error(sendCtx.Err()))
		ss.responses.Release(response)
		return sendCtx.Err()
	default:
	}

	select {
	case <-time.After(ss.poisionTime):
		ss.logger.Error("poision morpc client session detected, close it")
		ss.responses.Release(response)
		if err := ss.stream.Close(); err != nil {
			ss.logger.Error("fail to close poision morpc client session", zap.Error(err))
		}
		return moerr.NewStreamClosedNoCtx()
	case ss.sendChan <- message{timeout: ContextTimeout(sendCtx, ss.sendTimeout), response: response}:
		return nil
	}
}

// newUnsubscriptionResponse constructs response for unsubscription.
// go:inline
func newUnsubscriptionResponse(
	table api.TableID,
) *logtail.LogtailResponse_UnsubscribeResponse {
	return &logtail.LogtailResponse_UnsubscribeResponse{
		UnsubscribeResponse: &logtail.UnSubscribeResponse{
			Table: &table,
		},
	}
}

// newUpdateResponse constructs response for publishment.
// go:inline
func newUpdateResponse(
	from, to timestamp.Timestamp, tails ...logtail.TableLogtail,
) *logtail.LogtailResponse_UpdateResponse {
	return &logtail.LogtailResponse_UpdateResponse{
		UpdateResponse: &logtail.UpdateResponse{
			From:        &from,
			To:          &to,
			LogtailList: tails,
		},
	}
}

// newSubscritpionResponse constructs response for subscription.
// go:inline
func newSubscritpionResponse(
	tail logtail.TableLogtail,
) *logtail.LogtailResponse_SubscribeResponse {
	return &logtail.LogtailResponse_SubscribeResponse{
		SubscribeResponse: &logtail.SubscribeResponse{
			Logtail: tail,
		},
	}
}

// newErrorResponse constructs response for error condition.
// go:inline
func newErrorResponse(
	table api.TableID, code uint16, message string,
) *logtail.LogtailResponse_Error {
	return &logtail.LogtailResponse_Error{
		Error: &logtail.ErrorResponse{
			Table: &table,
			Status: logtail.Status{
				Code:    uint32(code),
				Message: message,
			},
		},
	}
}
