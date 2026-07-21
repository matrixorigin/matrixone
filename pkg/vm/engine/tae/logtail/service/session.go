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
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	maxSegmentedLogtailResponseSize = math.MaxInt32
	// Deleted sessions are diagnostic history, not live owners. Keep the
	// history bounded independently of connection churn.
	maxDeletedSessionHistory = 1024
)

type TableState int

const (
	TableOnSubscription TableState = iota
	TableSubscribed
	TableNotFound
)

type tableSubscription struct {
	state      TableState
	generation uint64
}

var (
	// responseBufferSize is the buffer channel capacity for every morpc stream.
	// We couldn't set this size to an unlimited value,
	// because channel's memory is allocated according to the specified size.
	// NOTE: we afford 1MiB heap memory for every morpc stream.
	// but items within the channel would consume extra heap memory.
	responseBufferSize = 1024 * 1024 / int(unsafe.Sizeof(message{}))
)

// SessionManager manages all client sessions.
type SessionManager struct {
	sync.RWMutex
	clients        map[morpcStream]*Session
	deletedClients []deletedSessionRecord
}

type deletedSessionRecord struct {
	remote    string
	deletedAt time.Time
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
	responses LogtailResponsePool,
	notifier SessionErrorNotifier,
	stream morpcStream,
	sendTimeout time.Duration,
	poisonTime time.Duration,
	progressInterval time.Duration,
	transportProbeInterval time.Duration,
) *Session {
	sm.Lock()
	defer sm.Unlock()

	if _, ok := sm.clients[stream]; !ok {
		sm.clients[stream] = NewSession(
			rootCtx, logger, responses, notifier, stream,
			sendTimeout, poisonTime, progressInterval, transportProbeInterval,
		)
	}
	return sm.clients[stream]
}

// DeleteSession deletes session from manager.
func (sm *SessionManager) DeleteSession(stream morpcStream) {
	sm.Lock()
	defer sm.Unlock()
	ss, ok := sm.clients[stream]
	if ok {
		delete(sm.clients, stream)
		deletedAt := time.Now()
		ss.mu.Lock()
		ss.deletedAt = deletedAt
		remote := ss.stream.remote
		ss.mu.Unlock()
		sm.recordDeletedSession(deletedSessionRecord{
			remote:    remote,
			deletedAt: deletedAt,
		})
	}
}

// recordDeletedSession retains a bounded amount of lightweight diagnostic
// history. In particular, it must never retain *Session: every live Session
// owns an approximately 1 MiB response channel backing array.
//
// Drop the oldest half when the limit is reached. This keeps insertion
// amortized O(1), preserves chronological order, and avoids doing an O(limit)
// copy on every disconnect once the history is full.
func (sm *SessionManager) recordDeletedSession(record deletedSessionRecord) {
	if len(sm.deletedClients) == maxDeletedSessionHistory {
		keep := maxDeletedSessionHistory / 2
		copy(sm.deletedClients[:keep], sm.deletedClients[len(sm.deletedClients)-keep:])
		clear(sm.deletedClients[keep:])
		sm.deletedClients = sm.deletedClients[:keep]
	}
	sm.deletedClients = append(sm.deletedClients, record)
}

func (sm *SessionManager) pruneDeletedSessionsBefore(cutoff time.Time) {
	sm.Lock()
	defer sm.Unlock()

	pos := 0
	for pos < len(sm.deletedClients) && sm.deletedClients[pos].deletedAt.Before(cutoff) {
		pos++
	}
	if pos == 0 {
		return
	}
	copy(sm.deletedClients, sm.deletedClients[pos:])
	clear(sm.deletedClients[len(sm.deletedClients)-pos:])
	sm.deletedClients = sm.deletedClients[:len(sm.deletedClients)-pos]
}

func (sm *SessionManager) HasSession(stream morpcStream) bool {
	sm.RLock()
	defer sm.RUnlock()
	_, ok := sm.clients[stream]
	return ok
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

// AddSession is only for test.
func (sm *SessionManager) AddSession(id uint64) {
	sm.Lock()
	defer sm.Unlock()
	stream := morpcStream{
		streamID: id,
	}
	if _, ok := sm.clients[stream]; !ok {
		sm.clients[stream] = &Session{
			stream: stream,
		}
	}
}

// AddDeletedSession is only for test.
func (sm *SessionManager) AddDeletedSession(_ uint64) {
	sm.Lock()
	defer sm.Unlock()
	sm.recordDeletedSession(deletedSessionRecord{
		deletedAt: time.Now(),
	})
}

func (sm *SessionManager) DeletedSessions() []*Session {
	sm.RLock()
	defer sm.RUnlock()
	sessions := make([]*Session, 0, len(sm.deletedClients))
	for _, record := range sm.deletedClients {
		sessions = append(sessions, &Session{
			stream:    morpcStream{remote: record.remote},
			deletedAt: record.deletedAt,
		})
	}
	return sessions
}

// message describes response to be sent.
type message struct {
	createAt time.Time
	timeout  time.Duration
	response *LogtailResponse
}

type transportProbeResult uint8

const (
	probeWroteHeartbeat transportProbeResult = iota
	probeWroteQueuedResponse
	probeQueueClosed
)

// morpcStream describes morpc stream.
type morpcStream struct {
	streamID uint64
	remote   string
	limit    int
	logger   *log.MOLogger
	cs       morpc.ClientSession
	segments LogtailServerSegmentPool
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
	if err := validateSegmentedResponseSize(size); err != nil {
		return err
	}
	buf := make([]byte, size)
	n, err := response.MarshalToSizedBuffer(buf[:size])
	if err != nil {
		return err
	}
	chunks := Split(buf[:n], s.limit)

	s.logger.Debug("send response by segment",
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

		st := time.Now()
		err := s.cs.Write(ctx, seg)
		v2.LogtailSendNetworkHistogram.Observe(time.Since(st).Seconds())
		if err != nil {
			return err
		}
	}
	v2.LogTailServerSendCounter.Add(1)
	return nil
}

func validateSegmentedResponseSize(size int) error {
	if size < 0 || size > maxSegmentedLogtailResponseSize {
		return moerr.NewInvalidInputNoCtxf(
			"logtail response size %d exceeds segmented protocol limit %d",
			size, maxSegmentedLogtailResponseSize,
		)
	}
	return nil
}

// Session manages subscription for logtail client.
type Session struct {
	sessionCtx  context.Context
	cancelFunc  context.CancelFunc
	wg          sync.WaitGroup
	cleanupOnce sync.Once
	// enqueueMu serializes the final response hand-off with PostClean. It is
	// not held while writing to the transport.
	enqueueMu sync.RWMutex

	logger      *log.MOLogger
	sendTimeout time.Duration
	responses   LogtailResponsePool
	notifier    SessionErrorNotifier

	stream     morpcStream
	poisonTime time.Duration
	sendChan   chan message

	active int32

	mu             sync.RWMutex
	tables         map[TableID]tableSubscription
	nextGeneration uint64

	progressInterval       time.Duration
	progressTimer          *time.Timer
	transportProbeInterval time.Duration
	publishMu              sync.Mutex
	exactFrom              timestamp.Timestamp
	publishInit            sync.Once
	// sentThrough is owned by the sender goroutine. Unlike exactFrom, which is
	// advanced after a response is queued, it tracks only update responses that
	// have been written successfully and is therefore safe to advertise in a
	// transport heartbeat.
	sentThrough timestamp.Timestamp

	deletedAt time.Time
	sendMu    struct {
		sync.Mutex
		lastBeforeSend time.Time
		lastAfterSend  time.Time
	}
}

type SessionErrorNotifier interface {
	NotifySessionError(*Session, error)
}

// NewSession constructs a session for logtail client.
func NewSession(
	rootCtx context.Context,
	logger *log.MOLogger,
	responses LogtailResponsePool,
	notifier SessionErrorNotifier,
	stream morpcStream,
	sendTimeout time.Duration,
	poisonTime time.Duration,
	progressInterval time.Duration,
	transportProbeInterval time.Duration,
) *Session {
	ctx, cancel := context.WithCancel(rootCtx)
	ss := &Session{
		sessionCtx:             ctx,
		cancelFunc:             cancel,
		logger:                 logger.With(zap.Uint64("stream-id", stream.streamID), zap.String("remote", stream.remote)),
		sendTimeout:            sendTimeout,
		responses:              responses,
		notifier:               notifier,
		stream:                 stream,
		poisonTime:             poisonTime,
		sendChan:               make(chan message, responseBufferSize), // buffer response for morpc client session
		tables:                 make(map[TableID]tableSubscription),
		progressInterval:       progressInterval,
		progressTimer:          time.NewTimer(progressInterval),
		transportProbeInterval: transportProbeInterval,
	}

	ss.logger.Info("initialize new session for morpc stream")
	transportCtx := stream.cs.SessionCtx()
	if transportCtx == nil {
		// Test and legacy ClientSession implementations may not expose a
		// transport context. A nil channel keeps this select case disabled.
		transportCtx = context.Background()
	}

	sender := func() {
		var notifyErr error
		defer func() {
			// A notifier is allowed to synchronously clean the session. Publish
			// sender termination first so PostClean cannot wait on this goroutine.
			ss.wg.Done()
			if notifyErr != nil {
				ss.notifier.NotifySessionError(ss, notifyErr)
			}
		}()

		var cnt int64
		timer := time.NewTimer(ss.transportProbeInterval)
		defer timer.Stop()

		for {
			select {
			case <-ss.sessionCtx.Done():
				ss.logger.Error("stop session sender", zap.Error(ss.sessionCtx.Err()))
				return

			case <-transportCtx.Done():
				err := transportCtx.Err()
				ss.logger.Info("transport session closed", zap.Error(err))
				// The logtail session is owned by the transport connection. Cancel
				// it before notifying the server reaper so no publisher can enqueue
				// more responses while the session is being removed.
				ss.cancelFunc()
				notifyErr = err
				return

			case <-timer.C:
				result, err := ss.sendProbeOrPending(cnt)
				if result == probeQueueClosed {
					ss.logger.Info("session sender channel closed")
					return
				}
				if err != nil {
					notifyErr = err
					return
				}
				if result == probeWroteQueuedResponse {
					cnt++
				}
				timer.Reset(ss.transportProbeInterval)

			case msg, ok := <-ss.sendChan:
				if !ok {
					ss.logger.Info("session sender channel closed")
					return
				}
				v2.LogTailSendQueueSizeGauge.Set(float64(len(ss.sendChan)))
				if err := ss.sendMessage(msg, cnt); err != nil {
					notifyErr = err
					return
				}
				cnt++
				timer.Reset(ss.transportProbeInterval)
			}
		}
	}

	ss.wg.Add(1)
	go sender()

	return ss
}

func (ss *Session) sendMessage(msg message, cnt int64) error {
	defer ss.responses.Release(msg.response)

	ctx, cancel := context.WithTimeoutCause(ss.sessionCtx, msg.timeout, moerr.CauseNewSession)
	defer cancel()

	now := time.Now()
	v2.LogtailSendLatencyHistogram.Observe(float64(now.Sub(msg.createAt).Seconds()))
	defer func() {
		v2.LogtailSendTotalHistogram.Observe(time.Since(now).Seconds())
	}()

	ss.OnBeforeSend(now)
	err := ss.stream.write(ctx, msg.response)
	ss.OnAfterSend(now, cnt, msg.response.ProtoSize())
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		if logutil.IsExpectedConnectionCloseError(err) {
			ss.logger.Debug("fail to send logtail response (connection closed)",
				zap.Error(err),
				zap.String("timeout", msg.timeout.String()),
				zap.String("remote address", ss.RemoteAddress()),
			)
		} else {
			ss.logger.Error("fail to send logtail response",
				zap.Error(err),
				zap.String("timeout", msg.timeout.String()),
				zap.String("remote address", ss.RemoteAddress()),
			)
		}
		return err
	}
	if update := msg.response.GetUpdateResponse(); update != nil && update.To != nil {
		ss.sentThrough = *update.To
	} else if subscribe := msg.response.GetSubscribeResponse(); subscribe != nil && subscribe.Logtail.Ts != nil {
		// The disttae client advances every consumer timestamp after applying a
		// subscription response, so subsequent transport probes must not report
		// an older frontier.
		ss.sentThrough = *subscribe.Logtail.Ts
	}
	return nil
}

// sendProbeOrPending linearizes a transport probe after every response that is
// already queued. A response enqueued after the non-blocking receive below is
// allowed to follow the probe, but the probe still advertises only sentThrough,
// never the newer enqueue frontier in exactFrom.
func (ss *Session) sendProbeOrPending(cnt int64) (transportProbeResult, error) {
	select {
	case msg, ok := <-ss.sendChan:
		if !ok {
			return probeQueueClosed, nil
		}
		v2.LogTailSendQueueSizeGauge.Set(float64(len(ss.sendChan)))
		return probeWroteQueuedResponse, ss.sendMessage(msg, cnt)
	default:
	}

	if ss.TableCount() == 0 {
		ss.logger.Error("no tables are subscribed yet, close this session")
		return probeWroteHeartbeat, moerr.NewInternalErrorNoCtx("no tables are subscribed")
	}
	return probeWroteHeartbeat, ss.sendHeartbeat()
}

func (ss *Session) sendHeartbeat() error {
	from := ss.sentThrough
	resp := ss.responses.Acquire()
	resp.Response = newUpdateResponse(from, from)
	defer ss.responses.Release(resp)
	ctx, cancel := context.WithTimeout(ss.sessionCtx, ss.sendTimeout)
	defer cancel()
	return ss.stream.write(ctx, resp)
}

// Drop closes sender goroutine.
func (ss *Session) PostClean() {
	ss.cleanupOnce.Do(func() {
		ss.logger.Info("clean session for morpc stream")

		// close morpc stream, maybe verbose
		if err := ss.stream.Close(); err != nil {
			ss.logger.Error("fail to close morpc client session", zap.Error(err))
		}

		ss.cancelFunc()
		ss.progressTimer.Stop()
		// Wait for any in-flight response hand-off that began before cancel,
		// then keep new hand-offs excluded until the sender and queue are fully
		// drained. Subsequent senders observe sessionCtx cancellation after this
		// critical section and release their response without enqueueing it.
		ss.enqueueMu.Lock()
		defer ss.enqueueMu.Unlock()
		ss.wg.Wait()

		left := len(ss.sendChan)

		// release all left responses in sendChan
		if left > 0 {
			i := 0
			for resp := range ss.sendChan {
				ss.responses.Release(resp.response)
				i++
				if i >= left {
					break
				}
			}
			ss.logger.Info("release left responses", zap.Int("left", left))
		}
	})
}

// Register registers table for client.
//
// The returned true value indicates repeated subscription.
func (ss *Session) Register(id TableID, table api.TableID) bool {
	repeated, _ := ss.RegisterWithGeneration(id, table)
	return repeated
}

// RegisterWithGeneration starts one logical subscription attempt. A table may
// be subscribed again after cancellation, so completion must carry this token
// rather than relying on the table ID alone.
func (ss *Session) RegisterWithGeneration(id TableID, table api.TableID) (bool, uint64) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if entry, ok := ss.tables[id]; ok {
		return true, entry.generation
	}
	ss.nextGeneration++
	ss.tables[id] = tableSubscription{
		state:      TableOnSubscription,
		generation: ss.nextGeneration,
	}
	return false, ss.nextGeneration
}

// Unsubscribe unsubscribes table.
func (ss *Session) Unregister(id TableID) TableState {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	entry, ok := ss.tables[id]
	if !ok {
		return TableNotFound
	}
	delete(ss.tables, id)
	return entry.state
}

// unregisterGeneration cancels one asynchronous subscription attempt without
// deleting a newer attempt for the same table.
func (ss *Session) unregisterGeneration(id TableID, generation uint64) TableState {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	entry, ok := ss.tables[id]
	if !ok || entry.generation != generation {
		return TableNotFound
	}
	delete(ss.tables, id)
	return entry.state
}

// ListTable takes a snapshot of all
func (ss *Session) ListSubscribedTable() []TableID {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	ids := make([]TableID, 0, len(ss.tables))
	for id, entry := range ss.tables {
		if entry.state == TableSubscribed {
			ids = append(ids, id)
		}
	}
	return ids
}

// FilterLogtail selects logtail for expected tables.
func (ss *Session) FilterLogtail(tails ...wrapLogtail) []logtail.TableLogtail {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	qualified := make([]logtail.TableLogtail, 0, 4)
	for _, t := range tails {
		if entry, ok := ss.tables[t.id]; ok && entry.state == TableSubscribed {
			qualified = append(qualified, t.tail)
		} else {
			ss.logger.Debug("table not subscribed, filter out",
				zap.String("id", string(t.id)),
				zap.String("checkpoint", t.tail.CkpLocation),
			)
		}
	}
	return qualified
}

// Publish publishes incremental logtail.
func (ss *Session) Publish(
	ctx context.Context, from, to timestamp.Timestamp, closeCB func(), wraps ...wrapLogtail,
) error {
	// no need to send incremental logtail if no table subscribed
	if atomic.LoadInt32(&ss.active) <= 0 {
		if closeCB != nil {
			closeCB()
		}
		return nil
	}

	// keep `logtail.UpdateResponse.From` monotonous
	ss.publishMu.Lock()
	ss.publishInit.Do(func() {
		ss.exactFrom = from
	})
	exactFrom := ss.exactFrom
	ss.publishMu.Unlock()

	qualified := ss.FilterLogtail(wraps...)
	// if there's no incremental logtail, heartbeat by interval
	if len(qualified) == 0 {
		select {
		case <-ss.progressTimer.C:
			break
		default:
			if closeCB != nil {
				closeCB()
			}
			return nil
		}
	}

	sendCtx, cancel := context.WithTimeoutCause(ctx, ss.sendTimeout, moerr.CausePublish)
	defer cancel()

	beforeSend := time.Now()
	err := ss.TrySendUpdateResponse(sendCtx, exactFrom, to, closeCB, qualified...)
	if err == nil {
		ss.progressTimer.Reset(ss.progressInterval)
		ss.publishMu.Lock()
		ss.exactFrom = to
		ss.publishMu.Unlock()
	} else {
		err = moerr.AttachCause(sendCtx, err)
		ss.logger.Error("send update response failed",
			zap.String("send timeout value", ss.sendTimeout.String()),
			zap.String("send duration", time.Since(beforeSend).String()),
			zap.Error(err),
		)
		ss.notifier.NotifySessionError(ss, err)
	}
	return err
}

// AdvanceState marks table as subscribed.
func (ss *Session) AdvanceState(id TableID) {
	ss.logger.Debug("mark table as subscribed", zap.String("table-id", string(id)))

	ss.mu.Lock()
	defer ss.mu.Unlock()

	entry, ok := ss.tables[id]
	if !ok {
		return
	}
	entry.state = TableSubscribed
	ss.tables[id] = entry
}

// CompleteSubscription is the only transition from a pull result to an active
// table. It holds the state lock through enqueueing the response, making an
// unsubscribe linearizable with respect to a late phase result.
func (ss *Session) CompleteSubscription(
	sendCtx context.Context, id TableID, generation uint64,
	tail logtail.TableLogtail, closeCB func(),
) (bool, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	entry, ok := ss.tables[id]
	if !ok || entry.state != TableOnSubscription || entry.generation != generation {
		if closeCB != nil {
			closeCB()
		}
		return false, nil
	}
	if err := ss.SendSubscriptionResponse(sendCtx, tail, closeCB); err != nil {
		delete(ss.tables, id)
		return true, err
	}
	entry.state = TableSubscribed
	ss.tables[id] = entry
	return true, nil
}

// SendErrorResponse sends error response to logtail client.
func (ss *Session) SendErrorResponse(
	sendCtx context.Context, table api.TableID, code uint16, message string,
) error {
	ss.logger.Warn("send error response", zap.Any("table", table), zap.Uint16("code", code), zap.String("message", message))

	resp := ss.responses.Acquire()
	resp.Response = newErrorResponse(table, code, message)
	return ss.SendResponse(sendCtx, resp)
}

// SendSubscriptionResponse sends a subscription response without waiting for
// queue capacity. Subscription completion runs on the single global logtail
// sender, so a congested session must reconnect and rebuild from a snapshot
// rather than delay subscription or incremental progress for every session.
func (ss *Session) SendSubscriptionResponse(
	sendCtx context.Context, tail logtail.TableLogtail, closeCB func(),
) error {
	ss.logger.Info("send subscription response", zap.Any("table", tail.Table), zap.String("To", tail.Ts.String()))

	resp := ss.responses.Acquire()
	resp.closeCB = closeCB
	resp.Response = newSubscritpionResponse(tail)
	err := ss.sendResponse(sendCtx, resp, false)
	if err == nil {
		atomic.AddInt32(&ss.active, 1)
	}
	return err
}

// SendUnsubscriptionResponse sends unsubscription response.
func (ss *Session) SendUnsubscriptionResponse(
	sendCtx context.Context, table api.TableID,
) error {
	ss.logger.Info("send unsubscription response", zap.Any("table", table))

	resp := ss.responses.Acquire()
	resp.Response = newUnsubscriptionResponse(table)
	return ss.SendResponse(sendCtx, resp)
}

// CompleteUnsubscription sends the protocol response and updates the session
// activity count only when a previously active table was actually removed.
func (ss *Session) CompleteUnsubscription(
	sendCtx context.Context, table api.TableID, state TableState,
) error {
	if state == TableSubscribed {
		atomic.AddInt32(&ss.active, -1)
	}
	return ss.SendUnsubscriptionResponse(sendCtx, table)
}

// SendUpdateResponse sends publishment response.
func (ss *Session) SendUpdateResponse(
	sendCtx context.Context, from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail,
) error {
	ss.logger.Debug("send incremental logtail", zap.Any("From", from.String()), zap.String("To", to.String()), zap.Int("tables", len(tails)))

	resp := ss.responses.Acquire()
	resp.closeCB = closeCB
	resp.Response = newUpdateResponse(from, to, tails...)
	return ss.SendResponse(sendCtx, resp)
}

// TrySendUpdateResponse never waits for a congested session. Incremental
// publication is global progress: a slow consumer must reconnect and take a
// snapshot rather than delaying healthy consumers.
func (ss *Session) TrySendUpdateResponse(
	sendCtx context.Context, from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail,
) error {
	resp := ss.responses.Acquire()
	resp.closeCB = closeCB
	resp.Response = newUpdateResponse(from, to, tails...)
	return ss.sendResponse(sendCtx, resp, false)
}

// SendResponse sends response.
//
// If the sender of Session finished, it would block until
// sendCtx/sessionCtx cancelled or timeout.
func (ss *Session) SendResponse(
	sendCtx context.Context, response *LogtailResponse,
) error {
	return ss.sendResponse(sendCtx, response, true)
}

func (ss *Session) sendResponse(
	sendCtx context.Context, response *LogtailResponse, wait bool,
) error {
	ss.enqueueMu.RLock()
	defer ss.enqueueMu.RUnlock()
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
	if !wait {
		select {
		case <-ss.sessionCtx.Done():
			ss.logger.Error("session context done", zap.Error(ss.sessionCtx.Err()))
			ss.responses.Release(response)
			return ss.sessionCtx.Err()
		case <-sendCtx.Done():
			ss.logger.Error("send context done", zap.Error(sendCtx.Err()))
			ss.responses.Release(response)
			return sendCtx.Err()
		case ss.sendChan <- message{timeout: ContextTimeout(sendCtx, ss.sendTimeout), response: response, createAt: time.Now()}:
			return nil
		default:
			ss.responses.Release(response)
			if err := ss.stream.Close(); err != nil {
				ss.logger.Error("fail to close congested morpc client session", zap.Error(err))
			}
			return moerr.NewStreamClosedNoCtx()
		}
	}

	select {
	case <-ss.sessionCtx.Done():
		ss.logger.Error("session context done", zap.Error(ss.sessionCtx.Err()))
		ss.responses.Release(response)
		return ss.sessionCtx.Err()
	case <-time.After(ss.poisonTime):
		ss.logger.Error("poison morpc client session detected, close it",
			zap.Int("buffer-capacity", cap(ss.sendChan)),
			zap.Int("buffer-length", len(ss.sendChan)),
		)
		ss.responses.Release(response)
		if err := ss.stream.Close(); err != nil {
			ss.logger.Error("fail to close poision morpc client session", zap.Error(err))
		}
		return moerr.NewStreamClosedNoCtx()
	case ss.sendChan <- message{timeout: ContextTimeout(sendCtx, ss.sendTimeout), response: response, createAt: time.Now()}:
		return nil
	}
}

func (ss *Session) Active() int {
	return int(atomic.LoadInt32(&ss.active))
}

func (ss *Session) Tables() map[TableID]TableState {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	tables := make(map[TableID]TableState, len(ss.tables))
	for k, v := range ss.tables {
		tables[k] = v.state
	}
	return tables
}

func (ss *Session) TableCount() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.tables)
}

func (ss *Session) OnBeforeSend(t time.Time) {
	ss.sendMu.Lock()
	defer ss.sendMu.Unlock()
	ss.sendMu.lastBeforeSend = t
}

func (ss *Session) OnAfterSend(before time.Time, count int64, size int) {
	ss.sendMu.Lock()
	defer ss.sendMu.Unlock()
	now := time.Now()
	cost := now.Sub(before)
	if cost > 10*time.Second {
		ss.logger.Info("send logtail too much",
			zap.Int64("sendRound", count),
			zap.Duration("duration", cost),
			zap.Int("msg size", size))
	}
	ss.sendMu.lastAfterSend = now
}

func (ss *Session) LastBeforeSend() time.Time {
	ss.sendMu.Lock()
	defer ss.sendMu.Unlock()
	return ss.sendMu.lastBeforeSend
}

func (ss *Session) LastAfterSend() time.Time {
	ss.sendMu.Lock()
	defer ss.sendMu.Unlock()
	return ss.sendMu.lastAfterSend
}

func (ss *Session) RemoteAddress() string {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.stream.remote
}

func (ss *Session) DeletedAt() time.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.deletedAt
}

// newUnsubscriptionResponse constructs response for unsubscription.
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
