// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	taeLogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const (
	// reconnection related constants.
	// maxTimeToWaitServerResponse : max time to wait for server response. if time exceed, do reconnection.
	// retryReconnect : if reconnect tn failed. push client will retry after time retryReconnect.
	maxTimeToWaitServerResponse = 60 * time.Second
	retryReconnect              = 20 * time.Millisecond

	// push client related constants.
	// maxSubscribeRequestPerSecond : max number of subscribe request we allowed per second.
	maxSubscribeRequestPerSecond = 10000

	// subscribe related constants.
	// periodToCheckTableSubscribeSucceed : check table subscribe status period after push client send a subscribe request.
	// maxTimeToCheckTableSubscribeSucceed : max time to wait for table subscribe succeed. if time exceed, return error.
	periodToCheckTableSubscribeSucceed  = 1 * time.Millisecond
	maxTimeToCheckTableSubscribeSucceed = 30 * time.Second
	maxCheckRangeTableSubscribeSucceed  = int(maxTimeToCheckTableSubscribeSucceed / periodToCheckTableSubscribeSucceed)

	periodToCheckTableUnSubscribeSucceed  = 1 * time.Millisecond
	maxTimeToCheckTableUnSubscribeSucceed = 30 * time.Second
	maxCheckRangeTableUnSubscribeSucceed  = int(maxTimeToCheckTableUnSubscribeSucceed / periodToCheckTableUnSubscribeSucceed)

	// unsubscribe process related constants.
	// unsubscribe process scan the table every 20 minutes, and unsubscribe table which was unused for 1 hour.
	unsubscribeTimer = 1 * time.Hour

	// gc blocks and BlockIndexByTSEntry in partition state
	gcPartitionStateTimer = 1 * time.Hour

	// log tail consumer related constants.
	// if buffer is almost full (percent > consumerWarningPercent, we will send a message to log.
	consumerNumber         = 4
	consumerBufferLength   = 8192
	consumerWarningPercent = 0.9

	defaultRPCReadTimeout = time.Second * 30

	logTag = "[logtail-consumer]"
)

type SubscribeState int32

const (
	InvalidSubState SubscribeState = iota
	Subscribing
	SubRspReceived
	Subscribed
	Unsubscribing
	Unsubscribed
	SubRspTableNotExist

	FakeLogtailServerAddress = "fake address for ut"
)

var (
	unsubscribeProcessTicker = 20 * time.Minute
	gcPartitionStateTicker   = 20 * time.Minute
	gcSnapshotTicker         = 20 * time.Minute // snapshot GC interval

	defaultGetLogTailAddrTimeoutDuration = time.Minute * 10
	defaultServerTimeout                 = time.Minute * 10
	defaultDialServerTimeout             = time.Second * 3
	defaultDialServerInterval            = time.Second
)

// PushClient is a structure responsible for all operations related to the log tail push model.
// It provides the following methods:
//
//		-----------------------------------------------------------------------------------------------------
//		 1. checkTxnTimeIsLegal : block the process until we have received enough log tail (T_log >= T_txn)
//		 2. TryToSubscribeTable : block the process until we subscribed a table succeed.
//		 3. subscribeTable	   : send a table subscribe request to service.
//		 4. subSysTables : subscribe mo_databases, mo_tables, mo_columns
//		 5. receiveTableLogTailContinuously   : start (1 + consumerNumber) routine to receive log tail from service.
//
//	 Watch out for the following points:
//		 1. if we want to lock both subscriber and subscribed, we should lock subscriber first.
//		-----------------------------------------------------------------------------------------------------
type PushClient struct {
	serviceID string
	// Responsible for sending subscription / unsubscription requests to the service
	// and receiving the log tail from service.
	subscriber *logTailSubscriber

	// Record the timestamp of last log received by CN.
	receivedLogTailTime syncLogTailTimestamp
	dca                 delayedCacheApply

	// Record the subscription status of a tables.
	subscribed subscribedTable
	// Preserve replay capacity without holding the subscription state lock over I/O.
	// Initialized under subscribed.rw and retained across transport reconnects.
	checkpointReplay chan struct{}

	// timestampWaiter is used to notify the latest commit timestamp
	timestampWaiter client.TimestampWaiter

	// connectC is the channel which is used to control the connection
	// flow.
	connector *connector

	// initialized is true means that it is not the first time to init push client.
	initialized bool

	mu struct {
		sync.Mutex
		paused         bool
		pause          *logtailPauseRequest
		connectPending *logtailPauseRequest
		connectSet     bool
	}
	// pauseC is the channel used to control whether the receiver is paused.
	pauseC chan *logtailPauseRequest

	consumeErrC chan logtailConsumerError
	receiver    []*routineController
	eng         *Engine

	LogtailRPCClientFactory func(context.Context, string, string, morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error)

	reconnectHandler func()

	lifecycleMu sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
	closeErr    error
	closed      atomic.Bool
	wg          sync.WaitGroup
}

type delayedCacheApply struct {
	// replayCatalogCache and consumeEntry may be called concurrently.
	// In consumeEntry, update logtail entries of the three tables should be applied to the catalog cache AFTER
	// replayCatalogCache is finished.
	// Note: Do not let consumeEntry wait for replayCatalogCache
	// because replayCatalogCache will be wating for consumeEntry's goroutine to change subcribing status.

	// Consider the lock contention:
	// consumeEntry handling the three tables will run on a single goroutine,
	// and on the other goroutine, replayCatalogCache will attempt to acquire the lock only once.
	// Therefore, the lock contention is not serious.
	sync.Mutex
	replayed bool
	flist    []func()
}

func (c *PushClient) dcaTryDelay(isSub bool, f func()) (delayed bool) {
	c.dca.Lock()
	defer c.dca.Unlock()
	if c.dca.replayed {
		// replay finished, no need to delay
		return false
	}
	if isSub {
		return true
	}
	c.dca.flist = append(c.dca.flist, f)
	return true
}

func (c *PushClient) dcaConfirmAndApply() {
	c.dca.Lock()
	defer c.dca.Unlock()
	c.dca.replayed = true
	for _, f := range c.dca.flist {
		f()
	}
	c.dca.flist = c.dca.flist[:0]
}

type State struct {
	LatestTS  timestamp.Timestamp
	SubTables map[uint64]SubTableStatus
}

func (c *PushClient) LatestLogtailAppliedTime() timestamp.Timestamp {
	return c.receivedLogTailTime.getTimestamp()
}

// AcquireLogtailReadBarrier obtains a TN-ordered publication frontier and
// waits until the normal CN logtail apply pipeline reaches it. It does not
// bypass, duplicate, or special-case catalog application.
func (c *PushClient) AcquireLogtailReadBarrier(
	ctx context.Context,
) (timestamp.Timestamp, error) {
	c.lifecycleMu.Lock()
	ownerCtx := c.ctx
	subscriber := c.subscriber
	closed := c.closed.Load()
	c.lifecycleMu.Unlock()
	if closed {
		return timestamp.Timestamp{}, context.Canceled
	}
	if subscriber == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "logtail subscriber is not initialized")
	}
	// Reconnect may replace a transport while this caller is waiting. Keep
	// the read bounded by both the request and the existing PushClient owner.
	if ownerCtx != nil {
		barrierCtx, cancel := context.WithCancelCause(ctx)
		stop := context.AfterFunc(ownerCtx, func() { cancel(context.Cause(ownerCtx)) })
		defer cancel(context.Canceled)
		defer stop()
		if ownerCtx.Err() != nil {
			cancel(context.Cause(ownerCtx))
		}
		ctx = barrierCtx
	}
	return acquireAppliedLogtailReadBarrier(
		ctx, c.timestampWaiter, subscriber.readBarrier,
	)
}

func acquireAppliedLogtailReadBarrier(
	ctx context.Context,
	timestampWaiter client.TimestampWaiter,
	readBarrier func(context.Context) (timestamp.Timestamp, error),
) (timestamp.Timestamp, error) {
	if timestampWaiter == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "logtail timestamp waiter is not initialized")
	}
	frontier, err := readBarrier(ctx)
	if err != nil {
		return timestamp.Timestamp{}, moerr.AttachCause(ctx, err)
	}
	applied, err := timestampWaiter.GetTimestamp(ctx, frontier)
	if err != nil {
		return timestamp.Timestamp{}, moerr.AttachCause(ctx, err)
	}
	if applied.Less(frontier) {
		return timestamp.Timestamp{}, moerr.NewInternalErrorf(
			ctx,
			"logtail waiter returned %s before read barrier %s",
			applied.DebugString(), frontier.DebugString(),
		)
	}
	return frontier, nil
}

func (c *PushClient) GetState() State {
	c.subscribed.rw.RLock()
	defer c.subscribed.rw.RUnlock()
	subTables := make(map[uint64]SubTableStatus, len(c.subscribed.m))
	for k, ent := range c.subscribed.m {
		subTables[k] = SubTableStatus{
			DBID:       ent.dbID,
			SubState:   ent.state,
			LatestTime: time.Unix(0, ent.lastTs.Load()),
		}
	}
	return State{
		LatestTS:  c.receivedLogTailTime.getTimestamp(),
		SubTables: subTables,
	}
}

// Only used for ut
func (c *PushClient) SetSubscribeState(dbId, tblId uint64, state SubscribeState) {
	c.subscribed.rw.Lock()
	defer c.subscribed.rw.Unlock()
	ent := &subEntry{
		dbID:  dbId,
		state: state,
	}
	ent.lastTs.Store(time.Now().UnixNano())
	c.subscribed.m[tblId] = ent
}

func (c *PushClient) IsSubscriberReady() bool {
	return c.subscriber.ready()
}

func (c *PushClient) IsSubscribed(tblId uint64) bool {
	c.subscribed.rw.RLock()
	defer c.subscribed.rw.RUnlock()
	if _, ok := c.subscribed.m[tblId]; ok {
		return true
	}
	return false
}

func (c *PushClient) start(ctx context.Context) context.Context {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.ctx != nil {
		return c.ctx
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	if c.closed.Load() {
		c.cancel()
	}
	return c.ctx
}

func (c *PushClient) runOwned(fn func()) bool {
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return false
	}
	c.wg.Add(1)
	c.lifecycleMu.Unlock()
	go func() {
		defer c.wg.Done()
		fn()
	}()
	return true
}

type connector struct {
	first  atomic.Bool
	signal chan struct{}
	// Closed once by connect after the first successful catalog replay. A
	// receiver paused during that replay must then request ordinary recovery.
	firstReplayDone chan struct{}

	client *PushClient
	engine *Engine
}

func newConnector(c *PushClient, e *Engine) *connector {
	co := &connector{
		signal:          make(chan struct{}, 1),
		firstReplayDone: make(chan struct{}),
		client:          c,
		engine:          e,
	}
	co.first.Store(true)
	return co
}

func (c *connector) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logutil.Info("logtail.consumer.stopped")
			return

		case <-c.signal:
			c.client.mu.Lock()
			req, pending := c.client.mu.connectPending, c.client.mu.connectSet
			c.client.mu.connectPending, c.client.mu.connectSet = nil, false
			valid := pending && (req == nil && c.first.Load() || req != nil && c.client.mu.pause == req)
			c.client.mu.Unlock()
			if !valid {
				continue
			}
			c.client.connect(ctx, c.engine, req)
		}
	}
}

func (c *PushClient) init(
	ctx context.Context,
	serviceAddr string,
	timestampWaiter client.TimestampWaiter,
	e *Engine,
) error {
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return context.Canceled
	}
	c.wg.Add(1)
	if c.subscriber == nil {
		c.subscriber = newLogTailSubscriber()
	}
	c.lifecycleMu.Unlock()
	defer c.wg.Done()

	c.serviceID = e.GetService()
	if !c.initialized {
		// The waiter belongs to the PushClient generation, not to an individual
		// transport connection. Reconnect reuses it and must not race barrier
		// callers by rewriting the same field.
		c.timestampWaiter = timestampWaiter
	}

	// lock all.
	// release subscribed lock when init finished.
	// release subscriber lock when we received enough response from service.
	c.receivedLogTailTime.ready.Store(false)
	c.dca = delayedCacheApply{}
	c.subscriber.setNotReady()
	c.subscribed.rw.Lock()
	defer func() {
		c.subscribed.rw.Unlock()
	}()

	c.receivedLogTailTime.initLogTailTimestamp(timestampWaiter)
	c.subscribed.m = make(map[uint64]*subEntry)

	if !c.initialized {
		c.connector = newConnector(c, e)
		c.receiver = make([]*routineController, consumerNumber)
		c.consumeErrC = make(chan logtailConsumerError, consumerNumber)
		c.pauseC = make(chan *logtailPauseRequest, 1)
	}
	c.initialized = true

	err := c.subscriber.init(
		ctx,
		e.GetService(),
		serviceAddr,
		c.LogtailRPCClientFactory,
	)
	if err != nil {
		return err
	}
	if c.closed.Load() {
		_ = c.subscriber.closeClient()
		return context.Canceled
	}
	return nil
}

func (c *PushClient) SetReconnectHandler(handler func()) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.reconnectHandler = handler
}

func (c *PushClient) notifyReconnect() {
	c.lifecycleMu.Lock()
	handler := c.reconnectHandler
	c.lifecycleMu.Unlock()
	if handler != nil {
		handler()
	}
}

func (c *PushClient) validLogTailMustApplied(snapshotTS timestamp.Timestamp) {
	// If the client is not ready, do not check. There is another checking logic
	// before create a new transaction, so we do not need to check here if it
	// is not ready yet.
	if !c.receivedLogTailTime.ready.Load() {
		return
	}

	// At the time of transaction creation, a ts is obtained as the start timestamp of the transaction.
	// To ensure that the complete data is visible at the start of the transaction, the logtail of
	// all < snapshot ts is waited until it is applied when the transaction is created inside the txn client.
	//
	// Inside the txn client, there is a waiter waiting for the LogTail to be applied, which will continuously
	// receive the ts applied by the PushClient, and then the transaction will use the maximum applied LogTail
	// ts currently received + 1 as the transaction's snapshot ts to ensure that the transaction can see the
	// log tails corresponding to the max(applied log tail ts in txn client).
	//
	// So here we need to use snapshotTS.Prev() to check.
	recTS := c.receivedLogTailTime.getTimestamp()
	if snapshotTS.Prev().LessEq(recTS) {
		return
	}

	// If reconnect, receivedLogTailTime will reset. But latestAppliedLogTailTS is always keep the latest applied
	// logtail ts.
	ts := c.receivedLogTailTime.latestAppliedLogTailTS.Load()
	if ts != nil && ts.GreaterEq(snapshotTS.Prev()) {
		return
	}
	panic(fmt.Sprintf("BUG: all log tail must be applied before %s, received applied %s, last applied %+v",
		snapshotTS.Prev().DebugString(),
		recTS.DebugString(),
		ts))
}

func (c *PushClient) canServeTableSnapshot(
	dbId, tblId uint64,
	ps *logtailreplay.PartitionState,
	snapshot timestamp.Timestamp,
) bool {
	canServe, _ := c.canServeTableSnapshotNoWait(dbId, tblId, ps, snapshot)
	return canServe
}

func (c *PushClient) waitCanServeTableSnapshot(
	ctx context.Context,
	accId, dbId, tblId uint64,
	ps *logtailreplay.PartitionState,
	pending bool,
	snapshot timestamp.Timestamp,
) (*logtailreplay.PartitionState, bool, error) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		canServe, needWait := canServeTableSnapshotWithPending(ps, snapshot, pending)
		if canServe || !needWait {
			return ps, canServe, nil
		}

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-ticker.C:
			ps, pending = c.getSubscribedSnapshotAndPending(ctx, accId, dbId, tblId)
		}
	}
}

func (c *PushClient) canServeTableSnapshotNoWait(
	dbId, tblId uint64,
	ps *logtailreplay.PartitionState,
	snapshot timestamp.Timestamp,
) (canServe bool, needWait bool) {
	return canServeTableSnapshotWithPending(
		ps,
		snapshot,
		c.subscribed.hasPendingUpdate(dbId, tblId),
	)
}

func canServeTableSnapshotWithPending(
	ps *logtailreplay.PartitionState,
	snapshot timestamp.Timestamp,
	pending bool,
) (canServe bool, needWait bool) {
	snapshotTS := types.TimestampToTS(snapshot)
	if ps == nil || !ps.CanServe(snapshotTS) {
		return false, false
	}
	if snapshot.IsEmpty() {
		return true, false
	}

	visibleTS := types.TimestampToTS(snapshot.Prev())
	appliedTo := ps.GetAppliedTo()
	if !appliedTo.IsEmpty() && appliedTo.GE(&visibleTS) {
		return true, false
	}

	// If no update for this table is queued or being applied, the global
	// logtail waiter already proves this snapshot is within the CN applied
	// waterline. Only block the latest-state fast path when this table has a
	// known pending update and the current state has not applied up to the
	// statement snapshot yet.
	if pending {
		return false, true
	}
	return true, false
}

func (c *PushClient) skipSubIfSubscribed(
	ctx context.Context,
	acctId uint64,
	tableID uint64,
	dbID uint64,
	accessTime *int64,
) (bool, *logtailreplay.PartitionState, bool) {

	//if table has been subscribed, return quickly.
	if ps, ok, _, pending := c.isSubscribedWithPendingAccessTime(ctx, acctId, dbID, tableID, accessTime); ok {
		return true, ps, pending
	}
	return false, nil, false
}

func (c *PushClient) toSubscribeTable(
	ctx context.Context,
	accId uint64,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
	pendingOut ...*bool,
) (ps *logtailreplay.PartitionState, err error) {
	return c.toSubscribeTableWithAccessTime(ctx, accId, tableID, tableName, dbID, dbName, nil, pendingOut...)
}

// accessTime is a caller-local idle-GC bookkeeping sample shared only across
// consecutive warm reads. It has no role in snapshot visibility or readiness.
func (c *PushClient) toSubscribeTableWithAccessTime(
	ctx context.Context,
	accId, tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
	accessTime *int64,
	pendingOut ...*bool,
) (ps *logtailreplay.PartitionState, err error) {
	var pending *bool
	if len(pendingOut) > 0 {
		pending = pendingOut[0]
	}

	var (
		skip           bool
		state          SubscribeState
		injected       bool
		initialPending bool
	)

	if injected, _ = objectio.LogCNSubscribeTableFailInjected(
		dbName, tableName,
	); injected {
		return nil,
			moerr.NewInternalErrorNoCtx("injected subscribe table err")
	}

	if skip, ps, initialPending = c.skipSubIfSubscribed(ctx, accId, tableID, dbID, accessTime); skip {
		if pending != nil {
			*pending = initialPending
		}
		return ps, nil
	}
	if accessTime != nil {
		// Cold/waiting paths sample their own current time below. The next warm
		// access must not inherit a value from before that potentially long wait.
		*accessTime = 0
	}

	state, err = c.toSubIfUnsubscribed(ctx, dbID, tableID)
	if err != nil {
		return nil, err
	}

	// state machine for subscribe table.
	//Unsubscribed -> Subscribing -> SubRspReceived -> Subscribed-->Unsubscribing-->Unsubscribed
	for {

		switch state {

		case Subscribing:
			//wait for the next possible state: subscribed or unsubscribed or unsubscribing or Subscribing
			state, err = c.waitUntilSubscribingChanged(ctx, dbID, tableID)
			if err != nil {
				return nil, err
			}
		case SubRspReceived:
			state, err = c.loadAndConsumeLatestCkp(ctx, accId, tableID, tableName, dbID, dbName)
			if err != nil {
				return nil, err
			}
		case SubRspTableNotExist:
			c.subscribed.clearTable(dbID, tableID)
			return nil, moerr.NewNoSuchTable(ctx, fmt.Sprintf("%s(%d)", dbName, dbID), fmt.Sprintf("%s(%d)", tableName, tableID))
		case Unsubscribing:
			//need to wait for unsubscribe succeed for making the subscribe and unsubscribe execute in order,
			// otherwise the partition state will leak log tails.
			state, err = c.waitUntilUnsubscribingChanged(ctx, dbID, tableID)
			if err != nil {
				return nil, err
			}

		case Subscribed:
			//if table has been subscribed, return the ps.
			ps, _, state, initialPending = c.isSubscribedWithPending(ctx, accId, dbID, tableID)
			if ps != nil {
				if pending != nil {
					*pending = initialPending
				}
				logutil.Info(
					fmt.Sprintf("%s-subscribe-ok", logTag),
					zap.Uint64("table-id", tableID),
					zap.String("table-name", tableName),
					zap.Uint64("db-id", dbID),
					zap.String("ps", fmt.Sprintf("%p", ps)),
				)
				return
			}

		case Unsubscribed:
			panic("Impossible Path")

		}

	}

}

// TryToSubscribeTable subscribe a table and block until subscribe succeed.
func (c *PushClient) TryToSubscribeTable(
	ctx context.Context,
	accId, dbId, tblId uint64,
	dbName, tblName string,
) (err error) {
	_, err = c.toSubscribeTable(ctx, accId, tblId, tblName, dbId, dbName)
	return
}

// this method will ignore lock check, subscribe a table and block until subscribe succeed.
// developer should use this method carefully.
// in most time, developer should use TryToSubscribeTable instead.
func (c *PushClient) forcedSubscribeTable(
	ctx context.Context,
	dbId, tblId uint64) error {
	s := c.subscriber

	if err := s.sendSubscribe(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		return err
	}
	ticker := time.NewTicker(periodToCheckTableSubscribeSucceed)
	defer ticker.Stop()

	for i := 0; i < maxCheckRangeTableSubscribeSucceed; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if ok := c.subscribed.isSubscribed(dbId, tblId); ok {
				return nil
			}
		}
	}
	return moerr.NewInternalError(ctx, "forced subscribe table timeout")
}

func (c *PushClient) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := c.subscriber.sendSubscribe(ctx, tblId)
		if err != nil {
			return err
		}
		logutil.Info(
			fmt.Sprintf("%s-send-subscribe-ok", logTag),
			zap.Uint64("table-id", tblId.TbId),
			zap.Uint64("db-id", tblId.DbId),
		)
		return nil
	}
}

func (c *PushClient) subSysTables(ctx context.Context) error {
	if enabled, p := objectio.CNSubSysErrInjected(); enabled && rand.Intn(100000) < p {
		return moerr.NewInternalError(ctx, "FIND_TABLE sub sys error injected")
	}
	// push subscription to Table `mo_database`, `mo_table`, `mo_column` of mo_catalog.
	databaseId := uint64(catalog.MO_CATALOG_ID)
	tableIds := []uint64{catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID}

	var err error
	for _, ti := range tableIds {
		err = c.forcedSubscribeTable(ctx, databaseId, ti)
		if err != nil {
			break
		}
	}

	if err != nil {
		logutil.Error(
			"logtail.consumer.connect.to.tn.logtail.server.failed",
			zap.String("service", c.serviceID),
			zap.Error(err),
		)
	}
	return err
}

// A pause acknowledgement means the receiver has finished its current
// receive and synchronous dispatch. Consumer work requires a separate join.
type logtailPauseRequest struct {
	reconnect bool
	ack       chan struct{}
	resume    chan struct{}
}

func (c *PushClient) pause(s bool) *logtailPauseRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pauseLocked(s)
}

// pauseIfCurrent validates the error source in the same critical section that
// creates the pause. A consumer from a retired stream cannot pause its successor.
func (c *PushClient) pauseIfCurrent(source *service.LogtailClient, generation uint64, reconnect bool) *logtailPauseRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subscriber == nil {
		return nil
	}
	c.subscriber.mu.RLock()
	defer c.subscriber.mu.RUnlock()
	if source == nil || c.subscriber.logTailClient != source || c.subscriber.transportGeneration != generation {
		return nil
	}
	return c.pauseLockedWithClient(reconnect, source)
}

func (c *PushClient) pauseLocked(s bool) *logtailPauseRequest {
	var client *service.LogtailClient
	if c.subscriber != nil {
		client = c.subscriber.client()
	}
	return c.pauseLockedWithClient(s, client)
}

func (c *PushClient) pauseLockedWithClient(s bool, client *service.LogtailClient) *logtailPauseRequest {
	if c.mu.paused {
		return c.mu.pause
	}
	// Note
	// If subSysTables fails to send a successful request, receiveLogtails will receive nothing until the context is done. In this case, we attempt to stop the receiveLogtails goroutine immediately.
	// The break signal left in the channel will interrupt the normal receiving process, but this is not an issue because reconnecting will create a new channel.
	if client != nil {
		client.BreakoutReceive()
	}
	req := &logtailPauseRequest{reconnect: s, ack: make(chan struct{}), resume: make(chan struct{})}
	select {
	case c.pauseC <- req:
		c.mu.paused = true
		c.mu.pause = req
	default:
		logutil.Info("logtail.consumer.already.set.to.pause")
		return nil
	}
	return req
}

func (c *PushClient) resume(req *logtailPauseRequest) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req == nil || c.mu.pause != req {
		return false
	}
	select {
	case <-req.ack:
	default:
		return false
	}
	c.mu.paused = false
	c.mu.pause = nil
	close(req.resume)
	return true
}

func waitLogtailPause(ctx context.Context, req *logtailPauseRequest) bool {
	if req == nil {
		return false
	}
	select {
	case <-req.ack:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *PushClient) receiveOneLogtail(ctx context.Context, e *Engine, client *service.LogtailClient, generation uint64) error {
	ctx, cancel := context.WithTimeoutCause(ctx, maxTimeToWaitServerResponse, moerr.CauseReceiveOneLogtail)
	defer cancel()

	// Client receives one logtail counter.
	defer v2.LogTailClientReceiveCounter.Add(1)

	if enabled, p := objectio.CNRecvErrInjected(); enabled && rand.Intn(100000) < p {
		return moerr.NewInternalError(ctx, "FIND_TABLE random error")
	}

	resp := c.subscriber.receiveResponse(ctx, client)
	if resp.err != nil {
		resp.err = moerr.AttachCause(ctx, resp.err)
		// POSSIBLE ERROR: context deadline exceeded, rpc closed, decode error.
		logutil.Error(
			"logtail.consumer.receive.error.from.logtail.client",
			zap.Error(resp.err),
		)
		return resp.err
	}

	receiveAt := time.Now()
	v2.LogtailTotalReceivedCounter.Inc()
	if res := resp.response.GetSubscribeResponse(); res != nil { // consume subscribe response
		v2.LogtailSubscribeReceivedCounter.Inc()
		if err := dispatchSubscribeResponse(ctx, e, res, c.receiver, receiveAt); err != nil {
			err = moerr.AttachCause(ctx, err)
			logutil.Error(
				"logtail.consumer.dispatch.subscribe.response.failed",
				zap.Error(err),
			)
			return err
		}
	} else if res := resp.response.GetUpdateResponse(); res != nil { // consume update response
		if len(res.LogtailList) > 0 {
			v2.LogtailUpdateReceivedCounter.Inc()
		} else {
			v2.LogtailHeartbeatReceivedCounter.Inc()
		}

		if err := dispatchUpdateResponse(ctx, e, res, c.receiver, receiveAt); err != nil {
			err = moerr.AttachCause(ctx, err)
			logutil.Error(
				"logtail.consumer.dispatch.update.response.failed",
				zap.Error(err),
			)
			return err
		}
	} else if unResponse := resp.response.GetUnsubscribeResponse(); unResponse != nil { // consume unsubscribe response
		v2.LogtailUnsubscribeReceivedCounter.Inc()

		if err := dispatchUnSubscribeResponse(ctx, e, unResponse, c.receiver, receiveAt); err != nil {
			err = moerr.AttachCause(ctx, err)
			logutil.Error(
				"logtail.consumer.dispatch.unsubscribe.response.failed",
				zap.Error(err),
			)
			return err
		}
	} else if errRsp := resp.response.GetError(); errRsp != nil {
		status := errRsp.GetStatus()
		table := errRsp.GetTable()
		database, tableID := table.GetDbId(), table.GetTbId()
		if uint16(status.GetCode()) == moerr.OkExpectedEOB {
			c.subscribed.setTableSubNotExist(database, tableID)
		}
	}
	return nil
}

func (c *PushClient) receiveLogtails(ctx context.Context, e *Engine) {
	for {
		select {
		case <-ctx.Done():
			return

		case s := <-c.pauseC:
			logutil.Info("logtail.consumer.receiver.paused")
			if !c.waitForResume(ctx, s) {
				return
			}
			logutil.Info("logtail.consumer.receiver.resumed")

		default:
			client, generation := c.subscriber.clientAndGeneration()
			if err := c.receiveOneLogtail(ctx, e, client, generation); err != nil {
				logutil.Error(
					"logtail.consumer.receive.one.logtail.failed",
					zap.Error(err),
				)
				c.pauseIfCurrent(client, generation, !c.connector.first.Load())
			}
		}
	}
}

func (c *PushClient) waitForResume(ctx context.Context, req *logtailPauseRequest) bool {
	close(req.ack)
	var firstReplayDone <-chan struct{}
	if req.reconnect {
		c.sendConnectSig(ctx, req)
	} else {
		firstReplayDone = c.connector.firstReplayDone
	}
	for {
		select {
		case <-ctx.Done():
			return false
		case <-req.resume:
			return true
		case <-firstReplayDone:
			// First-connect retries own recovery until replay completes. After
			// that handoff, even an earlier pause(false) needs a new connection.
			c.sendConnectSig(ctx, req)
			firstReplayDone = nil
		}
	}
}

func (c *PushClient) completeFirstConnect(e *Engine) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connector.first.Store(false)
	// Serialize readiness with pause. A failed first connection must recover
	// through connect's existing reconnect branch before admitting transactions.
	if !c.mu.paused && !c.closed.Load() && c.subscriber != nil && c.subscriber.client() != nil {
		e.setPushClientStatus(true)
	}
	close(c.connector.firstReplayDone)
}

func (c *PushClient) startConsumers(ctx context.Context, e *Engine) {
	// new parallelNums routine to consume log tails.
	for i := range c.receiver {
		c.receiver[i] = c.createRoutineToConsumeLogTails(ctx, i, consumerBufferLength, e)
	}
}

func waitPushClient(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *PushClient) stopConsumers() {
	for i := range c.receiver {
		if c.receiver[i] != nil {
			c.receiver[i].close()
		}
	}
	for i := range c.receiver {
		if c.receiver[i] != nil {
			c.receiver[i].wait()
		}
	}
	logutil.Infof("%s %s: logtail consumers stopped", logTag, c.serviceID)
}

func (c *PushClient) sendConnectSig(ctx context.Context, req *logtailPauseRequest) {
	if ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if req != nil && c.mu.pause != req || req == nil && !c.connector.first.Load() {
		return
	}
	c.mu.connectPending, c.mu.connectSet = req, true
	select {
	case c.connector.signal <- struct{}{}:
	default:
	}
}

func (c *PushClient) run(ctx context.Context, e *Engine) {
	c.runOwned(func() {
		c.receiveLogtails(ctx, e)
	})

	// for the first time connector.
	c.sendConnectSig(ctx, nil)

	// A dead loop to receive log tail response from log tail service.
	// if any error happened, we should do reconnection.
	for {
		select {
		case failure := <-c.consumeErrC:
			// receive an error from sub-routine to consume log.
			logutil.Errorf("%s consume log tail failed, err: %s", logTag, failure.err)
			c.pauseIfCurrent(failure.client, failure.generation, !c.connector.first.Load())

		case <-ctx.Done():
			logutil.Infof("%s logtail consumer stopped", logTag)
			return
		}
	}
}

func (c *PushClient) waitTimestamp(ctx context.Context) error {
	timeout := time.NewTimer(time.Second * 10)
	defer timeout.Stop()

	ticker := time.NewTicker(time.Millisecond * 2)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// we should always make sure that all the log tail consume
			// routines have updated its timestamp.
			if !c.receivedLogTailTime.getTimestamp().IsEmpty() {
				return ctx.Err()
			}

		case <-timeout.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			panic("cannot receive timestamp")
		}
	}
}

func (c *PushClient) replayCatalogCache(ctx context.Context, e *Engine) (err error) {
	if enabled, p := objectio.CNReplayCacheErrInjected(); enabled && rand.Intn(100000) < p {
		return moerr.NewInternalError(ctx, "FIND_TABLE replay catalog cache error injected")
	}
	// replay mo_catalog cache
	var op client.TxnOperator
	var result executor.Result
	ts := c.receivedLogTailTime.getTimestamp()
	ccache := e.catalog.Load()
	typeTs := types.TimestampToTS(ts)
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"replayCatalogCache",
		0)
	op, err = e.cli.New(ctx, timestamp.Timestamp{}, client.WithSkipPushClientReady(), client.WithSnapshotTS(ts), createByOpt)
	if err != nil {
		return err
	}
	defer func() {
		//same timeout value as it in frontend
		ctx2, cancel := context.WithTimeoutCause(ctx, e.Hints().CommitOrRollbackTimeout, moerr.CauseReplayCatalogCache)
		defer cancel()
		if err != nil {
			_ = op.Rollback(ctx2)
		} else {
			_ = op.Commit(ctx2)
		}
	}()
	err = e.New(ctx, op)
	if err != nil {
		return err
	}

	// read databases
	result, err = execReadSql(ctx, op, catalog.MoDatabaseBatchQuery, true)
	if err != nil {
		return err
	}
	rowCntF := func(bat []*batch.Batch) string {
		return stringifySlice(bat, func(b any) string {
			return fmt.Sprintf("%d", b.(*batch.Batch).RowCount())
		})
	}
	logutil.Infof("FIND_TABLE read mo_catalog.mo_databases %v rows", rowCntF(result.Batches))
	defer result.Close()
	for _, b := range result.Batches {
		if err = fillTsVecForSysTableQueryBatch(b, typeTs, result.Mp); err != nil {
			return err
		}
		ccache.InsertDatabase(b)
	}

	// read tables
	result, err = execReadSql(ctx, op, catalog.MoTablesBatchQuery, true)
	if err != nil {
		return err
	}
	logutil.Infof("FIND_TABLE read mo_catalog.mo_tables %v rows", rowCntF(result.Batches))
	defer result.Close()
	for _, b := range result.Batches {
		if err = fillTsVecForSysTableQueryBatch(b, typeTs, result.Mp); err != nil {
			return err
		}
		e.tryAdjustSysTablesCreatedTimeWithBatch(b)
		ccache.InsertTable(b)
	}

	// read columns
	result, err = execReadSql(ctx, op, catalog.MoColumnsBatchQuery, true)
	if err != nil {
		return err
	}
	defer result.Close()
	logutil.Infof("FIND_TABLE read mo_catalog.mo_columns %v rows", rowCntF(result.Batches))

	if isColumnsBatchPerfectlySplitted(result.Batches) {
		for _, b := range result.Batches {
			if err = fillTsVecForSysTableQueryBatch(b, typeTs, result.Mp); err != nil {
				return err
			}
			ccache.InsertColumns(b)
		}
	} else {
		logutil.Info("FIND_TABLE merge mo_columns results")
		bat := result.Batches[0]
		for _, b := range result.Batches[1:] {
			bat, err = bat.Append(ctx, result.Mp, b)
			if err != nil {
				return err
			}
		}
		if err = fillTsVecForSysTableQueryBatch(bat, typeTs, result.Mp); err != nil {
			return err
		}
		ccache.InsertColumns(bat)
	}

	ccache.UpdateDuration(typeTs, types.MaxTs())
	c.dcaConfirmAndApply()
	return nil

}

func (c *PushClient) connect(ctx context.Context, e *Engine, req *logtailPauseRequest) {
	if c.connector.first.Load() {
		c.startConsumers(ctx, e)

		for {
			err := c.subSysTables(ctx)
			if err != nil {
				if ctx.Err() != nil || c.closed.Load() {
					return
				}
				if !c.retryFirstConnection(ctx, e) {
					return
				}
				continue
			}
			if err := c.waitTimestamp(ctx); err != nil {
				return
			}

			if err := c.replayCatalogCache(ctx, e); err != nil {
				logutil.Errorf("%s replay catalog cache failed, err %v", logTag, err)
				if !c.retryFirstConnection(ctx, e) {
					return
				}
				continue
			}

			c.completeFirstConnect(e)
			return
		}
	}

	if !waitLogtailPause(ctx, req) {
		return
	}
	e.setPushClientStatus(false)

	// the consumer goroutine is supposed to be stopped.
	c.stopConsumers()

	logutil.Infof("%s %s: clean finished, start to reconnect to tn log tail service", logTag, c.serviceID)
	for {
		if ctx.Err() != nil {
			logutil.Infof("%s mo context has done, exit log tail receive routine", logTag)
			return
		}

		tnLogTailServerBackend, err := e.getLogTailServiceAddr(ctx)
		if err != nil {
			return
		}
		if err := c.init(ctx, tnLogTailServerBackend, c.timestampWaiter, e); err != nil {
			logutil.Errorf("%s rebuild the cn log tail client failed, reason: %s", logTag, err)
			if !waitPushClient(ctx, retryReconnect) {
				return
			}
			continue
		}
		attemptClient, attemptGeneration := c.subscriber.clientAndGeneration()
		logutil.Infof("%s %s: client init finished", logTag, c.serviceID)

		// This is only for test.
		c.notifyReconnect()

		// clean memory table.
		err = e.init(ctx)
		if err != nil {
			// This new connection has not started any consumers yet.
			logutil.Errorf("%s rebuild memory-table failed, err: %s", logTag, err)
			if !waitPushClient(ctx, retryReconnect) {
				return
			}
			continue
		}
		logutil.Infof("%s %s: clean memory table finished", logTag, c.serviceID)

		// After init, start up again.
		c.startConsumers(ctx, e)

		if !c.resume(req) {
			return
		}
		err = c.subSysTables(ctx)
		if err != nil {
			//  send on closed channel error:
			// receive logtail error -> pause -> reconnect -------------------------> stop
			//                   |-> forced subscribe table timeout -> continue ----> resume
			// Any errors related to the logtail consumer should not be retried within the inner connect loop; they should be handled by the outer caller.
			// So we break the loop here.

			c.pause(true)
			logutil.Errorf("%s subscribe system tables failed, err %v", logTag, err)
			break
		}

		if err := c.waitTimestamp(ctx); err != nil {
			return
		}

		if err := c.replayCatalogCache(ctx, e); err != nil {
			c.pause(true)
			logutil.Errorf("%s replay catalog cache failed, err %v", logTag, err)
			break
		}

		c.mu.Lock()
		currentClient, currentGeneration := c.subscriber.clientAndGeneration()
		if !c.mu.paused && !c.closed.Load() && attemptClient != nil && currentClient == attemptClient && currentGeneration == attemptGeneration {
			e.setPushClientStatus(true)
		}
		c.mu.Unlock()
		logutil.Infof("%s %s: connected to server", logTag, c.serviceID)

		return
	}
}

// retryFirstConnection joins both the receiver's in-flight dispatch and all
// per-table consumers before rebuilding their partition and catalog state.
func (c *PushClient) retryFirstConnection(ctx context.Context, e *Engine) bool {
	req := c.pause(false)
	if !waitLogtailPause(ctx, req) {
		return false
	}
	c.stopConsumers()
	for ctx.Err() == nil && !c.closed.Load() {
		if !waitPushClient(ctx, time.Second) {
			return false
		}
		addr, err := e.getLogTailServiceAddr(ctx)
		if err != nil {
			return false
		}
		if err = c.init(ctx, addr, c.timestampWaiter, e); err != nil {
			logutil.Errorf("%s init push client failed: %v", logTag, err)
			continue
		}
		if err = e.init(ctx); err != nil {
			logutil.Errorf("%s rebuild memory-table failed: %v", logTag, err)
			continue
		}
		if ctx.Err() != nil || c.closed.Load() {
			return false
		}
		c.startConsumers(ctx, e)
		return c.resume(req)
	}
	return false
}

// UnsubscribeTable implements the LogtailEngine interface.
func (c *PushClient) UnsubscribeTable(ctx context.Context, accId, dbID, tbID uint64) error {
	if c.subscriber == nil {
		return moerr.NewInternalErrorf(ctx, "%s cannot unsubscribe table %d-%d as subscriber not initialized", logTag, dbID, tbID)
	}
	if !c.subscriber.ready() {
		return moerr.NewInternalErrorf(ctx, "%s cannot unsubscribe table %d-%d as logtail subscriber is not ready", logTag, dbID, tbID)
	}
	if !c.receivedLogTailTime.ready.Load() {
		return moerr.NewInternalErrorf(ctx, "%s cannot unsubscribe table %d-%d as logtail client is not ready", logTag, dbID, tbID)
	}
	if ifShouldNotDistribute(dbID, tbID) {
		return moerr.NewInternalErrorf(ctx, "%s cannot unsubscribe table %d-%d as table ID is not allowed", logTag, dbID, tbID)
	}
	c.subscribed.rw.Lock()
	defer c.subscribed.rw.Unlock()
	k := tbID
	ent, ok := c.subscribed.m[k]
	if !ok || ent.state != Subscribed {
		logutil.Infof("%s table %d-%d is not subscribed yet", logTag, dbID, tbID)
		return nil
	}

	dbID = ent.dbID
	ent.state = Unsubscribing

	table := api.TableID{DbId: dbID, TbId: tbID}
	err := c.subscriber.sendUnSubscribe(ctx, table)
	if err != nil {
		logutil.Errorf("%s cannot unsubscribe table %d-%d, err: %v", logTag, dbID, tbID, err)
		return err
	}
	logutil.Infof("%s send unsubscribe table %d-%d request succeed", logTag, dbID, tbID)
	return nil
}

func (c *PushClient) doGCUnusedTable(ctx context.Context) {
	cutoff := time.Now().Add(-unsubscribeTimer).UnixNano()

	// Phase 1: read phase, collect candidates
	type unsubJob struct {
		dbID uint64
		tId  uint64
	}
	var toClean []unsubJob

	c.subscribed.rw.RLock()
	for k, ent := range c.subscribed.m {
		if ifShouldNotDistribute(ent.dbID, k) {
			// never unsubscribe the mo_databases, mo_tables, mo_columns.
			continue
		}
		if ent.state == Subscribed && ent.lastTs.Load() <= cutoff {
			toClean = append(toClean, unsubJob{dbID: ent.dbID, tId: k})
		}
	}
	c.subscribed.rw.RUnlock()

	if len(toClean) == 0 {
		return
	}

	// Phase 2: write phase, re-check and mark
	jobs := make([]unsubJob, 0, len(toClean))
	c.subscribed.rw.Lock()
	for _, job := range toClean {
		ent, ok := c.subscribed.m[job.tId]
		if !ok || ent.state != Subscribed {
			continue // state changed
		}
		// Re-check timestamp: table may have been accessed between Phase 1 and Phase 2
		if ent.lastTs.Load() > cutoff {
			continue // recently accessed, skip
		}
		ent.state = Unsubscribing
		jobs = append(jobs, job)
	}
	c.subscribed.rw.Unlock()

	// Phase 3: send RPCs without holding the lock
	// On failure, revert state to Subscribed so GC can retry next round
	var failedJobs []unsubJob
	for _, job := range jobs {
		if err := c.subscriber.sendUnSubscribe(
			ctx,
			api.TableID{DbId: job.dbID, TbId: job.tId}); err == nil {
			logutil.Infof("%s send unsubscribe tbl[db: %d, tbl: %d] request succeed",
				logTag,
				job.dbID,
				job.tId)
		} else {
			logutil.Errorf("%s send unsubsribe tbl[dbId: %d, tblId: %d] request failed, err : %s",
				logTag,
				job.dbID,
				job.tId,
				err.Error())
			failedJobs = append(failedJobs, job)
		}
	}

	// Phase 4: revert failed jobs to Subscribed state for retry with backoff
	// Set timestamp to (now - unsubscribeTimer + unsubscribeProcessTicker) so it will be
	// retried after one GC cycle (20 min) instead of waiting full unsubscribeTimer (1 hour)
	if len(failedJobs) > 0 {
		backoffTs := time.Now().Add(-unsubscribeTimer + unsubscribeProcessTicker).UnixNano()
		c.subscribed.rw.Lock()
		for _, job := range failedJobs {
			if ent, ok := c.subscribed.m[job.tId]; ok && ent.state == Unsubscribing {
				ent.state = Subscribed
				ent.lastTs.Store(backoffTs)
				logutil.Infof("%s reverted tbl[db: %d, tbl: %d] to Subscribed for retry (backoff ~20min)",
					logTag,
					job.dbID,
					job.tId)
			}
		}
		c.subscribed.rw.Unlock()
	}
}

func (c *PushClient) doGCPartitionState(ctx context.Context, e *Engine) {
	parts := make(map[[2]uint64]*logtailreplay.Partition)
	e.Lock()
	for ids, part := range e.partitions {
		parts[ids] = part
	}
	e.Unlock()
	ts := types.BuildTS(time.Now().UTC().UnixNano()-gcPartitionStateTimer.Nanoseconds()*1, 0)
	collector := logtailreplay.NewTruncateCollector()
	for ids, part := range parts {
		part.Truncate(ctx, ids, ts, collector)
	}

	// Batch print: every 100 items per line
	infos := collector.GetAll()
	const itemsPerLine = 100
	for i := 0; i < len(infos); i += itemsPerLine {
		end := i + itemsPerLine
		if end > len(infos) {
			end = len(infos)
		}
		items := infos[i:end]

		logutil.Info(
			"partition.state.truncate",
			zap.Int("count", len(items)),
			zap.Int("total-count", len(infos)),
			zap.Any("items", items),
		)
	}

	e.catalog.Load().GC(ts.ToTimestamp())
}

func (c *PushClient) partitionStateGCTicker(ctx context.Context, e *Engine) {
	ticker := time.NewTicker(gcPartitionStateTicker)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logutil.Info("logtail.consumer.partition.state.gc.ticker.stopped")
			return

		case <-ticker.C:
			if c.subscriber == nil {
				continue
			}
			if !c.receivedLogTailTime.ready.Load() {
				continue
			}
		}

		c.doGCPartitionState(ctx, e)
	}
}

// TryGC checks if ready and runs GC for PushClient resources (unused tables)
func (c *PushClient) TryGC(ctx context.Context) {
	if c.subscriber == nil {
		return
	}
	if !c.subscriber.ready() {
		return
	}
	c.doGCUnusedTable(ctx)
}

// subscribedTable used to record table subscribed status.
// only if m[table T] = true, T has been subscribed.
type subscribedTable struct {
	eng *Engine
	rw  sync.RWMutex

	// value is table's subscription entry with atomic timestamp.
	m map[uint64]*subEntry
}

// subEntry holds subscription state with atomic timestamp for lock-free updates.
type subEntry struct {
	dbID      uint64
	state     SubscribeState
	lastTs    atomic.Int64 // UnixNano, for GC to check if table is still in use
	pendingTo atomic.Pointer[timestamp.Timestamp]
}

// SubTableStatus is used for external API compatibility (e.g., GetState).
type SubTableStatus struct {
	DBID       uint64
	SubState   SubscribeState
	LatestTime time.Time
}

// getSubscribedSnapshotAndPending captures the pending marker before the immutable
// partition snapshot while holding the subscription generation. If pending is
// clear, the later snapshot includes every update whose marker was cleared.
func (c *PushClient) getSubscribedSnapshotAndPending(
	ctx context.Context,
	accId, dbId, tId uint64,
) (*logtailreplay.PartitionState, bool) {
	s := &c.subscribed
	s.rw.RLock()
	ent, exist := s.m[tId]
	if !exist {
		s.rw.RUnlock()
		return nil, false
	}
	if ent.dbID != dbId || ent.state != Subscribed {
		s.rw.RUnlock()
		return nil, false
	}

	now := time.Now().UnixNano()
	if now-ent.lastTs.Load() > int64(time.Minute) {
		ent.lastTs.Store(now)
	}
	pending := ent.pendingTo.Load() != nil
	ps := c.eng.GetOrCreateLatestPart(ctx, accId, dbId, tId).Snapshot()
	s.rw.RUnlock()
	return ps, pending
}

func (c *PushClient) isSubscribed(
	ctx context.Context,
	accId, dbId, tId uint64,
) (*logtailreplay.PartitionState, bool, SubscribeState) {
	ps, ok, state, _ := c.isSubscribedWithPending(ctx, accId, dbId, tId)
	return ps, ok, state
}

func (c *PushClient) isSubscribedWithPending(
	ctx context.Context,
	accId, dbId, tId uint64,
) (*logtailreplay.PartitionState, bool, SubscribeState, bool) {
	return c.isSubscribedWithPendingAccessTime(ctx, accId, dbId, tId, nil)
}

func (c *PushClient) isSubscribedWithPendingAccessTime(
	ctx context.Context,
	accId, dbId, tId uint64,
	accessTime *int64,
) (*logtailreplay.PartitionState, bool, SubscribeState, bool) {

	s := &c.subscribed

	// Single critical section under RLock to avoid creating partitions
	// for tables that are being unsubscribed concurrently.
	s.rw.RLock()
	ent, exist := s.m[tId]
	if !exist {
		s.rw.RUnlock()
		return nil, false, Unsubscribed, false
	}
	if ent.state != Subscribed {
		st := ent.state
		s.rw.RUnlock()
		return nil, false, st, false
	}
	// Update timestamp (with sampling) while holding the read lock to keep
	// state consistent with the partition creation below.
	var now int64
	if accessTime != nil {
		now = *accessTime
	}
	if now == 0 {
		now = time.Now().UnixNano()
		if accessTime != nil {
			*accessTime = now
		}
	}
	if now-ent.lastTs.Load() > int64(time.Minute) {
		ent.lastTs.Store(now)
	}
	pending := ent.pendingTo.Load() != nil
	ps := c.eng.GetOrCreateLatestPart(ctx, accId, dbId, tId).Snapshot()
	s.rw.RUnlock()

	return ps, true, Subscribed, pending
}

// getSubscribedSnapshotForPKCheck requires an open push-client admission gate
// and captures the pending marker before the immutable partition snapshot while
// holding the subscription generation.
// If there is no pending update, the later snapshot includes every table
// update known when the global logtail waterline reached the lock timestamp.
func (c *PushClient) getSubscribedSnapshotForPKCheck(
	ctx context.Context,
	accId, dbId, tId uint64,
) (*logtailreplay.PartitionState, bool, SubscribeState, bool) {
	if !c.receivedLogTailTime.ready.Load() {
		return nil, false, InvalidSubState, false
	}

	s := &c.subscribed
	s.rw.RLock()
	ent, exist := s.m[tId]
	if !exist {
		s.rw.RUnlock()
		return nil, false, Unsubscribed, false
	}
	if ent.dbID != dbId || ent.state != Subscribed {
		state := ent.state
		s.rw.RUnlock()
		return nil, false, state, false
	}

	now := time.Now().UnixNano()
	if now-ent.lastTs.Load() > int64(time.Minute) {
		ent.lastTs.Store(now)
	}
	pending := ent.pendingTo.Load() != nil
	ps := c.eng.GetOrCreateLatestPart(ctx, accId, dbId, tId).Snapshot()
	if !c.receivedLogTailTime.ready.Load() {
		s.rw.RUnlock()
		return nil, false, InvalidSubState, false
	}
	s.rw.RUnlock()
	return ps, true, Subscribed, pending
}

func (c *PushClient) toSubIfUnsubscribed(ctx context.Context, dbId, tblId uint64) (SubscribeState, error) {
	c.subscribed.rw.Lock()
	defer c.subscribed.rw.Unlock()
	_, ok := c.subscribed.m[tblId]
	if !ok {
		if !c.subscriber.ready() {
			if err := func() error {
				c.subscribed.rw.Unlock()
				defer c.subscribed.rw.Lock()
				if err := c.subscriber.waitReady(ctx); err != nil {
					return err
				}
				return nil
			}(); err != nil {
				return InvalidSubState, err
			}
		}
		ent, exist := c.subscribed.m[tblId]
		if exist {
			// The lock was released while waiting for the subscriber to become
			// ready. Another waiter may already own the subscription attempt, or
			// its response may already have advanced the state. Preserve that
			// state so concurrent waiters cannot send duplicate requests or move
			// SubRspReceived back to Subscribing.
			return ent.state, nil
		}
		c.subscribed.m[tblId] = &subEntry{
			dbID:  dbId,
			state: Subscribing,
		}

		if err := c.subscribeTable(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
			//restore the table status.
			delete(c.subscribed.m, tblId)
			return Unsubscribed, err
		}
	}
	return c.subscribed.m[tblId].state, nil

}

func (s *subscribedTable) isSubscribed(dbId, tblId uint64) bool {
	s.rw.RLock()
	ent, exist := s.m[tblId]
	if exist && ent.state == Subscribed {
		// Update timestamp atomically
		ent.lastTs.Store(time.Now().UnixNano())
		s.rw.RUnlock()
		return true
	}
	s.rw.RUnlock()
	return false
}

func (s *subscribedTable) setTablePendingUpdate(dbId, tblId uint64, to timestamp.Timestamp) {
	if to.IsEmpty() {
		return
	}
	s.rw.RLock()
	ent, exist := s.m[tblId]
	if exist && ent.dbID == dbId && (ent.state == Subscribed || ent.state == SubRspReceived) {
		ts := to
		ent.pendingTo.Store(&ts)
	}
	s.rw.RUnlock()
}

func (s *subscribedTable) clearTablePendingUpdate(dbId, tblId uint64, applied timestamp.Timestamp) {
	if applied.IsEmpty() {
		return
	}
	s.rw.RLock()
	ent, exist := s.m[tblId]
	if !exist || ent.dbID != dbId {
		s.rw.RUnlock()
		return
	}
	s.rw.RUnlock()

	for {
		pending := ent.pendingTo.Load()
		if pending == nil || pending.Greater(applied) {
			return
		}
		if ent.pendingTo.CompareAndSwap(pending, nil) {
			return
		}
	}
}

func (s *subscribedTable) hasPendingUpdate(dbId, tblId uint64) bool {
	s.rw.RLock()
	ent, exist := s.m[tblId]
	if !exist || ent.dbID != dbId || ent.state != Subscribed {
		s.rw.RUnlock()
		return false
	}
	pending := ent.pendingTo.Load()
	s.rw.RUnlock()
	return pending != nil
}

// consumeLatestCkp consume the latest checkpoint of the table if not consumed, and return the latest partition state.
func (c *PushClient) loadAndConsumeLatestCkp(
	ctx context.Context,
	accId uint64,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
) (SubscribeState, error) {

	c.subscribed.rw.Lock()
	if c.checkpointReplay == nil {
		c.checkpointReplay = make(chan struct{}, 1)
	}
	gate := c.checkpointReplay
	c.subscribed.rw.Unlock()
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return InvalidSubState, ctx.Err()
	}

	for {
		if err := ctx.Err(); err != nil {
			return InvalidSubState, err
		}
		c.subscribed.rw.Lock()
		ent, exist := c.subscribed.m[tableID]
		if exist && ent.dbID != dbID {
			c.subscribed.rw.Unlock()
			return InvalidSubState, moerr.NewInternalErrorf(ctx, "subscription database mismatch for table %d", tableID)
		}
		if exist && (ent.state == SubRspReceived || ent.state == Subscribed) {
			// Ordinary loading protects an actively used pending subscription
			// from optional idle cleanup before releasing its state fence.
			ent.lastTs.Store(time.Now().UnixNano())
			// Pin the partition under the subscription generation, before I/O.
			part := c.eng.GetOrCreateLatestPart(ctx, accId, dbID, tableID)
			c.subscribed.rw.Unlock()
			err := c.eng.consumeLatestCkp(ctx, part, tableID, tableName, dbID, dbName)
			if ctx.Err() != nil {
				return InvalidSubState, ctx.Err()
			}
			c.subscribed.rw.Lock()
			current := c.subscribed.m[tableID]
			if current != ent || ent.dbID != dbID || (ent.state != SubRspReceived && ent.state != Subscribed) {
				c.subscribed.rw.Unlock()
				continue
			}
			if err != nil {
				c.subscribed.rw.Unlock()
				return InvalidSubState, err
			}
			ent.state = Subscribed
			ent.lastTs.Store(time.Now().UnixNano())
			c.subscribed.rw.Unlock()
			return Subscribed, nil
		}
		// If cleared during replay, use the existing subscribe-start transition.
		if !exist {
			if !c.subscriber.ready() {
				c.subscribed.rw.Unlock()
				return Unsubscribed, moerr.NewInternalError(ctx, "log tail subscriber is not ready")
			}
			c.subscribed.m[tableID] = &subEntry{dbID: dbID, state: Subscribing}
			if err := c.subscribeTable(ctx, api.TableID{DbId: dbID, TbId: tableID}); err != nil {
				delete(c.subscribed.m, tableID)
				c.subscribed.rw.Unlock()
				return Unsubscribed, err
			}
			c.subscribed.rw.Unlock()
			return Subscribing, nil
		}
		state := ent.state
		c.subscribed.rw.Unlock()
		return state, nil
	}
}

func (c *PushClient) waitUntilSubscribingChanged(ctx context.Context, dbId, tblId uint64) (SubscribeState, error) {
	ticker := time.NewTicker(periodToCheckTableSubscribeSucceed)
	defer ticker.Stop()

	for i := 0; i < maxCheckRangeTableSubscribeSucceed; i++ {
		select {
		case <-ctx.Done():
			return InvalidSubState, ctx.Err()
		case <-ticker.C:
			ok, state, err := c.isNotSubscribing(ctx, dbId, tblId)
			if err != nil {
				return state, err
			} else if ok {
				return state, nil
			}
		}
	}
	return InvalidSubState, moerr.NewInternalErrorf(
		ctx,
		"wait for table subscribing changed timeout: db-id: %d, tbl-id: %d, timeout: %s",
		dbId, tblId, maxTimeToCheckTableSubscribeSucceed,
	)
}

func (c *PushClient) waitUntilUnsubscribingChanged(ctx context.Context, dbId, tblId uint64) (SubscribeState, error) {
	ticker := time.NewTicker(periodToCheckTableUnSubscribeSucceed)
	defer ticker.Stop()

	for i := 0; i < maxCheckRangeTableUnSubscribeSucceed; i++ {
		select {
		case <-ctx.Done():
			return InvalidSubState, ctx.Err()
		case <-ticker.C:
			ok, state, err := c.isNotUnsubscribing(ctx, dbId, tblId)
			if err != nil {
				return state, err
				//return nil, state
			} else if ok {
				return state, nil
			}
		}
	}

	return InvalidSubState, moerr.NewInternalErrorf(
		ctx,
		"wait for table unsubscribing changed timeout: db-id: %d, tbl-id: %d, timeout: %s",
		dbId, tblId, maxTimeToCheckTableUnSubscribeSucceed,
	)
}

func (c *PushClient) isNotSubscribing(ctx context.Context, dbId, tblId uint64) (bool, SubscribeState, error) {
	c.subscribed.rw.Lock()
	defer c.subscribed.rw.Unlock()
	ent, exist := c.subscribed.m[tblId]
	if exist {
		if ent.state == Subscribing {
			return false, ent.state, nil
		}
		return true, ent.state, nil
	}
	//table is unsubscribed
	if !c.subscriber.ready() {
		// let wait the subscriber ready.
		return false, Unsubscribed, nil //moerr.NewInternalError(ctx, "log tail subscriber is not ready")
	}
	c.subscribed.m[tblId] = &subEntry{
		dbID:  dbId,
		state: Subscribing,
	}
	if err := c.subscribeTable(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		//restore the table status.
		delete(c.subscribed.m, tblId)
		return true, Unsubscribed, err
	}
	return true, Subscribing, nil
}

// isUnsubscribed check if the table is unsubscribed, if yes, set the table status to subscribing and do subscribe.
func (c *PushClient) isNotUnsubscribing(ctx context.Context, dbId, tblId uint64) (bool, SubscribeState, error) {
	c.subscribed.rw.Lock()
	defer c.subscribed.rw.Unlock()
	ent, exist := c.subscribed.m[tblId]
	if exist {
		if ent.state == Unsubscribing {
			return false, ent.state, nil
		} else {
			return true, ent.state, nil
		}
	}
	//table is unsubscribed
	if !c.subscriber.ready() {
		return false, Unsubscribed, nil //moerr.NewInternalError(ctx, "log tail subscriber is not ready")
	}
	c.subscribed.m[tblId] = &subEntry{
		dbID:  dbId,
		state: Subscribing,
	}
	if err := c.subscribeTable(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		//restore the table status.
		delete(c.subscribed.m, tblId)
		return true, Unsubscribed, err
	}
	return true, Subscribing, nil
}

func (c *PushClient) Disconnect() error {
	c.mu.Lock()
	subscriber := c.subscriber
	if subscriber == nil {
		c.mu.Unlock()
		return nil
	}
	// Disconnect is a transport-level fault injection used by reconnect
	// handling. It must not retire the PushClient owner; Engine.Close is the
	// lifecycle operation that marks the client permanently closed.
	// Capture and detach under transportMu so a concurrent init cannot replace
	// this client between the pause request and detachment.
	subscriber.transportMu.Lock()
	client := subscriber.client()
	if client != nil && c.connector != nil && c.pauseC != nil && !c.closed.Load() {
		c.pauseLockedWithClient(!c.connector.first.Load(), client)
	}
	subscriber.swapClient(nil)
	subscriber.transportMu.Unlock()
	c.mu.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}

func (c *PushClient) Close() error {
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		// Cancel before waiting for the control lock: a disconnect holding that
		// lock may be waiting for transport initialization to observe shutdown.
		cancel := c.cancel
		if cancel != nil {
			cancel()
		}
		c.mu.Lock()
		c.closed.Store(true)
		c.receivedLogTailTime.ready.Store(false)
		if c.subscriber != nil {
			c.subscriber.setNotReady()
		}
		c.mu.Unlock()
		subscriber := c.subscriber
		c.lifecycleMu.Unlock()

		// Close the current logtail client before joining consumers. Receive can
		// be blocked in the transport and only the transport close can release
		// it promptly after the owner context is cancelled.
		if subscriber != nil {
			subscriber.breakoutReceive()
			c.closeErr = subscriber.closeClient()
		}
		c.wg.Wait()
		// All producers and the supervisor have exited. Drop queued source
		// identities so a retained closed PushClient does not retain streams.
		for len(c.consumeErrC) > 0 {
			<-c.consumeErrC
		}
		if subscriber != nil {
			if err := subscriber.closeTransport(); c.closeErr == nil {
				c.closeErr = err
			}
		}
	})
	return c.closeErr
}

func (s *subscribedTable) setTableSubNotExist(dbId, tblId uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.setTableSubNotExistLocked(dbId, tblId)
}

func (s *subscribedTable) setTableSubNotExistLocked(dbId, tblId uint64) {
	ent := &subEntry{
		dbID:  dbId,
		state: SubRspTableNotExist,
	}
	ent.lastTs.Store(time.Now().UnixNano())
	s.m[tblId] = ent
	logutil.Error(
		"logtail.consumer.set.table.sub.not.exist",
		zap.Uint64("db-id", dbId),
		zap.Uint64("tbl-id", tblId),
	)
}

func (s *subscribedTable) clearTable(dbId, tblId uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	delete(s.m, tblId)
}

func (s *subscribedTable) setTableSubscribed(dbId, tblId uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	ent := &subEntry{
		dbID:  dbId,
		state: Subscribed,
	}
	ent.lastTs.Store(time.Now().UnixNano())
	s.m[tblId] = ent
	logutil.Info(
		"logtail.consumer.set.table.subscribed",
		zap.Uint64("db-id", dbId),
		zap.Uint64("tbl-id", tblId),
	)
}

func (s *subscribedTable) setTableSubRspReceived(dbId, tblId uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.setTableSubRspReceivedLocked(dbId, tblId)
}

func (s *subscribedTable) setTableSubRspReceivedLocked(dbId, tblId uint64) {
	ent := &subEntry{
		dbID:  dbId,
		state: SubRspReceived,
	}
	ent.lastTs.Store(time.Now().UnixNano())
	s.m[tblId] = ent
	logutil.Info(
		"logtail.consumer.set.table.sub.rsp.received",
		zap.String("service", s.eng.service),
		zap.Uint64("db-id", dbId),
		zap.Uint64("tbl-id", tblId),
		zap.Any("subscribed-table", s),
	)
}

func (s *subscribedTable) setTableUnsubscribe(dbId, tblId uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.setTableUnsubscribeLocked(dbId, tblId)
}

func (s *subscribedTable) setTableUnsubscribeLocked(dbId, tblId uint64) {
	s.eng.cleanMemoryTableWithTable(dbId, tblId)
	delete(s.m, tblId)
	logutil.Info(
		"logtail.consumer.set.table.unsubscribe",
		zap.Uint64("db-id", dbId),
		zap.Uint64("tbl-id", tblId),
	)
}

// syncLogTailTimestamp is a global log tail timestamp for a cn node.
// support `getTimestamp()` method to get time of last received log.
type syncLogTailTimestamp struct {
	timestampWaiter        client.TimestampWaiter
	ready                  atomic.Bool
	tList                  []atomic.Value
	latestAppliedLogTailTS atomic.Pointer[timestamp.Timestamp]
}

func (r *syncLogTailTimestamp) initLogTailTimestamp(timestampWaiter client.TimestampWaiter) {
	ts := r.getTimestamp()
	if !ts.IsEmpty() {
		r.latestAppliedLogTailTS.Store(&ts)
	}

	r.timestampWaiter = timestampWaiter
	if len(r.tList) == 0 {
		r.tList = make([]atomic.Value, consumerNumber+1)
	}
	for i := range r.tList {
		r.tList[i].Store(timestamp.Timestamp{})
	}
}

func (r *syncLogTailTimestamp) getTimestamp() timestamp.Timestamp {
	var minT timestamp.Timestamp
	for i := 0; i < len(r.tList); i++ {
		t := r.tList[i].Load().(timestamp.Timestamp)
		if i == 0 {
			minT = t
		} else {
			if t.Less(minT) {
				minT = t
			}
		}
	}
	return minT
}

func (r *syncLogTailTimestamp) updateTimestamp(
	index int,
	newTimestamp timestamp.Timestamp,
	receiveAt time.Time) {
	start := time.Now()
	v2.LogTailApplyNotifyLatencyDurationHistogram.Observe(start.Sub(receiveAt).Seconds())
	defer func() {
		v2.LogTailApplyNotifyDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	r.tList[index].Store(newTimestamp)
	if r.ready.Load() {
		ts := r.getTimestamp()
		r.timestampWaiter.NotifyLatestCommitTS(ts)
	}
}

type logTailSubscriber struct {
	sid           string
	transportMu   sync.Mutex
	rpcClient     morpc.RPCClient
	rpcStream     morpc.Stream
	logTailClient *service.LogtailClient
	// transportGeneration is guarded together with logTailClient by mu.
	// Zero disables optional generation-based admission after exhaustion.
	transportGeneration uint64

	mu struct {
		sync.RWMutex
		cond  *sync.Cond
		ready bool
	}

	sendSubscribe   func(context.Context, api.TableID) error
	sendUnSubscribe func(context.Context, api.TableID) error
}

func newLogTailSubscriber() *logTailSubscriber {
	l := &logTailSubscriber{}
	l.mu.cond = sync.NewCond(&l.mu)
	return l
}

func clientIsPreparing(context.Context, api.TableID) error {
	return moerr.NewInternalErrorNoCtx("log tail client is not ready")
}

type logTailSubscriberResponse struct {
	response *service.LogtailResponse
	err      error
}

// XXX generate a rpc client and new a stream.
// we should hide these code into service's NewClient method next day.
func DefaultNewRpcStreamToTnLogTailService(
	ctx context.Context,
	sid string,
	serviceAddr string,
	rpcClient morpc.RPCClient,
) (morpc.RPCClient, morpc.Stream, error) {
	created := rpcClient == nil
	if created {
		logger := logutil.GetGlobalLogger().Named("cn-log-tail-client")
		codec := morpc.NewMessageCodec(
			sid,
			func() morpc.Message {
				return &service.LogtailResponseSegment{}
			},
		)
		factory := morpc.NewGoettyBasedBackendFactory(codec,
			morpc.WithBackendGoettyOptions(
				goetty.WithSessionRWBUfferSize(1<<20, 1<<20),
			),
			morpc.WithBackendLogger(logger),
			morpc.WithBackendReadTimeout(defaultRPCReadTimeout),
		)

		c, err := morpc.NewClient(
			"logtail-client",
			factory,
			morpc.WithClientLogger(logger),
		)
		if err != nil {
			return nil, nil, err
		}
		rpcClient = c
	}

	stream, err := rpcClient.NewStream(ctx, serviceAddr, true)
	if err != nil {
		if created {
			_ = rpcClient.Close()
		}
		return nil, nil, err
	}

	return rpcClient, stream, nil
}

func (s *logTailSubscriber) init(
	ctx context.Context,
	sid string,
	serviceAddr string,
	rpcStreamFactory func(context.Context, string, string, morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error)) (err error) {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()

	s.sid = sid

	// clear the old status.
	s.sendSubscribe = clientIsPreparing
	s.sendUnSubscribe = clientIsPreparing
	if oldClient := s.swapClient(nil); oldClient != nil {
		_ = oldClient.Close()
	}

	rpcClient, rpcStream, err := rpcStreamFactory(ctx, sid, serviceAddr, s.rpcClient)
	if err != nil {
		return err
	}

	s.rpcClient = rpcClient
	if s.rpcStream != nil {
		s.rpcStream.Close(true)
		s.rpcStream = nil
	}

	s.rpcStream = rpcStream

	// new the log tail client.
	logTailClient, err := service.NewLogtailClient(
		ctx,
		s.rpcStream,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond),
	)
	if err != nil {
		return err
	}
	s.swapClient(logTailClient)

	s.sendSubscribe = s.subscribeTable
	s.sendUnSubscribe = s.unSubscribeTable
	return nil
}

func (s *logTailSubscriber) swapClient(client *service.LogtailClient) *service.LogtailClient {
	s.mu.Lock()
	old := s.logTailClient
	s.logTailClient = client
	if client == nil {
		s.transportGeneration = 0
	} else {
		s.transportGeneration = nextLogtailClientGeneration()
	}
	s.mu.Unlock()
	return old
}

// clientAndGeneration takes the client and its generation in one snapshot.
// Queued errors retain this ordinary client incarnation across reconnects.
func (s *logTailSubscriber) clientAndGeneration() (*service.LogtailClient, uint64) {
	s.mu.RLock()
	client, generation := s.logTailClient, s.transportGeneration
	s.mu.RUnlock()
	return client, generation
}

func (s *logTailSubscriber) client() *service.LogtailClient {
	s.mu.RLock()
	client := s.logTailClient
	s.mu.RUnlock()
	return client
}

func (s *logTailSubscriber) closeClient() error {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	return s.closeClientLocked()
}

func (s *logTailSubscriber) closeClientLocked() error {
	client := s.swapClient(nil)
	if client == nil {
		return nil
	}
	return client.Close()
}

func (s *logTailSubscriber) closeTransport() error {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()

	var firstErr error
	if err := s.closeClientLocked(); err != nil {
		firstErr = err
	}
	if s.rpcStream != nil {
		if err := s.rpcStream.Close(true); err != nil && firstErr == nil {
			firstErr = err
		}
		s.rpcStream = nil
	}
	if s.rpcClient != nil {
		if err := s.rpcClient.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.rpcClient = nil
	}
	return firstErr
}

func (s *logTailSubscriber) breakoutReceive() {
	if client := s.client(); client != nil {
		client.BreakoutReceive()
	}
}

func (s *logTailSubscriber) setReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.ready = true
	s.mu.cond.Broadcast()
}

func (s *logTailSubscriber) setNotReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.ready = false
}

func (s *logTailSubscriber) ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.ready
}

func (s *logTailSubscriber) waitReady(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// If ctx is already cancelled, return immediately. We check this before
	// registering AfterFunc to avoid unnecessary callback registration.
	// Note: context.AfterFunc always runs its callback in a separate goroutine,
	// never synchronously in the caller's goroutine.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// sync.Cond cannot observe context cancellation itself. Register the
	// wakeup while holding the same mutex as Wait: if cancellation races the
	// ctx.Err check above, the callback waits for Wait to atomically release
	// the lock and then broadcasts, so no wakeup can be lost.
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.mu.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	for !s.mu.ready {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.mu.cond.Wait()
	}
	return nil
}

// waitReadyClient captures the client belonging to the ready stream
// generation under the same lock as the ready flag. A reconnect can either
// close that captured old client or publish a later ready generation, but a
// caller can never observe the new client while it is still initializing.
func (s *logTailSubscriber) waitReadyClient(
	ctx context.Context,
) (*service.LogtailClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.mu.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	for !s.mu.ready {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.cond.Wait()
	}
	if s.logTailClient == nil {
		return nil, moerr.NewStreamClosedNoCtx()
	}
	return s.logTailClient, nil
}

// can't call this method directly.
func (s *logTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	client := s.client()
	if client == nil {
		return moerr.NewStreamClosedNoCtx()
	}
	err := client.Subscribe(ctx, tblId)
	return moerr.AttachCause(ctx, err)
}

// can't call this method directly.
func (s *logTailSubscriber) unSubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	client := s.client()
	if client == nil {
		return moerr.NewStreamClosedNoCtx()
	}
	err := client.Unsubscribe(ctx, tblId)
	return moerr.AttachCause(ctx, err)
}

func (s *logTailSubscriber) receiveResponse(deadlineCtx context.Context, client *service.LogtailClient) logTailSubscriberResponse {
	if client == nil {
		return logTailSubscriberResponse{err: moerr.NewStreamClosedNoCtx()}
	}
	r, err := client.Receive(deadlineCtx)
	resp := logTailSubscriberResponse{
		response: r,
		err:      err,
	}
	return resp
}

func (s *logTailSubscriber) readBarrier(ctx context.Context) (timestamp.Timestamp, error) {
	for {
		client, err := s.waitReadyClient(ctx)
		if err != nil {
			return timestamp.Timestamp{}, err
		}
		frontier, err := client.ReadBarrier(ctx)
		if err == nil {
			return frontier, nil
		}
		if ctx.Err() != nil {
			return timestamp.Timestamp{}, context.Cause(ctx)
		}
		// The old stream's private cancellation must not cancel a live SQL
		// caller. Retry only this read-only barrier, only after its transport
		// was replaced, and obtain a new TN frontier before the apply wait.
		if (!errors.Is(err, context.Canceled) && !moerr.IsMoErrCode(err, moerr.ErrStreamClosed)) ||
			s.client() == client {
			return timestamp.Timestamp{}, err
		}
	}
}

func waitServerReady(ctx context.Context, addr string) error {
	network := "tcp"
	if strings.HasSuffix(addr, ".sock") {
		network = "unix"
		addr = strings.TrimPrefix(addr, "unix://")
	}
	serverFatal := time.NewTimer(defaultServerTimeout)
	defer serverFatal.Stop()
	ticker := time.NewTicker(defaultDialServerInterval)
	defer ticker.Stop()
	dialer := net.Dialer{Timeout: defaultDialServerTimeout}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if addr == FakeLogtailServerAddress {
			return nil
		}
		conn, err := dialer.DialContext(ctx, network, addr)
		if err == nil {
			_ = conn.Close()
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-serverFatal.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			panic(fmt.Sprintf("could not connect to logtail server for %s", defaultServerTimeout))
		case <-ticker.C:
			logutil.Warn("logtail.consumer.wait.server.ready", zap.String("addr", addr))
		}
	}
}

func (e *Engine) getLogTailServiceAddr(ctx context.Context) (string, error) {
	start := time.Now()
	timeout := time.NewTimer(defaultGetLogTailAddrTimeoutDuration)
	defer timeout.Stop()
	ticker := time.NewTicker(time.Millisecond * 20)
	defer ticker.Stop()

	for {
		cluster, err := clusterservice.GetMOClusterWithContext(ctx, e.service)
		if err != nil {
			return "", err
		}
		tnServices, err := clusterservice.GetAllTNServicesWithContext(ctx, cluster)
		if err != nil {
			return "", err
		}
		if len(tnServices) > 0 && tnServices[0].LogTailServiceAddress != "" {
			addr := tnServices[0].LogTailServiceAddress
			logutil.Info("logtail.consumer.get.logtail.service.addr",
				zap.String("addr", addr),
				zap.Duration("cost", time.Since(start)),
			)
			return addr, ctx.Err()
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timeout.C:
			if err := ctx.Err(); err != nil {
				return "", err
			}
			panic(fmt.Sprintf("cannot get logtail service address, timeout %s",
				defaultGetLogTailAddrTimeoutDuration))
		case <-ticker.C:
		}
	}
}

func (e *Engine) InitLogTailPushModel(ctx context.Context, timestampWaiter client.TimestampWaiter) error {
	ownerCtx := e.pClient.start(ctx)
	logTailServerAddr, err := e.getLogTailServiceAddr(ownerCtx)
	if err != nil {
		return err
	}

	// Wait for logtail server is ready.
	if err := waitServerReady(ownerCtx, logTailServerAddr); err != nil {
		return err
	}

	// try to init log tail client. if failed, retry.
	for {
		if err := ownerCtx.Err(); err != nil {
			logutil.Info(
				"logtail.consumer.init.push.model.failed",
				zap.Error(err),
			)
			return err
		}

		// get log tail service address.
		if err := e.pClient.init(ownerCtx, logTailServerAddr, timestampWaiter, e); err != nil {
			logutil.Error(
				"logtail.consumer.init.push.model.client.failed",
				zap.Error(err),
			)
			continue
		}
		break
	}
	e.pClient.eng = e
	e.pClient.subscribed.eng = e

	if !e.pClient.runOwned(func() {
		e.pClient.connector.run(ownerCtx)
	}) {
		return context.Canceled
	}

	// Start a goroutine that never stops to receive logtail from TN logtail server.
	if !e.pClient.runOwned(func() {
		e.pClient.run(ownerCtx, e)
	}) {
		return context.Canceled
	}

	return nil
}

func ifShouldNotDistribute(dbId, tblId uint64) bool {
	return dbId == catalog.MO_CATALOG_ID && tblId <= catalog.MO_COLUMNS_ID
}

func dispatchSubscribeResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.SubscribeResponse,
	recRoutines []*routineController,
	receiveAt time.Time) error {
	lt := response.Logtail
	tbl := lt.GetTable()

	notDistribute := ifShouldNotDistribute(tbl.DbId, tbl.TbId)
	if notDistribute {
		// time check for issue #10833.
		startTime := time.Now()
		defer func() {
			tDuration := time.Since(startTime)
			if tDuration > time.Millisecond*5 {
				logutil.Warn(
					"logtail.consumer.slow.subscribe.response",
					zap.Uint64("db-id", tbl.DbId),
					zap.Uint64("tbl-id", tbl.TbId),
					zap.Duration("cost", tDuration),
				)
			}
		}()

		if err := e.consumeSubscribeResponse(ctx, response, false, receiveAt); err != nil {
			return err
		}
		e.pClient.subscribed.setTableSubscribed(tbl.DbId, tbl.TbId)
	} else {
		routineIndex := tbl.TbId % consumerNumber
		recRoutines[routineIndex].sendSubscribeResponse(ctx, response, receiveAt, e.captureReplayClusterKey(&lt, *lt.Ts))
	}
	// no matter how we consume the response, should update all timestamp.
	e.pClient.receivedLogTailTime.updateTimestamp(consumerNumber, *lt.Ts, receiveAt)
	for i := range recRoutines {
		recRoutines[i].updateTimeFromT(*lt.Ts, receiveAt)
	}
	return nil
}

func dispatchUpdateResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.UpdateResponse,
	recRoutines []*routineController,
	receiveAt time.Time) error {
	list := response.GetLogtailList()

	// loops for mo_database, mo_tables, mo_columns.
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_DATABASE_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false, receiveAt, *response.To); err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_TABLES_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false, receiveAt, *response.To); err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_COLUMNS_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false, receiveAt, *response.To); err != nil {
				return err
			}
		}
	}

	var (
		lastLogtailIndex [consumerNumber]int
		hasLogtail       [consumerNumber]bool
	)
	for i := range lastLogtailIndex {
		lastLogtailIndex[i] = -1
	}
	for index := 0; index < len(list); index++ {
		table := list[index].Table
		if ifShouldNotDistribute(table.DbId, table.TbId) {
			continue
		}
		recIndex := table.TbId % consumerNumber
		lastLogtailIndex[recIndex] = index
	}
	for index := 0; index < len(list); index++ {
		table := list[index].Table
		if ifShouldNotDistribute(table.DbId, table.TbId) {
			continue
		}
		recIndex := table.TbId % consumerNumber
		hasLogtail[recIndex] = true
		e.pClient.subscribed.setTablePendingUpdate(table.DbId, table.TbId, *response.To)
		recRoutines[recIndex].sendTableLogTailWithTimestamp(
			list[index],
			*response.To,
			lastLogtailIndex[recIndex] == index,
			receiveAt,
			e.captureReplayClusterKey(&list[index], *response.To),
		)
	}
	// should update all the timestamp.
	e.pClient.receivedLogTailTime.updateTimestamp(consumerNumber, *response.To, receiveAt)
	for i := range recRoutines {
		if !hasLogtail[i] {
			recRoutines[i].updateTimeFromT(*response.To, receiveAt)
		}
	}

	n := 0
	for i := range recRoutines {
		n += len(recRoutines[i].signalChan)
	}
	v2.LogTailApplyQueueSizeGauge.Set(float64(n))
	return nil
}

func dispatchUnSubscribeResponse(
	_ context.Context,
	_ *Engine,
	response *logtail.UnSubscribeResponse,
	recRoutines []*routineController,
	receiveAt time.Time) error {
	tbl := response.Table
	notDistribute := ifShouldNotDistribute(tbl.DbId, tbl.TbId)
	if notDistribute {
		logutil.Error(
			"logtail.consumer.dispatch.unsubscribe.response.unexpected",
			zap.Uint64("db-id", tbl.DbId),
			zap.Uint64("tbl-id", tbl.TbId),
		)
		return nil
	}
	routineIndex := tbl.TbId % consumerNumber
	recRoutines[routineIndex].sendUnSubscribeResponse(response, receiveAt)

	return nil
}

type routineController struct {
	routineId  int
	closeChan  chan bool
	signalChan chan routineControlCmd
	done       chan struct{}

	// two pools to provide cmdConsumeLog and cmdConsumeTime
	cmdLogPool  sync.Pool
	cmdTimePool sync.Pool

	// monitor the consumption speed of logs.
	warningBufferLen int
}

// A queued consumer failure retains the stream that produced the consumer.
// This identity must be checked when the failure is used to request recovery.
type logtailConsumerError struct {
	client     *service.LogtailClient
	generation uint64
	err        error
}

func (rc *routineController) sendSubscribeResponse(
	_ context.Context,
	r *logtail.SubscribeResponse,
	receiveAt time.Time,
	keys ...*readutil.ClusterKeyDescriptor) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Info(
			"logtail.consumer.slow.consume.routine",
			zap.Int("routine-id", rc.routineId),
			zap.Int("signal-chan-len", l),
		)
	}

	rc.send(&cmdToConsumeSub{log: r, receiveAt: receiveAt, clusterKey: firstReplayClusterKey(keys)})
}

func (rc *routineController) sendTableLogTail(r logtail.TableLogtail, receiveAt time.Time) {
	rc.sendTableLogTailWithTimestamp(r, timestamp.Timestamp{}, false, receiveAt)
}

func (rc *routineController) sendTableLogTailWithTimestamp(
	r logtail.TableLogtail,
	applied timestamp.Timestamp,
	notifyApplied bool,
	receiveAt time.Time,
	keys ...*readutil.ClusterKeyDescriptor) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Info(
			"logtail.consumer.slow.send.table.log.tail",
			zap.Int("routine-id", rc.routineId),
			zap.Int("signal-chan-len", l),
		)
	}

	log := rc.cmdLogPool.Get().(*cmdToConsumeLog)
	log.log = r
	log.applied = applied
	log.notifyApplied = notifyApplied
	log.receiveAt = receiveAt
	log.clusterKey = firstReplayClusterKey(keys)
	if !rc.send(log) {
		log.log = logtail.TableLogtail{}
		log.applied = timestamp.Timestamp{}
		log.notifyApplied = false
		log.receiveAt = time.Time{}
		log.clusterKey = nil
		rc.cmdLogPool.Put(log)
	}
}

func (rc *routineController) updateTimeFromT(
	t timestamp.Timestamp,
	receiveAt time.Time) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Info(
			"logtail.consumer.slow.update.time",
			zap.Int("routine-id", rc.routineId),
			zap.Int("signal-chan-len", l),
		)
	}

	updateTime := rc.cmdTimePool.Get().(*cmdToUpdateTime)
	updateTime.time = t
	updateTime.receiveAt = receiveAt
	if !rc.send(updateTime) {
		updateTime.time = timestamp.Timestamp{}
		updateTime.receiveAt = time.Time{}
		rc.cmdTimePool.Put(updateTime)
	}
}

func (rc *routineController) sendUnSubscribeResponse(r *logtail.UnSubscribeResponse, receiveAt time.Time) {
	// debug for issue #10138.
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Info(
			"logtail.consumer.slow.send.unsubscribe.response",
			zap.Int("routine-id", rc.routineId),
			zap.Int("signal-chan-len", l),
		)
	}

	rc.send(&cmdToConsumeUnSub{log: r, receiveAt: receiveAt})
}

func (rc *routineController) close() {
	if rc.done == nil {
		select {
		case rc.closeChan <- true:
		default:
		}
		return
	}
	select {
	case rc.closeChan <- true:
	case <-rc.done:
	}
}

func (rc *routineController) wait() {
	if rc.done != nil {
		<-rc.done
	}
}

func (rc *routineController) send(cmd routineControlCmd) bool {
	if rc.done == nil {
		rc.signalChan <- cmd
		return true
	}
	select {
	case rc.signalChan <- cmd:
		return true
	case <-rc.done:
		return false
	}
}

func (c *PushClient) createRoutineToConsumeLogTails(
	ctx context.Context, routineId int, signalBufferLength int, e *Engine,
) *routineController {

	var transport *service.LogtailClient
	var generation uint64
	if c.subscriber != nil {
		transport, generation = c.subscriber.clientAndGeneration()
	}
	singleRoutineToConsumeLogTail := func(ctx context.Context, engine *Engine, receiver *routineController, errRet chan logtailConsumerError) {
		defer close(receiver.done)
		errHappen := false
		for {
			select {
			case <-ctx.Done():
				return

			case cmd := <-receiver.signalChan:
				if cmd == nil {
					return
				}
				if errHappen {
					continue
				}
				if err := cmd.action(ctx, engine, receiver); err != nil {
					errHappen = true
					// The supervisor may already have exited on owner cancellation.
					select {
					case errRet <- (logtailConsumerError{client: transport, generation: generation, err: err}):
					case <-ctx.Done():
						return
					}
				}

			case <-receiver.closeChan:
				return
			}
		}
	}

	controller := &routineController{
		routineId:  routineId,
		closeChan:  make(chan bool, 1),
		signalChan: make(chan routineControlCmd, signalBufferLength),
		done:       make(chan struct{}),
		cmdLogPool: sync.Pool{
			New: func() any {
				return &cmdToConsumeLog{}
			},
		},
		cmdTimePool: sync.Pool{
			New: func() any {
				return &cmdToUpdateTime{}
			},
		},

		// Debug for issue #10138.
		warningBufferLen: int(float64(signalBufferLength) * consumerWarningPercent),
	}

	if !c.runOwned(func() {
		singleRoutineToConsumeLogTail(ctx, e, controller, c.consumeErrC)
	}) {
		close(controller.done)
	}

	return controller
}

// a signal to control the routine which is responsible for consuming log tail.
type routineControlCmd interface {
	action(ctx context.Context, e *Engine, ctrl *routineController) error
}

type cmdToConsumeSub struct {
	clusterKey *readutil.ClusterKeyDescriptor
	log        *logtail.SubscribeResponse
	receiveAt  time.Time
}
type cmdToConsumeLog struct {
	clusterKey    *readutil.ClusterKeyDescriptor
	log           logtail.TableLogtail
	applied       timestamp.Timestamp
	notifyApplied bool
	receiveAt     time.Time
}
type cmdToUpdateTime struct {
	time      timestamp.Timestamp
	receiveAt time.Time
}
type cmdToConsumeUnSub struct {
	log       *logtail.UnSubscribeResponse
	receiveAt time.Time
}

func (cmd *cmdToConsumeSub) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeSubscribeResponse(ctx, response, true, cmd.receiveAt, cmd.clusterKey); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	s := &e.pClient.subscribed
	s.rw.Lock()
	s.setTableSubRspReceivedLocked(tbl.DbId, tbl.TbId)
	s.rw.Unlock()
	return nil
}

func (cmd *cmdToConsumeLog) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	defer func() {
		cmd.log = logtail.TableLogtail{}
		cmd.applied = timestamp.Timestamp{}
		cmd.notifyApplied = false
		cmd.receiveAt = time.Time{}
		cmd.clusterKey = nil
		ctrl.cmdLogPool.Put(cmd)
	}()
	response := cmd.log
	if err := e.consumeUpdateLogTail(ctx, response, true, cmd.receiveAt, cmd.applied, cmd.clusterKey); err != nil {
		return err
	}
	table := response.GetTable()
	if table != nil {
		e.pClient.subscribed.clearTablePendingUpdate(table.DbId, table.TbId, cmd.applied)
	}
	if cmd.notifyApplied {
		e.pClient.receivedLogTailTime.updateTimestamp(ctrl.routineId, cmd.applied, cmd.receiveAt)
	}
	return nil
}

func (cmd *cmdToUpdateTime) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	defer ctrl.cmdTimePool.Put(cmd)
	e.pClient.receivedLogTailTime.updateTimestamp(ctrl.routineId, cmd.time, cmd.receiveAt)
	return nil
}

func (cmd *cmdToConsumeUnSub) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	table := cmd.log.Table
	//e.cleanMemoryTableWithTable(table.DbId, table.TbId)
	s := &e.pClient.subscribed
	s.rw.Lock()
	s.setTableUnsubscribeLocked(table.DbId, table.TbId)
	s.rw.Unlock()
	return nil
}

func (e *Engine) consumeSubscribeResponse(
	ctx context.Context,
	rp *logtail.SubscribeResponse,
	lazyLoad bool,
	receiveAt time.Time,
	keys ...*readutil.ClusterKeyDescriptor) error {
	lt := rp.GetLogtail()
	applied := timestamp.Timestamp{}
	if lt.Ts != nil {
		applied = *lt.Ts
	}
	return updatePartitionOfPush(ctx, e, &lt, lazyLoad, receiveAt, true, applied, keys...)
}

func (e *Engine) consumeUpdateLogTail(
	ctx context.Context,
	rp logtail.TableLogtail,
	lazyLoad bool,
	receiveAt time.Time,
	applied timestamp.Timestamp,
	keys ...*readutil.ClusterKeyDescriptor) error {
	return updatePartitionOfPush(ctx, e, &rp, lazyLoad, receiveAt, false, applied, keys...)
}

// updatePartitionOfPush is the partition update method of log tail push model.
func updatePartitionOfPush(
	ctx context.Context,
	e *Engine,
	tl *logtail.TableLogtail,
	lazyLoad bool,
	receiveAt time.Time,
	isSub bool,
	applied timestamp.Timestamp,
	keys ...*readutil.ClusterKeyDescriptor) (err error) {
	start := time.Now()
	v2.LogTailApplyLatencyDurationHistogram.Observe(start.Sub(receiveAt).Seconds())
	defer func() {
		v2.LogTailApplyDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// after consume the logtail, enqueue it to global stats.
	defer func() {
		t0 := time.Now()
		e.globalStats.enqueue(tl)
		v2.LogtailUpdatePartitonEnqueueGlobalStatsDurationHistogram.Observe(time.Since(t0).Seconds())
	}()

	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()

	t0 := time.Now()
	partition := e.GetOrCreateLatestPart(ctx, uint64(tl.Table.AccId), dbId, tblId)
	v2.LogtailUpdatePartitonGetPartitionDurationHistogram.Observe(time.Since(t0).Seconds())

	t0 = time.Now()
	lockErr := partition.Lock(ctx)
	if lockErr != nil {
		v2.LogtailUpdatePartitonGetLockDurationHistogram.Observe(time.Since(t0).Seconds())
		return lockErr
	}
	defer partition.Unlock()
	v2.LogtailUpdatePartitonGetLockDurationHistogram.Observe(time.Since(t0).Seconds())

	if !partition.TableInfoOK {
		partition.TableInfo.ID = tblId
		partition.TableInfo.Name = tl.Table.GetTbName()
		partition.TableInfo.PrimarySeqnum = int(tl.Table.GetPrimarySeqnum())
		partition.TableInfoOK = true
	}

	state, doneMutate := partition.MutateState()

	var (
		ckpStart types.TS
		ckpEnd   types.TS
	)

	if lazyLoad {
		if len(tl.CkpLocation) > 0 {
			t0 = time.Now()
			ckpStart, ckpEnd = parseCkpDuration(tl)
			v2.LogtailUpdatePartitonHandleCheckpointDurationHistogram.Observe(time.Since(t0).Seconds())
		}

		t0 = time.Now()
		err = consumeLogTail(
			ctx,
			partition.TableInfo.PrimarySeqnum,
			e,
			state,
			tl,
			isSub,
			keys...,
		)
		v2.LogtailUpdatePartitonConsumeLogtailDurationHistogram.Observe(time.Since(t0).Seconds())

	} else {
		if len(tl.CkpLocation) > 0 {
			t0 = time.Now()
			//TODO::
			ckpStart, ckpEnd = parseCkpDuration(tl)
			v2.LogtailUpdatePartitonHandleCheckpointDurationHistogram.Observe(time.Since(t0).Seconds())
		}
		t0 = time.Now()
		err = consumeCkpsAndLogTail(ctx, partition.TableInfo.PrimarySeqnum, e, state, tl, dbId, tblId, isSub, keys...)
		v2.LogtailUpdatePartitonConsumeLogtailDurationHistogram.Observe(time.Since(t0).Seconds())
	}

	if err != nil {
		logutil.Error(
			"logtail.consumer.update.partition.of.push.error",
			zap.Uint64("tbl-id", tblId),
			zap.String("table-name", partition.TableInfo.Name),
			zap.Error(err),
		)
		return err
	}

	//After consume checkpoints finished ,then update the start and end of
	//the table's partition and catalog cache.
	if isSub {
		if len(tl.CkpLocation) != 0 {
			if !ckpStart.IsEmpty() || !ckpEnd.IsEmpty() {
				t0 = time.Now()
				state.UpdateDuration(ckpStart, types.MaxTs())
				if lazyLoad {
					state.AppendCheckpoint(tl.CkpLocation, partition)
				}
				// else {
				//Notice that the checkpoint duration is same among all mo system tables,
				//such as mo_databases, mo_tables, mo_columns.
				//	e.GetLatestCatalogCache().UpdateDuration(ckpStart, types.MaxTs())

				v2.LogtailUpdatePartitonUpdateTimestampsDurationHistogram.Observe(time.Since(t0).Seconds())
			}
		} else {
			state.UpdateDuration(types.TS{}, types.MaxTs())
			// leave this to replayCatalogCache
			// if !lazyLoad {
			// 	e.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())
			// }
		}
	}

	if applied.IsEmpty() && tl.Ts != nil {
		applied = *tl.Ts
	}
	state.UpdateAppliedTo(types.TimestampToTS(applied))

	doneMutate()

	return nil
}

func consumeLogTail(
	ctx context.Context,
	primarySeqnum int,
	engine *Engine,
	state *logtailreplay.PartitionState,
	lt *logtail.TableLogtail,
	isSub bool,
	keys ...*readutil.ClusterKeyDescriptor,
) error {
	// return hackConsumeLogtail(ctx, primarySeqnum, engine, state, lt)
	if lt.Table.DbName == "" {
		panic(fmt.Sprintf("missing fields %v", lt.Table.String()))
	}
	t0 := time.Now()
	for i := 0; i < len(lt.Commands); i++ {
		if err := consumeEntry(ctx, primarySeqnum,
			engine, engine.GetLatestCatalogCache(), state, &lt.Commands[i], isSub, keys...); err != nil {
			return err
		}
	}

	if catalog.IsSystemTable(lt.Table.TbId) {
		v2.LogtailUpdatePartitonConsumeLogtailCatalogTableDurationHistogram.Observe(time.Since(t0).Seconds())
	} else {
		v2.LogtailUpdatePartitonConsumeLogtailDurationHistogram.Observe(time.Since(t0).Seconds())
	}
	return nil
}

func parseCkpDuration(lt *logtail.TableLogtail) (start types.TS, end types.TS) {
	locationsAndVersions := strings.Split(lt.CkpLocation, ";")
	//check whether metLoc contains duration: [start, end]
	if !strings.Contains(locationsAndVersions[len(locationsAndVersions)-1], "[") {
		return
	}

	newlocs := locationsAndVersions[:len(locationsAndVersions)-1]
	lt.CkpLocation = strings.Join(newlocs, ";")

	duration := locationsAndVersions[len(locationsAndVersions)-1]
	pos1 := strings.Index(duration, "[")
	pos2 := strings.Index(duration, "]")
	sub := duration[pos1+1 : pos2]
	ds := strings.Split(sub, "_")
	return types.StringToTS(ds[0]), types.StringToTS(ds[1])
}

func consumeCkpsAndLogTail(
	ctx context.Context,
	primarySeqnum int,
	engine *Engine,
	state *logtailreplay.PartitionState,
	lt *logtail.TableLogtail,
	databaseId uint64,
	tableId uint64,
	isSub bool,
	keys ...*readutil.ClusterKeyDescriptor,
) (err error) {
	var closeCBs []func()
	if err = taeLogtail.ConsumeCheckpointEntries(
		ctx,
		engine.service,
		lt.CkpLocation,
		tableId, lt.Table.TbName,
		databaseId, lt.Table.DbName,
		state.HandleObjectEntry,
		engine.mp, engine.fs); err != nil {
		return
	}
	defer func() {
		for _, cb := range closeCBs {
			if cb != nil {
				cb()
			}
		}
	}()
	return consumeLogTail(ctx, primarySeqnum, engine, state, lt, isSub, keys...)
}

func firstReplayClusterKey(keys []*readutil.ClusterKeyDescriptor) *readutil.ClusterKeyDescriptor {
	if len(keys) == 1 {
		return keys[0]
	}
	return nil
}

// captureReplayClusterKey runs only at serialized dispatch, after catalog
// application and before user work is enqueued. Worker lag cannot change it.
func (e *Engine) captureReplayClusterKey(tail *logtail.TableLogtail, at timestamp.Timestamp) *readutil.ClusterKeyDescriptor {
	if tail == nil || tail.Table == nil || at.IsEmpty() {
		return nil
	}
	table := tail.Table
	e.RLock()
	partition := e.partitions[[2]uint64{table.DbId, table.TbId}]
	e.RUnlock()
	if partition != nil && !partition.Snapshot().WantsClusterKeyMetadata() {
		return nil
	}
	candidate := false
	for _, entry := range tail.Commands {
		if entry.EntryType != api.Entry_Insert || entry.Bat == nil || logtailreplay.IsMetaEntry(entry.TableName) {
			continue
		}
		for _, attr := range entry.Bat.Attrs {
			if strings.HasPrefix(attr, catalog.PrefixCBColName) {
				candidate = true
				break
			}
		}
		if candidate {
			break
		}
	}
	if !candidate {
		return nil
	}
	e.pClient.dca.Lock()
	defer e.pClient.dca.Unlock()
	if !e.pClient.dca.replayed {
		return nil
	}
	return e.GetLatestCatalogCache().ReplayClusterKey(uint32(table.AccId), table.DbId, table.TbId, table.TbName, at)
}

// Ordinary receiver recovery must not act on a retired client's queued error.
var logtailClientGeneration atomic.Uint64

func nextLogtailClientGeneration() uint64 {
	for {
		previous := logtailClientGeneration.Load()
		if previous == ^uint64(0) {
			return 0
		}
		if logtailClientGeneration.CompareAndSwap(previous, previous+1) {
			return previous + 1
		}
	}
}
