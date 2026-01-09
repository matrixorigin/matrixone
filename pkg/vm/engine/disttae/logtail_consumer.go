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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
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

	// timestampWaiter is used to notify the latest commit timestamp
	timestampWaiter client.TimestampWaiter

	// connectC is the channel which is used to control the connection
	// flow.
	connector *connector

	// initialized is true means that it is not the first time to init push client.
	initialized bool

	mu struct {
		sync.Mutex
		paused bool
	}
	// pauseC is the channel used to control whether the receiver is paused.
	pauseC  chan bool
	resumeC chan struct{}

	consumeErrC chan error
	receiver    []*routineController
	eng         *Engine

	LogtailRPCClientFactory func(context.Context, string, string, morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error)

	reconnectHandler func()
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

func (c *PushClient) dcaReset() {
	c.dca.Lock()
	defer c.dca.Unlock()
	c.dca.replayed = false
	c.dca.flist = c.dca.flist[:0]
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

type connector struct {
	first  atomic.Bool
	signal chan struct{}

	client *PushClient
	engine *Engine
}

func newConnector(c *PushClient, e *Engine) *connector {
	co := &connector{
		signal: make(chan struct{}, 10),
		client: c,
		engine: e,
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
			c.client.connect(ctx, c.engine)
		}
	}
}

func (c *PushClient) init(
	ctx context.Context,
	serviceAddr string,
	timestampWaiter client.TimestampWaiter,
	e *Engine,
) error {

	c.serviceID = e.GetService()
	c.timestampWaiter = timestampWaiter
	if c.subscriber == nil {
		c.subscriber = newLogTailSubscriber()
	}

	// lock all.
	// release subscribed lock when init finished.
	// release subscriber lock when we received enough response from service.
	c.receivedLogTailTime.e = e
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
		c.consumeErrC = make(chan error, consumerNumber)
		c.pauseC = make(chan bool, 1)
		c.resumeC = make(chan struct{})
	}
	c.initialized = true

	return c.subscriber.init(
		ctx,
		e.GetService(),
		serviceAddr,
		c.LogtailRPCClientFactory,
	)
}

func (c *PushClient) SetReconnectHandler(handler func()) {
	c.reconnectHandler = handler
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

func (c *PushClient) skipSubIfSubscribed(
	ctx context.Context,
	acctId uint64,
	tableID uint64,
	dbID uint64,
) (bool, *logtailreplay.PartitionState) {

	//if table has been subscribed, return quickly.
	if ps, ok, _ := c.isSubscribed(ctx, acctId, dbID, tableID); ok {
		return true, ps
	}
	return false, nil
}

func (c *PushClient) toSubscribeTable(
	ctx context.Context,
	accId uint64,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
) (ps *logtailreplay.PartitionState, err error) {

	var (
		skip     bool
		state    SubscribeState
		injected bool
	)

	if injected, _ = objectio.LogCNSubscribeTableFailInjected(
		dbName, tableName,
	); injected {
		return nil,
			moerr.NewInternalErrorNoCtx("injected subscribe table err")
	}

	if skip, ps = c.skipSubIfSubscribed(ctx, accId, tableID, dbID); skip {
		return ps, nil
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
			ps, _, state = c.isSubscribed(ctx, accId, dbID, tableID)
			if ps != nil {
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

func (c *PushClient) pause(s bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.paused {
		return
	}
	// Note
	// If subSysTables fails to send a successful request, receiveLogtails will receive nothing until the context is done. In this case, we attempt to stop the receiveLogtails goroutine immediately.
	// The break signal left in the channel will interrupt the normal receiving process, but this is not an issue because reconnecting will create a new channel.
	c.subscriber.logTailClient.BreakoutReceive()
	select {
	case c.pauseC <- s:
		c.mu.paused = true
	default:
		logutil.Info("logtail.consumer.already.set.to.pause")
	}
}

func (c *PushClient) resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.resumeC <- struct{}{}:
		c.mu.paused = false
	default:
		logutil.Info("logtail.consumer.not.in.pause.state")
	}
}

func (c *PushClient) receiveOneLogtail(ctx context.Context, e *Engine) error {
	ctx, cancel := context.WithTimeoutCause(ctx, maxTimeToWaitServerResponse, moerr.CauseReceiveOneLogtail)
	defer cancel()

	// Client receives one logtail counter.
	defer v2.LogTailClientReceiveCounter.Add(1)

	if enabled, p := objectio.CNRecvErrInjected(); enabled && rand.Intn(100000) < p {
		return moerr.NewInternalError(ctx, "FIND_TABLE random error")
	}

	resp := c.subscriber.receiveResponse(ctx)
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
		if uint16(status.GetCode()) == moerr.OkExpectedEOB {
			c.subscribed.setTableSubNotExist(
				errRsp.GetTable().GetDbId(),
				errRsp.GetTable().GetTbId())
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
			if s {
				c.sendConnectSig()
			}

			// Wait for resuming logtail receiver.
			select {
			case <-ctx.Done():
				return

			case <-c.resumeC:
			}
			logutil.Info("logtail.consumer.receiver.resumed")

		default:
			if err := c.receiveOneLogtail(ctx, e); err != nil {
				logutil.Error(
					"logtail.consumer.receive.one.logtail.failed",
					zap.Error(err),
				)
				c.pause(!c.connector.first.Load())
			}
		}
	}
}

func (c *PushClient) startConsumers(ctx context.Context, e *Engine) {
	// new parallelNums routine to consume log tails.
	for i := range c.receiver {
		c.receiver[i] = c.createRoutineToConsumeLogTails(ctx, i, consumerBufferLength, e)
	}
}

func (c *PushClient) stopConsumers() {
	for i := range c.receiver {
		c.receiver[i].close()
	}
	logutil.Infof("%s %s: logtail consumers stopped", logTag, c.serviceID)
}

func (c *PushClient) sendConnectSig() {
	if c.connector.first.Load() {
		c.connector.signal <- struct{}{}
		return
	}

	for {
		select {
		case c.connector.signal <- struct{}{}:
			logutil.Infof("%s reconnect signal is received", logTag)
			return
		default:
			logutil.Infof("%s reconnect chan is full", logTag)
			time.Sleep(time.Second)
		}
	}
}

func (c *PushClient) run(ctx context.Context, e *Engine) {
	go c.receiveLogtails(ctx, e)

	// for the first time connector.
	c.sendConnectSig()

	// A dead loop to receive log tail response from log tail service.
	// if any error happened, we should do reconnection.
	for {
		select {
		case err := <-c.consumeErrC:
			// receive an error from sub-routine to consume log.
			logutil.Errorf("%s consume log tail failed, err: %s", logTag, err)
			c.pause(!c.connector.first.Load())

		case <-ctx.Done():
			_ = c.subscriber.rpcClient.Close()
			logutil.Infof("%s logtail consumer stopped", logTag)
			return
		}
	}
}

func (c *PushClient) waitTimestamp() {
	timeout := time.NewTimer(time.Second * 10)
	defer timeout.Stop()

	ticker := time.NewTicker(time.Millisecond * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// we should always make sure that all the log tail consume
			// routines have updated its timestamp.
			if !c.receivedLogTailTime.getTimestamp().IsEmpty() {
				return
			}

		case <-timeout.C:
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

func (c *PushClient) connect(ctx context.Context, e *Engine) {
	if c.connector.first.Load() {
		c.startConsumers(ctx, e)

		for {
			c.dcaReset()
			err := c.subSysTables(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					logutil.Errorf("%s connect failed as context canceled: %v", logTag, ctx.Err())
					return
				}
				c.pause(false)
				time.Sleep(time.Second)

				tnLogTailServerBackend := e.getLogTailServiceAddr()
				if err := c.init(ctx, tnLogTailServerBackend, c.timestampWaiter, e); err != nil {
					logutil.Errorf("%s init push client failed: %v", logTag, err)
					continue
				}

				c.resume()
				continue
			}
			c.waitTimestamp()

			if err := c.replayCatalogCache(ctx, e); err != nil {
				c.pause(false)
				logutil.Errorf("%s replay catalog cache failed, err %v", logTag, err)
				continue
			}

			e.setPushClientStatus(true)
			c.connector.first.Store(false)
			return
		}
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

		tnLogTailServerBackend := e.getLogTailServiceAddr()
		if err := c.init(ctx, tnLogTailServerBackend, c.timestampWaiter, e); err != nil {
			logutil.Errorf("%s rebuild the cn log tail client failed, reason: %s", logTag, err)
			time.Sleep(retryReconnect)
			continue
		}
		logutil.Infof("%s %s: client init finished", logTag, c.serviceID)

		// This is only for test.
		if c.reconnectHandler != nil {
			c.reconnectHandler()
		}

		// clean memory table.
		err := e.init(ctx)
		if err != nil {
			logutil.Errorf("%s rebuild memory-table failed, err: %s", logTag, err)
			time.Sleep(retryReconnect)
			continue
		}
		logutil.Infof("%s %s: clean memory table finished", logTag, c.serviceID)

		// After init, start up again.
		c.startConsumers(ctx, e)

		c.resume()

		c.dcaReset()
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

		c.waitTimestamp()

		if err := c.replayCatalogCache(ctx, e); err != nil {
			c.pause(true)
			logutil.Errorf("%s replay catalog cache failed, err %v", logTag, err)
			break
		}

		e.setPushClientStatus(true)
		logutil.Infof("%s %s: connected to server", logTag, c.serviceID)

		return
	}
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

	if err := c.subscriber.sendUnSubscribe(ctx, api.TableID{DbId: dbID, TbId: tbID}); err != nil {
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

func (c *PushClient) unusedTableGCTicker(ctx context.Context) {
	ticker := time.NewTicker(unsubscribeProcessTicker)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logutil.Infof("%s unsubscribe process exit.", logTag)
			return

		case <-ticker.C:
			if c.subscriber == nil {
				continue
			}
			if !c.subscriber.ready() {
				continue
			}
		}

		c.doGCUnusedTable(ctx)
	}
}

func (c *PushClient) doGCPartitionState(ctx context.Context, e *Engine) {
	parts := make(map[[2]uint64]*logtailreplay.Partition)
	e.Lock()
	for ids, part := range e.partitions {
		parts[ids] = part
	}
	e.Unlock()
	ts := types.BuildTS(time.Now().UTC().UnixNano()-gcPartitionStateTimer.Nanoseconds()*5, 0)
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
	dbID   uint64
	state  SubscribeState
	lastTs atomic.Int64 // UnixNano, for GC to check if table is still in use
}

// SubTableStatus is used for external API compatibility (e.g., GetState).
type SubTableStatus struct {
	DBID       uint64
	SubState   SubscribeState
	LatestTime time.Time
}

func (c *PushClient) isSubscribed(
	ctx context.Context,
	accId, dbId, tId uint64,
) (*logtailreplay.PartitionState, bool, SubscribeState) {

	s := &c.subscribed

	// Single critical section under RLock to avoid creating partitions
	// for tables that are being unsubscribed concurrently.
	s.rw.RLock()
	ent, exist := s.m[tId]
	if !exist {
		s.rw.RUnlock()
		return nil, false, Unsubscribed
	}
	if ent.state != Subscribed {
		st := ent.state
		s.rw.RUnlock()
		return nil, false, st
	}
	// Update timestamp (with sampling) while holding the read lock to keep
	// state consistent with the partition creation below.
	now := time.Now().UnixNano()
	if now-ent.lastTs.Load() > int64(time.Minute) {
		ent.lastTs.Store(now)
	}
	ps := c.eng.GetOrCreateLatestPart(ctx, accId, dbId, tId).Snapshot()
	s.rw.RUnlock()

	return ps, true, Subscribed
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
		if exist && ent.state == Subscribed {
			return Subscribed, nil
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
	defer c.subscribed.rw.Unlock()
	ent, exist := c.subscribed.m[tableID]
	if exist && (ent.state == SubRspReceived || ent.state == Subscribed) {
		_, err := c.eng.LazyLoadLatestCkp(ctx, accId, tableID, tableName, dbID, dbName)
		if err != nil {
			return InvalidSubState, err
		}
		//update state and timestamp
		ent.state = Subscribed
		ent.lastTs.Store(time.Now().UnixNano())
		return Subscribed, nil
	}
	//if unsubscribed, need to subscribe table.
	if !exist {
		if !c.subscriber.ready() {
			return Unsubscribed, moerr.NewInternalError(ctx, "log tail subscriber is not ready")
		}
		c.subscribed.m[tableID] = &subEntry{
			dbID:  dbID,
			state: Subscribing,
		}
		if err := c.subscribeTable(ctx, api.TableID{DbId: dbID, TbId: tableID}); err != nil {
			//restore the table status.
			delete(c.subscribed.m, tableID)
			return Unsubscribed, err
		}
		return Subscribing, nil
	}
	return ent.state, nil
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
	defer c.mu.Unlock()
	return c.subscriber.logTailClient.Close()
}

func (s *subscribedTable) setTableSubNotExist(dbId, tblId uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
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
	e                      *Engine
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
	tnNodeID      int
	rpcClient     morpc.RPCClient
	rpcStream     morpc.Stream
	logTailClient *service.LogtailClient

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
	if rpcClient == nil {
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
		return nil, nil, err
	}

	return rpcClient, stream, nil
}

func (s *logTailSubscriber) init(
	ctx context.Context,
	sid string,
	serviceAddr string,
	rpcStreamFactory func(context.Context, string, string, morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error)) (err error) {
	// XXX we assume that we have only 1 tn now.
	s.tnNodeID = 0
	s.sid = sid

	// clear the old status.
	s.sendSubscribe = clientIsPreparing
	s.sendUnSubscribe = clientIsPreparing
	if s.logTailClient != nil {
		_ = s.logTailClient.Close()
		s.logTailClient = nil
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
	s.logTailClient, err = service.NewLogtailClient(
		ctx,
		s.rpcStream,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond),
	)
	if err != nil {
		return err
	}

	s.sendSubscribe = s.subscribeTable
	s.sendUnSubscribe = s.unSubscribeTable
	return nil
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
	for !s.mu.ready {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.mu.cond.Wait()
	}
	return nil
}

// can't call this method directly.
func (s *logTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	err := s.logTailClient.Subscribe(ctx, tblId)
	return moerr.AttachCause(ctx, err)
}

// can't call this method directly.
func (s *logTailSubscriber) unSubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	err := s.logTailClient.Unsubscribe(ctx, tblId)
	return moerr.AttachCause(ctx, err)
}

func (s *logTailSubscriber) receiveResponse(deadlineCtx context.Context) logTailSubscriberResponse {
	r, err := s.logTailClient.Receive(deadlineCtx)
	resp := logTailSubscriberResponse{
		response: r,
		err:      err,
	}
	return resp
}

func waitServerReady(addr string) {
	network := "tcp"
	if strings.HasSuffix(addr, ".sock") {
		network = "unix"
		addr = strings.TrimPrefix(addr, "unix://")
	}
	// If the logtail server is ready, just return and do not wait.
	if address.RemoteAddressAvail(network, addr, defaultDialServerTimeout) || addr == FakeLogtailServerAddress {
		return
	}

	// If we still cannot connect to logtail server for serverTimeout, we consider
	// it has something wrong happened and panic immediately.
	serverFatal := time.NewTimer(defaultServerTimeout)
	defer serverFatal.Stop()

	ticker := time.NewTicker(defaultDialServerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if address.RemoteAddressAvail(network, addr, defaultDialServerTimeout) {
				return
			}
			logutil.Warn(
				"logtail.consumer.wait.server.ready",
				zap.String("addr", addr),
			)

		case <-serverFatal.C:
			panic(fmt.Sprintf("could not connect to logtail server for %s", defaultServerTimeout))
		}
	}
}

func (e *Engine) getLogTailServiceAddr() string {
	start := time.Now()
	timeout := time.NewTimer(defaultGetLogTailAddrTimeoutDuration)
	defer timeout.Stop()
	ticker := time.NewTicker(time.Millisecond * 20)
	defer ticker.Stop()

	for {
		tnServices := e.GetTNServices()
		if len(tnServices) > 0 && tnServices[0].LogTailServiceAddress != "" {
			addr := tnServices[0].LogTailServiceAddress
			logutil.Info("logtail.consumer.get.logtail.service.addr",
				zap.String("addr", addr),
				zap.Duration("cost", time.Since(start)),
			)
			return addr
		}

		select {
		case <-timeout.C:
			panic(fmt.Sprintf("cannot get logtail service address, timeout %s",
				defaultGetLogTailAddrTimeoutDuration))
		case <-ticker.C:
		}
	}
}

func (e *Engine) InitLogTailPushModel(ctx context.Context, timestampWaiter client.TimestampWaiter) error {
	logTailServerAddr := e.getLogTailServiceAddr()

	// Wait for logtail server is ready.
	waitServerReady(logTailServerAddr)

	// try to init log tail client. if failed, retry.
	for {
		if err := ctx.Err(); err != nil {
			logutil.Info(
				"logtail.consumer.init.push.model.failed",
				zap.Error(err),
			)
			return err
		}

		// get log tail service address.
		if err := e.pClient.init(ctx, logTailServerAddr, timestampWaiter, e); err != nil {
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

	go e.pClient.connector.run(ctx)

	// Start a goroutine that never stops to receive logtail from TN logtail server.
	go e.pClient.run(ctx, e)

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
		recRoutines[routineIndex].sendSubscribeResponse(ctx, response, receiveAt)
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
			if err := e.consumeUpdateLogTail(ctx, list[i], false, receiveAt); err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_TABLES_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false, receiveAt); err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_COLUMNS_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false, receiveAt); err != nil {
				return err
			}
		}
	}

	for index := 0; index < len(list); index++ {
		table := list[index].Table
		if ifShouldNotDistribute(table.DbId, table.TbId) {
			continue
		}
		recIndex := table.TbId % consumerNumber
		recRoutines[recIndex].sendTableLogTail(list[index], receiveAt)
	}
	// should update all the timestamp.
	e.pClient.receivedLogTailTime.updateTimestamp(consumerNumber, *response.To, receiveAt)
	for i := range recRoutines {
		recRoutines[i].updateTimeFromT(*response.To, receiveAt)
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

	// two pools to provide cmdConsumeLog and cmdConsumeTime
	cmdLogPool  sync.Pool
	cmdTimePool sync.Pool

	// monitor the consumption speed of logs.
	warningBufferLen int
}

func (rc *routineController) sendSubscribeResponse(
	_ context.Context,
	r *logtail.SubscribeResponse,
	receiveAt time.Time) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Info(
			"logtail.consumer.slow.consume.routine",
			zap.Int("routine-id", rc.routineId),
			zap.Int("signal-chan-len", l),
		)
	}

	rc.signalChan <- &cmdToConsumeSub{log: r, receiveAt: receiveAt}
}

func (rc *routineController) sendTableLogTail(r logtail.TableLogtail, receiveAt time.Time) {
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
	log.receiveAt = receiveAt
	rc.signalChan <- log
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
	rc.signalChan <- updateTime
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

	rc.signalChan <- &cmdToConsumeUnSub{log: r, receiveAt: receiveAt}
}

func (rc *routineController) close() {
	rc.closeChan <- true
}

func (c *PushClient) createRoutineToConsumeLogTails(
	ctx context.Context, routineId int, signalBufferLength int, e *Engine,
) *routineController {

	singleRoutineToConsumeLogTail := func(ctx context.Context, engine *Engine, receiver *routineController, errRet chan error) {
		errHappen := false
		for {
			select {
			case <-ctx.Done():
				return

			case cmd := <-receiver.signalChan:
				if errHappen {
					continue
				}
				if err := cmd.action(ctx, engine, receiver); err != nil {
					errHappen = true
					errRet <- err
				}

			case <-receiver.closeChan:
				close(receiver.closeChan)
				close(receiver.signalChan)
				return
			}
		}
	}

	controller := &routineController{
		routineId:  routineId,
		closeChan:  make(chan bool),
		signalChan: make(chan routineControlCmd, signalBufferLength),
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

	go singleRoutineToConsumeLogTail(ctx, e, controller, c.consumeErrC)

	return controller
}

// a signal to control the routine which is responsible for consuming log tail.
type routineControlCmd interface {
	action(ctx context.Context, e *Engine, ctrl *routineController) error
}

type cmdToConsumeSub struct {
	log       *logtail.SubscribeResponse
	receiveAt time.Time
}
type cmdToConsumeLog struct {
	log       logtail.TableLogtail
	receiveAt time.Time
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
	if err := e.consumeSubscribeResponse(ctx, response, true, cmd.receiveAt); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	e.pClient.subscribed.setTableSubRspReceived(tbl.DbId, tbl.TbId)
	return nil
}

func (cmd *cmdToConsumeLog) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	defer ctrl.cmdLogPool.Put(cmd)
	response := cmd.log
	if err := e.consumeUpdateLogTail(ctx, response, true, cmd.receiveAt); err != nil {
		return err
	}
	return nil
}

func (cmd *cmdToUpdateTime) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	defer ctrl.cmdTimePool.Put(cmd)
	e.pClient.receivedLogTailTime.updateTimestamp(ctrl.routineId, cmd.time, cmd.receiveAt)
	return nil
}

func (cmd *cmdToConsumeUnSub) action(ctx context.Context, e *Engine, _ *routineController) error {
	table := cmd.log.Table
	//e.cleanMemoryTableWithTable(table.DbId, table.TbId)
	e.pClient.subscribed.setTableUnsubscribe(table.DbId, table.TbId)
	return nil
}

func (e *Engine) consumeSubscribeResponse(
	ctx context.Context,
	rp *logtail.SubscribeResponse,
	lazyLoad bool,
	receiveAt time.Time) error {
	lt := rp.GetLogtail()
	return updatePartitionOfPush(ctx, e, &lt, lazyLoad, receiveAt, true)
}

func (e *Engine) consumeUpdateLogTail(
	ctx context.Context,
	rp logtail.TableLogtail,
	lazyLoad bool,
	receiveAt time.Time) error {
	return updatePartitionOfPush(ctx, e, &rp, lazyLoad, receiveAt, false)
}

// updatePartitionOfPush is the partition update method of log tail push model.
func updatePartitionOfPush(
	ctx context.Context,
	e *Engine,
	tl *logtail.TableLogtail,
	lazyLoad bool,
	receiveAt time.Time,
	isSub bool) (err error) {
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
		err = consumeCkpsAndLogTail(ctx, partition.TableInfo.PrimarySeqnum, e, state, tl, dbId, tblId, isSub)
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
) error {
	// return hackConsumeLogtail(ctx, primarySeqnum, engine, state, lt)
	if lt.Table.DbName == "" {
		panic(fmt.Sprintf("missing fields %v", lt.Table.String()))
	}
	t0 := time.Now()
	for i := 0; i < len(lt.Commands); i++ {
		if err := consumeEntry(ctx, primarySeqnum,
			engine, engine.GetLatestCatalogCache(), state, &lt.Commands[i], isSub); err != nil {
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
	return consumeLogTail(ctx, primarySeqnum, engine, state, lt, isSub)
}
