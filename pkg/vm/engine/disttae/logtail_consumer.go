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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/address"
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
	// defaultRequestDeadline : default deadline for every request (subscribe and unsubscribe).
	maxSubscribeRequestPerSecond = 10000
	defaultRequestDeadline       = 2 * time.Minute

	// subscribe related constants.
	// periodToCheckTableSubscribeSucceed : check table subscribe status period after push client send a subscribe request.
	// maxTimeToCheckTableSubscribeSucceed : max time to wait for table subscribe succeed. if time exceed, return error.
	periodToCheckTableSubscribeSucceed  = 1 * time.Millisecond
	maxTimeToCheckTableSubscribeSucceed = 30 * time.Second
	maxCheckRangeTableSubscribeSucceed  = int(maxTimeToCheckTableSubscribeSucceed / periodToCheckTableSubscribeSucceed)

	// unsubscribe process related constants.
	// unsubscribe process scan the table every 20 minutes, and unsubscribe table which was unused for 1 hour.
	unsubscribeProcessTicker = 20 * time.Minute
	unsubscribeTimer         = 1 * time.Hour

	// gc blocks and BlockIndexByTSEntry in partition state
	gcPartitionStateTicker = 20 * time.Minute
	gcPartitionStateTimer  = 1 * time.Hour

	// log tail consumer related constants.
	// if buffer is almost full (percent > consumerWarningPercent, we will send a message to log.
	consumerNumber         = 4
	consumerBufferLength   = 8192
	consumerWarningPercent = 0.9

	logTag = "[logtail-consumer]"
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
	receiver    []routineController
}

type State struct {
	LatestTS  timestamp.Timestamp
	SubTables map[SubTableID]SubTableStatus
}

func (c *PushClient) GetState() State {
	c.subscribed.mutex.Lock()
	defer c.subscribed.mutex.Unlock()
	subTables := make(map[SubTableID]SubTableStatus, len(c.subscribed.m))
	for k, v := range c.subscribed.m {
		subTables[k] = v
	}
	return State{
		LatestTS:  c.receivedLogTailTime.getTimestamp(),
		SubTables: subTables,
	}
}

type connector struct {
	first  atomic.Bool
	signal chan struct{}

	client *PushClient
	engine *Engine
}

func newConnector(c *PushClient, e *Engine) *connector {
	co := &connector{
		signal: make(chan struct{}),
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
			logutil.Infof("%s logtail consumer stopped", logTag)
			return

		case <-c.signal:
			c.client.connect(ctx, c.engine)
		}
	}
}

func (c *PushClient) init(
	serviceAddr string,
	timestampWaiter client.TimestampWaiter,
	serviceID string,
	e *Engine,
) error {

	c.serviceID = serviceID
	c.timestampWaiter = timestampWaiter
	if c.subscriber == nil {
		c.subscriber = new(logTailSubscriber)
	}

	// lock all.
	// release subscribed lock when init finished.
	// release subscriber lock when we received enough response from service.
	c.receivedLogTailTime.ready.Store(false)
	c.subscriber.setNotReady()
	c.subscribed.mutex.Lock()
	defer func() {
		c.subscribed.mutex.Unlock()
	}()

	c.receivedLogTailTime.initLogTailTimestamp(timestampWaiter)
	c.subscribed.m = make(map[SubTableID]SubTableStatus)

	if !c.initialized {
		c.connector = newConnector(c, e)
		c.receiver = make([]routineController, consumerNumber)
		c.consumeErrC = make(chan error, consumerNumber)
		c.pauseC = make(chan bool, 1)
		c.resumeC = make(chan struct{})
	}
	c.initialized = true

	return c.subscriber.init(serviceAddr)
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

// TryToSubscribeTable subscribe a table and block until subscribe succeed.
func (c *PushClient) TryToSubscribeTable(
	ctx context.Context,
	dbId, tblId uint64) error {
	if c.subscribed.getTableSubscribe(dbId, tblId) {
		return nil
	}
	if err := c.subscribeTable(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		return err
	}
	ticker := time.NewTicker(periodToCheckTableSubscribeSucceed)
	defer ticker.Stop()

	for i := 0; i < maxCheckRangeTableSubscribeSucceed; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if c.subscribed.getTableSubscribe(dbId, tblId) {
				return nil
			}
		}
	}
	logutil.Debugf("%s didn't receive tbl[db: %d, tbl: %d] subscribe response within %s",
		logTag, dbId, tblId, maxTimeToCheckTableSubscribeSucceed)
	return moerr.NewInternalError(ctx, "an error has occurred about table subscription, please try again.")
}

// this method will ignore lock check, subscribe a table and block until subscribe succeed.
// developer should use this method carefully.
// in most time, developer should use TryToSubscribeTable instead.
func (c *PushClient) forcedSubscribeTable(
	ctx context.Context,
	dbId, tblId uint64) error {
	s := c.subscriber

	if err := s.doSubscribe(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		return err
	}
	ticker := time.NewTicker(periodToCheckTableSubscribeSucceed)
	defer ticker.Stop()

	for i := 0; i < maxCheckRangeTableSubscribeSucceed; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if c.subscribed.getTableSubscribe(dbId, tblId) {
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
	case b := <-c.subscriber.requestLock:
		err := c.subscriber.doSubscribe(ctx, tblId)
		c.subscriber.requestLock <- b
		if err != nil {
			return err
		}
		logutil.Debugf("%s send subscribe tbl[db: %d, tbl: %d] request succeed", logTag, tblId.DbId, tblId.TbId)
		return nil
	}
}

func (c *PushClient) subSysTables(ctx context.Context) error {
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
		logutil.Errorf("%s %s: connect to tn log tail server failed, err %v", logTag, c.serviceID, err)
	}
	return err
}

func (c *PushClient) pause(s bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.paused {
		return
	}
	select {
	case c.pauseC <- s:
		c.mu.paused = true
	default:
		logutil.Infof("%s already set to pause", logTag)
	}
}

func (c *PushClient) resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.resumeC <- struct{}{}:
		c.mu.paused = false
	default:
		logutil.Infof("%s not in pause state", logTag)
	}
}

func (c *PushClient) receiveOneLogtail(ctx context.Context, e *Engine) error {
	ctx, cancel := context.WithTimeout(ctx, maxTimeToWaitServerResponse)
	defer cancel()

	resp := c.subscriber.receiveResponse(ctx)
	if resp.err != nil {
		// POSSIBLE ERROR: context deadline exceeded, rpc closed, decode error.
		logutil.Errorf("%s receive an error from log tail client, err: %s", logTag, resp.err)
		return resp.err
	}

	receiveAt := time.Now()
	v2.LogtailTotalReceivedCounter.Inc()
	if res := resp.response.GetSubscribeResponse(); res != nil { // consume subscribe response
		v2.LogtailSubscribeReceivedCounter.Inc()
		if err := dispatchSubscribeResponse(ctx, e, res, c.receiver, receiveAt); err != nil {
			logutil.Errorf("%s dispatch subscribe response failed, err: %s", logTag, err)
			return err
		}
	} else if res := resp.response.GetUpdateResponse(); res != nil { // consume update response
		if len(res.LogtailList) > 0 {
			v2.LogtailUpdateReceivedCounter.Inc()
		} else {
			v2.LogtailHeartbeatReceivedCounter.Inc()
		}

		if err := dispatchUpdateResponse(ctx, e, res, c.receiver, receiveAt); err != nil {
			logutil.Errorf("%s dispatch update response failed, err: %s", logTag, err)
			return err
		}
	} else if unResponse := resp.response.GetUnsubscribeResponse(); unResponse != nil { // consume unsubscribe response
		v2.LogtailUnsubscribeReceivedCounter.Inc()

		if err := dispatchUnSubscribeResponse(ctx, e, unResponse, c.receiver, receiveAt); err != nil {
			logutil.Errorf("%s dispatch unsubscribe response failed, err: %s", logTag, err)
			return err
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
			logutil.Infof("%s logtail receiver paused", logTag)
			if s {
				c.sendConnectSig()
			}

			// Wait for resuming logtail receiver.
			<-c.resumeC
			logutil.Infof("%s logtail receiver resumed", logTag)

		default:
			if err := c.receiveOneLogtail(ctx, e); err != nil {
				logutil.Errorf("%s receive one logtail failed, err: %v", logTag, err)
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
	for _, r := range c.receiver {
		r.close()
	}
	logutil.Infof("%s %s: logtail consumers stopped", logTag, c.serviceID)
}

func (c *PushClient) sendConnectSig() {
	if c.connector.first.Load() {
		c.connector.signal <- struct{}{}
		return
	}

	select {
	case c.connector.signal <- struct{}{}:
		logutil.Infof("%s reconnect signal is received", logTag)
	default:
		logutil.Infof("%s connecting is in progress", logTag)
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

func (c *PushClient) connect(ctx context.Context, e *Engine) {
	if c.connector.first.Load() {
		c.startConsumers(ctx, e)

		for {
			err := c.subSysTables(ctx)
			if err != nil {
				c.pause(false)
				time.Sleep(time.Second)

				tnLogTailServerBackend := e.getTNServices()[0].LogTailServiceAddress
				if err := c.init(tnLogTailServerBackend, c.timestampWaiter, c.serviceID, e); err != nil {
					logutil.Errorf("%s init push client failed: %v", logTag, err)
					continue
				}

				c.resume()
				continue
			}
			c.waitTimestamp()
			e.setPushClientStatus(true)
			c.connector.first.Store(false)
			return
		}
	}

	e.setPushClientStatus(false)

	c.stopConsumers()

	logutil.Infof("%s %s: clean finished, start to reconnect to tn log tail service", logTag, c.serviceID)
	for {
		if ctx.Err() != nil {
			logutil.Infof("%s mo context has done, exit log tail receive routine", logTag)
			return
		}

		tnLogTailServerBackend := e.getTNServices()[0].LogTailServiceAddress
		if err := c.init(tnLogTailServerBackend, c.timestampWaiter, c.serviceID, e); err != nil {
			logutil.Errorf("%s rebuild the cn log tail client failed, reason: %s", logTag, err)
			time.Sleep(retryReconnect)
			continue
		}
		logutil.Infof("%s %s: client init finished", logTag, c.serviceID)

		// set all the running transaction to be aborted.
		e.abortAllRunningTxn()
		logutil.Infof("%s %s: abort all running transactions finished", logTag, c.serviceID)

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

		err = c.subSysTables(ctx)
		if err != nil {
			logutil.Errorf("%s subscribe system tables failed, err %v", logTag, err)
			continue
		}

		c.waitTimestamp()
		e.setPushClientStatus(true)
		logutil.Infof("%s %s: connected to server", logTag, c.serviceID)

		return
	}
}

// UnsubscribeTable implements the LogtailEngine interface.
func (c *PushClient) UnsubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	if !c.receivedLogTailTime.ready.Load() {
		return moerr.NewInternalError(ctx, "%s cannot unsubscribe table %d-%d as logtail client is not ready", logTag, dbID, tbID)
	}
	if c.subscriber == nil {
		return moerr.NewInternalError(ctx, "%s cannot unsubscribe table %d-%d as subscriber not initialized", logTag, dbID, tbID)
	}
	if ifShouldNotDistribute(dbID, tbID) {
		return moerr.NewInternalError(ctx, "%s cannot unsubscribe table %d-%d as table ID is not allowed", logTag, dbID, tbID)
	}
	c.subscribed.mutex.Lock()
	defer c.subscribed.mutex.Unlock()
	k := SubTableID{DatabaseID: dbID, TableID: tbID}
	status, ok := c.subscribed.m[k]
	if !ok {
		logutil.Infof("%s table %d-%d is not subscribed yet", logTag, dbID, tbID)
		return nil
	}
	if err := c.subscriber.doUnSubscribe(ctx, api.TableID{DbId: dbID, TbId: tbID}); err != nil {
		logutil.Errorf("%s cannot unsubscribe table %d-%d, err: %v", logTag, dbID, tbID, err)
		return err
	}
	c.subscribed.m[k] = SubTableStatus{
		IsDeleting: true,
		LatestTime: status.LatestTime,
	}
	logutil.Infof("%s send unsubscribe table %d-%d request succeed", logTag, dbID, tbID)
	return nil
}

func (c *PushClient) unusedTableGCTicker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(unsubscribeProcessTicker)
		for {
			select {
			case <-ctx.Done():
				logutil.Infof("%s unsubscribe process exit.", logTag)
				ticker.Stop()
				return

			case <-ticker.C:
				if !c.receivedLogTailTime.ready.Load() {
					continue
				}
				if c.subscriber == nil {
					continue
				}
			}
			shouldClean := time.Now().Add(-unsubscribeTimer)

			// lock the subscriber and subscribed map.
			b := <-c.subscriber.requestLock
			c.subscribed.mutex.Lock()
			logutil.Infof("%s start to unsubscribe unused table", logTag)
			func() {
				defer func() {
					c.subscriber.requestLock <- b
					c.subscribed.mutex.Unlock()
				}()

				var err error
				for k, v := range c.subscribed.m {
					if ifShouldNotDistribute(k.DatabaseID, k.TableID) {
						// never unsubscribe the mo_databases, mo_tables, mo_columns.
						continue
					}

					if !v.LatestTime.After(shouldClean) {
						if err = c.subscriber.doUnSubscribe(ctx, api.TableID{DbId: k.DatabaseID, TbId: k.TableID}); err == nil {
							c.subscribed.m[k] = SubTableStatus{
								IsDeleting: true,
								LatestTime: v.LatestTime,
							}
							logutil.Debugf("%s send unsubscribe tbl[db: %d, tbl: %d] request succeed", logTag, k.DatabaseID, k.TableID)
							continue
						}
						logutil.Errorf("%s sign tbl[dbId: %d, tblId: %d] unsubscribing failed, err : %s", logTag, k.DatabaseID, k.TableID, err.Error())
						break
					}
				}
			}()

			logutil.Infof("%s unsubscribe unused table finished.", logTag)
		}
	}()
}

func (c *PushClient) partitionStateGCTicker(ctx context.Context, e *Engine) {
	go func() {
		ticker := time.NewTicker(gcPartitionStateTicker)
		for {
			select {
			case <-ctx.Done():
				logutil.Infof("%s GC partition_state process exit.", logTag)
				ticker.Stop()
				return

			case <-ticker.C:
				if !c.receivedLogTailTime.ready.Load() {
					continue
				}
				if c.subscriber == nil {
					continue
				}
			}
			parts := make(map[[2]uint64]*logtailreplay.Partition)
			e.Lock()
			for ids, part := range e.partitions {
				parts[ids] = part
			}
			e.Unlock()
			ts := types.BuildTS(time.Now().UTC().UnixNano()-gcPartitionStateTimer.Nanoseconds()*5, 0)
			logutil.Infof("%s GC partition_state %v", logTag, ts.ToString())
			for ids, part := range parts {
				part.Truncate(ctx, ids, ts)
			}
		}
	}()
}

type SubTableID struct {
	DatabaseID uint64
	TableID    uint64
}

// subscribedTable used to record table subscribed status.
// only if m[table T] = true, T has been subscribed.
type subscribedTable struct {
	mutex sync.Mutex

	// value is table's latest use time.
	m map[SubTableID]SubTableStatus
}

type SubTableStatus struct {
	IsDeleting bool
	LatestTime time.Time
}

func (s *subscribedTable) getTableSubscribe(dbId, tblId uint64) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	status, ok := s.m[SubTableID{DatabaseID: dbId, TableID: tblId}]
	if ok {
		if status.IsDeleting {
			ok = false
		} else {
			s.m[SubTableID{DatabaseID: dbId, TableID: tblId}] = SubTableStatus{
				IsDeleting: false,
				LatestTime: time.Now(),
			}
		}
	}
	return ok
}

func (s *subscribedTable) setTableSubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.m[SubTableID{DatabaseID: dbId, TableID: tblId}] = SubTableStatus{
		IsDeleting: false,
		LatestTime: time.Now(),
	}
	logutil.Infof("%s subscribe tbl[db: %d, tbl: %d] succeed", logTag, dbId, tblId)
}

func (s *subscribedTable) setTableUnsubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.m, SubTableID{DatabaseID: dbId, TableID: tblId})
	logutil.Infof("%s unsubscribe tbl[db: %d, tbl: %d] succeed", logTag, dbId, tblId)
}

// syncLogTailTimestamp is a global log tail timestamp for a cn node.
// support `getTimestamp()` method to get time of last received log.
type syncLogTailTimestamp struct {
	timestampWaiter        client.TimestampWaiter
	ready                  atomic.Bool
	tList                  []atomic.Pointer[timestamp.Timestamp]
	latestAppliedLogTailTS atomic.Pointer[timestamp.Timestamp]
}

func (r *syncLogTailTimestamp) initLogTailTimestamp(timestampWaiter client.TimestampWaiter) {
	ts := r.getTimestamp()
	if !ts.IsEmpty() {
		r.latestAppliedLogTailTS.Store(&ts)
	}

	r.timestampWaiter = timestampWaiter
	if len(r.tList) == 0 {
		r.tList = make([]atomic.Pointer[timestamp.Timestamp], consumerNumber+1)
	}
	for i := range r.tList {
		r.tList[i].Store(new(timestamp.Timestamp))
	}
}

func (r *syncLogTailTimestamp) getTimestamp() timestamp.Timestamp {
	var minT timestamp.Timestamp
	for i := 0; i < len(r.tList); i++ {
		t := *r.tList[i].Load()
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
	r.tList[index].Store(&newTimestamp)
	if r.ready.Load() {
		ts := r.getTimestamp()
		r.timestampWaiter.NotifyLatestCommitTS(ts)
	}
}

type logTailSubscriber struct {
	tnNodeID      int
	rpcClient     morpc.RPCClient
	rpcStream     morpc.Stream
	logTailClient *service.LogtailClient

	ready bool

	requestLock   chan bool
	doSubscribe   func(context.Context, api.TableID) error
	doUnSubscribe func(context.Context, api.TableID) error
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
func (s *logTailSubscriber) newRpcStreamToTnLogTailService(serviceAddr string) error {
	if s.rpcClient == nil {
		logger := logutil.GetGlobalLogger().Named("cn-log-tail-client")
		codec := morpc.NewMessageCodec(func() morpc.Message {
			return &service.LogtailResponseSegment{}
		})
		factory := morpc.NewGoettyBasedBackendFactory(codec,
			morpc.WithBackendGoettyOptions(
				goetty.WithSessionRWBUfferSize(1<<20, 1<<20),
			),
			morpc.WithBackendLogger(logger),
		)

		c, err := morpc.NewClient(
			"logtail-client",
			factory,
			morpc.WithClientLogger(logger),
		)
		if err != nil {
			return err
		}
		s.rpcClient = c
	}

	if s.rpcStream != nil {
		s.rpcStream.Close(true)
		s.rpcStream = nil
	}

	stream, err := s.rpcClient.NewStream(serviceAddr, true)
	if err != nil {
		return err
	}

	s.rpcStream = stream
	return nil
}

func (s *logTailSubscriber) init(serviceAddr string) (err error) {
	// XXX we assume that we have only 1 tn now.
	s.tnNodeID = 0

	// clear the old status.
	s.doSubscribe = clientIsPreparing
	s.doUnSubscribe = clientIsPreparing
	if s.logTailClient != nil {
		_ = s.logTailClient.Close()
		s.logTailClient = nil
	}

	if err := s.newRpcStreamToTnLogTailService(serviceAddr); err != nil {
		return err
	}

	// new the log tail client.
	s.logTailClient, err = service.NewLogtailClient(s.rpcStream, service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	if err != nil {
		return err
	}

	s.doSubscribe = s.subscribeTable
	s.doUnSubscribe = s.unSubscribeTable
	if s.requestLock == nil {
		s.requestLock = make(chan bool, 1)
		s.ready = false
	}
	return nil
}

func (s *logTailSubscriber) setReady() {
	if !s.ready && s.requestLock != nil {
		s.requestLock <- true
		s.ready = true
	}
}

func (s *logTailSubscriber) setNotReady() {
	if s.ready && s.requestLock != nil {
		<-s.requestLock
		s.ready = false
	}
}

// can't call this method directly.
func (s *logTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel := context.WithTimeout(ctx, defaultRequestDeadline)
		_ = cancel
		return s.logTailClient.Subscribe(newCtx, tblId)
	}
	return s.logTailClient.Subscribe(ctx, tblId)
}

// can't call this method directly.
func (s *logTailSubscriber) unSubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel := context.WithTimeout(ctx, defaultRequestDeadline)
		_ = cancel
		return s.logTailClient.Unsubscribe(newCtx, tblId)
	}
	return s.logTailClient.Unsubscribe(ctx, tblId)
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
	dialTimeout := time.Second * 2
	// If the logtail server is ready, just return and do not wait.
	if address.RemoteAddressAvail(addr, dialTimeout) {
		return
	}

	// If we still cannot connect to logtail server for serverTimeout, we consider
	// it has something wrong happened and panic immediately.
	serverTimeout := time.Minute * 5
	serverFatal := time.NewTimer(serverTimeout)
	defer serverFatal.Stop()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	var resetTimout time.Duration
	started := time.Now()

	for {
		current := time.Now()
		// Calculation the proper reset timeout duration.
		if current.Sub(started) < time.Minute {
			resetTimout = time.Second
		} else if current.Sub(started) < time.Minute*3 {
			resetTimout = time.Second * 10
		} else {
			resetTimout = time.Second * 30
		}

		select {
		case <-timer.C:
			if address.RemoteAddressAvail(addr, dialTimeout) {
				return
			}
			timer.Reset(resetTimout)
			logutil.Warnf("%s logtail server is not ready yet", logTag)

		case <-serverFatal.C:
			panic(fmt.Sprintf("could not connect to logtail server for %s", serverTimeout))
		}
	}
}

func (e *Engine) InitLogTailPushModel(ctx context.Context, timestampWaiter client.TimestampWaiter) error {
	tnStores := e.getTNServices()
	if len(tnStores) == 0 {
		return moerr.NewInternalError(ctx, "no TN store found")
	}

	logTailServerAddr := tnStores[0].LogTailServiceAddress

	// Wait for logtail server is ready.
	waitServerReady(logTailServerAddr)

	// try to init log tail client. if failed, retry.
	for {
		if err := ctx.Err(); err != nil {
			logutil.Infof("%s mo context has done, init log tail client failed.", logTag)
			return err
		}

		// get log tail service address.
		if err := e.pClient.init(logTailServerAddr, timestampWaiter, e.ls.GetServiceID(), e); err != nil {
			logutil.Errorf("%s client init failed, err is %s", logTag, err)
			continue
		}
		break
	}

	go e.pClient.connector.run(ctx)

	// Start a goroutine that never stops to receive logtail from TN logtail server.
	go e.pClient.run(ctx, e)

	e.pClient.unusedTableGCTicker(ctx)
	e.pClient.partitionStateGCTicker(ctx, e)
	return nil
}

func ifShouldNotDistribute(dbId, tblId uint64) bool {
	return dbId == catalog.MO_CATALOG_ID && tblId <= catalog.MO_RESERVED_MAX
}

func dispatchSubscribeResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.SubscribeResponse,
	recRoutines []routineController,
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
				logutil.Warnf("%s consume subscribe response for tbl[dbId: %d, tblID: %d] cost %s",
					logTag, tbl.DbId, tbl.TbId, tDuration.String())
			}
		}()

		if err := e.consumeSubscribeResponse(ctx, response, false, receiveAt); err != nil {
			return err
		}
		e.pClient.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	} else {
		routineIndex := tbl.TbId % consumerNumber
		recRoutines[routineIndex].sendSubscribeResponse(ctx, response, receiveAt)
	}
	// no matter how we consume the response, should update all timestamp.
	e.pClient.receivedLogTailTime.updateTimestamp(consumerNumber, *lt.Ts, receiveAt)
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*lt.Ts, receiveAt)
	}
	return nil
}

func dispatchUpdateResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.UpdateResponse,
	recRoutines []routineController,
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
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*response.To, receiveAt)
	}

	n := 0
	for _, c := range recRoutines {
		n += len(c.signalChan)
	}
	v2.LogTailApplyQueueSizeGauge.Set(float64(n))
	return nil
}

func dispatchUnSubscribeResponse(
	_ context.Context,
	_ *Engine,
	response *logtail.UnSubscribeResponse,
	recRoutines []routineController,
	receiveAt time.Time) error {
	tbl := response.Table
	notDistribute := ifShouldNotDistribute(tbl.DbId, tbl.TbId)
	if notDistribute {
		logutil.Errorf("%s unexpected unsubscribe response for tbl[dbId: %d, tblID: %d]",
			logTag, tbl.DbId, tbl.TbId)
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

	// monitor the consumption speed of logs.
	warningBufferLen int
}

func (rc *routineController) sendSubscribeResponse(
	ctx context.Context,
	r *logtail.SubscribeResponse,
	receiveAt time.Time) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Infof("%s consume-routine %d signalChan len is %d, maybe consume is too slow", logTag, rc.routineId, l)
	}

	rc.signalChan <- cmdToConsumeSub{log: r, receiveAt: receiveAt}
}

func (rc *routineController) sendTableLogTail(r logtail.TableLogtail, receiveAt time.Time) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Infof("%s consume-routine %d signalChan len is %d, maybe consume is too slow", logTag, rc.routineId, l)
	}

	rc.signalChan <- cmdToConsumeLog{log: r, receiveAt: receiveAt}
}

func (rc *routineController) updateTimeFromT(
	t timestamp.Timestamp,
	receiveAt time.Time) {
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Infof("%s consume-routine %d signalChan len is %d, maybe consume is too slow", logTag, rc.routineId, l)
	}

	rc.signalChan <- cmdToUpdateTime{time: t, receiveAt: receiveAt}
}

func (rc *routineController) sendUnSubscribeResponse(r *logtail.UnSubscribeResponse, receiveAt time.Time) {
	// debug for issue #10138.
	if l := len(rc.signalChan); l > rc.warningBufferLen {
		rc.warningBufferLen = l
		logutil.Infof("%s consume-routine %d signalChan len is %d, maybe consume is too slow", logTag, rc.routineId, l)
	}

	rc.signalChan <- cmdToConsumeUnSub{log: r, receiveAt: receiveAt}
}

func (rc *routineController) close() {
	rc.closeChan <- true
}

func (c *PushClient) createRoutineToConsumeLogTails(
	ctx context.Context, routineId int, signalBufferLength int, e *Engine,
) routineController {

	singleRoutineToConsumeLogTail := func(ctx context.Context, engine *Engine, receiver *routineController, errRet chan error) {
		errHappen := false
		for {
			select {
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

	controller := routineController{
		routineId:  routineId,
		closeChan:  make(chan bool),
		signalChan: make(chan routineControlCmd, signalBufferLength),

		// Debug for issue #10138.
		warningBufferLen: int(float64(signalBufferLength) * consumerWarningPercent),
	}

	go singleRoutineToConsumeLogTail(ctx, e, &controller, c.consumeErrC)

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

func (cmd cmdToConsumeSub) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeSubscribeResponse(ctx, response, true, cmd.receiveAt); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	e.pClient.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	return nil
}

func (cmd cmdToConsumeLog) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeUpdateLogTail(ctx, response, true, cmd.receiveAt); err != nil {
		return err
	}
	return nil
}

func (cmd cmdToUpdateTime) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	e.pClient.receivedLogTailTime.updateTimestamp(ctrl.routineId, cmd.time, cmd.receiveAt)
	return nil
}

func (cmd cmdToConsumeUnSub) action(ctx context.Context, e *Engine, _ *routineController) error {
	table := cmd.log.Table
	e.cleanMemoryTableWithTable(table.DbId, table.TbId)
	e.pClient.subscribed.setTableUnsubscribe(table.DbId, table.TbId)
	return nil
}

func (e *Engine) consumeSubscribeResponse(
	ctx context.Context,
	rp *logtail.SubscribeResponse,
	lazyLoad bool,
	receiveAt time.Time) error {
	lt := rp.GetLogtail()
	return updatePartitionOfPush(ctx, e.pClient.subscriber.tnNodeID, e, &lt, lazyLoad, receiveAt)
}

func (e *Engine) consumeUpdateLogTail(
	ctx context.Context,
	rp logtail.TableLogtail,
	lazyLoad bool,
	receiveAt time.Time) error {
	return updatePartitionOfPush(ctx, e.pClient.subscriber.tnNodeID, e, &rp, lazyLoad, receiveAt)
}

// updatePartitionOfPush is the partition update method of log tail push model.
func updatePartitionOfPush(
	ctx context.Context,
	tnId int,
	e *Engine,
	tl *logtail.TableLogtail,
	lazyLoad bool,
	receiveAt time.Time) (err error) {
	start := time.Now()
	v2.LogTailApplyLatencyDurationHistogram.Observe(start.Sub(receiveAt).Seconds())
	defer func() {
		v2.LogTailApplyDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// after consume the logtail, enqueue it to global stats.
	defer func() { e.globalStats.enqueue(tl) }()

	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()

	partition := e.getPartition(dbId, tblId)

	lockErr := partition.Lock(ctx)
	if lockErr != nil {
		return lockErr
	}
	defer partition.Unlock()

	state, doneMutate := partition.MutateState()

	key := e.catalog.GetTableById(dbId, tblId)

	if lazyLoad {
		if len(tl.CkpLocation) > 0 {
			state.AppendCheckpoint(tl.CkpLocation, partition)
		}

		err = consumeLogTailOfPushWithLazyLoad(
			ctx,
			key.PrimarySeqnum,
			e,
			state,
			tl,
		)
	} else {
		err = consumeLogTailOfPushWithoutLazyLoad(ctx, key.PrimarySeqnum, e, state, tl, dbId, key.Id, key.Name)
	}

	if err != nil {
		logutil.Errorf("%s consume %d-%s log tail error: %v\n", logTag, key.Id, key.Name, err)
		return err
	}

	partition.TS = *tl.Ts

	doneMutate()

	return nil
}

func consumeLogTailOfPushWithLazyLoad(
	ctx context.Context,
	primarySeqnum int,
	engine *Engine,
	state *logtailreplay.PartitionState,
	lt *logtail.TableLogtail,
) error {
	return hackConsumeLogtail(ctx, primarySeqnum, engine, state, lt)
}

func consumeLogTailOfPushWithoutLazyLoad(
	ctx context.Context,
	primarySeqnum int,
	engine *Engine,
	state *logtailreplay.PartitionState,
	lt *logtail.TableLogtail,
	databaseId uint64,
	tableId uint64,
	tableName string,
) (err error) {
	var entries []*api.Entry
	var closeCBs []func()
	if entries, closeCBs, err = taeLogtail.LoadCheckpointEntries(
		ctx,
		lt.CkpLocation,
		tableId, tableName,
		databaseId, "", engine.mp, engine.fs); err != nil {
		return
	}
	defer func() {
		for _, cb := range closeCBs {
			cb()
		}
	}()
	for _, entry := range entries {
		if err = consumeEntry(ctx, primarySeqnum,
			engine, state, entry); err != nil {
			return
		}
	}
	return hackConsumeLogtail(ctx, primarySeqnum, engine, state, lt)
}

func hackConsumeLogtail(
	ctx context.Context,
	primarySeqnum int,
	engine *Engine,
	state *logtailreplay.PartitionState,
	lt *logtail.TableLogtail) error {
	var packer *types.Packer
	put := engine.packerPool.Get(&packer)
	defer put.Put()

	switch lt.Table.TbId {
	case catalog.MO_TABLES_ID:
		primarySeqnum = catalog.MO_TABLES_CATALOG_VERSION_IDX + 1
		for i := 0; i < len(lt.Commands); i++ {
			if lt.Commands[i].EntryType == api.Entry_Insert {
				bat, _ := batch.ProtoBatchToBatch(lt.Commands[i].Bat)
				accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + 2))
				names := bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + 2)
				databases := bat.GetVector(catalog.MO_TABLES_RELDATABASE_IDX + 2)
				vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
				for i, acc := range accounts {
					packer.EncodeUint32(acc)
					packer.EncodeStringType(names.GetBytesAt(i))
					packer.EncodeStringType(databases.GetBytesAt(i))
					if err := vector.AppendBytes(vec, packer.Bytes(), false, engine.mp); err != nil {
						panic(err)
					}
					packer.Reset()
				}
				hackVec, _ := vector.VectorToProtoVector(vec)
				lt.Commands[i].Bat.Vecs = append(lt.Commands[i].Bat.Vecs, hackVec)
				vec.Free(engine.mp)
			}
			if lt.Commands[i].EntryType == api.Entry_Delete {
				continue
			}
			if lt.Commands[i].EntryType == api.Entry_SpecialDelete {
				lt.Commands[i].EntryType = api.Entry_Delete
			}
			if err := consumeEntry(ctx, primarySeqnum,
				engine, state, &lt.Commands[i]); err != nil {
				return err
			}
		}
		return nil
	case catalog.MO_DATABASE_ID:
		primarySeqnum = catalog.MO_DATABASE_DAT_TYPE_IDX + 1
		for i := 0; i < len(lt.Commands); i++ {
			if lt.Commands[i].EntryType == api.Entry_Insert {
				bat, _ := batch.ProtoBatchToBatch(lt.Commands[i].Bat)
				accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + 2))
				names := bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + 2)
				vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
				for i, acc := range accounts {
					packer.EncodeUint32(acc)
					packer.EncodeStringType(names.GetBytesAt(i))
					if err := vector.AppendBytes(vec, packer.Bytes(), false, engine.mp); err != nil {
						panic(err)
					}
					packer.Reset()
				}
				hackVec, _ := vector.VectorToProtoVector(vec)
				lt.Commands[i].Bat.Vecs = append(lt.Commands[i].Bat.Vecs, hackVec)
				vec.Free(engine.mp)
			}
			if lt.Commands[i].EntryType == api.Entry_Delete {
				continue
			}
			if lt.Commands[i].EntryType == api.Entry_SpecialDelete {
				lt.Commands[i].EntryType = api.Entry_Delete
			}
			if err := consumeEntry(ctx, primarySeqnum,
				engine, state, &lt.Commands[i]); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < len(lt.Commands); i++ {
		if err := consumeEntry(ctx, primarySeqnum,
			engine, state, &lt.Commands[i]); err != nil {
			return err
		}
	}
	return nil
}
