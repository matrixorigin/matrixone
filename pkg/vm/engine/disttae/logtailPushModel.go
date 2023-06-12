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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	taeLogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const (
	// if we didn't receive response from dn log tail server within time maxTimeToWaitServerResponse.
	// we assume that client has lost connect to server and will start a reconnect.
	maxTimeToWaitServerResponse = 60 * time.Second

	// once we reconnect dn failed. we will retry after time retryReconnect.
	retryReconnect = 20 * time.Millisecond

	// max number of subscribe request we allowed per second.
	maxSubscribeRequestPerSecond = 10000

	// once we send a table subscribe request, we check table subscribe status each periodToCheckTableSubscribeSucceed.
	// If check time exceeds maxTimeToCheckTableSubscribeSucceed, print debug log and return error.
	periodToCheckTableSubscribeSucceed  = 1 * time.Millisecond
	maxTimeToCheckTableSubscribeSucceed = 10 * time.Second
	maxCheckRangeTableSubscribeSucceed  = int(maxTimeToCheckTableSubscribeSucceed / periodToCheckTableSubscribeSucceed)

	// default deadline for context to send a rpc message.
	defaultTimeOutToSubscribeTable = 2 * time.Minute

	// each unsubscribeProcessTicker, we scan the table subscribe record.
	// to unsubscribe the table which was unused for a long time (more than unsubscribeTimer).
	unsubscribeProcessTicker = 20 * time.Minute
	unsubscribeTimer         = 1 * time.Hour
)

const (
	// routine number to consume log tail.
	parallelNums = 4

	// each routine's log tail buffer size.
	bufferLength = 256
)

// pushClient is a structure responsible for all operations related to the log tail push model.
// It provides the following methods:
//
//	-----------------------------------------------------------------------------------------------------
//	 1. checkTxnTimeIsLegal : block the process until we have received enough log tail (T_log >= T_txn)
//	 2. TryToSubscribeTable : block the process until we subscribed a table succeed.
//	 3. subscribeTable	   : send a table subscribe request to service.
//	 4. unsubscribeTable	   : send a table un subscribe request to service.
//	 5. firstTimeConnectToLogTailServer : subscribe mo_databases, mo_tables, mo_columns
//	 6. receiveTableLogTailContinuously   : start (1 + parallelNums) routine to receive log tail from service.
//	-----------------------------------------------------------------------------------------------------
type pushClient struct {
	// Responsible for sending subscription / unsubscription requests to the service
	// and receiving the log tail from service.
	subscriber *logTailSubscriber

	// Record the timestamp of last log received by CN.
	receivedLogTailTime syncLogTailTimestamp

	// Record the subscription status of a tables.
	subscribed subscribedTable

	// timestampWaiter is used to notify the latest commit timestamp
	timestampWaiter client.TimestampWaiter
}

func (client *pushClient) init(
	serviceAddr string,
	timestampWaiter client.TimestampWaiter) error {
	client.timestampWaiter = timestampWaiter
	client.receivedLogTailTime.initLogTailTimestamp(timestampWaiter)
	client.subscribed.initTableSubscribeRecord()

	if client.subscriber == nil {
		client.subscriber = new(logTailSubscriber)
	}
	err := client.subscriber.init(serviceAddr)
	if err != nil {
		return err
	}
	return nil
}

func (client *pushClient) validLogTailMustApplied(snapshotTS timestamp.Timestamp) {
	// At the time of transaction creation, a ts is obtained as the start timestamp of the transaction.
	// To ensure that the complete data is visible at the start of the transaction, the logtail of
	// all < snapshot ts is waited until it is applied when the transaction is created inside the txn client.
	//
	// Inside the txn client, there is a waiter waiting for the LogTail to be applied, which will continuously
	// receive the ts applied by the pushClient, and then the transaction will use the maximum applied LogTail
	// ts currently received + 1 as the transaction's snapshot ts to ensure that the transaction can see the
	// log tails corresponding to the max(applied log tail ts in txn client).
	//
	// So here we need to use snapshotTS.Prev() to check.
	if client.receivedLogTailTime.greatEq(snapshotTS.Prev()) {
		return
	}

	// If reconnect, receivedLogTailTime will reset. But latestAppliedLogTailTS is always keep the latest applied
	// logtail ts.
	ts := client.receivedLogTailTime.latestAppliedLogTailTS.Load()
	if ts != nil && ts.GreaterEq(snapshotTS.Prev()) {
		return
	}
	panic(fmt.Sprintf("BUG: all log tail must be applied before %s, received applied %s, last applied %+v",
		snapshotTS.Prev().DebugString(),
		client.receivedLogTailTime.getTimestamp().DebugString(),
		ts))
}

// TryToSubscribeTable subscribe a table and block until subscribe succeed.
func (client *pushClient) TryToSubscribeTable(
	ctx context.Context,
	dbId, tblId uint64) error {
	if client.subscribed.getTableSubscribe(dbId, tblId) {
		return nil
	}
	if err := client.subscribeTable(ctx, api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		return err
	}
	ticker := time.NewTicker(periodToCheckTableSubscribeSucceed)
	defer ticker.Stop()

	for i := 0; i < maxCheckRangeTableSubscribeSucceed; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if client.subscribed.getTableSubscribe(dbId, tblId) {
				return nil
			}
		}
	}
	logutil.Debugf("didn't receive tbl[db: %d, tbl: %d] subscribe response within %s",
		dbId, tblId, maxTimeToCheckTableSubscribeSucceed)
	return moerr.NewInternalError(ctx, "an error has occurred about table subscription, please try again.")
}

// this method will ignore lock check, subscribe a table and block until subscribe succeed.
// developer should use this method carefully.
// in most time, developer should use TryToSubscribeTable instead.
func (client *pushClient) forcedSubscribeTable(
	ctx context.Context,
	dbId, tblId uint64) error {
	s := client.subscriber

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
			if client.subscribed.getTableSubscribe(dbId, tblId) {
				return nil
			}
		}
	}
	return moerr.NewInternalError(ctx, "forced subscribe table timeout")
}

func (client *pushClient) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b := <-client.subscriber.requestLock:
		err := client.subscriber.doSubscribe(ctx, tblId)
		client.subscriber.requestLock <- b
		if err != nil {
			return err
		}
		logutil.Debugf("[log-tail-push-client] send subscribe tbl[db: %d, tbl: %d] request succeed", tblId.DbId, tblId.TbId)
		return nil
	}
}

func (client *pushClient) unsubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b := <-client.subscriber.requestLock:
		err := client.subscriber.doUnSubscribe(ctx, tblId)
		client.subscriber.requestLock <- b
		if err != nil {
			return err
		}
		logutil.Debugf("[log-tail-push-client] send unsubscribe tbl[db: %d, tbl: %d] request succeed", tblId.DbId, tblId.TbId)
		return nil
	}
}

func (client *pushClient) firstTimeConnectToLogTailServer(
	ctx context.Context) error {
	var err error
	// push subscription to Table `mo_database`, `mo_table`, `mo_column` of mo_catalog.
	databaseId := uint64(catalog.MO_CATALOG_ID)
	tableIds := []uint64{catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID}

	ch := make(chan error)
	go func() {
		var er error
		for _, ti := range tableIds {
			er = client.forcedSubscribeTable(ctx, databaseId, ti)
			if er != nil {
				break
			}
		}
		ch <- er
	}()

	err = <-ch
	close(ch)

	if err != nil {
		logutil.Errorf("[log-tail-push-client] connect to dn log tail server failed")
	}
	return err
}

func (client *pushClient) receiveTableLogTailContinuously(ctx context.Context, e *Engine) {
	connectMsg := make(chan error)

	// we should always make sure that we have received connection message from `connectMsg` channel if we want to do reconnect.
	// if not, it will cause some goroutine leak.
	hasReceivedConnectionMsg := false

	go func() {
		for {
			// new parallelNums routine to consume log tails.
			consumeErr := make(chan error, parallelNums)
			receiver := make([]routineController, parallelNums)
			for i := range receiver {
				receiver[i] = createRoutineToConsumeLogTails(ctx, i, bufferLength, e, consumeErr)
			}

			ch := make(chan logTailSubscriberResponse, 1)

			// A dead loop to receive log tail response from log tail service.
			// if any error happened, we should do reconnection.
			for {
				deadline, cancel := context.WithTimeout(ctx, maxTimeToWaitServerResponse)
				select {
				case ch <- client.subscriber.receiveResponse(deadline):
					// receive a response from log tail service.
					client.subscriber.receivedResp = nil
					cancel()

					resp := <-ch
					if resp.err != nil {
						// POSSIBLE ERROR: context deadline exceeded, rpc closed, decode error.
						logutil.Errorf("[log-tail-push-client] receive an error from log tail client, err : '%s'.", resp.err)
						goto cleanAndReconnect
					}

					response := resp.response
					// consume subscribe response
					if sResponse := response.GetSubscribeResponse(); sResponse != nil {
						if err := distributeSubscribeResponse(
							ctx, e, sResponse, receiver); err != nil {
							logutil.Errorf("[log-tail-push-client] distribute subscribe response failed, err : '%s'.", err)
							goto cleanAndReconnect
						}
						continue
					}

					// consume update response
					if upResponse := response.GetUpdateResponse(); upResponse != nil {
						if err := distributeUpdateResponse(
							ctx, e, upResponse, receiver); err != nil {
							logutil.Errorf("[log-tail-push-client] distribute update response failed, err : '%s'.", err)
							goto cleanAndReconnect
						}
						continue
					}

					// consume unsubscribe response
					if unResponse := response.GetUnsubscribeResponse(); unResponse != nil {
						if err := distributeUnSubscribeResponse(
							ctx, e, unResponse, receiver); err != nil {
							logutil.Errorf("[log-tail-push-client] distribute unsubscribe response failed, err : '%s'.", err)
							goto cleanAndReconnect
						}
						continue
					}

				case err := <-consumeErr:
					// receive an error from sub-routine to consume log.
					logutil.Errorf("[log-tail-push-client] consume log tail failed. err '%s'", err)
					cancel()
					goto cleanAndReconnect

				case err := <-connectMsg:
					cancel()
					hasReceivedConnectionMsg = true
					if err != nil {
						logutil.Errorf("[log-tail-push-client] connect to dn log tail service failed, reason: %s", err)
						goto cleanAndReconnect
					}

					client.receivedLogTailTime.ready.Store(true)
					client.subscriber.setReady()
					logutil.Infof("[log-tail-push-client] connect to dn log tail service succeed.")
					continue
				}
			}

		cleanAndReconnect:
			logutil.Infof("[log-tail-push-client] start to clean log tail consume routines")
			for _, r := range receiver {
				r.close()
			}
			if !hasReceivedConnectionMsg {
				<-connectMsg
			}

			logutil.Debugf("[log-tail-push-client] clean finished, start to reconnect to dn log tail service")
			for {
				dnLogTailServerBackend := e.getDNServices()[0].LogTailServiceAddress
				if err := client.init(dnLogTailServerBackend, client.timestampWaiter); err != nil {
					logutil.Error("[log-tail-push-client] rebuild the cn log tail client failed.")
					time.Sleep(retryReconnect)
					continue
				}

				// once we reconnect succeed, should clean partition here.
				e.cleanMemoryTable()

				hasReceivedConnectionMsg = false

				go func() {
					err := client.firstTimeConnectToLogTailServer(ctx)
					connectMsg <- err
				}()
				break
			}
		}
	}()

	err := client.firstTimeConnectToLogTailServer(ctx)
	connectMsg <- err
}

func (client *pushClient) unusedTableGCTicker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(unsubscribeProcessTicker)
		for {
			<-ticker.C

			t := time.Now()
			client.subscribed.mutex.Lock()
			func() {
				defer client.subscribed.mutex.Unlock()

				shouldClean := t.Add(-unsubscribeTimer)
				for k, v := range client.subscribed.m {
					if ifShouldNotDistribute(k.db, k.tbl) {
						// never unsubscribe the mo_databases, mo_tables, mo_columns.
						continue
					}
					if !v.isDeleting && !v.latestTime.After(shouldClean) {
						if err := client.unsubscribeTable(ctx, api.TableID{DbId: k.db, TbId: k.tbl}); err == nil {
							client.subscribed.m[k] = tableSubscribeStatus{
								isDeleting: true,
								latestTime: v.latestTime,
							}
							continue
						} else {
							logutil.Errorf("sign tbl[dbId: %d, tblId: %d] unsubscribing failed, err : %s", k.db, k.tbl, err.Error())
						}
					}
				}
			}()
		}
	}()
}

type subscribeID struct {
	db  uint64
	tbl uint64
}

// subscribedTable used to record table subscribed status.
// only if m[table T] = true, T has been subscribed.
type subscribedTable struct {
	mutex sync.Mutex

	// value is table's latest use time.
	m map[subscribeID]tableSubscribeStatus
}

type tableSubscribeStatus struct {
	isDeleting bool
	latestTime time.Time
}

func (s *subscribedTable) initTableSubscribeRecord() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.m = make(map[subscribeID]tableSubscribeStatus)
}

func (s *subscribedTable) getTableSubscribe(dbId, tblId uint64) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	status, ok := s.m[subscribeID{dbId, tblId}]
	if ok {
		if status.isDeleting {
			ok = false
		} else {
			s.m[subscribeID{dbId, tblId}] = tableSubscribeStatus{
				isDeleting: false,
				latestTime: time.Now(),
			}
		}
	}
	return ok
}

func (s *subscribedTable) setTableSubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.m[subscribeID{dbId, tblId}] = tableSubscribeStatus{
		isDeleting: false,
		latestTime: time.Now(),
	}
	logutil.Debugf("subscribe tbl[db: %d, tbl: %d] succeed", dbId, tblId)
}

func (s *subscribedTable) setTableUnsubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.m, subscribeID{dbId, tblId})
	logutil.Debugf("unsubscribe tbl[db: %d, tbl: %d] succeed", dbId, tblId)
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
	r.latestAppliedLogTailTS.Store(&ts)

	r.timestampWaiter = timestampWaiter
	r.ready.Store(false)
	if len(r.tList) == 0 {
		r.tList = make(
			[]atomic.Pointer[timestamp.Timestamp],
			parallelNums+1,
		)
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

func (r *syncLogTailTimestamp) updateTimestamp(index int, newTimestamp timestamp.Timestamp) {
	r.tList[index].Store(&newTimestamp)
	if r.ready.Load() {
		ts := r.getTimestamp()
		r.timestampWaiter.NotifyLatestCommitTS(ts)
	}
}

func (r *syncLogTailTimestamp) greatEq(txnTime timestamp.Timestamp) bool {
	if r.ready.Load() {
		t := r.getTimestamp()
		return txnTime.LessEq(t)
	}
	return false
}

type logTailSubscriber struct {
	dnNodeID      int
	logTailClient *service.LogtailClient

	ready        bool
	receivedResp *logTailSubscriberResponse

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
func newRpcStreamToDnLogTailService(serviceAddr string) (morpc.Stream, error) {
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

	c, err1 := morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(10000),
		morpc.WithClientTag("cn-log-tail-client"),
		morpc.WithClientLogger(logger),
	)
	if err1 != nil {
		return nil, err1
	}

	stream, err2 := c.NewStream(serviceAddr, true)
	return stream, err2
}

func (s *logTailSubscriber) init(serviceAddr string) (err error) {
	// XXX we assume that we have only 1 dn now.
	s.dnNodeID = 0

	s.receivedResp = nil
	// if requestLock is not nil, it's most likely called by reconnect process.
	// we need to set it not ready first to ensure that no one can subscribe or unsubscribe table during reconnect.
	if s.requestLock != nil {
		s.setNotReady()
	}

	s.doSubscribe = clientIsPreparing
	s.doUnSubscribe = clientIsPreparing

	// close the old stream
	oldClient := s.logTailClient
	if oldClient != nil {
		_ = oldClient.Close()
	}

	stream, err := newRpcStreamToDnLogTailService(serviceAddr)
	if err != nil {
		return err
	}

	// new the log tail client.
	s.logTailClient, err = service.NewLogtailClient(stream, service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
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
	if !s.ready {
		s.requestLock <- true
		s.ready = true
	}
}

func (s *logTailSubscriber) setNotReady() {
	if s.ready {
		<-s.requestLock
		s.ready = false
	}
}

// can't call this method directly.
func (s *logTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel := context.WithTimeout(ctx, defaultTimeOutToSubscribeTable)
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
		newCtx, cancel := context.WithTimeout(ctx, defaultTimeOutToSubscribeTable)
		_ = cancel
		return s.logTailClient.Unsubscribe(newCtx, tblId)
	}
	return s.logTailClient.Unsubscribe(ctx, tblId)
}

func (s *logTailSubscriber) receiveResponse(deadlineCtx context.Context) logTailSubscriberResponse {
	if s.receivedResp != nil {
		return *s.receivedResp
	}

	r, err := s.logTailClient.Receive(deadlineCtx)
	resp := logTailSubscriberResponse{
		response: r,
		err:      err,
	}
	s.receivedResp = &resp
	return resp
}

func (e *Engine) InitLogTailPushModel(
	ctx context.Context,
	timestampWaiter client.TimestampWaiter) error {

	// try to init log tail client. if failed, retry.
	for {
		// get log tail service address.
		dnLogTailServerBackend := e.getDNServices()[0].LogTailServiceAddress
		if err := e.pClient.init(dnLogTailServerBackend, timestampWaiter); err != nil {
			continue
		}
		break
	}

	e.pClient.receiveTableLogTailContinuously(ctx, e)
	e.pClient.unusedTableGCTicker(ctx)
	return nil
}

func ifShouldNotDistribute(dbId, tblId uint64) bool {
	return dbId == catalog.MO_CATALOG_ID && tblId <= catalog.MO_COLUMNS_ID
}

func distributeSubscribeResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.SubscribeResponse,
	recRoutines []routineController) error {
	lt := response.Logtail
	tbl := lt.GetTable()
	notDistribute := ifShouldNotDistribute(tbl.DbId, tbl.TbId)
	if notDistribute {
		if err := e.consumeSubscribeResponse(ctx, response, false); err != nil {
			return err
		}
		e.pClient.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	} else {
		routineIndex := tbl.TbId % parallelNums
		recRoutines[routineIndex].sendSubscribeResponse(ctx, response)
	}
	// no matter how we consume the response, should update all timestamp.
	e.pClient.receivedLogTailTime.updateTimestamp(parallelNums, *lt.Ts)
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*lt.Ts)
	}
	return nil
}

func distributeUpdateResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.UpdateResponse,
	recRoutines []routineController) error {
	list := response.GetLogtailList()

	// loops for mo_database, mo_tables, mo_columns.
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_DATABASE_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false); err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_TABLES_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false); err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(list); i++ {
		table := list[i].Table
		if table.TbId == catalog.MO_COLUMNS_ID {
			if err := e.consumeUpdateLogTail(ctx, list[i], false); err != nil {
				return err
			}
		}
	}

	for index := 0; index < len(list); index++ {
		table := list[index].Table
		if ifShouldNotDistribute(table.DbId, table.TbId) {
			continue
		}
		recIndex := table.TbId % parallelNums
		recRoutines[recIndex].sendTableLogTail(list[index])
	}
	// should update all the timestamp.
	e.pClient.receivedLogTailTime.updateTimestamp(parallelNums, *response.To)
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*response.To)
	}
	return nil
}

func distributeUnSubscribeResponse(
	_ context.Context,
	_ *Engine,
	response *logtail.UnSubscribeResponse,
	recRoutines []routineController) error {
	tbl := response.Table
	notDistribute := ifShouldNotDistribute(tbl.DbId, tbl.TbId)
	if notDistribute {
		logutil.Errorf("unexpected unsubscribe response for tbl[dbId: %d, tblID: %d]",
			tbl.DbId, tbl.TbId)
		return nil
	}
	routineIndex := tbl.TbId % parallelNums
	recRoutines[routineIndex].sendUnSubscribeResponse(response)

	return nil
}

type routineController struct {
	routineId  int
	closeChan  chan bool
	signalChan chan routineControlCmd
}

func (rc *routineController) sendSubscribeResponse(ctx context.Context, r *logtail.SubscribeResponse) {
	rc.signalChan <- cmdToConsumeSub{log: r}
}

func (rc *routineController) sendTableLogTail(r logtail.TableLogtail) {
	rc.signalChan <- cmdToConsumeLog{log: r}
}

func (rc *routineController) updateTimeFromT(t timestamp.Timestamp) {
	rc.signalChan <- cmdToUpdateTime{time: t}
}

func (rc *routineController) sendUnSubscribeResponse(r *logtail.UnSubscribeResponse) {
	rc.signalChan <- cmdToConsumeUnSub{log: r}
}

func (rc *routineController) close() {
	rc.closeChan <- true
}

func createRoutineToConsumeLogTails(
	ctx context.Context,
	routineId int, signalBufferLength int,
	e *Engine, errOut chan error) routineController {

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
	}

	go singleRoutineToConsumeLogTail(ctx, e, &controller, errOut)

	return controller
}

// a signal to control the routine which is responsible for consuming log tail.
type routineControlCmd interface {
	action(ctx context.Context, e *Engine, ctrl *routineController) error
}

type cmdToConsumeSub struct{ log *logtail.SubscribeResponse }
type cmdToConsumeLog struct{ log logtail.TableLogtail }
type cmdToUpdateTime struct{ time timestamp.Timestamp }
type cmdToConsumeUnSub struct{ log *logtail.UnSubscribeResponse }

func (cmd cmdToConsumeSub) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeSubscribeResponse(ctx, response, true); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	e.pClient.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	return nil
}

func (cmd cmdToConsumeLog) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeUpdateLogTail(ctx, response, true); err != nil {
		return err
	}
	return nil
}

func (cmd cmdToUpdateTime) action(ctx context.Context, e *Engine, ctrl *routineController) error {
	e.pClient.receivedLogTailTime.updateTimestamp(ctrl.routineId, cmd.time)
	return nil
}

func (cmd cmdToConsumeUnSub) action(ctx context.Context, e *Engine, _ *routineController) error {
	table := cmd.log.Table
	e.cleanMemoryTableWithTable(table.DbId, table.TbId)
	e.pClient.subscribed.setTableUnsubscribe(table.DbId, table.TbId)
	return nil
}

func (e *Engine) consumeSubscribeResponse(ctx context.Context, rp *logtail.SubscribeResponse,
	lazyLoad bool) error {
	lt := rp.GetLogtail()
	return updatePartitionOfPush(ctx, e.pClient.subscriber.dnNodeID, e, &lt, lazyLoad)
}

func (e *Engine) consumeUpdateLogTail(ctx context.Context, rp logtail.TableLogtail,
	lazyLoad bool) error {
	return updatePartitionOfPush(ctx, e.pClient.subscriber.dnNodeID, e, &rp, lazyLoad)
}

// updatePartitionOfPush is the partition update method of log tail push model.
func updatePartitionOfPush(
	ctx context.Context,
	dnId int,
	e *Engine, tl *logtail.TableLogtail, lazyLoad bool) (err error) {
	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()

	partition := e.getPartition(dbId, tblId)

	select {
	case <-partition.Lock():
		defer partition.Unlock()
	case <-ctx.Done():
		return ctx.Err()
	}

	state, doneMutate := partition.MutateState()

	key := e.catalog.GetTableById(dbId, tblId)

	if lazyLoad {
		if len(tl.CkpLocation) > 0 {
			state.AppendCheckpoint(tl.CkpLocation)
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
		logutil.Errorf("consume %d-%s log tail error: %v\n", key.Id, key.Name, err)
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
) (err error) {
	for i := 0; i < len(lt.Commands); i++ {
		if err = consumeEntry(ctx, primarySeqnum,
			engine, state, &lt.Commands[i]); err != nil {
			return
		}
	}
	return nil
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
	if entries, err = taeLogtail.LoadCheckpointEntries(
		ctx,
		lt.CkpLocation,
		tableId, tableName,
		databaseId, "", engine.fs); err != nil {
		return
	}
	for _, entry := range entries {
		if err = consumeEntry(ctx, primarySeqnum,
			engine, state, entry); err != nil {
			return
		}
	}

	for i := 0; i < len(lt.Commands); i++ {
		if err = consumeEntry(ctx, primarySeqnum,
			engine, state, &lt.Commands[i]); err != nil {
			return
		}
	}
	return nil
}
