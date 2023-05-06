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
	// each periodToCheckTxnTimestamp, we check if we have received enough log for a new txn.
	// If still not within maxBlockTimeToNewTransaction, the transaction creation fails.
	maxBlockTimeToNewTransaction = 1 * time.Minute
	periodToCheckTxnTimestamp    = 1 * time.Millisecond

	// if we didn't receive response from dn log tail server within time maxTimeToWaitServerResponse.
	// we assume that client has lost connect to server and will start a reconnect.
	maxTimeToWaitServerResponse = 60 * time.Second

	// once we reconnect dn failed. we will retry after time retryReconnect.
	retryReconnect = 20 * time.Millisecond

	// max number of subscribe request we allowed per second.
	maxSubscribeRequestPerSecond = 10000

	// once we send a table subscribe request, we check table subscribe status each periodToCheckTableSubscribeSucceed.
	// If check time exceeds noticeTimeToCheckTableSubscribeSucceed, warning log will be printed.
	periodToCheckTableSubscribeSucceed     = 1 * time.Millisecond
	noticeTimeToCheckTableSubscribeSucceed = 10 * time.Second

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
	bufferLength = 128
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

func (client *pushClient) checkTxnTimeIsLegal(
	ctx context.Context, txnTime timestamp.Timestamp) error {
	if client.receivedLogTailTime.greatEq(txnTime) {
		return nil
	}
	ticker := time.NewTicker(periodToCheckTxnTimestamp)
	defer ticker.Stop()

	for i := maxBlockTimeToNewTransaction; i > 0; i -= periodToCheckTxnTimestamp {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if client.receivedLogTailTime.greatEq(txnTime) {
				return nil
			}
		}
	}
	logutil.Errorf("new txn failed because lack of enough log tail. txn time is [%s]", txnTime.String())
	return moerr.NewTxnError(ctx, "new txn failed. please retry")
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

	noticeRange := int(noticeTimeToCheckTableSubscribeSucceed / periodToCheckTableSubscribeSucceed)
	for j := 1; ; j++ {
		for i := 0; i < noticeRange; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if client.subscribed.getTableSubscribe(dbId, tblId) {
					return nil
				}
			}
		}
		logutil.Warnf("didn't receive tbl[db: %d, tbl: %d] subscribe response for a long time [%d * %s]",
			dbId, tblId, j, noticeTimeToCheckTableSubscribeSucceed)
	}
}

func (client *pushClient) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case subscriber := <-client.subscriber.lockSubscriber:
		client.subscriber.lockSubscriber <- subscriber
		if err := subscriber(ctx, tblId); err != nil {
			return err
		}
		logutil.Infof("send subscribe tbl[db: %d, tbl: %d] request succeed", tblId.DbId, tblId.TbId)
		return nil
	}
}

func (client *pushClient) unsubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case unsubscriber := <-client.subscriber.lockUnSubscriber:
		client.subscriber.lockUnSubscriber <- unsubscriber
		if err := unsubscriber(ctx, tblId); err != nil {
			return err
		}
		logutil.Infof("send unsubscribe tbl[db: %d, tbl: %d] request succeed", tblId.DbId, tblId.TbId)
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
		for _, ti := range tableIds {
			er := client.TryToSubscribeTable(ctx, databaseId, ti)
			if er != nil {
				ch <- er
				return
			}
		}
		ch <- nil
	}()

	select {
	case <-ctx.Done():
		logutil.Errorf("connect to dn log tail server failed")
		return ctx.Err()
	case err = <-ch:
		if err != nil {
			return err
		}
		client.receivedLogTailTime.ready.Store(true)
		return nil
	}
}

func (client *pushClient) receiveTableLogTailContinuously(e *Engine) {
	reconnectErr := make(chan error)

	go func() {
		for {
			// new parallelNums routine to consume log tails.
			errChan := make(chan error, parallelNums)
			receiver := make([]routineController, parallelNums)
			for i := range receiver {
				receiver[i] = createRoutineToConsumeLogTails(i, bufferLength, e, errChan)
			}

			ctx := context.TODO()
			ch := make(chan logTailSubscriberResponse, 1)
			// a dead loop to receive log, if lost connect, should reconnect.
			for {
				deadline, cancel := context.WithTimeout(ctx, maxTimeToWaitServerResponse)
				select {
				case <-deadline.Done():
					// max wait time is out.
					goto cleanAndReconnect

				case ch <- client.subscriber.receiveResponse():
					// receive a response from log tail service.
					cancel()

				case err := <-errChan:
					// receive an error from sub-routine to consume log.
					logutil.ErrorField(err)
					cancel()
					goto cleanAndReconnect

				case err := <-reconnectErr:
					cancel()
					if err != nil {
						logutil.Errorf("reconnect to dn log tail service failed, reason: %s", err)
						goto cleanAndReconnect
					}
					logutil.Infof("reconnect to dn log tail service succeed")
					continue
				}

				resp := <-ch
				if resp.err != nil {
					// may rpc close error or decode error.
					logutil.ErrorField(resp.err)
					goto cleanAndReconnect
				}

				response := resp.response
				// consume subscribe response
				if sResponse := response.GetSubscribeResponse(); sResponse != nil {
					if err := distributeSubscribeResponse(
						ctx, e, sResponse, receiver); err != nil {
						logutil.ErrorField(err)
						goto cleanAndReconnect
					}
					continue
				}

				// consume update response
				if upResponse := response.GetUpdateResponse(); upResponse != nil {
					if err := distributeUpdateResponse(
						ctx, e, upResponse, receiver); err != nil {
						logutil.ErrorField(err)
						goto cleanAndReconnect
					}
					continue
				}

				// consume unsubscribe response
				if unResponse := response.GetUnsubscribeResponse(); unResponse != nil {
					if err := distributeUnSubscribeResponse(
						ctx, e, unResponse, receiver); err != nil {
						logutil.ErrorField(err)
						goto cleanAndReconnect
					}
					continue
				}
			}

		cleanAndReconnect:
			for _, r := range receiver {
				r.close()
			}
			logutil.Infof("start to reconnect to dn log tail service")
			for {
				dnLogTailServerBackend := e.getDNServices()[0].LogTailServiceAddress
				if err := client.init(dnLogTailServerBackend, client.timestampWaiter); err != nil {
					logutil.Error("rebuild the cn log tail client failed.")
					time.Sleep(retryReconnect)
					continue
				}

				// once we reconnect succeed, should clean partition here.
				e.cleanMemoryTable()

				go func() {
					err := client.firstTimeConnectToLogTailServer(context.TODO())
					reconnectErr <- err
				}()
				break
			}
		}
	}()
}

func (client *pushClient) unusedTableGCTicker() {
	go func() {
		ctx := context.TODO()
		ticker := time.NewTicker(unsubscribeProcessTicker)
		for {
			<-ticker.C

			t := time.Now()
			client.subscribed.mutex.Lock()

			shouldClean := t.Add(-unsubscribeTimer)
			for k, v := range client.subscribed.m {
				if ifShouldNotDistribute(k.db, k.tbl) {
					// never unsubscribe the mo_databases, mo_tables, mo_columns.
					continue
				}
				if !v.isDeleting && !v.latestTime.After(shouldClean) {
					if err := client.unsubscribeTable(ctx, api.TableID{DbId: k.db, TbId: k.tbl}); err == nil {
						logutil.Infof("sign tbl[dbId: %d, tblId: %d] unsubscribing", k.db, k.tbl)
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
			client.subscribed.mutex.Unlock()
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
	s.m = make(map[subscribeID]tableSubscribeStatus)
	s.mutex.Unlock()
}

func (s *subscribedTable) getTableSubscribe(dbId, tblId uint64) bool {
	s.mutex.Lock()
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
	s.mutex.Unlock()
	return ok
}

func (s *subscribedTable) setTableSubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	s.m[subscribeID{dbId, tblId}] = tableSubscribeStatus{
		isDeleting: false,
		latestTime: time.Now(),
	}
	s.mutex.Unlock()
	logutil.Infof("subscribe tbl[db: %d, tbl: %d] succeed", dbId, tblId)
}

func (s *subscribedTable) setTableUnsubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	delete(s.m, subscribeID{dbId, tblId})
	s.mutex.Unlock()
	logutil.Infof("unsubscribe tbl[db: %d, tbl: %d] succeed", dbId, tblId)
}

// syncLogTailTimestamp is a global log tail timestamp for a cn node.
// support `getTimestamp()` method to get time of last received log.
type syncLogTailTimestamp struct {
	timestampWaiter client.TimestampWaiter
	ready           atomic.Bool
	tList           []struct {
		time timestamp.Timestamp
		sync.RWMutex
	}
}

func (r *syncLogTailTimestamp) initLogTailTimestamp(timestampWaiter client.TimestampWaiter) {
	r.timestampWaiter = timestampWaiter
	r.ready.Store(false)
	if len(r.tList) == 0 {
		r.tList = make([]struct {
			time timestamp.Timestamp
			sync.RWMutex
		}, parallelNums+1)
	} else {
		for i := range r.tList {
			r.tList[i].Lock()
		}
		for i := range r.tList {
			r.tList[i].time = timestamp.Timestamp{}
		}
		for i := range r.tList {
			r.tList[i].Unlock()
		}
	}
}

func (r *syncLogTailTimestamp) getTimestamp() timestamp.Timestamp {
	r.tList[0].RLock()
	minT := r.tList[0].time
	r.tList[0].RUnlock()
	for i := 1; i < len(r.tList); i++ {
		r.tList[i].RLock()
		tempT := r.tList[i].time
		r.tList[i].RUnlock()
		if tempT.Less(minT) {
			minT = tempT
		}
	}
	return minT
}

func (r *syncLogTailTimestamp) updateTimestamp(index int, newTimestamp timestamp.Timestamp) {
	r.tList[index].Lock()
	r.tList[index].time = newTimestamp
	r.tList[index].Unlock()
	r.timestampWaiter.NotifyLatestCommitTS(r.getTimestamp())
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

	// return a table subscribe method.
	lockSubscriber chan func(context.Context, api.TableID) error
	// return a table unsubscribe method.
	lockUnSubscriber chan func(ctx context.Context, id api.TableID) error
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

	// close the old stream
	oldClient := s.logTailClient
	if oldClient != nil {
		_ = oldClient.Close()
	}

	// if channel is not nil here, it's most likely called by reconnect process.
	// we need to take out the content of channel to ensure that
	// the subscriber can't be used during the init process.
	if s.lockSubscriber == nil {
		s.lockSubscriber = make(chan func(context.Context, api.TableID) error, 1)
	} else {
		<-s.lockSubscriber
	}
	s.lockSubscriber <- clientIsPreparing

	if s.lockUnSubscriber == nil {
		s.lockUnSubscriber = make(chan func(context.Context, api.TableID) error, 1)
	} else {
		<-s.lockUnSubscriber
	}
	s.lockUnSubscriber <- clientIsPreparing

	stream, err := newRpcStreamToDnLogTailService(serviceAddr)
	if err != nil {
		return err
	}

	// new the log tail client.
	s.logTailClient, err = service.NewLogtailClient(stream,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	if err != nil {
		return err
	}
	<-s.lockSubscriber
	<-s.lockUnSubscriber

	s.lockSubscriber <- s.subscribeTable
	s.lockUnSubscriber <- s.unSubscribeTable
	return nil
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

func (s *logTailSubscriber) receiveResponse() logTailSubscriberResponse {
	r, err := s.logTailClient.Receive()
	return logTailSubscriberResponse{
		response: r,
		err:      err,
	}
}

func (e *Engine) SetPushModelFlag(turnOn bool) {
	e.usePushModel = turnOn
}

func (e *Engine) UsePushModelOrNot() bool {
	return e.usePushModel
}

func (e *Engine) InitLogTailPushModel(
	ctx context.Context,
	timestampWaiter client.TimestampWaiter) error {
	e.SetPushModelFlag(true)

	// get log tail service address.
	dnLogTailServerBackend := e.getDNServices()[0].LogTailServiceAddress
	if err := e.pClient.init(
		dnLogTailServerBackend,
		timestampWaiter); err != nil {
		return err
	}
	e.pClient.receiveTableLogTailContinuously(e)
	e.pClient.unusedTableGCTicker()

	// first time connect to log tail server, should push subscription of some table on `mo_catalog` database.
	if err := e.pClient.firstTimeConnectToLogTailServer(ctx); err != nil {
		return err
	}
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
		recRoutines[routineIndex].sendSubscribeResponse(response)
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
	e *Engine,
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
	ctx        context.Context
	routineId  int
	closeChan  chan bool
	signalChan chan routineControlCmd
}

func (rc *routineController) sendSubscribeResponse(r *logtail.SubscribeResponse) {
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
	routineId int, signalBufferLength int,
	e *Engine, errOut chan error) routineController {
	controller := routineController{
		ctx:        context.TODO(),
		routineId:  routineId,
		closeChan:  make(chan bool),
		signalChan: make(chan routineControlCmd, signalBufferLength),
	}

	go func(engine *Engine, receiver *routineController, errRet chan error) {
		for {
			select {
			case cmd := <-receiver.signalChan:
				if err := cmd.action(engine, receiver); err != nil {
					errRet <- err
				}

			case <-receiver.closeChan:
				close(receiver.closeChan)
				close(receiver.signalChan)
				return
			}
		}
	}(e, &controller, errOut)

	return controller
}

// a signal to control the routine which is responsible for consuming log tail.
type routineControlCmd interface {
	action(e *Engine, ctrl *routineController) error
}

type cmdToConsumeSub struct{ log *logtail.SubscribeResponse }
type cmdToConsumeLog struct{ log logtail.TableLogtail }
type cmdToUpdateTime struct{ time timestamp.Timestamp }
type cmdToConsumeUnSub struct{ log *logtail.UnSubscribeResponse }

func (cmd cmdToConsumeSub) action(e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeSubscribeResponse(ctrl.ctx, response, true); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	e.pClient.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	return nil
}

func (cmd cmdToConsumeLog) action(e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeUpdateLogTail(ctrl.ctx, response, true); err != nil {
		return err
	}
	return nil
}

func (cmd cmdToUpdateTime) action(e *Engine, ctrl *routineController) error {
	e.pClient.receivedLogTailTime.updateTimestamp(ctrl.routineId, cmd.time)
	return nil
}

func (cmd cmdToConsumeUnSub) action(e *Engine, _ *routineController) error {
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

	partitions := e.getPartitions(dbId, tblId)
	partition := partitions[dnId]

	select {
	case <-partition.Lock():
		defer partition.Unlock()
	case <-ctx.Done():
		return ctx.Err()
	}

	state, doneMutate := partition.MutateState()

	key := e.catalog.GetTableById(dbId, tblId)

	if lazyLoad {
		state.Checkpoints = append(state.Checkpoints, tl.CkpLocation)

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
