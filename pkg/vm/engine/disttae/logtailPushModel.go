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
	"sort"
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
	taeLogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const (
	// when starting a new transaction, we check the txn time every periodToCheckTxnTimestamp until
	// the time is legal (less or equal to cn global log tail time).
	// If still illegal within maxTimeToNewTransaction, the transaction creation fails.
	maxTimeToNewTransaction   = 1 * time.Minute
	periodToCheckTxnTimestamp = 1 * time.Millisecond

	// if cn-log-tail-client does not receive response within time maxTimeToWaitServerResponse.
	// we assume that client has lost connect to server.
	// and will reconnect in a period of periodToReconnectDnLogServer.
	maxTimeToWaitServerResponse  = 10 * time.Second
	periodToReconnectDnLogServer = 10 * time.Second

	// max number of subscribe request we allowed per second.
	maxSubscribeRequestPerSecond = 10000

	// we check the subscribe status in a period of periodToCheckTableSubscribeSucceed
	// after subscribe a table first time.
	periodToCheckTableSubscribeSucceed = 1 * time.Millisecond

	// default deadline for context to send a rpc message.
	defaultTimeOutToSubscribeTable = 2 * time.Minute
)

const (
	// routine number to consume log tail.
	parallelNums = 4

	// each routine's log tail buffer size.
	bufferLength = 100
)

// to ensure we can pass the SCA for unused code.
var subscribedT subscribedTable
var _ = subscribedT.setTableUnsubscribe
var testLogTailSubscriber logTailSubscriber
var _ = testLogTailSubscriber.unSubscribeTable

type subscribeID struct {
	db  uint64
	tbl uint64
}

// subscribedTable used to record table subscribed status.
// only if m[table T] = true, T has been subscribed.
type subscribedTable struct {
	m     map[subscribeID]bool
	mutex sync.RWMutex
}

func (s *subscribedTable) initTableSubscribeRecord() {
	s.mutex.Lock()
	s.m = make(map[subscribeID]bool)
	s.mutex.Unlock()
}

func (s *subscribedTable) getTableSubscribe(dbId, tblId uint64) bool {
	s.mutex.RLock()
	_, ok := s.m[subscribeID{dbId, tblId}]
	s.mutex.RUnlock()
	return ok
}

func (s *subscribedTable) setTableSubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	s.m[subscribeID{dbId, tblId}] = true
	s.mutex.Unlock()
}

func (s *subscribedTable) setTableUnsubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	delete(s.m, subscribeID{dbId, tblId})
	s.mutex.Unlock()
}

// syncLogTailTimestamp is a global log tail timestamp for a cn node.
// support `getTimestamp()` method to get time of last received log.
type syncLogTailTimestamp struct {
	ready atomic.Bool
	tList []struct {
		time timestamp.Timestamp
		sync.RWMutex
	}
}

func (r *syncLogTailTimestamp) initLogTailTimestamp() {
	r.ready.Store(false)
	r.tList = make([]struct {
		time timestamp.Timestamp
		sync.RWMutex
	}, parallelNums+1)
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
}

func (r *syncLogTailTimestamp) greatEq(txnTime timestamp.Timestamp) bool {
	if r.ready.Load() {
		t := r.getTimestamp()
		return txnTime.LessEq(t)
	}
	return false
}

func (r *syncLogTailTimestamp) blockUntilTxnTimeIsLegal(
	ctx context.Context, txnTime timestamp.Timestamp) error {
	// if block time is too long, return error.
	maxBlockTime := maxTimeToNewTransaction
	for {
		if maxBlockTime < 0 {
			return moerr.NewTxnError(ctx,
				"new txn failed. please retry.")
		}
		if r.greatEq(txnTime) {
			return nil
		}
		time.Sleep(periodToCheckTxnTimestamp)
		maxBlockTime -= periodToCheckTxnTimestamp
	}
}

// logTailSubscriber is responsible for
// sending subscribe request and unsubscribe request to dn.
type logTailSubscriber struct {
	dnNodeID      int
	logTailClient *service.LogtailClient
}

type logTailSubscriberResponse struct {
	response *service.LogtailResponse
	err      error
}

func (s *logTailSubscriber) init(serviceAddr string) (err error) {
	// XXX we assume that we have only 1 dn now.
	s.dnNodeID = 0

	// XXX generate a rpc client and new a stream.
	// we should hide these code into NewClient method next day.
	codec := morpc.NewMessageCodec(func() morpc.Message {
		return &service.LogtailResponseSegment{}
	})
	factory := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(1<<20, 1<<20),
		),
		morpc.WithBackendLogger(logutil.GetGlobalLogger().Named("cn-log-tail-client-backend")),
	)

	c, err1 := morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(10000),
		morpc.WithClientTag("cn-log-tail-client"),
	)
	if err1 != nil {
		return err1
	}

	stream, err2 := c.NewStream(serviceAddr, true)
	if err2 != nil {
		return err2
	}
	// new the log tail client.
	s.logTailClient, err = service.NewLogtailClient(stream,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	return err
}

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

// XXX There is not a good place to use the method unsubscribe now.
// should make a good way to unsubscribe table once a table was unused for a long time.
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
	ctx context.Context) error {
	e.SetPushModelFlag(true)

	// init global log time to be zero. and clear the record of subscription table.
	e.receiveLogTailTime.initLogTailTimestamp()
	e.subscribed.initTableSubscribeRecord()

	// init log tail client to send request and receive response.
	if err := e.initTableLogTailSubscriber(); err != nil {
		return err
	}
	e.ParallelToReceiveTableLogTail()

	// first time connect to log tail server, should push subscription of some table on `mo_catalog` database.
	if err := e.firstTimeConnectToLogTailServer(ctx); err != nil {
		return err
	}
	return nil
}

func (e *Engine) initTableLogTailSubscriber() error {
	// close the old rpc client.
	if e.subscriber != nil && e.subscriber.logTailClient != nil {
		if err := e.subscriber.logTailClient.Close(); err != nil {
			return err
		}
	}
	e.subscriber = new(logTailSubscriber)
	dnLogTailServerBackend := e.getDNServices()[0].LogTailServiceAddress
	return e.subscriber.init(dnLogTailServerBackend)
}

func (e *Engine) firstTimeConnectToLogTailServer(
	ctx context.Context) error {
	var err error
	// push subscription to Table `mo_database`, `mo_table`, `mo_column` of mo_catalog.
	databaseId := uint64(catalog.MO_CATALOG_ID)
	tableIds := []uint64{catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID}

	ch := make(chan error)
	go func() {
		for _, ti := range tableIds {
			er := e.tryToGetTableLogTail(ctx, databaseId, ti)
			if er != nil {
				ch <- er
				return
			}
		}
		ch <- nil
	}()

	select {
	case <-ctx.Done():
		return moerr.NewInternalError(ctx, "connect to dn log tail server failed")
	case err = <-ch:
		if err != nil {
			return err
		}
		e.receiveLogTailTime.ready.Store(true)
		return nil
	}
}

func (e *Engine) tryToGetTableLogTail(
	ctx context.Context,
	dbId, tblId uint64) error {
	// if table has been subscribed, just return.
	// if not, subscribe it and poll to check if we receive the log.
	if !e.subscribed.getTableSubscribe(dbId, tblId) {
		if err := e.subscriber.subscribeTable(ctx,
			api.TableID{DbId: dbId, TbId: tblId}); err != nil {
			return err
		}
		// poll until table was subscribed.
		for !e.subscribed.getTableSubscribe(dbId, tblId) {
			time.Sleep(periodToCheckTableSubscribeSucceed)
		}
	}
	// XXX we can move the subscribe-status-check here.
	return nil
}

func (e *Engine) ParallelToReceiveTableLogTail() {
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

				case ch <- e.subscriber.receiveResponse():
					// receive a response from log tail service.
					cancel()

				case err := <-errChan:
					// receive an error from sub-routine to consume log.
					logutil.ErrorField(err)
					goto cleanAndReconnect
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
				if uResponse := response.GetUpdateResponse(); uResponse != nil {
					if err := distributeUpdateResponse(
						ctx, e, uResponse, receiver); err != nil {
						logutil.ErrorField(err)
						goto cleanAndReconnect
					}
					continue
				}
			}

		cleanAndReconnect:
			e.subscriber.logTailClient.Close()
			e.subscriber = nil
			for _, r := range receiver {
				r.close()
			}
			e.receiveLogTailTime.initLogTailTimestamp()
			e.subscribed.initTableSubscribeRecord()
			for {
				if err := e.initTableLogTailSubscriber(); err != nil {
					logutil.Error("rebuild the cn log tail client failed.")
					continue
				}
				if err := e.firstTimeConnectToLogTailServer(ctx); err == nil {
					logutil.Info("reconnect to dn log tail server succeed.")
					break
				}
				logutil.Error("reconnect to dn log tail server failed.")
				time.Sleep(periodToReconnectDnLogServer)
			}
		}
	}()
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
		e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	} else {
		routineIndex := tbl.TbId % parallelNums
		recRoutines[routineIndex].sendSubscribeResponse(response)
	}
	// no matter how we consume the response, should update all timestamp.
	e.receiveLogTailTime.updateTimestamp(parallelNums, *lt.Ts)
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
	logList(list).Sort()

	// after sort, the smaller tblId, the smaller the index.
	var index int
	for index = 0; index < len(list); index++ {
		table := list[index].Table
		notDistribute := ifShouldNotDistribute(table.DbId, table.TbId)
		if !notDistribute {
			break
		}
		if err := e.consumeUpdateLogTail(ctx, list[index], false); err != nil {
			return err
		}
	}
	for ; index < len(list); index++ {
		table := list[index].Table
		recIndex := table.TbId % parallelNums
		recRoutines[recIndex].sendTableLogTail(list[index])
	}
	// should update all the timestamp.
	e.receiveLogTailTime.updateTimestamp(parallelNums, *response.To)
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*response.To)
	}
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

func (cmd cmdToConsumeSub) action(e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeSubscribeResponse(ctrl.ctx, response, true); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
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
	e.receiveLogTailTime.updateTimestamp(ctrl.routineId, cmd.time)
	return nil
}

func (e *Engine) consumeSubscribeResponse(ctx context.Context, rp *logtail.SubscribeResponse,
	lazyLoad bool) error {
	lt := rp.GetLogtail()
	return updatePartitionOfPush(ctx, e.subscriber.dnNodeID, e, &lt, lazyLoad)
}

func (e *Engine) consumeUpdateLogTail(ctx context.Context, rp logtail.TableLogtail,
	lazyLoad bool) error {
	return updatePartitionOfPush(ctx, e.subscriber.dnNodeID, e, &rp, lazyLoad)
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
	case <-partition.lock:
		defer func() {
			partition.lock <- struct{}{}
		}()
	case <-ctx.Done():
		return ctx.Err()
	}

	state, doneMutate := partition.MutateState()

	key := e.catalog.GetTableById(dbId, tblId)

	if lazyLoad {
		state.Checkpoints = append(state.Checkpoints, tl.CkpLocation)

		err = consumeLogTailOfPushWithLazyLoad(
			ctx,
			key.PrimaryIdx,
			e,
			state,
			tl,
		)
	} else {
		err = consumeLogTailOfPushWithoutLazyLoad(ctx, key.PrimaryIdx, e, state, tl, dbId, key.Id, key.Name)
	}

	if err != nil {
		logutil.Errorf("consume %d-%s log tail error: %v\n", key.Id, key.Name, err)
		return err
	}

	partition.ts = *tl.Ts

	doneMutate()

	return nil
}

func consumeLogTailOfPushWithLazyLoad(
	ctx context.Context,
	primaryIdx int,
	engine *Engine,
	state *PartitionState,
	lt *logtail.TableLogtail,
) (err error) {
	for i := 0; i < len(lt.Commands); i++ {
		if err = consumeEntry(ctx, primaryIdx,
			engine, state, &lt.Commands[i]); err != nil {
			return
		}
	}
	return nil
}

func consumeLogTailOfPushWithoutLazyLoad(
	ctx context.Context,
	primaryIdx int,
	engine *Engine,
	state *PartitionState,
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
		if err = consumeEntry(ctx, primaryIdx,
			engine, state, entry); err != nil {
			return
		}
	}

	for i := 0; i < len(lt.Commands); i++ {
		if err = consumeEntry(ctx, primaryIdx,
			engine, state, &lt.Commands[i]); err != nil {
			return
		}
	}
	return nil
}

type logList []logtail.TableLogtail

func (ls logList) Len() int { return len(ls) }
func (ls logList) Less(i, j int) bool {
	if ls[i].Ts.Less(*ls[j].Ts) {
		return true
	}
	if ls[i].Ts.Equal(*ls[j].Ts) {
		return compareTableIdLess(ls[i].Table.TbId, ls[j].Table.TbId)
	}
	return false
}
func (ls logList) Swap(i, j int) { ls[i], ls[j] = ls[j], ls[i] }
func (ls logList) Sort() {
	sort.Sort(ls)
}

func compareTableIdLess(i1, i2 uint64) bool {
	return i1 < i2
}
