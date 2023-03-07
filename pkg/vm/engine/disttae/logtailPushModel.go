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
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	logtail2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const (
	// when starting a new transaction, we check the txn time every periodToCheckTxnTimestamp until
	// the time is legal (less or equal to cn global log tail time).
	// If still illegal within maxTimeToNewTransaction, the transaction creation fails.
	maxTimeToNewTransaction   = 5 * time.Minute
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
// records the received last log tail timestamp.
type syncLogTailTimestamp struct {
	t timestamp.Timestamp
	sync.RWMutex
}

func (r *syncLogTailTimestamp) initLogTailTimestamp() {
	r.updateTimestamp(timestamp.Timestamp{})
}

func (r *syncLogTailTimestamp) getTimestamp() timestamp.Timestamp {
	r.RLock()
	t := r.t
	r.RUnlock()
	return t
}

func (r *syncLogTailTimestamp) updateTimestamp(newTimestamp timestamp.Timestamp) {
	r.Lock()
	r.t = newTimestamp
	r.Unlock()
}

func (r *syncLogTailTimestamp) greatEq(txnTime timestamp.Timestamp) bool {
	t := r.getTimestamp()
	return txnTime.LessEq(t)
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
	e.receiveLogTailTime.initLogTailTimestamp()
	e.subscribed.initTableSubscribeRecord()
	if err := e.initTableLogTailSubscriber(); err != nil {
		return err
	}
	e.StartToReceiveTableLogTail()
	if err := e.firstTimeConnectToLogTailServer(ctx); err != nil {
		return err
	}
	e.SetPushModelFlag(true)
	return nil
}

func (e *Engine) initTableLogTailSubscriber() error {
	// close the old rpc client.
	if e.subscriber != nil {
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
			er := e.subscriber.subscribeTable(ctx,
				api.TableID{DbId: databaseId, TbId: ti})
			if err != nil {
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
		return err
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
		for {
			if e.subscribed.getTableSubscribe(dbId, tblId) {
				break
			}
			time.Sleep(periodToCheckTableSubscribeSucceed)
		}
	}
	// XXX we can move the subscribe-status-check here.
	return nil
}

func (e *Engine) StartToReceiveTableLogTail() {
	// set up a background routine to receive table log.
	// if lost connection with log tail server. it should reconnect.
	go func() {
		ctx := context.TODO()
		for {
			ch := make(chan logTailSubscriberResponse, 1)
			reconect := false
			for {
				if reconect {
					break
				}

				deadline, cancel := context.WithTimeout(ctx, maxTimeToWaitServerResponse)
				select {
				case <-deadline.Done():
					reconect = true
					continue
				case ch <- e.subscriber.receiveResponse():
					cancel()
				}

				resp := <-ch
				if resp.err != nil {
					// decoded error or rpc err. should reconnect soon.
					logutil.Errorf("receive a error from dn log tail server, err is %s", resp.err.Error())
					reconect = true
					break
				}

				subscriber := e.subscriber
				response := resp.response
				switch {
				case response.GetSubscribeResponse() != nil:
					logTail := response.GetSubscribeResponse().GetLogtail()
					logTs := logTail.GetTs()
					if err := updatePartitionOfPush(ctx, subscriber.dnNodeID, e, &logTail, *logTs); err != nil {
						logutil.Errorf("update partition failed. err is %s", err)
						reconect = true
						break
					}
					tbl := logTail.GetTable()
					e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
					e.receiveLogTailTime.updateTimestamp(*logTs)

				case response.GetUpdateResponse() != nil:
					logLists := response.GetUpdateResponse().GetLogtailList()
					to := response.GetUpdateResponse().GetTo()

					// the purpose of sorting is to put log of mo.db, mo.table, mo.column first.
					// ensure the consistency with the pull process.
					logList(logLists).Sort()

					for _, l := range logLists {
						if err := updatePartitionOfPush(ctx, subscriber.dnNodeID, e, &l, *l.Ts); err != nil {
							logutil.Errorf("update partition failed. err is %s", err)
							reconect = true
							break
						}
					}
					if !reconect {
						e.receiveLogTailTime.updateTimestamp(*to)
					}

				default:
					// errResponse and unSubscribeResponse. we have no need to handle these now.
					//case resp.r.GetError() != nil:
					//case resp.r.GetUnsubscribeResponse() != nil:
				}
			}

			// reconnect to log tail server.
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

// updatePartitionOfPush is the partition update method of log tail push model.
func updatePartitionOfPush(
	ctx context.Context,
	dnId int,
	e *Engine, tl *logtail.TableLogtail, _ timestamp.Timestamp) (err error) {
	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()

	partitions := e.getPartitions(dbId, tblId)
	partition := partitions[dnId]

	state, doneMutate := partition.MutateState()

	key := e.catalog.GetTableById(dbId, tblId)

	if err = consumeLogTailOfPush(
		ctx,
		dbId,
		"",
		key.Id,
		key.Name,
		key.PrimaryIdx,
		e,
		state,
		tl,
	); err != nil {
		logutil.Errorf("consume %d-%s log tail error: %v\n", key.Id, key.Name, err)
		return err
	}

	doneMutate()

	select {
	case <-partition.lock:
	case <-ctx.Done():
		return ctx.Err()
	}
	partition.ts = *tl.Ts
	partition.lock <- struct{}{}

	return nil
}

func consumeLogTailOfPush(
	ctx context.Context,
	databaseId uint64,
	databaseName string,
	tableId uint64,
	tableName string,
	primaryIdx int,
	engine *Engine,
	state *PartitionState,
	lt *logtail.TableLogtail,
) (err error) {
	var entries []*api.Entry

	if entries, err = logtail2.LoadCheckpointEntries(
		ctx,
		lt.CkpLocation,
		tableId, tableName,
		databaseId, databaseName, engine.fs); err != nil {
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
