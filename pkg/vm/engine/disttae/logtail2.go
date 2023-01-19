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
	"sort"
	"sync"
	"time"
)

const (
	// we check if txn is legal in a period of periodToCheckTxnTimestamp
	// when we open a new transaction.
	periodToCheckTxnTimestamp = 1 * time.Millisecond

	// we check if we subscribe a table succeed
	// in a period of periodToCheckTableSubscribeSucceed when we first time subscribe a table.
	periodToCheckTableSubscribeSucceed = 1 * time.Millisecond

	// we try to reconnect to log tail server in a period of periodToReconnectDnLogServer.
	periodToReconnectDnLogServer = 10 * time.Second

	// maxTimeToCheckTxnTimestamp is the max duration we open a new txn.
	// if over, return error.
	maxTimeToCheckTxnTimestamp = 5 * time.Minute

	// maxSubscribeRequestPerSecond record how many client request
	// we supported to subscribe table per second.
	maxSubscribeRequestPerSecond = 10000

	// if we hadn't received log tail server response for maxTimeToWaitServerResponse.
	// we assume that we lost connect, can will reconnect.
	maxTimeToWaitServerResponse = 10 * time.Second

	// defaultTimeOutToSubscribeTable
	// if ctx without a dead time. we will set it 2 minute to send a subscribe-table message.
	defaultTimeOutToSubscribeTable = 2 * time.Minute
)

type subscribeID struct {
	db  uint64
	tbl uint64
}

// subscribedTable records cn table subscribe status.
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
	r.RLock()
	t := r.t
	r.RUnlock()
	return txnTime.LessEq(t)
}

func (r *syncLogTailTimestamp) blockUntilTxnTimeIsLegal(
	ctx context.Context, txnTime timestamp.Timestamp) error {
	// if block time is too long, return error.
	maxBlockTime := maxTimeToCheckTxnTimestamp
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

type logTailSubscriber struct {
	dnNodeID      int
	logTailClient *service.LogtailClient
}

func (s *logTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, _ := context.WithTimeout(ctx, defaultTimeOutToSubscribeTable)
		return s.logTailClient.Subscribe(newCtx, tblId)
	}
	return s.logTailClient.Subscribe(ctx, tblId)
}

func (s *logTailSubscriber) unSubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, _ := context.WithTimeout(ctx, defaultTimeOutToSubscribeTable)
		return s.logTailClient.Unsubscribe(newCtx, tblId)
	}
	return s.logTailClient.Unsubscribe(ctx, tblId)
}

func (e *Engine) InitLogTailPushModel(
	ctx context.Context) error {
	e.receiveLogTailTime.initLogTailTimestamp()
	e.subscribed.initTableSubscribeRecord()
	if err := e.initTableLogTailSubscriber(); err != nil {
		return err
	}
	e.StartToReceiveTableLogTail()
	if err := e.connectToLogTailServer(ctx); err != nil {
		return err
	}
	// XXX it's really stupid.
	e.db.cnE = e
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
	cluster, err := e.getClusterDetails()
	if err != nil {
		return err
	}
	// XXX we assume that we have only 1 dn now.
	e.subscriber.dnNodeID = 0
	dnLogTailServerBackend := cluster.DNStores[0].LogtailServerAddress
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

	c, err := morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(10000),
		morpc.WithClientTag("cn-log-tail-client"),
	)

	s, err := c.NewStream(dnLogTailServerBackend, true)
	if err != nil {
		return err
	}
	// new the log tail client.
	e.subscriber.logTailClient, err = service.NewLogtailClient(s,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	return nil
}

func (e *Engine) connectToLogTailServer(
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
	// subscribe table if it's never subscribed.
	// and poll to check if we receive the log.
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
	type response struct {
		r   *service.LogtailResponse
		err error
	}
	generateResponse := func(ltR *service.LogtailResponse, err error) response {
		return response{r: ltR, err: err}
	}
	// set up a background routine to receive table log.
	// if fail to connect log tail server.
	// it should reconnect.
	go func() {
		ctx := context.TODO()
		for {
			ch := make(chan response, 1)
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
				case ch <- generateResponse(e.subscriber.logTailClient.Receive()):
					cancel()
				}

				resp := <-ch
				if resp.err != nil {
					// get a rpc err from log tail server. and should reconnect soon.
					logutil.Error(
						fmt.Sprintf("receive a error from dn log tail server, err is %s",
							resp.err.Error()))
					reconect = true
				} else {
					subscriber := e.subscriber
					switch {
					case resp.r.GetSubscribeResponse() != nil:
						lt := resp.r.GetSubscribeResponse().GetLogtail()
						logTs := lt.GetTs()
						if err := updatePartition2(ctx, subscriber.dnNodeID, e, &lt, *logTs); err != nil {
							logutil.Errorf("update partition failed. err is %s", err)
							reconect = true
							break
						}
						tbl := lt.GetTable()
						e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
						e.receiveLogTailTime.updateTimestamp(*logTs)

					case resp.r.GetUpdateResponse() != nil:
						logLists := resp.r.GetUpdateResponse().GetLogtailList()
						to := resp.r.GetUpdateResponse().GetTo()

						logList(logLists).Sort()
						for _, l := range logLists {
							if err := updatePartition2(ctx, subscriber.dnNodeID, e, &l, *l.Ts); err != nil {
								logutil.Errorf("update partition failed. err is %s", err)
								reconect = true
								break
							}
						}
						e.receiveLogTailTime.updateTimestamp(*to)

					default:
						// errResponse and unSubscribeResponse.
						// we have no need to handle these now.
						//case resp.r.GetError() != nil:
						//case resp.r.GetUnsubscribeResponse() != nil:
					}
				}
			}

			// reconnect to log tail server.
			e.receiveLogTailTime.initLogTailTimestamp()
			e.subscribed.initTableSubscribeRecord()
			for {
				if err := e.initTableLogTailSubscriber(); err != nil {
					logutil.Error("rebuild the cn log tail client failed")
					continue
				}
				if err := e.connectToLogTailServer(ctx); err == nil {
					logutil.Info("reconnect to dn log tail server success.")
					break
				}
				logutil.Error("reconnect to dn log tail server failed.")
				time.Sleep(periodToReconnectDnLogServer)
			}
		}
	}()
}

func updatePartition2(
	ctx context.Context,
	dnId int,
	e *Engine, tl *logtail.TableLogtail, ts timestamp.Timestamp) (err error) {
	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()

	partitions := e.db.getPartitions(dbId, tblId)
	partition := partitions[dnId]

	key := e.catalog.GetTableById(dbId, tblId)
	tbl := &table{
		db: &database{
			databaseId:   dbId,
			databaseName: "",
			txn: &Transaction{
				catalog: e.catalog,
			},
			fs: e.fs,
		},
		parts:        partitions,
		tableId:      key.Id,
		tableName:    key.Name,
		defs:         key.Defs,
		tableDef:     key.TableDef,
		primaryIdx:   key.PrimaryIdx,
		clusterByIdx: key.ClusterByIdx,
		relKind:      key.Kind,
		viewdef:      key.ViewDef,
		comment:      key.Comment,
		partition:    key.Partition,
		createSql:    key.CreateSql,
		constraint:   key.Constraint,
	}

	if err = consumeLogTail2(ctx,
		dnId, tbl, e.db, partition, tl); err != nil {
		logutil.Errorf("consume %d-%s log tail error: %v\n", tbl.tableId, tbl.tableName, err)
		return err
	}
	partition.Lock()
	partition.ts = *tl.Ts
	partition.Unlock()
	return nil
}

func consumeLogTail2(
	ctx context.Context,
	idx int, tbl *table,
	db *DB, mvcc MVCC, lt *logtail.TableLogtail) (err error) {
	var entries []*api.Entry

	if entries, err = logtail2.LoadCheckpointEntries(
		ctx,
		lt.CkpLocation,
		tbl.tableId, tbl.tableName,
		tbl.db.databaseId, tbl.db.databaseName, tbl.db.fs); err != nil {
		return
	}
	for _, entry := range entries {
		if err = consumeEntry(idx, tbl.primaryIdx, tbl, ctx,
			db, mvcc, entry); err != nil {
			return
		}
	}

	for i := 0; i < len(lt.Commands); i++ {
		if err = consumeEntry(idx, tbl.primaryIdx, tbl, ctx,
			db, mvcc, &lt.Commands[i]); err != nil {
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
	return
}

func compareTableIdLess(i1, i2 uint64) bool {
	return i1 < i2
}

func debugToPrintLogList(ls []logtail.TableLogtail) string {
	if len(ls) == 0 {
		return ""
	}
	str := "log list are:\n"
	for i, l := range ls {
		did, tid := l.Table.DbId, l.Table.TbId
		str += fmt.Sprintf("\t log: %d, dn: %d, tbl: %d\n", i, did, tid)
		if len(l.Commands) > 0 {
			str += "\tcommands are :\n"
		}
		for j, command := range l.Commands {
			typ := "insert"
			if command.EntryType == 1 {
				typ = "delete"
			} else if command.EntryType == 2 {
				typ = "update"
			}
			str += fmt.Sprintf("\t\t %d: [dnName: %s, tableName: %s, typ: %s]\n",
				j, command.DatabaseName, command.TableName, typ)
		}
	}
	return str
}
