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
	"database/sql"
	"fmt"
	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
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
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// we check if txn is legal in a period of periodToCheckTxnTimestamp when we
	// open a new transaction.
	periodToCheckTxnTimestamp = 10 * time.Millisecond

	// we check if log tail is ready in a period of periodToCheckLogTailReady when we first time
	// subscribe a table.
	periodToCheckLogTailReady = 10 * time.Millisecond

	// periodToReconnectDnLogServer is a period of reconnect to log tail server if lost server message for a long time.
	periodToReconnectDnLogServer = 20 * time.Second

	// maxTimeToCheckTxnTimestamp is the max duration we check the txn timestamp.
	// if over, return an error message.
	maxTimeToCheckTxnTimestamp = 10 * time.Minute

	// maxSubscribeRequestPerSecond record how many client request we supported to subscribe table per second.
	maxSubscribeRequestPerSecond = 10000

	// reconnectDeadTime if the time of losing response with dn reaches reconnectDeadTime, we will reconnect.
	reconnectDeadTime = 5 * time.Minute

	// defaultPeriodToSubscribeTable if ctx without a dead time. we will set it 2 minute to send a subscribe-table message.
	defaultPeriodToSubscribeTable = 2 * time.Minute
)

// cnLogTailTimestamp each cn-node will hold one global log time.
// it's the last log time on all subscribed tables.
// the t is init to 0 when cn start or reconnect to dn.
var cnLogTailTimestamp struct {
	t timestamp.Timestamp
	sync.RWMutex
}

func initCnLogTailTimestamp() {
	zeroT := timestamp.Timestamp{
		// if multi Dn, NodeID shouldn't be 0.
		NodeID:       0,
		PhysicalTime: 0,
		LogicalTime:  0,
	}
	UpdateCnLogTimestamp(zeroT)
}

func getCnLogTimestamp() timestamp.Timestamp {
	cnLogTailTimestamp.Lock()
	t := cnLogTailTimestamp.t
	cnLogTailTimestamp.Unlock()
	return t
}

func UpdateCnLogTimestamp(newTimestamp timestamp.Timestamp) {
	cnLogTailTimestamp.Lock()
	cnLogTailTimestamp.t = newTimestamp
	cnLogTailTimestamp.Unlock()
}

// WaitUntilTxnTimeIsLegal check if txnTime is legal periodically. and return if legal.
func WaitUntilTxnTimeIsLegal(ctx context.Context,
	txnTime timestamp.Timestamp, level sql.IsolationLevel) error {
	// if we support the ReadCommit level, we should set the txnTime as cnLogTailTimestamp.
	t := maxTimeToCheckTxnTimestamp
	for {
		if t <= 0 {
			// XXX I'm not sure if it is a good error info.
			return moerr.NewTxnError(ctx, "start txn failed due to txn timestamp. please retry.")
		}
		if txnTimeIsLegal(txnTime) {
			return nil
		}
		time.Sleep(periodToCheckTxnTimestamp)
		t -= periodToCheckTxnTimestamp
	}
}

func txnTimeIsLegal(txnTime timestamp.Timestamp) bool {
	cnLogTailTimestamp.RLock()
	b := txnTime.LessEq(cnLogTailTimestamp.t)
	cnLogTailTimestamp.RUnlock()
	return b
}

// tableSubscribeRecord is records this cn node's table subscription
// the key is table-id, value is true or false.
var tableSubscribeRecord = &sync.Map{}

type subscribeID struct {
	db  uint64
	tbl uint64
}

func initTableSubscribeRecord() {
	newM := &sync.Map{}
	atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(tableSubscribeRecord)), unsafe.Pointer(newM))
}

func setTableSubscribe(dbId, tblId uint64) {
	tableSubscribeRecord.Store(subscribeID{dbId, tblId}, true)
}

func getTableSubscribe(dbId, tblId uint64) bool {
	_, b := tableSubscribeRecord.Load(subscribeID{dbId, tblId})
	return b
}

type TableLogTailSubscriber struct {
	dnNodeID int

	logTailClient *service.LogtailClient
	streamSender  morpc.Stream

	// engine to get table info
	engine *Engine
	// if dead time, we should reconnect to dn.
	deadTime timestamp.Timestamp
}

// if we support multi dn next day. cnLogTailSubscriber should be cnLogTailSubscribers.
// and each one will be related to one dn node.
var cnLogTailSubscriber *TableLogTailSubscriber

func InitCnLogTailSubscriber(
	ctx context.Context,
	engine *Engine) error {
	initCnLogTailTimestamp()
	initTableSubscribeRecord()

	cnLogTailSubscriber = &TableLogTailSubscriber{
		dnNodeID: 0,
		engine:   engine,
	}
	if err := cnLogTailSubscriber.newCnLogTailClient(); err != nil {
		return err
	}
	cnLogTailSubscriber.StartReceiveTableLogTail()
	if err := cnLogTailSubscriber.connectToLogTailServer(ctx); err != nil {
		return err
	}
	return nil
}

func (logSub *TableLogTailSubscriber) newCnLogTailClient() error {
	var logTailClient *service.LogtailClient
	var s morpc.Stream

	// if reconnected, close the old rpc client.
	if logSub.logTailClient != nil {
		err := logSub.logTailClient.Close()
		if err != nil {
			return err
		}
	}

	engine := logSub.engine
	cluster, err := engine.getClusterDetails()
	if err != nil {
		return err
	}
	// XXX we assume that we have only 1 DN now.
	dnLogTailServerBackend := cluster.DNStores[0].LogtailServerAddress
	{
		// If cn client is not ready, init it here.
		if !cnclient.IsCNClientReady() {
			if err = cnclient.NewCNClient(&cnclient.ClientConfig{}); err != nil {
				return err
			}
		}
	}

	// XXX test code.
	codec := morpc.NewMessageCodec(func() morpc.Message {
		return &service.LogtailResponseSegment{}
	})
	factory := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(1<<15, 1<<15),
		),
		morpc.WithBackendLogger(logutil.GetGlobalLogger().Named("cn-log-tail-client-backend")),
	)

	c, err := morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(10000),
		morpc.WithClientTag("cn-log-tail-client"),
	)

	s, err = c.NewStream(dnLogTailServerBackend, true)
	if err != nil {
		return err
	}
	logTailClient, err = service.NewLogtailClient(s,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	if err != nil {
		return err
	}
	logSub.logTailClient = logTailClient
	logSub.streamSender = s
	return nil
}

func (logSub *TableLogTailSubscriber) connectToLogTailServer(
	ctx context.Context) error {
	var err error

	// push subscription to Table `mo_database`, `mo_table`, `mo_column` of mo_catalog.
	dIds := uint64(catalog.MO_CATALOG_ID)
	tIds := []uint64{catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID}
	txnT := getCnLogTimestamp()

	ch := make(chan error)
	go func() {
		for _, ti := range tIds {
			er := TryToGetTableLogTail(ctx, txnT, dIds, ti)
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
		return err
	}
}

func (logSub *TableLogTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	if _, ok := ctx.Deadline(); !ok {
		nctx, _ := context.WithTimeout(ctx, defaultPeriodToSubscribeTable)
		return logSub.logTailClient.Subscribe(nctx, tblId)
	}
	return logSub.logTailClient.Subscribe(ctx, tblId)
}

func (logSub *TableLogTailSubscriber) StartReceiveTableLogTail() {
	// start a background routine to receive table log tail and update related structure.
	go func() {
		type response struct {
			r   *service.LogtailResponse
			err error
		}
		generateResponse := func(ltR *service.LogtailResponse, err error) response {
			return response{r: ltR, err: err}
		}

		ctx := context.TODO()
		for {

			ch := make(chan response, 1)
			needReconnect := false
			// receive the log from dn log tail server.
			for {
				deadLine, cancel := context.WithTimeout(ctx, reconnectDeadTime)
				select {
				case <-deadLine.Done():
					// should log some information about log client restart.
					// and how to do the restart work is a question.
					needReconnect = true
				case ch <- generateResponse(cnLogTailSubscriber.logTailClient.Receive()):
					cancel()
				}

				if needReconnect {
					break
				}

				resp := <-ch
				if resp.err == nil {
					// if we receive the response, update the partition and global timestamp.
					switch {
					case resp.r.GetSubscribeResponse() != nil:
						lt := resp.r.GetSubscribeResponse().GetLogtail()
						logTs := lt.GetTs()

						if err := updatePartition2(ctx, logSub.dnNodeID, logSub.engine, &lt); err != nil {
							needReconnect = true
							break
						}
						UpdateCnLogTimestamp(*logTs)

					case resp.r.GetUpdateResponse() != nil:
						logLists := resp.r.GetUpdateResponse().GetLogtailList()
						to := resp.r.GetUpdateResponse().GetTo()

						logList(logLists).Sort()

						for _, l := range logLists {
							if err := updatePartition2(ctx, logSub.dnNodeID, logSub.engine, &l); err != nil {
								logutil.Error("cnLogTailClient : update table partition failed.")
								needReconnect = true
								break
							}
						}
						UpdateCnLogTimestamp(*to)

					// XXX we do not handle these message now.
					case resp.r.GetError() != nil:
					case resp.r.GetUnsubscribeResponse() != nil:
					}
				} else {
					logutil.Error(fmt.Sprintf("receive a error from dn log tail server, err is %s", resp.err.Error()))
				}
				if needReconnect {
					break
				}
			}
			// init related structure again and reconnect to log tail server.
			initCnLogTailTimestamp()
			initTableSubscribeRecord()
			logutil.Error("cn log tail client may lost connect to dn log tail server, and try to reconnect")
			for {
				if err := logSub.newCnLogTailClient(); err != nil {
					logutil.Error("rebuild the cn log tail client failed")
					continue
				}
				if err := logSub.connectToLogTailServer(ctx); err == nil {
					logutil.Info("reconnect to dn log tail server success.")
					break
				}
				logutil.Error("reconnect to dn log tail server failed.")
				time.Sleep(periodToReconnectDnLogServer)
			}
		}
	}()
}

// TryToGetTableLogTail will check if table was subscribed or not.
// if subscribed already, just return.
// if not, subscribe the table and wait until receive the log.
func TryToGetTableLogTail(
	ctx context.Context,
	txnTimestamp timestamp.Timestamp,
	dbId, tblId uint64) error {
	if getTableSubscribe(dbId, tblId) {
		return nil
	}
	if err := cnLogTailSubscriber.subscribeTable(ctx,
		api.TableID{DbId: dbId, TbId: tblId}); err != nil {
		return err
	}
	e := cnLogTailSubscriber.engine
	partitions := e.db.getPartitions(dbId, tblId)
	// wait until each partition has the legal ts (ts >= txn ts).
	for i := 0; i < len(partitions); i++ {
		partition := partitions[i]
		for {
			partition.Lock()
			if partition.ts.GreaterEq(txnTimestamp) {
				partition.Unlock()
				break
			}
			partition.Unlock()
			time.Sleep(periodToCheckLogTailReady)
			continue
		}
	}
	setTableSubscribe(dbId, tblId)
	return nil
}

func updatePartition2(
	ctx context.Context,
	dnId int,
	e *Engine, tl *logtail.TableLogtail) (err error) {
	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()

	partitions := e.db.getPartitions(dbId, tblId)
	partition := partitions[dnId]

	key := e.catalog.GetTableById(dbId, tblId, *tl.Ts)
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
	// if contains at least 2 type of update of mo_database, mo_table, mo_column
	// ids of Mo_database, Mo_table, Mo_column are 1, 2, 3
	occurAny := false
	tableOccurs := [4]bool{false, false, false, false}
	needSort := false
	for _, l := range ls {
		id := l.Table.TbId
		if id < 4 {
			if tableOccurs[id] {
				continue
			}
			if occurAny {
				needSort = true
				break
			}
			occurAny = true
			tableOccurs[id] = true
		}
	}
	if needSort {
		sort.Sort(ls)
	}
	return
}

func compareTableIdLess(i1, i2 uint64) bool {
	if i1 > 4 || i2 > 4 {
		return false
	}
	return i1 < i2
}
