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
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	logtail2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// we check if txn is legal in a period of gapToCheckTxnTimestamp when we
	// open a new transaction.
	gapToCheckTxnTimestamp = 10 * time.Millisecond

	// maxTimeToCheckTxnTimestamp is the max duration we check the txn timestamp.
	// if over, return an error message.
	maxTimeToCheckTxnTimestamp = 10 * time.Minute

	// maxSubscribeRequestPerSecond record how many client request we supported to subscribe table per second.
	maxSubscribeRequestPerSecond = 10000

	// reconnectDeadTime if the time of losing response with dn reaches reconnectDeadTime, we will reconnect.
	reconnectDeadTime = 5 * time.Minute

	// we check if log tail is ready in a period of gapToCheckIfLogTailReady when we first time
	// subscribe a table.
	gapToCheckIfLogTailReady = 10 * time.Millisecond
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

func UpdateCnLogTimestamp(newTimestamp timestamp.Timestamp) {
	cnLogTailTimestamp.Lock()
	cnLogTailTimestamp.t = newTimestamp
	cnLogTailTimestamp.Unlock()
}

// WaitUntilTxnTimeIsLegal check if txnTime is legal periodically. and return if legal.
func WaitUntilTxnTimeIsLegal(ctx context.Context,
	txnTime *timestamp.Timestamp, level sql.IsolationLevel) error {
	// if we support the ReadCommit level, we should set the txnTime as cnLogTailTimestamp.
	t := maxTimeToCheckTxnTimestamp
	for {
		if t <= 0 {
			// XXX I'm not sure it is a good error info.
			return moerr.NewTxnError(ctx, "start txn failed due to txn timestamp. please retry.")
		}
		if txnTimeIsLegal(*txnTime) {
			return nil
		}
		time.Sleep(gapToCheckTxnTimestamp)
		t -= gapToCheckTxnTimestamp
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
var tableSubscribeRecord *sync.Map

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

var cnLogTailSubscriber *TableLogTailSubscriber

func initCnLogTailSubscriber(
	engine *Engine) error {
	var logTailClient *service.LogtailClient
	// clean the old subscriber if it's a reconnect action.
	if old := cnLogTailSubscriber; old != nil {
		_ = cnLogTailSubscriber.logTailClient.Close()
	}

	cluster, err := engine.getClusterDetails()
	if err != nil {
		return err
	}
	// XXX we assume that we have only 1 DN now.
	dnLogTailServerBackend := cluster.DNStores[0].LogtailServerAddress
	s, err := cnclient.GetStreamSender(dnLogTailServerBackend)
	if err != nil {
		return err
	}
	logTailClient, err = service.NewLogtailClient(s,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	if err != nil {
		return err
	}
	cnLogTailSubscriber = &TableLogTailSubscriber{
		logTailClient: logTailClient,
		streamSender:  s,
		engine:        engine,
	}
	cnLogTailSubscriber.startReceiveTableLogTail()
	return nil
}

func (logSub *TableLogTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	return logSub.logTailClient.Subscribe(ctx, tblId)
}

func (logSub *TableLogTailSubscriber) startReceiveTableLogTail() {
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
			initCnLogTailTimestamp()
			initTableSubscribeRecord()

			ch := make(chan response)
			for {
				deadLine, cf := context.WithTimeout(ctx, reconnectDeadTime)
				select {
				case <-deadLine.Done():
					// should log some information about log client restart.
					// and how to do the restart work is a question.
					cf()
					break
				case ch <- generateResponse(logSub.logTailClient.Receive()):
					cf()
				}
				resp := <-ch
				if resp.err == nil {
					// if we receive the response, update the partition and global timestamp.
					switch {
					case resp.r.GetSubscribeResponse() != nil:
						lt := resp.r.GetSubscribeResponse().GetLogtail()
						logTs := lt.GetTs()
						if err := updatePartition2(ctx, logSub.dnNodeID, logSub.engine, &lt); err != nil {

						}
						UpdateCnLogTimestamp(*logTs)

					case resp.r.GetUpdateResponse() != nil:
						logLists := resp.r.GetUpdateResponse().GetLogtailList()
						for _, l := range logLists {
							if err := updatePartition2(ctx, logSub.dnNodeID, logSub.engine, &l); err != nil {
								break
							}
							UpdateCnLogTimestamp(*l.Ts)
						}

					case resp.r.GetError() != nil:
					case resp.r.GetUnsubscribeResponse() != nil:
					}
				}
			}
		}
	}()
}

// TryToGetTableLogTail will check if table was subscribed or not.
// if yes, just return.
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
			if partition.ts.GreaterEq(txnTimestamp) {
				break
			}
			time.Sleep(gapToCheckIfLogTailReady)
			continue
		}
	}
	setTableSubscribe(dbId, tblId)
	return nil
}

func updatePartition2(
	ctx context.Context,
	dnId int,
	e *Engine, tl *logtail.TableLogtail) error {
	// get table info by table id
	dbId, tblId := tl.Table.GetDbId(), tl.Table.GetTbId()
	partition := e.db.getPartitions(dbId, tblId)[dnId]

	txnOp, _ := e.cli.New()
	_, _, rel, err := e.GetRelationById(ctx, txnOp, tblId)
	if err != nil {
		return err
	}

	tbl := rel.(*table)
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
