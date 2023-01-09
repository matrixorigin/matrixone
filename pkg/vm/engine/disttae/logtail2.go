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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// gapToCheckTxnTimestamp is the time gap to check if txn timestamp is legal or not when a txn start.
	gapToCheckTxnTimestamp = 10 * time.Millisecond

	// maxSubscribeRequestPerSecond record how many client request we supported to subscribe table per second.
	maxSubscribeRequestPerSecond = 256

	// reconnectDeadTime if the time of losing response with dn reaches reconnectDeadTime, we will reconnect.
	reconnectDeadTime = 5 * time.Minute
)

// cnLogTailTimestamp each cn-node will hold one global log time.
// it's the last log time on all subscribed tables.
// the t is init to 0 when cn start or reconnect to dn.
var cnLogTailTimestamp struct {
	t     timestamp.Timestamp
	mutex sync.Mutex
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
	cnLogTailTimestamp.mutex.Lock()
	cnLogTailTimestamp.t = newTimestamp
	cnLogTailTimestamp.mutex.Unlock()
}

// WaitUntilTxnTimeIsLegal check if txnTime is legal periodically. and return if legal.
func WaitUntilTxnTimeIsLegal(txnTime *timestamp.Timestamp, level sql.IsolationLevel) {
	// if we support the ReadCommit level, we should set the txnTime as cnLogTailTimestamp.
	for {
		if txnTimeIsLegal(*txnTime) {
			return
		}
		time.Sleep(gapToCheckTxnTimestamp)
	}
}

func txnTimeIsLegal(txnTime timestamp.Timestamp) bool {
	cnLogTailTimestamp.mutex.Lock()
	b := txnTime.LessEq(cnLogTailTimestamp.t)
	cnLogTailTimestamp.mutex.Unlock()
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

func SetTableSubscribe(dbId, tblId uint64) {
	tableSubscribeRecord.Store(subscribeID{dbId, tblId}, true)
}

func GetTableSubscribe(dbId, tblId uint64) bool {
	_, b := tableSubscribeRecord.Load(subscribeID{dbId, tblId})
	return b
}

type TableLogTailSubscriber struct {
	dnNodeID      uint32
	logTailClient *service.LogtailClient
	streamSender  morpc.Stream
	// if dead time, we should reconnect to dn.
	deadTime timestamp.Timestamp
}

var cnLogTailSubscriber *TableLogTailSubscriber

func initCnLogTailSubscriber(dnLogTailServerBackend string) error {
	var logtailClient *service.LogtailClient
	// clean the old subscriber if it's a reconnect action.
	if old := cnLogTailSubscriber; old != nil {
		_ = cnLogTailSubscriber.logTailClient.Close()
	}
	s, err := cnclient.GetStreamSender(dnLogTailServerBackend)
	if err != nil {
		return err
	}
	logtailClient, err = service.NewLogtailClient(s,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	if err != nil {
		return err
	}
	cnLogTailSubscriber = &TableLogTailSubscriber{
		logTailClient: logtailClient,
		streamSender:  s,
	}
	return nil
}

func (logSub *TableLogTailSubscriber) SubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	return logSub.logTailClient.Subscribe(ctx, tblId)
}

func (logSub *TableLogTailSubscriber) StartReceiveTableLogTail(
	handleResponse func(tl *logtail.TableLogtail), // how to update the logtail.
) {
	// start a background routine to receive table log tail and update related structure.
	go func() {
		type response struct {
			r   *service.LogtailResponse
			err error
		}
		generateResponse := func(ltR *service.LogtailResponse, err error) response {
			return response{r: ltR, err: err}
		}

		ch := make(chan response)
		for {
			deadLine, cf := context.WithTimeout(context.TODO(), reconnectDeadTime)
			select {
			case <-deadLine.Done():
				initCnLogTailTimestamp()
				initTableSubscribeRecord()
				cf()
				return
			case ch <- generateResponse(logSub.logTailClient.Receive()):
				cf()
			}
			resp := <-ch
			if resp.err == nil {
				// if we receive the response, update the partition
				// and global timestamp.
				switch {
				case resp.r.GetError() != nil:
				case resp.r.GetSubscribeResponse() != nil:
					lt := resp.r.GetSubscribeResponse().GetLogtail()
					logTs := lt.GetTs()
					handleResponse(&lt)
					UpdateCnLogTimestamp(*logTs)
					SetTableSubscribe(lt.Table.DbId, lt.Table.TbId)
				case resp.r.GetUnsubscribeResponse() != nil:
				case resp.r.GetUpdateResponse() != nil:
					logLists := resp.r.GetUpdateResponse().GetLogtailList()
					for _, l := range logLists {
						handleResponse(&l)
						UpdateCnLogTimestamp(*l.Ts)
					}
				}
			}
		}
	}()
}
