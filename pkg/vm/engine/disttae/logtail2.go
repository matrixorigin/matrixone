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
	"database/sql"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// time gap to check if txn timestamp is legal or not when a txn start.
	gapToCheckTxnTimestamp = 10 * time.Millisecond
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

func initTableSubscribeRecord() {
	newM := &sync.Map{}
	atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(tableSubscribeRecord)), unsafe.Pointer(newM))
}

func SetTableSubscribe(tblId uint64) {
	tableSubscribeRecord.Store(tblId, true)
}

func GetTableSubscribe(tblId uint64) bool {
	_, b := tableSubscribeRecord.Load(tblId)
	return b
}
