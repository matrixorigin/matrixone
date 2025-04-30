// Copyright 2023 Matrix Origin
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

package client

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// leakChecker is used to detect leak txn which is not committed or aborted.
type leakChecker struct {
	sync.RWMutex
	logger         *log.MOLogger
	actives        map[string]ActiveTxn
	maxActiveAges  time.Duration
	leakHandleFunc func([]ActiveTxn)
	stopper        *stopper.Stopper
}

func newLeakCheck(
	maxActiveAges time.Duration,
	leakHandleFunc func([]ActiveTxn)) *leakChecker {
	logger := runtime.DefaultRuntime().Logger()
	return &leakChecker{
		logger:         logger,
		maxActiveAges:  maxActiveAges,
		leakHandleFunc: leakHandleFunc,
		stopper: stopper.NewStopper("txn-leak-checker",
			stopper.WithLogger(logger.RawLogger())),
		actives: make(map[string]ActiveTxn, 1000),
	}
}

func (lc *leakChecker) start() {
	if err := lc.stopper.RunTask(lc.check); err != nil {
		panic(err)
	}
}

func (lc *leakChecker) close() {
	lc.stopper.Stop()
}

func (lc *leakChecker) txnOpened(
	txnOp *txnOperator,
	txnID []byte,
	options txn.TxnOptions) {
	lc.Lock()
	defer lc.Unlock()
	lc.actives[util.UnsafeBytesToString(txnID)] = ActiveTxn{
		Options:  options,
		ID:       txnID,
		CreateAt: time.Now(),
		txnOp:    txnOp,
	}
}

func (lc *leakChecker) txnClosed(txnID []byte) {
	lc.Lock()
	defer lc.Unlock()
	delete(lc.actives, util.UnsafeBytesToString(txnID))
}

func (lc *leakChecker) check(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(lc.maxActiveAges):
			if values := lc.doCheck(); len(values) > 0 {
				lc.leakHandleFunc(values)
			}
		}
	}
}

func (lc *leakChecker) doCheck() []ActiveTxn {
	now := time.Now()
	var values []ActiveTxn
	lc.RLock()
	for _, txn := range lc.actives {
		if now.Sub(txn.CreateAt) >= lc.maxActiveAges {
			values = append(values, txn)
		}
	}
	lc.RUnlock()

	for _, txn := range values {
		if txn.txnOp != nil {
			txn.Options.Counter = txn.txnOp.counter()
			txn.Options.InRunSql = txn.txnOp.inRunSql()
			txn.Options.InCommit = txn.txnOp.inCommit()
			txn.Options.InRollback = txn.txnOp.inRollback()
			txn.Options.InIncrStmt = txn.txnOp.inIncrStmt()
			txn.Options.InRollbackStmt = txn.txnOp.inRollbackStmt()
			txn.Options.SessionInfo = txn.txnOp.opts.options.SessionInfo
		}
	}
	return values
}

type ActiveTxn struct {
	Options  txn.TxnOptions
	ID       []byte
	CreateAt time.Time
	txnOp    *txnOperator
}
