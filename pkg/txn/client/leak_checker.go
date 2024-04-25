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
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// leakChecker is used to detect leak txn which is not committed or aborted.
type leakChecker struct {
	sync.RWMutex
	logger         *log.MOLogger
	actives        []ActiveTxn
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
	lc.actives = append(lc.actives, ActiveTxn{
		Options:  options,
		ID:       txnID,
		CreateAt: time.Now(),
		txnOp:    txnOp,
	})
}

func (lc *leakChecker) txnClosed(txnID []byte) {
	lc.Lock()
	defer lc.Unlock()
	values := lc.actives[:0]
	for idx, txn := range lc.actives {
		if bytes.Equal(txn.ID, txnID) {
			values = append(values, lc.actives[idx+1:]...)
			break
		}
		values = append(values, txn)
	}
	lc.actives = values
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
	lc.RLock()
	defer lc.RUnlock()

	var values []ActiveTxn
	now := time.Now()
	for _, txn := range lc.actives {
		if now.Sub(txn.CreateAt) >= lc.maxActiveAges {
			if txn.txnOp != nil {
				txn.Options.Counter = txn.txnOp.counter()
				txn.Options.InRunSql = txn.txnOp.inRunSql()
				txn.Options.InCommit = txn.txnOp.inCommit()
				txn.Options.InRollback = txn.txnOp.inRollback()
				txn.Options.SessionInfo = txn.txnOp.options.SessionInfo
			}
			values = append(values, txn)
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
