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
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

// leakChecker is used to detect leak txn which is not committed or aborted.
type leakChecker struct {
	sync.RWMutex
	logger         *log.MOLogger
	actives        []activeTxn
	maxActiveAges  time.Duration
	leakHandleFunc func(txnID []byte, createAt time.Time, createBy string, options txn.TxnOptions)
	stopper        *stopper.Stopper
}

func newLeakCheck(
	maxActiveAges time.Duration,
	leakHandleFunc func(txnID []byte, createAt time.Time, createBy string, options txn.TxnOptions)) *leakChecker {
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
	createBy string) {
	if createBy == "" {
		createBy = "unknown"
	}

	lc.Lock()
	defer lc.Unlock()
	lc.actives = append(lc.actives, activeTxn{
		createBy: createBy,
		id:       txnID,
		createAt: time.Now(),
		txnOp:   txnOp,
		options: txnOp.options,
	})
}

func (lc *leakChecker) txnClosed(txnID []byte) {
	lc.Lock()
	defer lc.Unlock()
	values := lc.actives[:0]
	for idx, txn := range lc.actives {
		if bytes.Equal(txn.id, txnID) {
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
			lc.doCheck()
		}
	}
}

func (lc *leakChecker) doCheck() {
	lc.RLock()
	defer lc.RUnlock()

	now := time.Now()
	for _, txn := range lc.actives {
		if now.Sub(txn.createAt) >= lc.maxActiveAges {
			if txn.txnOp != nil {
				txn.options.Counter = txn.txnOp.counter()
				txn.options.InRunSql = txn.txnOp.inRunSql()
				txn.options.InCommit = txn.txnOp.inCommit()
				txn.options.InRollback = txn.txnOp.inRollback()
			}
			lc.leakHandleFunc(txn.id, txn.createAt, txn.createBy, txn.options)
		}
	}
}

type activeTxn struct {
	createBy string
	id       []byte
	createAt time.Time
	txnOp   *txnOperator
	options txn.TxnOptions
}
