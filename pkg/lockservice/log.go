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

package lockservice

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"go.uber.org/zap"
)

var (
	logger         *log.MOLogger
	loggerWithSkip *log.MOLogger
	once           sync.Once
)

func getLogger() *log.MOLogger {
	once.Do(initLoggers)
	return logger
}

func getWithSkipLogger() *log.MOLogger {
	once.Do(initLoggers)
	return loggerWithSkip
}

func initLoggers() {
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger = rt.Logger().Named("lockservice")
	loggerWithSkip = logger.WithOptions(zap.AddCallerSkip(1))
}

func logLocalLock(
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	opts LockOptions) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("try to lock on local",
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()))
	}
}

func logLocalLockRange(
	txn *activeTxn,
	tableID uint64,
	start, end []byte,
	mode pb.LockMode) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("try to lock range on local",
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("start", start),
			bytesField("end", end),
			zap.String("mode", mode.String()))
	}
}

func logLocalLockAdded(
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	opts LockOptions) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock added to local",
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()))
	}
}

func logLocalLockFailed(
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	options LockOptions,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to lock on local",
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", options.DebugString()),
			zap.Error(err))
	}
}

func logLocalLockWaitOn(
	txn *activeTxn,
	tableID uint64,
	w *waiter,
	key []byte,
	waitOn Lock) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		var waits [][]byte
		waitOn.waiters.iter(func(v *waiter) bool {
			waits = append(waits, v.txn.TxnID)
			return true
		})

		logger.Debug("lock wait on local",
			txnField(txn),
			zap.Uint64("table", tableID),
			zap.Stringer("waiter", w),
			bytesField("wait-on-key", key),
			zap.Stringer("wait-on", waitOn),
			bytesArrayField("wait-txn-list", waits))
	}
}

func logLocalLockWaitOnResult(
	txn *activeTxn,
	tableID uint64,
	key []byte,
	opts LockOptions,
	waiter *waiter,
	notify notifyValue) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock wait on local result",
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("wait-on-key", key),
			zap.String("opts", opts.DebugString()),
			zap.Stringer("waiter", waiter),
			zap.Stringer("result", notify))
	}
}

func logRemoteLock(
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock on remote",
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()))
	}
}

func logRemoteLockAdded(
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock added to remote",
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()))
	}
}

func logRemoteLockFailed(
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to lock on remote",
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()),
			zap.Error(err))
	}
}

func logTxnLockAdded(
	txn *activeTxn,
	rows [][]byte) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock added to txn",
			txnField(txn),
			bytesArrayField("rows", rows))
	}
}

func logLockUnlocked(
	txn *activeTxn,
	key []byte,
	lock Lock) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock unlocked",
			txnField(txn),
			bytesField("key", key),
			zap.Stringer("lock", lock))
	}
}

func logGetRemoteBindFailed(
	table uint64,
	err error) {
	getWithSkipLogger().Error("failed to get bind",
		zap.Uint64("table", table),
		zap.Error(err))
}

func logRemoteBindChanged(
	serviceID string,
	old, new pb.LockTable) {
	logger := getWithSkipLogger()
	logger.Info("bind changed",
		zap.String("service", serviceID),
		zap.String("old", old.DebugString()),
		zap.String("new", new.DebugString()))
}

func logLockTableCreated(
	serviceID string,
	bind pb.LockTable,
	remote bool) {
	logger := getWithSkipLogger()
	logger.Info("bind created",
		zap.String("service", serviceID),
		zap.Bool("remote", remote),
		zap.String("bind", bind.DebugString()))
}

func logLockTableClosed(
	bind pb.LockTable,
	remote bool) {
	logger := getWithSkipLogger()
	logger.Info("bind closed",
		zap.Bool("remote", remote),
		zap.String("bind", bind.DebugString()))
}

func logDeadLockFound(
	txn pb.WaitTxn,
	waiters *waiters) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("dead lock found",
			zap.String("txn", txn.DebugString()),
			waitTxnArrayField("wait-txn-list", waiters.waitTxns))
	}
}

func logAbortDeadLock(
	txn pb.WaitTxn,
	activeTxn *activeTxn) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("abort dead lock txn",
			zap.String("wait-txn", txn.DebugString()),
			txnField(activeTxn),
			waiterArrayField("blocked-waiters", activeTxn.blockedWaiters...))
	}
}

func logCheckDeadLockFailed(
	waitingTxn, txn pb.WaitTxn,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to check dead lock",
			zap.String("waiting-txn", waitingTxn.DebugString()),
			zap.String("txn", txn.DebugString()),
			zap.Error(err))
	}
}

func logKeepBindFailed(
	serviceID string,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to keep lock table bind",
			zap.String("serviceID", serviceID),
			zap.Error(err))
	}
}

func logKeepRemoteLocksFailed(
	bind pb.LockTable,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to keep remote locks",
			zap.String("bind", bind.DebugString()),
			zap.Error(err))
	}
}

func logPingFailed(
	serviceID string,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to ping lock service",
			zap.String("serviceID", serviceID),
			zap.Error(err))
	}
}

func logLocalBindsInvalid() {
	logger := getWithSkipLogger()
	logger.Error("all local lock table invalid")
}

func logUnlockTxn(
	serviceID string,
	txn *activeTxn) func() {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		return logger.DebugAction("unlock txn",
			txnField(txn))
	}
	return func() {}
}

func logTxnReadyToClose(
	serviceID string,
	txn *activeTxn) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		var tables []uint64
		for t := range txn.holdLocks {
			tables = append(tables, t)
		}

		logger.Debug("ready to unlock txn",
			txnField(txn),
			uint64ArrayField("tables", tables))
	}
}

func logTxnUnlockTable(
	serviceID string,
	txn *activeTxn,
	table uint64) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn unlock table",
			txnField(txn),
			zap.Uint64("table", table))
	}
}

func logTxnUnlockTableCompleted(
	serviceID string,
	txn *activeTxn,
	table uint64,
	cs *cowSlice) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		locks := cs.slice()
		defer locks.unref()
		logger.Debug("txn unlock table completed",
			txnField(txn),
			zap.Uint64("table", table),
			bytesArrayField("rows", locks.values[:locks.len()]))
	}
}

func logUnlockTableOnLocal(
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn unlock table on local",
			txnField(txn),
			zap.String("bind", bind.DebugString()))
	}
}

func logUnlockTableOnRemote(
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn unlock table on remote",
			txnField(txn),
			zap.String("bind", bind.DebugString()))
	}
}

func logUnlockTableOnRemoteFailed(
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn failed to unlock table on remote",
			txnField(txn),
			zap.String("bind", bind.DebugString()),
			zap.Error(err))
	}
}

func logWaitersAdded(
	holders *holders,
	added ...*waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		txns := make([][]byte, 0, len(holders.txns))
		for _, txn := range holders.txns {
			txns = append(txns, txn.TxnID)
		}

		logger.Debug("new waiters added",
			bytesArrayField("holders", txns),
			waiterArrayField("new-waiters", added...))
	}
}

func logWaiterGetNotify(
	w *waiter,
	v notifyValue) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter read notify",
			zap.Stringer("waiter", w),
			zap.Stringer("notify", v))
	}
}

func logWaiterNotified(
	w *waiter,
	v notifyValue) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter add notify",
			zap.Stringer("waiter", w),
			zap.Stringer("notify", v))
	}
}

func logWaiterNotifySkipped(
	w string,
	reason string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter notify skipped",
			zap.String("reason", reason),
			zap.String("waiter", w))
	}
}

func logWaiterStatusChanged(
	w *waiter,
	from, to waiterStatus) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter status changed",
			zap.Stringer("waiter", w),
			zap.Int("from-state", int(from)),
			zap.Int("to-state", int(to)))
	}
}

func logWaiterStatusUpdate(
	w *waiter,
	state waiterStatus) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter status set to new state",
			zap.Stringer("waiter", w),
			zap.Int("state", int(state)))
	}
}

func logWaiterContactPool(
	w *waiter,
	action string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter contact to pool",
			zap.String("action", action),
			zap.Stringer("waiter", w))
	}
}

func logTxnCreated(txn *activeTxn) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn created",
			txnField(txn))
	}
}

func txnField(txn *activeTxn) zap.Field {
	return zap.String("txn",
		fmt.Sprintf("%s(%s)",
			hex.EncodeToString(txn.txnID),
			txn.remoteService))
}

func bytesField(name string, value []byte) zap.Field {
	return zap.String(name, hex.EncodeToString(value))
}

func bytesArrayField(name string, values [][]byte) zap.Field {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for idx, row := range values {
		buffer.WriteString(fmt.Sprintf("%x", row))
		if idx != len(values)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("]")
	return zap.String(name, buffer.String())
}

func waitTxnArrayField(name string, values []pb.WaitTxn) zap.Field {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for idx, w := range values {
		buffer.WriteString(w.DebugString())
		if idx != len(values)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("]")
	return zap.String(name, buffer.String())
}

func uint64ArrayField(name string, values []uint64) zap.Field {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for idx, v := range values {
		buffer.WriteString(fmt.Sprintf("%d", v))
		if idx != len(values)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("]")
	return zap.String(name, buffer.String())
}

func waiterArrayField(name string, values ...*waiter) zap.Field {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for idx, v := range values {
		buffer.WriteString(v.String())
		if idx != len(values)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("]")
	return zap.String(name, buffer.String())
}
