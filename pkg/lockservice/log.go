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
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	opts LockOptions) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("try to lock on local",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()))
	}
}

func logLocalLockRow(
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	row []byte,
	mode pb.LockMode) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("try to lock row on local",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("row", row),
			zap.String("mode", mode.String()))
	}
}

func logLocalLockRange(
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	start, end []byte,
	mode pb.LockMode) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("try to lock range on local",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("start", start),
			bytesField("end", end),
			zap.String("mode", mode.String()))
	}
}

func logLocalLockAdded(
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	opts LockOptions) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock added to local",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()))
	}
}

func logLocalLockFailed(
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	options LockOptions,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to lock on local",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", options.DebugString()),
			zap.Error(err))
	}
}

func logLocalLockWaitOn(
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	waiter *waiter,
	key []byte,
	waitOn Lock) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		var waits [][]byte
		waitOn.waiter.waiters.iter(func(b []byte) bool {
			waits = append(waits, b)
			return true
		})

		logger.Debug("lock wait on local",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			zap.Stringer("waiter", waiter),
			bytesField("wait-on-key", key),
			zap.Stringer("wait-on", waitOn),
			bytesArrayField("wait-txn-list", waits))
	}
}

func logLocalLockWaitOnResult(
	serviceID string,
	txn *activeTxn,
	tableID uint64,
	key []byte,
	opts LockOptions,
	waiter *waiter,
	err error) {
	logger := getWithSkipLogger()
	level := zap.DebugLevel
	fn := logger.Debug
	if err != nil {
		level = zap.ErrorLevel
		fn = logger.Error
	}
	if logger.Enabled(level) {
		fn("lock wait on local result",
			serviceIDField(serviceID),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("wait-on-key", key),
			zap.String("opts", opts.DebugString()),
			zap.Stringer("waiter", waiter),
			zap.Any("result", err))
	}
}

func logRemoteLock(
	serviceID string,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock on remote",
			serviceIDField(serviceID),
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()))
	}
}

func logRemoteLockAdded(
	serviceID string,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock added to remote",
			serviceIDField(serviceID),
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()))
	}
}

func logRemoteLockFailed(
	serviceID string,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to lock on remote",
			serviceIDField(serviceID),
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()),
			zap.Error(err))
	}
}

func logTxnLockAdded(
	serviceID string,
	txn *activeTxn,
	rows [][]byte) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock added to txn",
			serviceIDField(serviceID),
			txnField(txn),
			bytesArrayField("rows", rows))
	}
}

func logGetRemoteBindFailed(
	serviceID string,
	table uint64,
	err error) {
	getWithSkipLogger().Error("failed to get bind",
		serviceIDField(serviceID),
		zap.Uint64("table", table),
		zap.Error(err))
}

func logRemoteBindChanged(
	serviceID string,
	old, new pb.LockTable) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("remote bind changed",
			serviceIDField(serviceID),
			zap.String("old", old.DebugString()),
			zap.String("new", new.DebugString()))
	}
}

func logLockTableCreated(
	serviceID string,
	bind pb.LockTable,
	remote bool) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock table created",
			serviceIDField(serviceID),
			zap.Bool("remote", remote),
			zap.String("bind", bind.DebugString()))
	}
}

func logLockTableClosed(
	serviceID string,
	bind pb.LockTable,
	remote bool) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock table closed",
			serviceIDField(serviceID),
			zap.Bool("remote", remote),
			zap.String("bind", bind.DebugString()))
	}
}

func logDeadLockFound(
	serviceID string,
	txn pb.WaitTxn,
	waiters *waiters) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("dead lock found",
			serviceIDField(serviceID),
			zap.String("txn", txn.DebugString()),
			waitTxnArrayField("wait-txn-list", waiters.waitTxns))
	}
}

func logCheckDeadLockFailed(
	serviceID string,
	waitingTxn, txn pb.WaitTxn,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to check dead lock",
			serviceIDField(serviceID),
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
			serviceIDField(serviceID),
			zap.Error(err))
	}
}

func logKeepRemoteLocksFailed(
	serviceID string,
	bind pb.LockTable,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to keep remote locks",
			serviceIDField(serviceID),
			zap.String("bind", bind.DebugString()),
			zap.Error(err))
	}
}

func logLocalBindsInvalid(serviceID string) {
	logger := getWithSkipLogger()
	logger.Error("all local lock table invalid",
		serviceIDField(serviceID))
}

func logUnlockTxn(
	serviceID string,
	txn *activeTxn) func() {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		return logger.DebugAction("unlock txn",
			serviceIDField(serviceID),
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
			serviceIDField(serviceID),
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
			serviceIDField(serviceID),
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
			serviceIDField(serviceID),
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
			serviceIDField(serviceID),
			txnField(txn),
			zap.String("bind", bind.DebugString()))
	}
}

func logUnlockTableKeyOnLocal(
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable,
	key []byte,
	lock Lock,
	next *waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn unlock table key on local",
			serviceIDField(serviceID),
			txnField(txn),
			bytesField("key", key),
			zap.Stringer("next", next),
			zap.Stringer("lock", lock),
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
			serviceIDField(serviceID),
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
			serviceIDField(serviceID),
			txnField(txn),
			zap.String("bind", bind.DebugString()),
			zap.Error(err))
	}
}

func logWaitersAdded(
	serviceID string,
	w *waiter,
	added ...*waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("new waiters added",
			serviceIDField(serviceID),
			zap.Stringer("holder", w),
			waiterArrayField("new-waiters", added...))
	}
}

func logWaiterGetNotify(
	serviceID string,
	w *waiter,
	v notifyValue) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter read notify",
			serviceIDField(serviceID),
			zap.Stringer("waiter", w),
			zap.Any("notify", v))
	}
}

func logWaiterNotified(
	serviceID string,
	w *waiter,
	v notifyValue) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter add notify",
			serviceIDField(serviceID),
			zap.Stringer("waiter", w),
			zap.Any("notify", v))
	}
}

func logWaiterNotifySkipped(
	serviceID string,
	w *waiter,
	reason string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter notify skipped",
			serviceIDField(serviceID),
			zap.String("reason", reason),
			zap.Stringer("waiter", w))
	}
}

func logWaiterStatusChanged(
	serviceID string,
	w *waiter,
	from, to waiterStatus) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter status changed",
			serviceIDField(serviceID),
			zap.Stringer("waiter", w),
			zap.Int("from-state", int(from)),
			zap.Int("to-state", int(to)))
	}
}

func logWaiterStatusUpdate(
	serviceID string,
	w *waiter,
	state waiterStatus) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter status set to new state",
			serviceIDField(serviceID),
			zap.Stringer("waiter", w),
			zap.Int("state", int(state)))
	}
}

func logWaiterContactPool(
	serviceID string,
	w *waiter,
	action string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter contact to pool",
			serviceIDField(serviceID),
			zap.String("action", action),
			zap.Stringer("waiter", w))
	}
}

func logWaiterClose(
	serviceID string,
	w *waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter close",
			serviceIDField(serviceID),
			zap.Stringer("waiter", w))
	}
}

func logWaiterFetchNextWaiter(
	serviceID string,
	w *waiter,
	next *waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("notify next waiter",
			serviceIDField(serviceID),
			zap.Stringer("waiter", w),
			zap.Stringer("next-waiter", next))
	}
}

func logWaiterClearNotify(
	serviceID string,
	w *waiter,
	reason string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter clear notify",
			serviceIDField(serviceID),
			zap.String("reason", reason),
			zap.Stringer("waiter", w))
	}
}

func logTxnCreated(
	serviceID string,
	txn *activeTxn) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn created",
			serviceIDField(serviceID),
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
		buffer.WriteString(hex.EncodeToString(row))
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

func serviceIDField(sid string) zap.Field {
	return zap.String("service", sid)
}
