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
	opts LockOptions,
	w *waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("try to lock on local",
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.Stringer("waiter", w))
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
	waiter *waiter,
	waitOn Lock) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock wait on local",
			txnField(txn),
			zap.Uint64("table", tableID),
			zap.Stringer("waiter", waiter),
			zap.Stringer("wait-on", waitOn))
	}
}

func logLocalLockWaitOnResult(
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
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
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.Stringer("waiter", waiter),
			zap.Any("result", err))
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

func logGetRemoteBindFailed(
	table uint64,
	err error) {
	getWithSkipLogger().Error("failed to get bind",
		zap.Uint64("table", table),
		zap.Error(err))
}

func logRemoteBindChanged(
	old, new pb.LockTable) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("remote bind changed",
			zap.String("old", old.DebugString()),
			zap.String("new", new.DebugString()))
	}
}

func logLockTableCreated(bind pb.LockTable, remote bool) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock table created",
			zap.Bool("remote", remote),
			zap.String("bind", bind.DebugString()))
	}
}

func logLockTableClosed(bind pb.LockTable, remote bool) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("lock table closed",
			zap.Bool("remote", remote),
			zap.String("bind", bind.DebugString()))
	}
}

func logDeadLockFound(
	txnID []byte,
	waiters *waiters) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("dead lock found",
			bytesField("txn-id", txnID),
			bytesArrayField("wait-txn-list", waiters.waitTxns))
	}
}

func logCheckDeadLockFailed(
	waitingTxnID []byte,
	txnID []byte,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to check dead lock",
			bytesField("waiting-txn-id", waitingTxnID),
			bytesField("txn-id", txnID),
			zap.Error(err))
	}
}

func logKeepBindFailed(err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to keep lock table bind",
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

func logLocalBindsInvalid() {
	logger := getWithSkipLogger()
	logger.Error("all local lock table invalid")
}

func logUnlockTxn(txn *activeTxn) func() {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		return logger.DebugAction("unlock txn",
			txnField(txn))
	}
	return func() {}
}

func logTxnReadyToClose(
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
	w *waiter,
	added ...*waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {

		logger.Debug("new waiters added",
			zap.Stringer("holder", w),
			waiterArrayField("new-waiters", added...))
	}
}

func logWaiterGetNotify(
	w *waiter,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter read notify",
			zap.Stringer("waiter", w),
			zap.Any("notify", err))
	}
}

func logWaiterNotified(
	w *waiter,
	err error) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter add notify",
			zap.Stringer("waiter", w),
			zap.Any("notify", err))
	}
}

func logWaiterNotifySkipped(
	w *waiter,
	reason string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter notify skipped",
			zap.String("reason", reason),
			zap.Stringer("waiter", w))
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

func logWaiterClose(
	w *waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter close",
			zap.Stringer("waiter", w))
	}
}

func logWaiterFetchNextWaiter(
	w *waiter,
	next *waiter) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("notify next waiter",
			zap.Stringer("waiter", w),
			zap.Stringer("next-waiter", next))
	}
}

func logWaiterClearNotify(
	w *waiter,
	reason string) {
	logger := getWithSkipLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("waiter clear notify",
			zap.String("reason", reason),
			zap.Stringer("waiter", w))
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
