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
	"github.com/matrixorigin/matrixone/pkg/common/util"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func getLogger(sid string) *log.MOLogger {
	return runtime.ServiceRuntime(sid).Logger().Named("lockservice")
}

func logLocalLock(
	logger *log.MOLogger,
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	opts LockOptions,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"try to lock on local",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
		)
	}
}

func logLocalLockRange(
	logger *log.MOLogger,
	txn *activeTxn,
	tableID uint64,
	start, end []byte,
	mode pb.LockMode,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"try to lock range on local",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("start", start),
			bytesField("end", end),
			zap.String("mode", mode.String()),
		)
	}
}

func logLocalLockAdded(
	logger *log.MOLogger,
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	opts LockOptions,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"lock added to local",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
		)
	}
}

func logHolderAdded(
	logger *log.MOLogger,
	c *lockContext,
	lock Lock,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		var waits [][]byte
		lock.waiters.iter(func(v *waiter) bool {
			waits = append(waits, v.txn.TxnID)
			return true
		})

		logger.Log(
			"holder added",
			getLogOptions(zap.DebugLevel),
			txnField(c.txn),
			zap.Uint64("table", c.result.LockedOn.Table),
			bytesArrayField("rows", c.rows),
			zap.String("opts", c.opts.DebugString()),
			waitTxnArrayField("holders", lock.holders.txns),
			bytesArrayField("waiters", waits),
		)
	}
}

func logLocalLockFailed(
	logger *log.MOLogger,
	txn *activeTxn,
	tableID uint64,
	rows [][]byte,
	options LockOptions,
	err error,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.ErrorLevel) {
		logger.Log(
			"failed to lock on local",
			getLogOptions(zap.ErrorLevel),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesArrayField("rows", rows),
			zap.String("opts", options.DebugString()),
			zap.Error(err),
		)
	}
}

func logLocalLockWaitOn(
	logger *log.MOLogger,
	txn *activeTxn,
	tableID uint64,
	w *waiter,
	key []byte,
	waitOn Lock,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		var waits [][]byte
		waitOn.waiters.iter(func(v *waiter) bool {
			waits = append(waits, v.txn.TxnID)
			return true
		})

		logger.Log(
			"lock wait on local",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", tableID),
			zap.Stringer("waiter", w),
			bytesField("wait-on-key", key),
			zap.Stringer("wait-on", waitOn),
			bytesArrayField("wait-txn-list", waits),
		)
	}
}

func logLocalLockWaitOnResult(
	logger *log.MOLogger,
	txn *activeTxn,
	tableID uint64,
	key []byte,
	opts LockOptions,
	waiter *waiter,
	notify notifyValue,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"lock wait on local result",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", tableID),
			bytesField("wait-on-key", key),
			zap.String("opts", opts.DebugString()),
			zap.Stringer("waiter", waiter),
			zap.Stringer("result", notify),
		)
	}
}

func logRemoteLock(
	logger *log.MOLogger,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"lock on remote",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()),
		)
	}
}

func logRemoteLockAdded(
	logger *log.MOLogger,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"lock added to remote",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			bytesArrayField("rows", rows),
			zap.String("opts", opts.DebugString()),
			zap.String("remote", remote.DebugString()),
		)
	}
}

func logRemoteLockFailed(
	logger *log.MOLogger,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	remote pb.LockTable,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"failed to lock on remote",
		getLogOptions(zap.ErrorLevel),
		txnField(txn),
		bytesArrayField("rows", rows),
		zap.String("opts", opts.DebugString()),
		zap.String("remote", remote.DebugString()),
		zap.Error(err),
	)
}

func logTxnLockAdded(
	logger *log.MOLogger,
	txn *activeTxn,
	rows [][]byte,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"lock added to txn",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			bytesArrayField("rows", rows),
		)
	}
}

func logLockUnlocked(
	logger *log.MOLogger,
	txn *activeTxn,
	key []byte,
	lock Lock,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"lock unlocked",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			bytesField("key", key),
			zap.Stringer("lock", lock),
		)
	}
}

func logGetRemoteBindFailed(
	logger *log.MOLogger,
	table uint64,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"failed to get bind",
		getLogOptions(zap.ErrorLevel),
		zap.Uint64("table", table),
		zap.Error(err),
	)
}

func logRemoteBindChanged(
	logger *log.MOLogger,
	serviceID string,
	old, new pb.LockTable,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"bind changed",
		getLogOptions(zap.InfoLevel),
		zap.String("service", serviceID),
		zap.String("old", old.DebugString()),
		zap.String("new", new.DebugString()),
	)
}

func logLockTableCreated(
	logger *log.MOLogger,
	serviceID string,
	bind pb.LockTable,
	remote bool,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"bind created",
		getLogOptions(zap.InfoLevel),
		zap.String("service", serviceID),
		zap.Bool("remote", remote),
		zap.String("bind", bind.DebugString()),
	)
}

func logLockTableClosed(
	logger *log.MOLogger,
	bind pb.LockTable,
	remote bool,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"bind closed",
		getLogOptions(zap.InfoLevel),
		zap.Bool("remote", remote),
		zap.String("bind", bind.DebugString()),
	)
}

func logDeadLockFound(
	logger *log.MOLogger,
	txn pb.WaitTxn,
	waiters *waiters,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log("dead lock found",
			getLogOptions(zap.DebugLevel),
			zap.String("txn", txn.DebugString()),
			waitTxnArrayField("wait-txn-list", waiters.waitTxns),
		)
	}
}

func logAbortDeadLock(
	logger *log.MOLogger,
	txn pb.WaitTxn,
	activeTxn *activeTxn,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"abort dead lock txn",
			getLogOptions(zap.DebugLevel),
			zap.String("wait-txn", txn.DebugString()),
			txnField(activeTxn),
			waiterArrayField("blocked-waiters", activeTxn.blockedWaiters...),
		)
	}
}

func logLockServiceStartSucc(
	logger *log.MOLogger,
	serviceID string,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"lock service start successfully",
		getLogOptions(zap.InfoLevel),
		zap.String("serviceID", serviceID),
	)
}

func logLockAllocatorStartSucc(
	logger *log.MOLogger,
	version uint64,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"lock allocator start successfully",
		getLogOptions(zap.InfoLevel),
		zap.Uint64("version", version),
	)
}

func logCheckDeadLockFailed(
	logger *log.MOLogger,
	waitingTxn, txn pb.WaitTxn,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"failed to check dead lock",
		getLogOptions(zap.ErrorLevel),
		zap.String("waiting-txn", waitingTxn.DebugString()),
		zap.String("txn", txn.DebugString()),
		zap.Error(err),
	)
}

func logKeepBindFailed(
	logger *log.MOLogger,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"failed to keep lock table bind",
		getLogOptions(zap.ErrorLevel),
		zap.Error(err),
	)
}

func logKeepRemoteLocksFailed(
	logger *log.MOLogger,
	bind pb.LockTable,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"failed to keep remote locks",
		getLogOptions(zap.ErrorLevel),
		zap.String("bind", bind.DebugString()),
		zap.Error(err),
	)
}

func logPingFailed(
	logger *log.MOLogger,
	serviceID string,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"failed to ping lock service",
		getLogOptions(zap.ErrorLevel),
		zap.String("serviceID", serviceID),
		zap.Error(err))
}

func logCanLockOnService(
	logger *log.MOLogger,
	serviceID string,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"if lock on service",
		getLogOptions(zap.InfoLevel),
		zap.String("serviceID", serviceID),
	)
}

func logLocalBindsInvalid(
	logger *log.MOLogger,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"all local lock table invalid",
		getLogOptions(zap.ErrorLevel),
	)
}

func logUnlockTxn(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
) func() {
	if logger == nil {
		return func() {}
	}

	if logger.Enabled(zap.DebugLevel) {
		return logger.LogAction(
			"unlock txn",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
		)
	}
	return func() {}
}

func logTxnReadyToClose(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"ready to unlock txn",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
		)
	}
}

func logTxnUnlockTable(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
	table uint64,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn unlock table",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", table),
		)
	}
}

func logTxnUnlockTableCompleted(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
	table uint64,
	cs *cowSlice,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		locks := cs.slice()
		defer locks.unref()
		logger.Log(
			"txn unlock table completed",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.Uint64("table", table),
			bytesArrayField("rows", locks.values[:locks.len()]),
		)
	}
}

func logUnlockTableOnLocal(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn unlock table on local",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.String("bind", bind.DebugString()),
		)
	}
}

func logUnlockTableOnRemote(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn unlock table on remote",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
			zap.String("bind", bind.DebugString()),
		)
	}
}

func logUnlockTableOnRemoteFailed(
	logger *log.MOLogger,
	serviceID string,
	txn *activeTxn,
	bind pb.LockTable,
	err error,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"txn failed to unlock table on remote",
		getLogOptions(zap.ErrorLevel),
		txnField(txn),
		zap.String("bind", bind.DebugString()),
		zap.Error(err),
	)
}

func logWaitersAdded(
	logger *log.MOLogger,
	holders *holders,
	added ...*waiter,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		txns := make([][]byte, 0, len(holders.txns))
		for _, txn := range holders.txns {
			txns = append(txns, txn.TxnID)
		}

		logger.Log(
			"new waiters added",
			getLogOptions(zap.DebugLevel),
			bytesArrayField("holders", txns),
			waiterArrayField("new-waiters", added...),
		)
	}
}

func logBindsMove(
	logger *log.MOLogger,
	binds []pb.LockTable,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"binds move",
		getLogOptions(zap.InfoLevel),
		bindsArrayField("binds", binds),
	)
}

func logStatus(
	logger *log.MOLogger,
	status pb.Status,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"service status",
		getLogOptions(zap.InfoLevel),
		zap.String("status", status.String()),
	)
}

func logCleanCannotCommitTxn(
	logger *log.MOLogger,
	txnID string,
	state int,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"clean cannot commit txn",
		getLogOptions(zap.InfoLevel),
		zap.String("txnID", hex.EncodeToString(util.UnsafeStringToBytes(txnID))),
		zap.Int("state", state),
	)
}

func logServiceStatus(
	logger *log.MOLogger,
	info string,
	serviceID string,
	status pb.Status,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"service status",
		getLogOptions(zap.InfoLevel),
		zap.String("info", info),
		zap.String("serviceID", serviceID),
		zap.String("status", status.String()),
	)
}

func logStatusChange(
	logger *log.MOLogger,
	from pb.Status,
	to pb.Status,
) {
	if logger == nil {
		return
	}

	logger.Log(
		"service status change",
		getLogOptions(zap.InfoLevel),
		zap.String("from", from.String()),
		zap.String("to", to.String()),
	)
}

func logWaiterGetNotify(
	logger *log.MOLogger,
	w *waiter,
	v notifyValue,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"waiter read notify",
			getLogOptions(zap.DebugLevel),
			zap.Stringer("waiter", w),
			zap.Stringer("notify", v),
		)
	}
}

func logWaiterNotified(
	logger *log.MOLogger,
	w *waiter,
	v notifyValue,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"waiter add notify",
			getLogOptions(zap.DebugLevel),
			zap.Stringer("waiter", w),
			zap.Stringer("notify", v),
		)
	}
}

func logWaiterNotifySkipped(
	logger *log.MOLogger,
	w string,
	reason string,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"waiter notify skipped",
			getLogOptions(zap.DebugLevel),
			zap.String("reason", reason),
			zap.String("waiter", w),
		)
	}
}

func logWaiterStatusChanged(
	logger *log.MOLogger,
	w *waiter,
	from, to waiterStatus,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"waiter status changed",
			getLogOptions(zap.DebugLevel),
			zap.Stringer("waiter", w),
			zap.Int("from-state", int(from)),
			zap.Int("to-state", int(to)),
		)
	}
}

func logTxnCreated(
	logger *log.MOLogger,
	txn *activeTxn,
) {
	if logger == nil {
		return
	}

	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn created",
			getLogOptions(zap.DebugLevel),
			txnField(txn),
		)
	}
}

func txnField(txn *activeTxn) zap.Field {
	if txn == nil {
		return zap.String("txn", "nil")
	}
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

func bindsArrayField(name string, values []pb.LockTable) zap.Field {
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

func getLogOptions(
	level zapcore.Level,
) log.LogOptions {
	return log.DefaultLogOptions().AddCallerSkip(1).WithLevel(level)
}
