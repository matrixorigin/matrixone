// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type commandManager struct {
	cmd           *txnbase.TxnCmd
	lsn           uint64
	csn           uint32
	driver        wal.Store
	maxWalSize    int
	preparedEntry entry.Entry
	preparedSize  int
	walBudget     *semaphore.Weighted
}

// Retain at most 4 KiB of serialized data per queued transaction. Large
// records are encoded again when they enter the WAL stage.
const maxRetainedPreflightRecordSize = 4 * 1024

// The append queues admit by count. Bound their additional encoded WAL payload
// while allowing many small records to be in flight.
const maxOutstandingEncodedWALBytes = 4 * MaxWalSize

var encodedWALBudget = semaphore.NewWeighted(maxOutstandingEncodedWALBytes)

type largeWALRecordEntry struct {
	entry.Entry
	budget *semaphore.Weighted
	size   int64
}

func (e *largeWALRecordEntry) Free() {
	e.Entry.Free()
	e.budget.Release(e.size)
}

func newCommandManager(driver wal.Store) *commandManager {
	return &commandManager{
		cmd:        txnbase.NewTxnCmd(),
		driver:     driver,
		maxWalSize: MaxWalSize,
		walBudget:  encodedWALBudget,
	}
}

func (mgr *commandManager) GetCSN() uint32 {
	return mgr.csn
}

func (mgr *commandManager) AddInternalCmd(cmd txnif.TxnCmd) {
	mgr.cmd.AddCmd(cmd)
}

func (mgr *commandManager) AddCmd(cmd txnif.TxnCmd) {
	mgr.cmd.AddCmd(cmd)
	mgr.csn++
}

func (mgr *commandManager) discardPreparedRecord() {
	if mgr.preparedEntry != nil {
		mgr.preparedEntry.Free()
		mgr.preparedEntry = nil
		mgr.preparedSize = 0
	}
}

func (mgr *commandManager) PreflightTxnRecord(store *txnStore) (err error) {
	if mgr.driver == nil || mgr.preparedEntry != nil {
		return nil
	}
	logEntry, walSize, err := mgr.marshalTxnRecord(store)
	if err != nil {
		return err
	}
	if walSize > maxRetainedPreflightRecordSize {
		logEntry.Free()
	} else {
		mgr.preparedEntry = logEntry
		mgr.preparedSize = walSize
	}
	return nil
}

func (mgr *commandManager) marshalTxnRecord(store *txnStore) (logEntry entry.Entry, walSize int, err error) {
	mgr.cmd.SetTxn(store.txn)

	t1 := time.Now()
	buf, err := mgr.cmd.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}
	if len(buf) > mgr.maxWalSize {
		return nil, 0, wal.NewRecordTooLargeError(len(buf), mgr.maxWalSize)
	}

	logEntry = entry.GetBase()
	info := &entry.Info{Group: wal.GroupUserTxn}
	logEntry.SetInfo(info)
	logEntry.SetApproxPayloadSize(int64(len(buf)))
	logEntry.SetType(IOET_WALEntry_TxnRecord)
	if err = logEntry.SetPayload(buf); err != nil {
		logEntry.Free()
		return nil, 0, err
	}
	infoBuf, err := info.Marshal()
	if err != nil {
		logEntry.Free()
		return nil, 0, err
	}
	logEntry.SetInfoBuf(infoBuf)
	if validator, ok := mgr.driver.(interface{ ValidateUserTxnRecordSize(int) error }); ok {
		// driver/entry.Entry adds the 8-byte DSN to the complete base entry,
		// including the fixed-size Info buffer materialized above.
		if err = validator.ValidateUserTxnRecordSize(logEntry.TotalSize() + 8); err != nil {
			logEntry.Free()
			return nil, 0, err
		}
	}
	logutil.Debugf("Marshal Command Size=%d", len(buf))
	if len(buf) > 10*mpool.MB {
		logutil.Info("BIG-TXN", zap.Int("wal-size", len(buf)), zap.String("txn", store.txn.String()))
	}
	if dur := time.Since(t1); dur >= time.Millisecond*500 {
		logutil.Warn("SlOW-LOG", zap.Int("wal-size", len(buf)),
			zap.String("txn", store.txn.String()), zap.Duration("marshal-log-entry-duration", dur))
	}
	return logEntry, len(buf), nil
}

func (mgr *commandManager) ApplyTxnRecord(store *txnStore) (logEntry entry.Entry, err error) {
	if mgr.driver == nil {
		return
	}
	var walSize int
	if mgr.preparedEntry != nil {
		logEntry = mgr.preparedEntry
		walSize = mgr.preparedSize
		mgr.preparedEntry = nil
		mgr.preparedSize = 0
	} else {
		// Reserve the maximum before serialization, then retain only the
		// actual encoded payload size until the entry is freed after WAL.
		if err = mgr.walBudget.Acquire(context.Background(), int64(mgr.maxWalSize)); err != nil {
			return nil, err
		}
		if logEntry, walSize, err = mgr.marshalTxnRecord(store); err != nil {
			mgr.walBudget.Release(int64(mgr.maxWalSize))
			return nil, err
		}
		if walSize > maxRetainedPreflightRecordSize {
			mgr.walBudget.Release(int64(mgr.maxWalSize - walSize))
			logEntry = &largeWALRecordEntry{Entry: logEntry, budget: mgr.walBudget, size: int64(walSize)}
		} else {
			mgr.walBudget.Release(int64(mgr.maxWalSize))
		}
		mgr.preparedSize = 0
	}

	t2 := time.Now()
	mgr.lsn, err = mgr.driver.AppendEntry(wal.GroupUserTxn, logEntry)
	if err != nil {
		logEntry.Free()
		logEntry = nil
	}
	if dur := time.Since(t2); dur >= time.Millisecond*20 {
		logutil.Warn(
			"SlOW-LOG",
			zap.Int("wal-size", walSize),
			zap.Uint64("lsn", mgr.lsn),
			zap.String("txn", store.txn.String()),
			zap.Duration("append-log-entry-duration", dur),
		)
	}

	return
}
