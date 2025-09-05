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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

type commandManager struct {
	cmd    *txnbase.TxnCmd
	lsn    uint64
	csn    uint32
	driver wal.Store
}

func newCommandManager(driver wal.Store, maxMessageSize uint64) *commandManager {
	return &commandManager{
		cmd:    txnbase.NewTxnCmd(maxMessageSize),
		driver: driver,
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

func (mgr *commandManager) ApplyTxnRecord(store *txnStore) (logEntry entry.Entry, err error) {
	if mgr.driver == nil {
		return
	}

	mgr.cmd.SetTxn(store.txn)

	store.AddEvent(txnif.WalPreparing)

	logEntry = entry.GetBase()
	info := &entry.Info{Group: wal.GroupUserTxn}
	logEntry.SetInfo(info)
	logEntry.SetApproxPayloadSize(mgr.cmd.ApproxSize())

	logEntry.RegisterGroupWalPreCallbacks(func() error {
		defer func() {
			store.DoneEvent(txnif.WalPreparing)
		}()

		var (
			err2 error
			buf  []byte
			t1   = time.Now()
		)

		if buf, err2 = mgr.cmd.MarshalBinary(); err2 != nil {
			return err2
		}

		logEntry.SetType(IOET_WALEntry_TxnRecord)
		if err2 = logEntry.SetPayload(buf); err2 != nil {
			return err2
		}

		logutil.Debugf("Marshal Command LSN=%d, Size=%d", info.GroupLSN, len(buf))

		if len(buf) > 10*mpool.MB {
			logutil.Info(
				"BIG-TXN",
				zap.Int("wal-size", len(buf)),
				zap.Uint64("lsn", info.GroupLSN),
				zap.String("txn", store.txn.String()),
			)
		}

		if dur := time.Since(t1); dur >= time.Millisecond*500 {
			logutil.Warn(
				"SlOW-LOG",
				zap.Int("wal-size", len(buf)),
				zap.Uint64("lsn", mgr.lsn),
				zap.String("txn", store.txn.String()),
				zap.Duration("marshal-log-entry-duration", dur),
			)
		}

		return nil
	})

	t2 := time.Now()
	mgr.lsn, err = mgr.driver.AppendEntry(wal.GroupUserTxn, logEntry)
	if dur := time.Since(t2); dur >= time.Millisecond*20 {
		logutil.Warn(
			"SlOW-LOG",
			zap.Uint64("lsn", mgr.lsn),
			zap.String("txn", store.txn.String()),
			zap.Duration("append-log-entry-duration", dur),
		)
	}

	return
}
