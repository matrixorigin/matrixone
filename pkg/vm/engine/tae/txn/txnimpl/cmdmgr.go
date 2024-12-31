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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type commandManager struct {
	cmd    *txnbase.TxnCmd
	lsn    uint64
	csn    uint32
	driver wal.Driver
}

func newCommandManager(driver wal.Driver, maxMessageSize uint64) *commandManager {
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
	info := &entry.Info{Group: wal.GroupPrepare}
	logEntry.SetInfo(info)

	// the wal ordered by the lsn is needed in the replay, so
	// this allocation cannot be done in the callback.
	mgr.lsn = mgr.driver.AllocateLSN(wal.GroupPrepare, logEntry)

	logEntry.RegisterGroupWalPreCallbacks(func() error {
		defer func() {
			store.DoneEvent(txnif.WalPreparing)
		}()

		var (
			err2 error
			buf  []byte
		)

		if buf, err2 = mgr.cmd.MarshalBinary(); err2 != nil {
			return err2
		}

		if len(buf) > 10*mpool.MB {
			logutil.Info(
				"BIG-TXN",
				zap.Int("wal-size", len(buf)),
				zap.Uint64("lsn", mgr.lsn),
				zap.String("txn", store.txn.String()),
			)
		}

		logEntry.SetType(IOET_WALEntry_TxnRecord)
		if err2 = logEntry.SetPayload(buf); err2 != nil {
			return err2
		}

		logutil.Debugf("Marshal Command LSN=%d, Size=%d", mgr.lsn, len(buf))

		return nil
	})

	err = mgr.driver.AppendEntry(logEntry)

	return
}
