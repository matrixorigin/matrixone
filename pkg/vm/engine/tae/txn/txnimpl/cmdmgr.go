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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
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

func (mgr *commandManager) ApplyTxnRecord(txn txnif.AsyncTxn) (logEntry entry.Entry, err error) {
	if mgr.driver == nil {
		return
	}
	t1 := time.Now()
	mgr.cmd.SetTxn(txn)
	var buf []byte
	if buf, err = mgr.cmd.MarshalBinary(); err != nil {
		return
	}
	if len(buf) > 10*mpool.MB {
		logutil.Info(
			"BIG-TXN",
			zap.Int("wal-size", len(buf)),
			zap.Uint64("lsn", mgr.lsn),
			zap.String("txn", txn.String()),
		)
	}
	logEntry = entry.GetBase()
	logEntry.SetType(IOET_WALEntry_TxnRecord)
	if err = logEntry.SetPayload(buf); err != nil {
		return
	}
	info := &entry.Info{
		Group: wal.GroupUserTxn,
	}
	logEntry.SetInfo(info)
	t2 := time.Now()
	mgr.lsn, err = mgr.driver.AppendEntry(wal.GroupUserTxn, logEntry)
	t3 := time.Now()
	if t3.Sub(t1) > time.Millisecond*500 {
		logutil.Warn(
			"SLOW-LOG",
			zap.String("txn", txn.String()),
			zap.Duration("make-log-entry-duration", t3.Sub(t1)),
		)
	}
	if t3.Sub(t2) > time.Millisecond*20 {
		logutil.Warn(
			"SLOW-LOG",
			zap.Duration("append-log-entry-duration", t3.Sub(t1)),
			zap.String("txn", txn.String()),
		)
	}
	logutil.Debugf("ApplyTxnRecord LSN=%d, Size=%d", mgr.lsn, len(buf))
	return
}
