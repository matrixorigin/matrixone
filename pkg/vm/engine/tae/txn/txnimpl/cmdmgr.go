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
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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

func newCommandManager(driver wal.Driver) *commandManager {
	return &commandManager{
		cmd:    txnbase.NewTxnCmd(),
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

func (mgr *commandManager) MakeLogIndex(csn uint32) *wal.Index {
	return &wal.Index{LSN: mgr.lsn, CSN: csn, Size: mgr.csn}
}

func (mgr *commandManager) ApplyTxnRecord(tid string, txn txnif.AsyncTxn) (logEntry entry.Entry, err error) {
	if mgr.driver == nil {
		return
	}
	mgr.cmd.SetCmdSize(mgr.csn)
	mgr.cmd.SetTxn(txn)
	var buf []byte
	if buf, err = mgr.cmd.Marshal(); err != nil {
		return
	}
	// logutil.Info("", common.OperationField("suxi-replay-cmd"),
	// common.OperandField(mgr.cmd.Desc()))
	logEntry = entry.GetBase()
	logEntry.SetType(ETTxnRecord)
	if err = logEntry.SetPayload(buf); err != nil {
		return
	}
	info := &entry.Info{
		Group: wal.GroupPrepare,
		TxnId: tid,
	}
	logEntry.SetInfo(info)
	mgr.lsn, err = mgr.driver.AppendEntry(wal.GroupPrepare, logEntry)
	logutil.Debugf("ApplyTxnRecord LSN=%d, Size=%d", mgr.lsn, len(buf))
	return
}
