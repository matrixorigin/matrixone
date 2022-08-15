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

package updates

import (
	"encoding/binary"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type AppendNode struct {
	sync.RWMutex
	commitTs types.TS
	txn      txnif.AsyncTxn
	logIndex *wal.Index
	startRow uint32
	maxRow   uint32
	mvcc     *MVCCHandle
	id       *common.ID
}

func MockAppendNode(ts types.TS, startRow, maxRow uint32, mvcc *MVCCHandle) *AppendNode {
	return &AppendNode{
		commitTs: ts,
		maxRow:   maxRow,
		mvcc:     mvcc,
	}
}

func NewCommittedAppendNode(
	ts types.TS,
	startRow, maxRow uint32,
	mvcc *MVCCHandle) *AppendNode {
	return &AppendNode{
		commitTs: ts,
		startRow: startRow,
		maxRow:   maxRow,
		mvcc:     mvcc,
	}
}

func NewAppendNode(
	txn txnif.AsyncTxn,
	startRow, maxRow uint32,
	mvcc *MVCCHandle) *AppendNode {
	//ts := uint64(0)
	var ts types.TS
	if txn != nil {
		ts = txn.GetCommitTS()
	}
	n := &AppendNode{
		txn:      txn,
		startRow: startRow,
		maxRow:   maxRow,
		commitTs: ts,
		mvcc:     mvcc,
	}
	return n
}

func (node *AppendNode) GeneralDesc() string {
	return fmt.Sprintf("TS=%d;StartRow=%d MaxRow=%d", node.commitTs, node.startRow, node.maxRow)
}
func (node *AppendNode) GeneralString() string {
	return node.GeneralDesc()
}
func (node *AppendNode) GeneralVerboseString() string {
	return node.GeneralDesc()
}

func (node *AppendNode) SetLogIndex(idx *wal.Index) {
	node.logIndex = idx
}
func (node *AppendNode) GetID() *common.ID {
	return node.id
}
func (node *AppendNode) GetCommitTS() types.TS {
	node.RLock()
	defer node.RUnlock()
	if node.txn != nil {
		return node.txn.GetCommitTS()
	}
	return node.commitTs
}
func (node *AppendNode) GetStartRow() uint32  { return node.startRow }
func (node *AppendNode) GetMaxRow() uint32    { return node.maxRow }
func (node *AppendNode) SetMaxRow(row uint32) { node.maxRow = row }

func (node *AppendNode) PrepareCommit() error {
	node.Lock()
	defer node.Unlock()
	node.commitTs = node.txn.GetCommitTS()
	return nil
}

func (node *AppendNode) ApplyCommit(index *wal.Index) error {
	node.Lock()
	defer node.Unlock()
	if node.txn == nil {
		panic("AppendNode | ApplyCommit | LogicErr")
	}
	node.txn = nil
	node.logIndex = index
	if node.mvcc != nil {
		logutil.Debugf("Set MaxCommitTS=%d, MaxVisibleRow=%d", node.commitTs, node.maxRow)
		node.mvcc.SetMaxVisible(node.commitTs)
	}
	// logutil.Infof("Apply1Index %s TS=%d", index.String(), n.commitTs)
	listener := node.mvcc.GetAppendListener()
	if listener == nil {
		return nil
	}
	return listener(node)
}

func (node *AppendNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(txnbase.MarshalID(node.mvcc.GetID()))
	if err != nil {
		return
	}
	n += int64(cn)
	if err = binary.Write(w, binary.BigEndian, node.startRow); err != nil {
		return
	}
	n += 4
	if err = binary.Write(w, binary.BigEndian, node.maxRow); err != nil {
		return
	}
	n += 4
	if err = binary.Write(w, binary.BigEndian, node.commitTs); err != nil {
		return
	}
	n += 8
	return
}

func (node *AppendNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	buf := make([]byte, txnbase.IDSize)
	if sn, err = r.Read(buf); err != nil {
		return
	}
	n = int64(sn)
	node.id = txnbase.UnmarshalID(buf)
	if err = binary.Read(r, binary.BigEndian, &node.startRow); err != nil {
		return
	}
	n += 4
	if err = binary.Read(r, binary.BigEndian, &node.maxRow); err != nil {
		return
	}
	n += 4
	if err = binary.Read(r, binary.BigEndian, &node.commitTs); err != nil {
		return
	}
	n += 8
	return
}

func (node *AppendNode) PrepareRollback() (err error) {
	node.mvcc.Lock()
	defer node.mvcc.Unlock()
	node.mvcc.DeleteAppendNodeLocked(node)
	return
}
func (node *AppendNode) ApplyRollback() (err error) { return }
func (node *AppendNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewAppendCmd(id, node)
	return
}
