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
	*txnbase.TxnMVCCNode
	sync.RWMutex
	startRow uint32
	maxRow   uint32
	mvcc     *MVCCHandle
	id       *common.ID
}

func MockAppendNode(ts types.TS, startRow, maxRow uint32, mvcc *MVCCHandle) *AppendNode {
	return &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{
			Start: ts,
			End:   ts,
		},
		maxRow: maxRow,
		mvcc:   mvcc,
	}
}

func NewCommittedAppendNode(
	ts types.TS,
	startRow, maxRow uint32,
	mvcc *MVCCHandle) *AppendNode {
	return &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{
			Start: ts,
			End:   ts,
		},
		startRow: startRow,
		maxRow:   maxRow,
		mvcc:     mvcc,
	}
}

func NewAppendNode(
	txn txnif.AsyncTxn,
	startRow, maxRow uint32,
	mvcc *MVCCHandle) *AppendNode {
	var ts types.TS
	if txn != nil {
		ts = txn.GetCommitTS()
	}
	n := &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{
			Start: ts,
			End:   ts,
			Txn:   txn,
		},
		startRow: startRow,
		maxRow:   maxRow,
		mvcc:     mvcc,
	}
	return n
}

func NewEmptyAppendNode() txnbase.VisibleNode {
	return &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{},
	}
}
func (node *AppendNode) CloneAll() txnbase.VisibleNode {
	panic("todo")
}
func (node *AppendNode) CloneData() txnbase.VisibleNode {
	panic("todo")
}
func (node *AppendNode) UpdateNode(txnbase.VisibleNode) {
	panic("todo")
}
func (node *AppendNode) GeneralDesc() string {
	return fmt.Sprintf("%s;StartRow=%d MaxRow=%d", node.TxnMVCCNode.String(), node.startRow, node.maxRow)
}
func (node *AppendNode) GeneralString() string {
	return node.GeneralDesc()
}
func (node *AppendNode) GeneralVerboseString() string {
	return node.GeneralDesc()
}

func (node *AppendNode) SetLogIndex(idx *wal.Index) {
	node.TxnMVCCNode.AddLogIndex(idx)
}
func (node *AppendNode) GetID() *common.ID {
	return node.id
}
func (node *AppendNode) GetCommitTS() types.TS {
	node.RLock()
	defer node.RUnlock()
	return node.GetEnd()
}
func (node *AppendNode) GetTxn() txnif.TxnReader {
	node.RLock()
	defer node.RUnlock()
	return node.GetTxnLocked()
}
func (node *AppendNode) GetTxnLocked() txnif.TxnReader {
	return node.TxnMVCCNode.GetTxn()
}
func (node *AppendNode) GetStartRow() uint32 { return node.startRow }
func (node *AppendNode) GetMaxRow() uint32 {
	return node.maxRow
}
func (node *AppendNode) SetMaxRow(row uint32) {
	node.maxRow = row
}

func (node *AppendNode) Prepare2PCPrepare() error {
	node.Lock()
	defer node.Unlock()
	_, err := node.TxnMVCCNode.Prepare2PCPrepare()
	return err
}

func (node *AppendNode) PrepareCommit() error {
	node.Lock()
	defer node.Unlock()
	_, err := node.TxnMVCCNode.PrepareCommit()
	return err
}

func (node *AppendNode) ApplyCommit(index *wal.Index) error {
	node.Lock()
	defer node.Unlock()
	if node.GetTxnLocked() == nil {
		panic("AppendNode | ApplyCommit | LogicErr")
	}
	node.TxnMVCCNode.ApplyCommit(index)
	if node.mvcc != nil {
		logutil.Debugf("Set MaxCommitTS=%v, MaxVisibleRow=%d", node.GetEnd(), node.GetMaxRow())
		node.mvcc.SetMaxVisible(node.GetEnd())
	}
	// logutil.Infof("Apply1Index %s TS=%d", index.String(), n.commitTs)
	listener := node.mvcc.GetAppendListener()
	if listener == nil {
		return nil
	}
	return listener(node)
}

func (node *AppendNode) ApplyRollback(index *wal.Index) (err error) {
	node.Lock()
	defer node.Unlock()
	node.TxnMVCCNode.ApplyRollback(index)
	return
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
	var sn int64
	sn, err = node.TxnMVCCNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
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
	var sn2 int64
	sn2, err = node.TxnMVCCNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn2
	return
}

func (node *AppendNode) PrepareRollback() (err error) {
	node.mvcc.Lock()
	defer node.mvcc.Unlock()
	node.mvcc.DeleteAppendNodeLocked(node)
	return
}
func (node *AppendNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewAppendCmd(id, node)
	return
}
