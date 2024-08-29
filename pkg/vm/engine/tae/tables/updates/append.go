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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type AppendNode struct {
	*txnbase.TxnMVCCNode
	startRow    uint32
	maxRow      uint32
	mvcc        *AppendMVCCHandle
	id          *common.ID
	NodeType    handle.DeleteType
	isTombstone bool
}

func CompareAppendNode(e, o *AppendNode) int {
	return e.Compare(o.TxnMVCCNode)
}

func MockAppendNode(ts types.TS, startRow, maxRow uint32, mvcc *AppendMVCCHandle) *AppendNode {
	return &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{
			Start: ts,
			End:   ts,
		},
		maxRow: maxRow,
		mvcc:   mvcc,
	}
}

func NewAppendNode(
	txn txnif.AsyncTxn,
	startRow, maxRow uint32,
	isTombstone bool,
	mvcc *AppendMVCCHandle) *AppendNode {
	var startTs, ts types.TS
	if txn != nil {
		startTs = txn.GetStartTS()
		ts = txn.GetPrepareTS()
	}
	n := &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{
			Start:   startTs,
			Prepare: ts,
			End:     txnif.UncommitTS,
			Txn:     txn,
		},
		startRow:    startRow,
		maxRow:      maxRow,
		mvcc:        mvcc,
		isTombstone: isTombstone,
	}
	return n
}

func NewEmptyAppendNode() *AppendNode {
	return &AppendNode{
		TxnMVCCNode: &txnbase.TxnMVCCNode{},
		id:          &common.ID{},
	}
}
func (node *AppendNode) IsTombstone() bool {
	return node.isTombstone
}
func (node *AppendNode) String() string {
	return node.GeneralDesc()
}
func (node *AppendNode) CloneAll() *AppendNode {
	panic("todo")
}
func (node *AppendNode) CloneData() *AppendNode {
	panic("todo")
}
func (node *AppendNode) Update(*AppendNode) {
	panic("todo")
}
func (node *AppendNode) IsNil() bool { return node == nil }
func (node *AppendNode) GeneralDesc() string {
	return fmt.Sprintf("%s;StartRow=%d MaxRow=%d", node.TxnMVCCNode.String(), node.startRow, node.maxRow)
}
func (node *AppendNode) GeneralString() string {
	return node.GeneralDesc()
}
func (node *AppendNode) GeneralVerboseString() string {
	return node.GeneralDesc()
}

func (node *AppendNode) GetID() *common.ID {
	return node.id
}
func (node *AppendNode) GetCommitTS() types.TS {
	return node.GetEnd()
}
func (node *AppendNode) GetStartRow() uint32 { return node.startRow }
func (node *AppendNode) GetMaxRow() uint32 {
	return node.maxRow
}
func (node *AppendNode) SetIsMergeCompact()   { node.NodeType = handle.DT_MergeCompact }
func (node *AppendNode) IsMergeCompact() bool { return node.NodeType == handle.DT_MergeCompact }
func (node *AppendNode) SetMaxRow(row uint32) {
	node.maxRow = row
}

func (node *AppendNode) PrepareCommit() error {
	node.mvcc.Lock()
	defer node.mvcc.Unlock()
	_, err := node.TxnMVCCNode.PrepareCommit()
	return err
}

func (node *AppendNode) ApplyCommit(id string) error {
	node.mvcc.Lock()
	defer node.mvcc.Unlock()
	if node.IsCommitted() {
		panic("AppendNode | ApplyCommit | LogicErr")
	}
	node.TxnMVCCNode.ApplyCommit(id)
	listener := node.mvcc.GetAppendListener()
	if listener == nil {
		return nil
	}
	return listener(node)
}

func (node *AppendNode) ApplyRollback() (err error) {
	node.mvcc.Lock()
	defer node.mvcc.Unlock()
	_, err = node.TxnMVCCNode.ApplyRollback()
	return
}

func (node *AppendNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(common.EncodeID(node.mvcc.GetID()))
	if err != nil {
		return
	}
	n += int64(cn)
	var sn1 int
	if sn1, err = w.Write(types.EncodeUint32(&node.startRow)); err != nil {
		return
	}
	n += int64(sn1)
	if sn1, err = w.Write(types.EncodeUint32(&node.maxRow)); err != nil {
		return
	}
	n += int64(sn1)
	var sn int64
	sn, err = node.TxnMVCCNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	if sn1, err = w.Write(types.EncodeBool(&node.isTombstone)); err != nil {
		return
	}
	n += int64(sn1)
	return
}

func (node *AppendNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	if node.id == nil {
		node.id = &common.ID{}
	}
	if _, err = r.Read(common.EncodeID(node.id)); err != nil {
		return
	}
	n += common.IDSize
	if sn, err = r.Read(types.EncodeUint32(&node.startRow)); err != nil {
		return
	}
	n += int64(sn)
	if sn, err = r.Read(types.EncodeUint32(&node.maxRow)); err != nil {
		return
	}
	n += int64(sn)
	var sn2 int64
	sn2, err = node.TxnMVCCNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn2
	if sn, err = r.Read(types.EncodeBool(&node.isTombstone)); err != nil {
		return
	}
	n += int64(sn)
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
