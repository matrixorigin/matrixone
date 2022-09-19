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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCHandle struct {
	*sync.RWMutex
	columns         map[uint16]*ColumnChain
	deletes         *DeleteChain
	holes           *roaring.Bitmap
	meta            *catalog.BlockEntry
	maxVisible      atomic.Value
	appends         *txnbase.MVCCSlice
	changes         uint32
	deletesListener func(uint64, common.RowGen, types.TS) error
	appendListener  func(txnif.AppendNode) error
}

func NewMVCCHandle(meta *catalog.BlockEntry) *MVCCHandle {
	node := &MVCCHandle{
		RWMutex: new(sync.RWMutex),
		columns: make(map[uint16]*ColumnChain),
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
	}
	node.deletes = NewDeleteChain(nil, node)
	if meta == nil {
		return node
	}
	for i := uint16(0); i < uint16(len(meta.GetSchema().ColDefs)); i++ {
		col := NewColumnChain(nil, i, node)
		node.columns[i] = col
	}
	return node
}

func (n *MVCCHandle) SetAppendListener(l func(txnif.AppendNode) error) {
	n.appendListener = l
}

func (n *MVCCHandle) GetAppendListener() func(txnif.AppendNode) error {
	return n.appendListener
}

func (n *MVCCHandle) SetDeletesListener(l func(uint64, common.RowGen, types.TS) error) {
	n.deletesListener = l
}

func (n *MVCCHandle) GetDeletesListener() func(uint64, common.RowGen, types.TS) error {
	return n.deletesListener
}

func (n *MVCCHandle) HasActiveAppendNode() bool {
	n.RLock()
	defer n.RUnlock()
	return !n.appends.IsCommitted()
}

func (n *MVCCHandle) AddHoles(start, end int) {
	if n.holes != nil {
		n.holes = roaring.New()
	}
	n.holes.AddRange(uint64(start), uint64(end))
}

func (n *MVCCHandle) HasHole() bool {
	return n.holes != nil && !n.holes.IsEmpty()
}

func (n *MVCCHandle) HoleCnt() int {
	if !n.HasHole() {
		return 0
	}
	return int(n.holes.GetCardinality())
}

func (n *MVCCHandle) IncChangeNodeCnt() {
	atomic.AddUint32(&n.changes, uint32(1))
}

func (n *MVCCHandle) ResetChangeNodeCnt() {
	atomic.StoreUint32(&n.changes, uint32(0))
}

func (n *MVCCHandle) GetChangeNodeCnt() uint32 {
	return atomic.LoadUint32(&n.changes)
}

func (n *MVCCHandle) GetColumnUpdateCnt(colIdx uint16) uint32 {
	return n.columns[colIdx].LoadUpdateCnt()
}

func (n *MVCCHandle) GetDeleteCnt() uint32 {
	return n.deletes.GetDeleteCnt()
}

func (n *MVCCHandle) SetMaxVisible(ts types.TS) {
	n.maxVisible.Store(ts)
}

func (n *MVCCHandle) LoadMaxVisible() types.TS {
	ts := n.maxVisible.Load().(types.TS)
	return ts
}

func (n *MVCCHandle) GetID() *common.ID { return n.meta.AsCommonID() }

func (n *MVCCHandle) StringLocked() string {
	s := ""
	if n.deletes.DepthLocked() > 0 {
		s = fmt.Sprintf("%s%s", s, n.deletes.StringLocked())
	}
	for _, chain := range n.columns {
		chain.RLock()
		if chain.DepthLocked() > 0 {
			s = fmt.Sprintf("%s\n%s", s, chain.StringLocked())
		}
		chain.RUnlock()
	}
	s = fmt.Sprintf("%s\n%s", s, n.appends.StringLocked())
	return s
}

func (n *MVCCHandle) GetColumnExclusiveLock(idx uint16) sync.Locker {
	col := n.columns[idx]
	col.Lock()
	return col.RWMutex
}

func (n *MVCCHandle) CheckNotDeleted(start, end uint32, ts types.TS) error {
	return n.deletes.PrepareRangeDelete(start, end, ts)
}

func (n *MVCCHandle) CreateDeleteNode(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	return n.deletes.AddNodeLocked(txn, deleteType)
}

func (n *MVCCHandle) OnReplayDeleteNode(deleteNode txnif.DeleteNode) {
	n.deletes.OnReplayNode(deleteNode.(*DeleteNode))
	n.TrySetMaxVisible(deleteNode.(*DeleteNode).commitTs)
}

func (n *MVCCHandle) CreateUpdateNode(colIdx uint16, txn txnif.AsyncTxn) txnif.UpdateNode {
	chain := n.columns[colIdx]
	return chain.AddNodeLocked(txn)
}

func (n *MVCCHandle) DropUpdateNode(colIdx uint16, node txnif.UpdateNode) {
	chain := n.columns[colIdx]
	chain.DeleteNodeLocked(node.(*ColumnUpdateNode))
}

func (n *MVCCHandle) PrepareUpdate(row uint32, colIdx uint16, update txnif.UpdateNode) error {

	chain := n.columns[colIdx]
	return chain.PrepareUpdate(row, update)
}

func (n *MVCCHandle) CheckNotUpdated(start, end uint32, ts types.TS) (err error) {
	for _, chain := range n.columns {
		for i := start; i <= end; i++ {
			if err = chain.view.PrepapreInsert(i, ts); err != nil {
				return
			}
		}
	}
	return
}

func (n *MVCCHandle) GetColumnChain(colIdx uint16) *ColumnChain {
	return n.columns[colIdx]
}

func (n *MVCCHandle) GetDeleteChain() *DeleteChain {
	return n.deletes
}
func (n *MVCCHandle) OnReplayAppendNode(an *AppendNode) {
	an.mvcc = n
	n.appends.InsertNode(an)
	n.TrySetMaxVisible(an.GetCommitTS())
}
func (n *MVCCHandle) TrySetMaxVisible(ts types.TS) {
	if ts.Greater(n.maxVisible.Load().(types.TS)) {
		n.maxVisible.Store(ts)
	}
}
func (n *MVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32) (an *AppendNode, created bool) {
	if n.appends.IsEmpty() || n.appends.IsCommitted() {
		an = NewAppendNode(txn, startRow, maxRow, n)
		n.appends.InsertNode(an)
		created = true
	} else {
		an = n.appends.GetUpdateNodeLocked().(*AppendNode)
		created = false
		an.Lock()
		defer an.Unlock()
		an.SetMaxRow(maxRow)
	}
	return
}

func (n *MVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	n.appends.DeleteNode(node)
}

func (n *MVCCHandle) IsVisibleLocked(row uint32, ts types.TS) (bool, error) {
	maxRow, visible, err := n.GetMaxVisibleRowLocked(ts)
	if !visible || err != nil {
		return visible, err
	}
	visible = maxRow >= row
	return visible, err
}

func (n *MVCCHandle) IsDeletedLocked(row uint32, ts types.TS, rwlocker *sync.RWMutex) (bool, error) {
	return n.deletes.IsDeleted(row, ts, rwlocker)
}

func (n *MVCCHandle) CollectAppendLogIndexesLocked(startTs, endTs types.TS) (indexes []*wal.Index, err error) {
	if n.appends.IsEmpty() {
		return
	}
	indexes = n.appends.CloneIndexInRange(startTs, endTs, n.RWMutex)
	return
}

func (n *MVCCHandle) GetMaxVisibleRowLocked(ts types.TS) (row uint32, visible bool, err error) {
	needWait, txn := n.appends.NeedWaitCommitting(ts)
	if needWait {
		n.RUnlock()
		state := txn.GetTxnState(true)
		n.RLock()
		if state == txnif.TxnStateUnknown {
			err = txnif.ErrTxnInternal
			return
		} else if state == txnif.TxnStateRollbacked || state == txnif.TxnStatePreparing {
			panic("append node shoul not be rollbacked")
		}
	}
	_, row, visible, err = n.getMaxVisibleRowLocked(ts)
	return
}

// GetTotalRow is only for replay
func (n *MVCCHandle) GetTotalRow() uint32 {
	van := n.appends.GetUpdateNodeLocked()
	if van == nil {
		return 0
	}
	an := van.(*AppendNode)
	delets := n.deletes.cnt
	return an.maxRow - delets
}

// TODO::it will be rewritten in V0.6,since maxVisible of MVCC handel
//
//	would not be increased monotonically.
func (n *MVCCHandle) getMaxVisibleRowLocked(ts types.TS) (int, uint32, bool, error) {
	offset, vnode := n.appends.GetNodeToRead(ts)
	if vnode == nil {
		return 0, 0, false, nil
	}
	node := vnode.(*AppendNode)
	node.RLock()
	defer node.RUnlock()
	return offset, node.GetMaxRow(), true, nil
}
