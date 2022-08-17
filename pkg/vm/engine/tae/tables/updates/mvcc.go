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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCHandle struct {
	*sync.RWMutex
	columns         map[uint16]*ColumnChain
	deletes         *DeleteChain
	holes           *roaring.Bitmap
	meta            *catalog.BlockEntry
	maxVisible      atomic.Value
	appends         []*AppendNode
	changes         uint32
	deletesListener func(uint64, common.RowGen, types.TS) error
	appendListener  func(txnif.AppendNode) error
}

func NewMVCCHandle(meta *catalog.BlockEntry) *MVCCHandle {
	node := &MVCCHandle{
		RWMutex: new(sync.RWMutex),
		columns: make(map[uint16]*ColumnChain),
		meta:    meta,
		appends: make([]*AppendNode, 0),
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
	if len(n.appends) == 0 {
		return false
	}
	node := n.appends[len(n.appends)-1]
	node.RLock()
	txn := node.txn
	node.RUnlock()
	return txn != nil
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
	for _, insert := range n.appends {
		s = fmt.Sprintf("%s\n%s", s, insert.GeneralString())
	}
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
	chain.DeleteNodeLocked(node.GetDLNode())
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
	n.appends = append(n.appends, an)
	n.TrySetMaxVisible(an.commitTs)
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
	if len(n.appends) == 0 {
		an = NewAppendNode(txn, startRow, maxRow, n)
		n.appends = append(n.appends, an)
		created = true
	} else {
		an = n.appends[len(n.appends)-1]
		an.RLock()
		nTxn := an.txn
		an.RUnlock()
		if nTxn != txn {
			an = NewAppendNode(txn, startRow, maxRow, n)
			n.appends = append(n.appends, an)
			created = true
		} else {
			created = false
			an.SetMaxRow(maxRow)
		}
	}
	return
}

func (n *MVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	for i := len(n.appends) - 1; i >= 0; i-- {
		if n.appends[i].maxRow == node.maxRow {
			n.appends = append(n.appends[:i], n.appends[i+1:]...)
		} else if n.appends[i].maxRow < node.maxRow {
			break
		}
	}
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
	if len(n.appends) == 0 {
		return
	}
	startOffset, _, startVisible, err := n.getMaxVisibleRowLocked(startTs.Prev())
	if err != nil {
		return
	}
	endOffset, _, endVisible, err := n.getMaxVisibleRowLocked(endTs)
	if err != nil {
		return
	}
	if !endVisible {
		return
	}
	if !startVisible {
		startOffset = 0
	} else {
		startOffset += 1
	}
	for i := endOffset; i >= startOffset; i-- {
		indexes = append(indexes, n.appends[i].logIndex)
	}
	return
}

func (n *MVCCHandle) GetMaxVisibleRowLocked(ts types.TS) (row uint32, visible bool, err error) {
	_, row, visible, err = n.getMaxVisibleRowLocked(ts)
	return
}

// GetTotalRow is only for replay
func (n *MVCCHandle) GetTotalRow() uint32 {
	if len(n.appends) == 0 {
		return 0
	}
	delets := n.deletes.cnt
	return n.appends[len(n.appends)-1].maxRow - delets
}

func (n *MVCCHandle) getMaxVisibleRowLocked(ts types.TS) (int, uint32, bool, error) {
	if len(n.appends) == 0 {
		return 0, 0, false, nil
	}
	maxVisible := n.LoadMaxVisible()
	lastAppend := n.appends[len(n.appends)-1]

	// 1. Last append node is in the window and it was already committed
	if ts.Greater(lastAppend.GetCommitTS()) && maxVisible.GreaterEq(lastAppend.GetCommitTS()) {
		return len(n.appends) - 1, lastAppend.GetMaxRow(), true, nil
	}
	start, end := 0, len(n.appends)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if n.appends[mid].GetCommitTS().Less(ts) {
			start = mid + 1
		} else if n.appends[mid].GetCommitTS().Greater(ts) {
			end = mid - 1
		} else {
			break
		}
	}
	if mid == 0 && n.appends[mid].GetCommitTS().Greater(ts) {
		// 2. The first node is found and it was committed after ts
		return 0, 0, false, nil
	} else if mid != 0 && n.appends[mid].GetCommitTS().Greater(ts) {
		// 3. A node (not first) is found and it was committed after ts. Use the prev node
		mid = mid - 1
	}
	var err error
	node := n.appends[mid]
	if node.GetCommitTS().Greater(n.LoadMaxVisible()) {
		node.RLock()
		txn := node.txn
		node.RUnlock()
		// Note: Maybe there is a deadlock risk here
		if txn != nil && node.GetCommitTS().Greater(n.LoadMaxVisible()) {
			node.mvcc.RUnlock()
			// Append node should not be rollbacked because apply append is the last step of prepare commit
			state := txn.GetTxnState(true)
			node.mvcc.RLock()
			// logutil.Infof("%d -- wait --> %s: %d", ts, txn.Repr(), state)
			if state == txnif.TxnStateUnknown {
				err = txnif.ErrTxnInternal
			} else if state == txnif.TxnStateRollbacked || state == txnif.TxnStateCommitting {
				panic("append node shoul not be rollbacked")
			}
		}
	}
	return mid, node.GetMaxRow(), true, err
}
