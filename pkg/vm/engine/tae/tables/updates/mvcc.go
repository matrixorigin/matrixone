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
	meta            *catalog.BlockEntry
	maxVisible      uint64
	appends         []*AppendNode
	changes         uint32
	deletesListener func(uint64, common.RowGen, uint64) error
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

func (n *MVCCHandle) SetDeletesListener(l func(uint64, common.RowGen, uint64) error) {
	n.deletesListener = l
}

func (n *MVCCHandle) GetDeletesListener() func(uint64, common.RowGen, uint64) error {
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

func (n *MVCCHandle) SetMaxVisible(ts uint64) {
	atomic.StoreUint64(&n.maxVisible, ts)
}

func (n *MVCCHandle) LoadMaxVisible() uint64 {
	return atomic.LoadUint64(&n.maxVisible)
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

func (n *MVCCHandle) CheckNotDeleted(start, end uint32, ts uint64) error {
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

func (n *MVCCHandle) CheckNotUpdated(start, end uint32, ts uint64) (err error) {
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
func (n *MVCCHandle) TrySetMaxVisible(ts uint64) {
	if ts > n.maxVisible {
		n.maxVisible = ts
	}
}
func (n *MVCCHandle) AddAppendNodeLocked(txn txnif.AsyncTxn, maxRow uint32) *AppendNode {
	an := NewAppendNode(txn, maxRow, n)
	n.appends = append(n.appends, an)
	return an
}

func (n *MVCCHandle) IsVisibleLocked(row uint32, ts uint64) (bool, error) {
	maxRow, visible, err := n.GetMaxVisibleRowLocked(ts)
	if !visible || err != nil {
		return visible, err
	}
	visible = maxRow >= row
	return visible, err
}

func (n *MVCCHandle) IsDeletedLocked(row uint32, ts uint64, rwlocker *sync.RWMutex) (bool, error) {
	return n.deletes.IsDeleted(row, ts, rwlocker)
}

func (n *MVCCHandle) CollectAppendLogIndexesLocked(startTs, endTs uint64) (indexes []*wal.Index, err error) {
	if len(n.appends) == 0 {
		return
	}
	startOffset, _, startVisible, err := n.getMaxVisibleRowLocked(startTs - 1)
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

func (n *MVCCHandle) GetMaxVisibleRowLocked(ts uint64) (row uint32, visible bool, err error) {
	_, row, visible, err = n.getMaxVisibleRowLocked(ts)
	return
}

//for replay
func (n *MVCCHandle) GetTotalRow() uint32 {
	if len(n.appends) == 0 {
		return 0
	}
	delets := n.deletes.cnt
	return n.appends[len(n.appends)-1].maxRow - delets
}

func (n *MVCCHandle) getMaxVisibleRowLocked(ts uint64) (int, uint32, bool, error) {
	if len(n.appends) == 0 {
		return 0, 0, false, nil
	}
	maxVisible := n.LoadMaxVisible()
	lastAppend := n.appends[len(n.appends)-1]

	// 1. Last append node is in the window and it was already committed
	if ts > lastAppend.GetCommitTS() && maxVisible >= lastAppend.GetCommitTS() {
		return len(n.appends) - 1, lastAppend.GetMaxRow(), true, nil
	}
	start, end := 0, len(n.appends)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if n.appends[mid].GetCommitTS() < ts {
			start = mid + 1
		} else if n.appends[mid].GetCommitTS() > ts {
			end = mid - 1
		} else {
			break
		}
	}
	if mid == 0 && n.appends[mid].GetCommitTS() > ts {
		// 2. The first node is found and it was committed after ts
		return 0, 0, false, nil
	} else if mid != 0 && n.appends[mid].GetCommitTS() > ts {
		// 3. A node (not first) is found and it was committed after ts. Use the prev node
		mid = mid - 1
	}
	var err error
	node := n.appends[mid]
	if node.GetCommitTS() > n.LoadMaxVisible() {
		node.RLock()
		txn := node.txn
		node.RUnlock()
		// Note: Maybe there is a deadlock risk here
		if txn != nil && node.GetCommitTS() > n.LoadMaxVisible() {
			// Append node should not be rollbacked because apply append is the last step of prepare commit
			state := txn.GetTxnState(true)
			// logutil.Infof("%d -- wait --> %s: %d", ts, txn.Repr(), state)
			if state == txnif.TxnStateUnknown {
				err = txnif.TxnInternalErr
			} else if state == txnif.TxnStateRollbacked {
				panic("append node shoul not be rollbacked")
			}
		}
	}
	return mid, node.GetMaxRow(), true, err
}
