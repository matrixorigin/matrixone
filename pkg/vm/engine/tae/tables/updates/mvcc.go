package updates

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type sharedLock struct {
	locker *sync.RWMutex
}

func (lock *sharedLock) Lock() {
	lock.locker.RLock()
}

func (lock *sharedLock) Unlock() {
	lock.locker.RUnlock()
}

func newSharedLock(locker *sync.RWMutex) *sharedLock {
	return &sharedLock{
		locker: locker,
	}
}

type MVCCHandle struct {
	*sync.RWMutex
	columns    map[uint16]*ColumnChain
	deletes    *DeleteChain
	meta       *catalog.BlockEntry
	maxVisible uint64
	appends    []*AppendNode
	changes    uint32
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
	return s
}

func (n *MVCCHandle) GetSharedLock() sync.Locker {
	locker := newSharedLock(n.RWMutex)
	locker.Lock()
	return locker
}

func (n *MVCCHandle) GetExclusiveLock() sync.Locker {
	n.Lock()
	return n.RWMutex
}

func (n *MVCCHandle) GetColumnSharedLock(idx uint16) sync.Locker {
	col := n.columns[idx]
	locker := newSharedLock(col.RWMutex)
	locker.Lock()
	return locker
}

func (n *MVCCHandle) GetColumnExclusiveLock(idx uint16) sync.Locker {
	col := n.columns[idx]
	col.Lock()
	return col.RWMutex
}

func (n *MVCCHandle) CheckNotDeleted(start, end uint32, ts uint64) error {
	return n.deletes.PrepareRangeDelete(start, end, ts)
}

func (n *MVCCHandle) CreateDeleteNode(txn txnif.AsyncTxn) txnif.DeleteNode {
	return n.deletes.AddNodeLocked(txn)
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

func (n *MVCCHandle) AddAppendNodeLocked(txn txnif.AsyncTxn, maxRow uint32) *AppendNode {
	an := NewAppendNode(txn, maxRow, n)
	n.appends = append(n.appends, an)
	return an
}

func (n *MVCCHandle) IsVisibleLocked(row uint32, ts uint64) bool {
	maxRow, ok := n.GetMaxVisibleRowLocked(ts)
	if !ok {
		return ok
	}
	return maxRow >= row
}

func (n *MVCCHandle) IsDeletedLocked(row uint32, ts uint64) bool {
	return n.deletes.IsDeleted(row, ts)
}

func (n *MVCCHandle) CollectAppendLogIndexesLocked(startTs, endTs uint64) (indexes []*wal.Index) {
	if len(n.appends) == 0 {
		return
	}
	startOffset, _, startOk := n.getMaxVisibleRowLocked(startTs)
	endOffset, _, endOk := n.getMaxVisibleRowLocked(endTs)
	if !endOk {
		return
	}
	if !startOk {
		startOffset = 0
	} else {
		startOffset += 1
	}
	for i := endOffset; i >= startOffset; i-- {
		indexes = append(indexes, n.appends[i].logIndex)
	}
	return
}

func (n *MVCCHandle) GetMaxVisibleRowLocked(ts uint64) (uint32, bool) {
	_, row, ok := n.getMaxVisibleRowLocked(ts)
	return row, ok
}

func (n *MVCCHandle) getMaxVisibleRowLocked(ts uint64) (int, uint32, bool) {
	if len(n.appends) == 0 {
		return 0, 0, false
	}
	// readLock := n.GetSharedLock()
	maxVisible := n.LoadMaxVisible()
	lastAppend := n.appends[len(n.appends)-1]

	// 1. Last append node is in the window and it was already committed
	if ts > lastAppend.GetCommitTS() && maxVisible >= lastAppend.GetCommitTS() {
		// readLock.Unlock()
		return len(n.appends) - 1, lastAppend.GetMaxRow(), true
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
		// readLock.Unlock()
		return 0, 0, false
	} else if mid != 0 && n.appends[mid].GetCommitTS() > ts {
		// 3. A node (not first) is found and it was committed after ts. Use the prev node
		mid = mid - 1
	}
	node := n.appends[mid]
	// readLock.Unlock()
	if node.GetCommitTS() < n.LoadMaxVisible() {
		node.RLock()
		txn := node.txn
		node.RUnlock()
		if txn != nil {
			txn.GetTxnState(true)
		}
	}
	return mid, node.GetMaxRow(), true
}
