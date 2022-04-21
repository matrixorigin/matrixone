package updates

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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

type MutationController struct {
	*sync.RWMutex
	columns    map[uint16]*ColumnChain
	deletes    *DeleteChain
	meta       *catalog.BlockEntry
	maxVisible uint64
	appends    []*AppendNode
}

func NewMutationNode(meta *catalog.BlockEntry) *MutationController {
	node := &MutationController{
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

func (n *MutationController) SetMaxVisible(ts uint64) {
	atomic.StoreUint64(&n.maxVisible, ts)
}

func (n *MutationController) LoadMaxVisible() uint64 {
	return atomic.LoadUint64(&n.maxVisible)
}

func (n *MutationController) GetID() *common.ID { return n.meta.AsCommonID() }

func (n *MutationController) StringLocked() string {
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

func (n *MutationController) GetSharedLock() sync.Locker {
	locker := newSharedLock(n.RWMutex)
	locker.Lock()
	return locker
}

func (n *MutationController) GetExclusiveLock() sync.Locker {
	n.Lock()
	return n.RWMutex
}

func (n *MutationController) GetColumnSharedLock(idx uint16) sync.Locker {
	col := n.columns[idx]
	locker := newSharedLock(col.RWMutex)
	locker.Lock()
	return locker
}

func (n *MutationController) GetColumnExclusiveLock(idx uint16) sync.Locker {
	col := n.columns[idx]
	col.Lock()
	return col.RWMutex
}

func (n *MutationController) CheckNotDeleted(start, end uint32, ts uint64) error {
	return n.deletes.PrepareRangeDelete(start, end, ts)
}

func (n *MutationController) CreateDeleteNode(txn txnif.AsyncTxn) txnif.DeleteNode {
	return n.deletes.AddNodeLocked(txn)
}

func (n *MutationController) CreateUpdateNode(colIdx uint16, txn txnif.AsyncTxn) txnif.UpdateNode {
	chain := n.columns[colIdx]
	return chain.AddNodeLocked(txn)
}

func (n *MutationController) DropUpdateNode(colIdx uint16, node txnif.UpdateNode) {
	chain := n.columns[colIdx]
	chain.DeleteNodeLocked(node.GetDLNode())
}

func (n *MutationController) PrepareUpdate(row uint32, colIdx uint16, update txnif.UpdateNode) error {

	chain := n.columns[colIdx]
	return chain.PrepareUpdate(row, update)
}

func (n *MutationController) CheckNotUpdated(start, end uint32, ts uint64) (err error) {
	for _, chain := range n.columns {
		for i := start; i <= end; i++ {
			if err = chain.view.PrepapreInsert(i, ts); err != nil {
				return
			}
		}
	}
	return
}

func (n *MutationController) GetColumnChain(colIdx uint16) *ColumnChain {
	return n.columns[colIdx]
}

func (n *MutationController) GetDeleteChain() *DeleteChain {
	return n.deletes
}

func (n *MutationController) AddAppendNodeLocked(txn txnif.AsyncTxn, maxRow uint32) *AppendNode {
	an := NewAppendNode(txn, maxRow, n)
	n.appends = append(n.appends, an)
	return an
}

func (n *MutationController) IsVisibleLocked(row uint32, ts uint64) bool {
	maxRow, ok := n.GetMaxVisibleRowLocked(ts)
	if !ok {
		return ok
	}
	return maxRow >= row
}

func (n *MutationController) GetMaxVisibleRowLocked(ts uint64) (uint32, bool) {
	if len(n.appends) == 0 {
		return 0, false
	}
	readLock := n.GetSharedLock()
	maxVisible := n.LoadMaxVisible()
	lastAppend := n.appends[len(n.appends)-1]

	// 1. Last append node is in the window and it was already committed
	if ts > lastAppend.GetCommitTS() && maxVisible >= lastAppend.GetCommitTS() {
		readLock.Unlock()
		return lastAppend.GetMaxRow(), true
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
		readLock.Unlock()
		return 0, false
	} else if mid != 0 && n.appends[mid].GetCommitTS() > ts {
		// 3. A node (not first) is found and it was committed after ts. Use the prev node
		mid = mid - 1
	}
	node := n.appends[mid]
	readLock.Unlock()
	if node.GetCommitTS() < n.LoadMaxVisible() {
		node.RLock()
		txn := node.txn
		node.RUnlock()
		if txn != nil {
			txn.GetTxnState(true)
		}
	}
	return node.GetMaxRow(), true
}
