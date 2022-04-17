package updates

import (
	"fmt"
	"sync"

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
	columns map[uint16]*ColumnChain
	deletes *DeleteChain
	meta    *catalog.BlockEntry
}

func NewMutationNode(meta *catalog.BlockEntry) *MutationController {
	node := &MutationController{
		RWMutex: new(sync.RWMutex),
		columns: make(map[uint16]*ColumnChain),
		meta:    meta,
	}
	node.deletes = NewDeleteChain(nil, node)
	for i := uint16(0); i < uint16(len(meta.GetSchema().ColDefs)); i++ {
		col := NewColumnChain(nil, i, node)
		node.columns[i] = col
	}
	return node
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
