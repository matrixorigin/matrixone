package updates

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type BlockUpdateChain struct {
	*common.Link
	*sync.RWMutex
	meta         *catalog.BlockEntry
	latestMerge  *BlockUpdateNode
	latestCommit *BlockUpdateNode
}

func NewUpdateChain(rwlocker *sync.RWMutex, meta *catalog.BlockEntry) *BlockUpdateChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &BlockUpdateChain{
		Link:    new(common.Link),
		RWMutex: rwlocker,
		meta:    meta,
	}
}

func (chain *BlockUpdateChain) UpdateLocked(node *BlockUpdateNode) {
	chain.Update(node.DLNode)
	if node.GetCommitTSLocked() != txnif.UncommitTS {
		chain.latestCommit = node
	}
}

func (chain *BlockUpdateChain) GetID() *common.ID            { return chain.meta.AsCommonID() }
func (chain *BlockUpdateChain) GetMeta() *catalog.BlockEntry { return chain.meta }

func (chain *BlockUpdateChain) AddNode(txn txnif.AsyncTxn) *BlockUpdateNode {
	// TODO: scan chain and fill base deletes and updates
	updates := NewBlockUpdates(txn, chain.meta, nil, nil)
	chain.Lock()
	defer chain.Unlock()
	node := NewBlockUpdateNode(chain, updates)
	return node
}

func (chain *BlockUpdateChain) AddNodeLocked(txn txnif.AsyncTxn) *BlockUpdateNode {
	updates := NewBlockUpdates(txn, chain.meta, nil, nil)
	node := NewBlockUpdateNode(chain, updates)
	return node
}

func (chain *BlockUpdateChain) String() string {
	chain.RLock()
	defer chain.RUnlock()
	return chain.StringLocked()
}

func (chain *BlockUpdateChain) StringLocked() string {
	var msg string
	chain.LoopChainLocked(func(n *BlockUpdateNode) bool {
		if msg == "" {
			msg = fmt.Sprintf("%s%s", msg, n.String())
		} else {
			msg = fmt.Sprintf("%s\n%s", msg, n.String())
		}
		return true
	}, false)
	return msg
}

func (chain *BlockUpdateChain) CollectCommittedUpdatesLocked(txn txnif.AsyncTxn) *BlockUpdates {
	var merged *BlockUpdates
	chain.LoopChainLocked(func(curr *BlockUpdateNode) bool {
		curr.RLock()
		if merged == nil {
			merged = NewMergeBlockUpdates(txn.GetStartTS(), chain.meta, nil, nil)
		}
		// If this node is uncommitted, skip it
		if curr.HasActiveTxnLocked() && curr.GetCommitTSLocked() == txnif.UncommitTS && (curr.txn.GetID() != txn.GetID()) {
			// if curr.HasActiveTxnLocked() && curr.GetCommitTSLocked() == txnif.UncommitTS && (curr.txn.GetID() != txn.GetID()) {
			curr.RUnlock()
			return true
		}
		if curr.GetCommitTSLocked() != txnif.UncommitTS && curr.GetCommitTSLocked() > txn.GetStartTS() {
			curr.RUnlock()
			return true
		}
		if curr.GetCommitTSLocked() < txn.GetStartTS() && curr.HasActiveTxnLocked() {
			currTxn := curr.txn
			curr.RUnlock()
			state := currTxn.GetTxnState(true)
			if state == txnif.TxnStateRollbacked {
				return true
			}
			curr.RLock()
		}
		merged.MergeLocked(curr.BlockUpdates)
		if curr.IsMerge() {
			curr.RUnlock()
			return false
		}
		curr.RUnlock()
		return true
	}, false)
	return merged
}

func (chain *BlockUpdateChain) AddMergeNode() *BlockUpdateNode {
	chain.Lock()
	defer chain.Unlock()
	var merge *BlockUpdates
	chain.LoopChainLocked(func(updates *BlockUpdateNode) bool {
		updates.RLock()
		if updates.GetCommitTSLocked() == txnif.UncommitTS {
			updates.RUnlock()
			return true
		}
		if merge == nil {
			merge = NewMergeBlockUpdates(updates.GetCommitTSLocked(), chain.meta, nil, nil)
		}
		merge.MergeLocked(updates.BlockUpdates)
		ret := true
		if updates.IsMerge() {
			ret = false
		}
		updates.RUnlock()
		return ret
	}, false)
	if merge == nil {
		return nil
	}
	node := NewBlockUpdateNode(chain, merge)
	chain.latestMerge = node
	return node
}

func (chain *BlockUpdateChain) LoopChainLocked(fn func(updateNode *BlockUpdateNode) bool, reverse bool) {
	wrapped := func(node *common.DLNode) bool {
		updates := node.GetPayload().(*BlockUpdateNode)
		return fn(updates)
	}
	chain.Loop(wrapped, reverse)
}

func (chain *BlockUpdateChain) FirstNodeLocked() (node *BlockUpdateNode) {
	return chain.GetHead().GetPayload().(*BlockUpdateNode)
}

func (chain *BlockUpdateChain) LatestMergeLocked() (node *BlockUpdateNode) {
	return chain.latestMerge
}

func (chain *BlockUpdateChain) LatestCommitLocked() (node *BlockUpdateNode) {
	return chain.latestCommit
}

func (chain *BlockUpdateChain) LatestMerge() (node *BlockUpdateNode) {
	chain.RLock()
	defer chain.RUnlock()
	return chain.latestMerge
}

func (chain *BlockUpdateChain) LatestCommit() (node *BlockUpdateNode) {
	chain.RLock()
	defer chain.RUnlock()
	return chain.latestCommit
}

func (chain *BlockUpdateChain) FirstNode() (node *BlockUpdateNode) {
	chain.RLock()
	defer chain.RUnlock()
	return chain.GetHead().GetPayload().(*BlockUpdateNode)
}

func (chain *BlockUpdateChain) DeleteUncommittedNodeLocked(n *BlockUpdateNode) {
	chain.Delete(n.DLNode)
}

// Read Related

// Locked
func (chain *BlockUpdateChain) CheckDeletedLocked(start, end uint32, txn txnif.AsyncTxn) (err error) {
	chain.LoopChainLocked(func(n *BlockUpdateNode) bool {
		n.RLock()
		defer n.RUnlock()
		overlap := n.HasDeleteOverlapLocked(start, end)
		if overlap {
			err = txnif.TxnWWConflictErr
		}
		if n.IsMerge() || err != nil {
			return false
		}
		return true
	}, false)
	return
}

func (chain *BlockUpdateChain) CheckColumnUpdatedLocked(row uint32, colIdx uint16, txn txnif.AsyncTxn) (err error) {
	chain.LoopChainLocked(func(n *BlockUpdateNode) bool {
		n.RLock()
		defer n.RUnlock()
		if !n.HasActiveTxnLocked() {
			return false
		}
		if n.HasColUpdateLocked(row, colIdx) && !n.IsSameTxnLocked(txn) {
			err = txnif.TxnWWConflictErr
			return false
		}
		return true
	}, false)
	return
}
