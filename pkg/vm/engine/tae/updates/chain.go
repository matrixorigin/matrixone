package updates

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type BlockUpdateChain struct {
	*common.Link
	*sync.RWMutex
	rwlocker     *sync.RWMutex
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

func (chain *BlockUpdateChain) GetMeta() *catalog.BlockEntry { return chain.meta }

func (chain *BlockUpdateChain) AddNode(txn txnif.AsyncTxn) *BlockUpdateNode {
	// TODO: scan chain and fill base deletes and updates
	updates := NewBlockUpdates(txn, chain.meta, nil, nil)
	chain.Lock()
	defer chain.Unlock()
	node := NewBlockUpdateNode(chain, updates)
	return node
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

// Read Related

// func (chain *BlockUpdateChain) CollectUpdates(txn txnif.AsyncTxn) *BlockUpdates {
// 	if chain == nil {
// 		return nil
// 	}
// 	if txn == nil {
// 		return nil
// 	}
// }
