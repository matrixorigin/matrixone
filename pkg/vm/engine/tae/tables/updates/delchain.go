package updates

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type DeleteChain struct {
	*sync.RWMutex
	*common.Link
	controller *MutationController
}

func NewDeleteChain(rwlocker *sync.RWMutex, controller *MutationController) *DeleteChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	chain := &DeleteChain{
		RWMutex:    rwlocker,
		Link:       new(common.Link),
		controller: controller,
	}
	return chain
}

func (chain *DeleteChain) StringLocked() string {
	msg := fmt.Sprintf("DeleteChain:")
	line := 1
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		n.RLock()
		msg = fmt.Sprintf("%s\n%d. %s", msg, line, n.StringLocked())
		n.RUnlock()
		line++
		return true
	}, false)
	return msg
}

func (chain *DeleteChain) GetController() *MutationController { return chain.controller }

func (chain *DeleteChain) LoopChainLocked(fn func(node *DeleteNode) bool, reverse bool) {
	wrapped := func(n *common.DLNode) bool {
		dnode := n.GetPayload().(*DeleteNode)
		return fn(dnode)
	}
	chain.Loop(wrapped, reverse)
}

func (chain *DeleteChain) IsDeleted(row uint32, ts uint64) (deleted bool) {
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		// Skip txn that started after ts
		if n.GetStartTS() > ts {
			return true
		}
		n.RLock()
		// Skip txn that committed|committing after ts
		if n.GetCommitTSLocked() > ts && n.GetStartTS() != ts {
			n.RUnlock()
			return true
		}
		overlap := n.HasOverlapLocked(row, row)
		txn := n.txn
		n.RUnlock()

		if overlap {
			if txn == nil || txn.GetStartTS() == ts {
				deleted = true
			} else {
				state := txn.GetTxnState(true)
				if state == txnif.TxnStateCommitted {
					deleted = true
				}
			}
		}
		if n.IsMerged() || deleted {
			return false
		}
		return true
	}, false)
	return
}

func (chain *DeleteChain) PrepareRangeDelete(start, end uint32, ts uint64) (err error) {
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		n.RLock()
		defer n.RUnlock()
		overlap := n.HasOverlapLocked(start, end)
		if overlap {
			if n.txn == nil || n.txn.GetStartTS() == ts {
				err = txnbase.ErrNotFound
			} else {
				err = txnif.TxnWWConflictErr
			}
			return false
		}
		return true
	}, false)
	return
}

func (chain *DeleteChain) UpdateLocked(node *DeleteNode) {
	chain.Update(node.DLNode)
}

func (chain *DeleteChain) RemoveNodeLocked(node txnif.DeleteNode) {
	chain.Delete(node.(*DeleteNode).DLNode)
}

func (chain *DeleteChain) DepthLocked() int { return chain.Link.Depth() }

func (chain *DeleteChain) AddNodeLocked(txn txnif.AsyncTxn) txnif.DeleteNode {
	node := NewDeleteNode(txn)
	node.AttachTo(chain)
	return node
}

func (chain *DeleteChain) AddMergeNode() txnif.DeleteNode {
	var merged *DeleteNode
	chain.RLock()
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		// Already have a latest merged node
		if n.IsMerged() && merged == nil {
			return false
		} else if n.IsMerged() && merged != nil {
			merged.MergeLocked(n)
			return false
		}
		n.RLock()
		txn := n.txn
		n.RUnlock()
		if txn != nil {
			return true
		}
		if merged == nil {
			merged = NewMergedNode(n.commitTs)
		}
		merged.MergeLocked(n)
		return true
	}, false)
	if merged != nil {
		merged.AttachTo(chain)
	}
	chain.RUnlock()
	return merged
}

// [startTs, endTs)
func (chain *DeleteChain) CollectDeletesInRange(startTs, endTs uint64) (mask *roaring.Bitmap) {
	startNode := chain.CollectDeletesLocked(startTs).(*DeleteNode)
	endNode := chain.CollectDeletesLocked(endTs - 1).(*DeleteNode)
	if endNode == nil {
		return
	}
	if startNode == nil {
		return endNode.GetDeleteMaskLocked()
		// return endNode.(*DeleteNode).GetDeleteMaskLocked()
	}
	mask = endNode.GetDeleteMaskLocked()
	mask2 := startNode.GetDeleteMaskLocked()
	mask.AndNot(mask2)
	return
}

func (chain *DeleteChain) CollectDeletesLocked(ts uint64) txnif.DeleteNode {
	var merged *DeleteNode
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		// Merged node is a loop breaker
		if n.IsMerged() {
			if n.GetCommitTSLocked() > ts {
				return true
			}
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n)
			return false
		}
		n.RLock()
		txn := n.txn
		if txn != nil && txn.GetStartTS() == ts {
			// Use the delete from the same active txn
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n)
		} else if txn != nil && n.GetCommitTSLocked() > ts {
			// Skip txn deletes committed after ts
			n.RUnlock()
			return true
		} else if txn != nil {
			// Wait committing txn with commit ts before ts
			n.RUnlock()
			state := txn.GetTxnState(true)
			// If the txn is rollbacked. skip to the next
			if state == txnif.TxnStateRollbacked {
				return true
			}
			n.RLock()
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n)
		} else if n.GetCommitTSLocked() <= ts {
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n)
		}
		n.RUnlock()
		return true
	}, false)
	return merged
}
