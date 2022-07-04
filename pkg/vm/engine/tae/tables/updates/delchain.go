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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type DeleteChain struct {
	*sync.RWMutex
	*common.Link
	mvcc *MVCCHandle
	cnt  uint32
}

func NewDeleteChain(rwlocker *sync.RWMutex, mvcc *MVCCHandle) *DeleteChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	chain := &DeleteChain{
		RWMutex: rwlocker,
		Link:    new(common.Link),
		mvcc:    mvcc,
	}
	return chain
}

func (chain *DeleteChain) AddDeleteCnt(cnt uint32) {
	atomic.AddUint32(&chain.cnt, cnt)
}

func (chain *DeleteChain) GetDeleteCnt() uint32 {
	return atomic.LoadUint32(&chain.cnt)
}

func (chain *DeleteChain) StringLocked() string {
	msg := "DeleteChain:"
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

func (chain *DeleteChain) GetController() *MVCCHandle { return chain.mvcc }

func (chain *DeleteChain) LoopChainLocked(fn func(node *DeleteNode) bool, reverse bool) {
	wrapped := func(n *common.DLNode) bool {
		dnode := n.GetPayload().(*DeleteNode)
		return fn(dnode)
	}
	chain.Loop(wrapped, reverse)
}

func (chain *DeleteChain) IsDeleted(row uint32, ts uint64, rwlocker *sync.RWMutex) (deleted bool, err error) {
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
				if rwlocker != nil {
					rwlocker.RUnlock()
				}
				state := txn.GetTxnState(true)
				if rwlocker != nil {
					rwlocker.RLock()
				}
				// logutil.Infof("%d -- wait --> %s: %d", ts, txn.Repr(), state)
				if state == txnif.TxnStateCommitted {
					deleted = true
				} else if state == txnif.TxnStateCommitting {
					logutil.Fatal("txn state error")
				} else if state == txnif.TxnStateUnknown {
					err = txnif.TxnInternalErr
				}
			}
		}
		if n.IsMerged() || deleted || err != nil {
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
				err = data.ErrNotFound
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

func (chain *DeleteChain) AddNodeLocked(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	node := NewDeleteNode(txn, deleteType)
	node.AttachTo(chain)
	return node
}

func (chain *DeleteChain) OnReplayNode(deleteNode *DeleteNode) {
	deleteNode.AttachTo(chain)
	chain.AddDeleteCnt(uint32(deleteNode.mask.GetCardinality()))
}

func (chain *DeleteChain) AddMergeNode() txnif.DeleteNode {
	var merged *DeleteNode
	chain.mvcc.RLock()
	// chain.RLock()
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		// Already have a latest merged node
		if n.IsMerged() && merged == nil {
			return false
		} else if n.IsMerged() && merged != nil {
			merged.MergeLocked(n, true)
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
		merged.MergeLocked(n, true)
		return true
	}, false)
	if merged != nil {
		merged.AttachTo(chain)
	}
	// chain.RUnlock()
	chain.mvcc.RUnlock()
	return merged
}

// [startTs, endTs)
func (chain *DeleteChain) CollectDeletesInRange(startTs, endTs uint64) (mask *roaring.Bitmap, indexes []*wal.Index, err error) {
	n, err := chain.CollectDeletesLocked(startTs, true)
	if err != nil {
		return
	}
	startNode := n.(*DeleteNode)
	// n, err = chain.CollectDeletesLocked(endTs-1, true)
	n, err = chain.CollectDeletesLocked(endTs, true)
	if err != nil {
		return
	}
	endNode := n.(*DeleteNode)
	if endNode == nil {
		return
	}
	if startNode == nil {
		mask = endNode.GetDeleteMaskLocked()
		indexes = endNode.logIndexes
		return
	}
	mask = endNode.GetDeleteMaskLocked()
	mask2 := startNode.GetDeleteMaskLocked()
	mask.AndNot(mask2)
	indexes = endNode.logIndexes[len(startNode.logIndexes):]
	return
}

func (chain *DeleteChain) CollectDeletesLocked(ts uint64, collectIndex bool) (txnif.DeleteNode, error) {
	var merged *DeleteNode
	var err error
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		// Merged node is a loop breaker
		if n.IsMerged() {
			if n.GetCommitTSLocked() > ts {
				return true
			}
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n, collectIndex)
			return false
		}
		n.RLock()
		txn := n.txn
		if txn != nil && txn.GetStartTS() == ts {
			// Use the delete from the same active txn
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n, collectIndex)
		} else if txn != nil && n.GetCommitTSLocked() > ts {
			// Skip txn deletes committed after ts
			n.RUnlock()
			return true
		} else if txn != nil {
			// Wait committing txn with commit ts before ts
			n.RUnlock()
			state := txn.GetTxnState(true)
			// logutil.Infof("%d -- wait --> %s: %d", ts, txn.Repr(), state)
			// If the txn is rollbacked. skip to the next
			if state == txnif.TxnStateRollbacked {
				return true
			} else if state == txnif.TxnStateUnknown {
				err = txnif.TxnInternalErr
				return false
			}
			n.RLock()
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n, collectIndex)
		} else if n.GetCommitTSLocked() <= ts {
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n, collectIndex)
		}
		n.RUnlock()
		return true
	}, false)
	return merged, err
}
