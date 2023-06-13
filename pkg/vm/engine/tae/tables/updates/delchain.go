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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func mockTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), types.TS{})
	return txn
}

func MockTxnWithStartTS(ts types.TS) *txnbase.Txn {
	txn := mockTxn()
	txn.StartTS = ts
	return txn
}

type DeleteChain struct {
	*sync.RWMutex
	*txnbase.MVCCChain[*DeleteNode]
	mvcc  *MVCCHandle
	nodes map[uint32]*DeleteNode
	cnt   atomic.Uint32
	mask  *nulls.Bitmap
}

func NewDeleteChain(rwlocker *sync.RWMutex, mvcc *MVCCHandle) *DeleteChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	chain := &DeleteChain{
		RWMutex:   rwlocker,
		MVCCChain: txnbase.NewMVCCChain((*DeleteNode).Less, NewEmptyDeleteNode),
		nodes:     make(map[uint32]*DeleteNode),
		mvcc:      mvcc,
		mask:      &nulls.Bitmap{},
	}
	return chain
}
func (chain *DeleteChain) AddDeleteCnt(cnt uint32) {
	chain.cnt.Add(cnt)
}

func (chain *DeleteChain) GetDeleteCnt() uint32 {
	return chain.cnt.Load()
}

func (chain *DeleteChain) StringLocked() string {
	msg := "DeleteChain:"
	line := 1
	chain.LoopChain(func(n *DeleteNode) bool {
		msg = fmt.Sprintf("%s\n%d. %s", msg, line, n.StringLocked())
		line++
		return true
	})
	return msg
}

func (chain *DeleteChain) GetController() *MVCCHandle { return chain.mvcc }

func (chain *DeleteChain) IsDeleted(row uint32, txn txnif.TxnReader, rwlocker *sync.RWMutex) (deleted bool, err error) {
	deleteNode := chain.GetDeleteNodeByRow(row)
	if deleteNode == nil {
		return false, nil
	}
	needWait, waitTxn := deleteNode.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		rwlocker.RUnlock()
		waitTxn.GetTxnState(true)
		rwlocker.RLock()
	}
	return deleteNode.IsVisible(txn), nil
}

func (chain *DeleteChain) PrepareRangeDelete(start, end uint32, ts types.TS) (err error) {
	if chain.hasOverLap(uint64(start), uint64(end)) {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (chain *DeleteChain) hasOverLap(start, end uint64) bool {
	if chain.mask.IsEmpty() {
		return false
	}
	var yes bool
	for i := start; i < end+1; i++ {
		if chain.mask.Contains(i) {
			yes = true
			break
		}
	}
	return yes
}

func (chain *DeleteChain) UpdateLocked(node *DeleteNode) {
	chain.MVCC.Update(node.GenericDLNode)
}

func (chain *DeleteChain) RemoveNodeLocked(node txnif.DeleteNode) {
	chain.MVCC.Delete(node.(*DeleteNode).GenericDLNode)
	chain.deleteInMaskByNode(node)
}

func (chain *DeleteChain) deleteInMaskByNode(node txnif.DeleteNode) {
	it := node.GetRowMaskRefLocked().Iterator()
	for it.HasNext() {
		row := it.Next()
		chain.mask.Del(uint64(row))
	}
}

func (chain *DeleteChain) insertInMaskByNode(node txnif.DeleteNode) {
	it := node.GetRowMaskRefLocked().Iterator()
	for it.HasNext() {
		row := it.Next()
		chain.mask.Add(uint64(row))
	}
}

func (chain *DeleteChain) insertInMaskByRange(start, end uint32) {
	chain.mask.AddRange(uint64(start), uint64(end+1))
}

func (chain *DeleteChain) DepthLocked() int { return chain.MVCC.Depth() }

func (chain *DeleteChain) AddNodeLocked(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	node := NewDeleteNode(txn, deleteType)
	node.AttachTo(chain)
	return node
}
func (chain *DeleteChain) InsertInDeleteView(row uint32, deleteNode *DeleteNode) {
	if chain.nodes[row] != nil {
		panic(fmt.Sprintf("row %d already in delete view", row))
	}
	chain.nodes[row] = deleteNode
}
func (chain *DeleteChain) DeleteInDeleteView(deleteNode *DeleteNode) {
	it := deleteNode.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		if chain.nodes[row] != deleteNode {
			panic(fmt.Sprintf("row %d not in delete view", row))
		}
		delete(chain.nodes, row)
	}
}
func (chain *DeleteChain) OnReplayNode(deleteNode *DeleteNode) {
	it := deleteNode.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		chain.InsertInDeleteView(row, deleteNode)
	}
	deleteNode.AttachTo(chain)
	chain.AddDeleteCnt(uint32(deleteNode.mask.GetCardinality()))
	chain.insertInMaskByNode(deleteNode)
	chain.mvcc.IncChangeNodeCnt()
}

func (chain *DeleteChain) AddMergeNode() txnif.DeleteNode {
	var merged *DeleteNode
	chain.mvcc.RLock()
	// chain.RLock()
	chain.LoopChain(func(n *DeleteNode) bool {
		// Already have a latest merged node
		if n.IsMerged() && merged == nil {
			return false
		} else if n.IsMerged() && merged != nil {
			merged.MergeLocked(n)
			return false
		}
		txn := n.GetTxn()
		if txn != nil {
			return true
		}
		if merged == nil {
			merged = NewMergedNode(n.GetCommitTSLocked())
		}
		merged.MergeLocked(n)
		return true
	})
	if merged != nil {
		merged.AttachTo(chain)
	}
	// chain.RUnlock()
	chain.mvcc.RUnlock()
	return merged
}

// CollectDeletesInRange collects [startTs, endTs)
func (chain *DeleteChain) CollectDeletesInRange(
	startTs, endTs types.TS,
	rwlocker *sync.RWMutex) (mask *nulls.Bitmap, err error) {
	chain.LoopChain(func(n *DeleteNode) bool {
		// Merged node is a loop breaker
		if n.IsMerged() {
			if n.GetCommitTSLocked().Greater(endTs) {
				return true
			}
			if mask == nil {
				mask = nulls.NewWithSize(int(n.mask.Maximum()))
			}
			mergeDelete(mask, n)
			return false
		}
		needWait, txnToWait := n.NeedWaitCommitting(endTs)
		if needWait {
			rwlocker.RUnlock()
			txnToWait.GetTxnState(true)
			rwlocker.RLock()
		}
		if n.IsVisibleByTS(endTs) && !n.IsVisibleByTS(startTs) {
			if mask == nil {
				mask = nulls.NewWithSize(int(n.mask.Maximum()))
			}
			mergeDelete(mask, n)
		}
		return true
	})
	return
}

// any uncommited node, return true
// any committed node with prepare ts within [from, to], return true
func (chain *DeleteChain) HasDeleteIntentsPreparedInLocked(from, to types.TS) (found bool) {
	chain.LoopChain(func(n *DeleteNode) bool {
		if n.IsMerged() {
			found, _ = n.PreparedIn(from, to)
			return false
		}

		if n.IsActive() {
			return true
		}

		found, _ = n.PreparedIn(from, to)
		if n.IsAborted() {
			found = false
		}
		return !found
	})
	return
}

func mergeDelete(mask *nulls.Bitmap, node *DeleteNode) {
	if node == nil || node.mask == nil {
		return
	}
	it := node.mask.Iterator()
	for it.HasNext() {
		mask.Add(uint64(it.Next()))
	}
}

func (chain *DeleteChain) CollectDeletesLocked(
	txn txnif.TxnReader,
	rwlocker *sync.RWMutex) (merged *nulls.Bitmap, err error) {
	merged = chain.mask.Clone()
	chain.LoopChain(func(n *DeleteNode) bool {
		needWait, txnToWait := n.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			rwlocker.RUnlock()
			txnToWait.GetTxnState(true)
			rwlocker.RLock()
		}
		if !n.IsVisible(txn) {
			it := n.GetDeleteMaskLocked().Iterator()
			for it.HasNext() {
				row := it.Next()
				merged.Del(uint64(row))
			}
		}
		return true
	})
	return merged, err
}

func (chain *DeleteChain) GetDeleteNodeByRow(row uint32) (n *DeleteNode) {
	return chain.nodes[row]
}
