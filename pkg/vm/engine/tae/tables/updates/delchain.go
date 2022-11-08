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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type DeleteChain struct {
	*sync.RWMutex
	*txnbase.MVCCChain
	mvcc  *MVCCHandle
	links map[uint32]*common.GenericSortedDList[txnif.MVCCNode]
	cnt   atomic.Uint32
}

func NewDeleteChain(rwlocker *sync.RWMutex, mvcc *MVCCHandle) *DeleteChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	chain := &DeleteChain{
		RWMutex:   rwlocker,
		MVCCChain: txnbase.NewMVCCChain(compareDeleteNode, NewEmptyDeleteNode),
		links:     make(map[uint32]*common.GenericSortedDList[txnif.MVCCNode]),
		mvcc:      mvcc,
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
	chain.LoopChain(func(vn txnif.MVCCNode) bool {
		n := vn.(*DeleteNode)
		n.chain.mvcc.RLock()
		msg = fmt.Sprintf("%s\n%d. %s", msg, line, n.StringLocked())
		n.chain.mvcc.RUnlock()
		line++
		return true
	})
	return msg
}

func (chain *DeleteChain) GetController() *MVCCHandle { return chain.mvcc }

func (chain *DeleteChain) IsDeleted(row uint32, ts types.TS, rwlocker *sync.RWMutex) (deleted bool, err error) {
	deleteNode := chain.GetDeleteNodeByRow(row)
	if deleteNode == nil {
		return false, nil
	}
	needWait, txn := deleteNode.NeedWaitCommitting(ts)
	if needWait {
		rwlocker.RUnlock()
		txn.GetTxnState(true)
		rwlocker.RLock()
	}
	return deleteNode.IsVisible(ts), nil
}

func (chain *DeleteChain) PrepareRangeDelete(start, end uint32, ts types.TS) (err error) {
	chain.LoopChain(
		func(vn txnif.MVCCNode) bool {
			n := vn.(*DeleteNode)
			overlap := n.HasOverlapLocked(start, end)
			if overlap {
				err = n.CheckConflict(ts)
				if err == nil {
					err = moerr.NewNotFound()
				}
				return false
			}
			return true
		})
	return
}

func (chain *DeleteChain) UpdateLocked(node *DeleteNode) {
	chain.MVCC.Update(node.GenericDLNode)
}

func (chain *DeleteChain) RemoveNodeLocked(node txnif.DeleteNode) {
	chain.MVCC.Delete(node.(*DeleteNode).GenericDLNode)
}

func (chain *DeleteChain) DepthLocked() int { return chain.MVCC.Depth() }

func (chain *DeleteChain) AddNodeLocked(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	node := NewDeleteNode(txn, deleteType)
	node.AttachTo(chain)
	return node
}
func (chain *DeleteChain) InsertInDeleteView(row uint32, deleteNode *DeleteNode) {
	var link *common.GenericSortedDList[txnif.MVCCNode]
	if link = chain.links[row]; link == nil {
		link = common.NewGenericSortedDList(compareDeleteNode)
		n := link.Insert(deleteNode)
		deleteNode.viewNodes[row] = n
		chain.links[row] = link
		return
	}
	link.Insert(deleteNode)
}
func (chain *DeleteChain) DeleteInDeleteView(deleteNode *DeleteNode) {
	it := deleteNode.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		link := chain.links[row]
		link.Delete(deleteNode.viewNodes[row])
		if link.Depth() == 0 {
			delete(chain.links, row)
		}
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
	chain.mvcc.IncChangeNodeCnt()
}

func (chain *DeleteChain) AddMergeNode() txnif.DeleteNode {
	var merged *DeleteNode
	chain.mvcc.RLock()
	// chain.RLock()
	chain.LoopChain(func(vn txnif.MVCCNode) bool {
		n := vn.(*DeleteNode)
		// Already have a latest merged node
		if n.IsMerged() && merged == nil {
			return false
		} else if n.IsMerged() && merged != nil {
			merged.MergeLocked(n, true)
			return false
		}
		txn := n.GetTxn()
		if txn != nil {
			return true
		}
		if merged == nil {
			merged = NewMergedNode(n.GetCommitTSLocked())
		}
		merged.MergeLocked(n, true)
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
	rwlocker *sync.RWMutex) (mask *roaring.Bitmap, indexes []*wal.Index, err error) {
	n, err := chain.CollectDeletesLocked(startTs, true, rwlocker)
	if err != nil {
		return
	}
	startNode := n.(*DeleteNode)
	// n, err = chain.CollectDeletesLocked(endTs-1, true)
	n, err = chain.CollectDeletesLocked(endTs, true, rwlocker)
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

// any uncommited node, return true
// any committed node with prepare ts within [from, to], return true
func (chain *DeleteChain) HasDeleteIntentsPreparedInLocked(from, to types.TS) (found bool) {
	chain.LoopChain(func(vn txnif.MVCCNode) bool {
		n := vn.(*DeleteNode)
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

func (chain *DeleteChain) CollectDeletesLocked(
	ts types.TS,
	collectIndex bool,
	rwlocker *sync.RWMutex) (txnif.DeleteNode, error) {
	var merged *DeleteNode
	var err error
	chain.LoopChain(func(vn txnif.MVCCNode) bool {
		n := vn.(*DeleteNode)
		// Merged node is a loop breaker
		if n.IsMerged() {
			if n.GetCommitTSLocked().Greater(ts) {
				return true
			}
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n, collectIndex)
			return false
		}
		needWait, txnToWait := n.NeedWaitCommitting(ts)
		if needWait {
			rwlocker.RUnlock()
			txnToWait.GetTxnState(true)
			rwlocker.RLock()
		}
		if n.IsVisible(ts) {
			if merged == nil {
				merged = NewMergedNode(n.GetCommitTSLocked())
			}
			merged.MergeLocked(n, collectIndex)
		}
		return true
	})
	return merged, err
}

func (chain *DeleteChain) GetDeleteNodeByRow(row uint32) (n *DeleteNode) {
	link := chain.links[row]
	if link == nil {
		return
	}
	link.Loop(func(vn *common.GenericDLNode[txnif.MVCCNode]) bool {
		n = vn.GetPayload().(*DeleteNode)
		return n.Aborted
	}, false)
	return
}
