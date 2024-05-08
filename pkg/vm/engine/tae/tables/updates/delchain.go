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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

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
	mvcc          *MVCCHandle
	links         map[uint32]*DeleteNode
	cnt           atomic.Uint32
	mask          *nulls.Bitmap // in memory del mask
	persistedMask *nulls.Bitmap // persisted del mask
}

func NewDeleteChain(rwlocker *sync.RWMutex, mvcc *MVCCHandle) *DeleteChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	chain := &DeleteChain{
		RWMutex:       rwlocker,
		MVCCChain:     txnbase.NewMVCCChain((*DeleteNode).Less, NewEmptyDeleteNode),
		links:         make(map[uint32]*DeleteNode),
		mvcc:          mvcc,
		mask:          &nulls.Bitmap{},
		persistedMask: &nulls.Bitmap{},
	}
	return chain
}
func (chain *DeleteChain) AddDeleteCnt(cnt uint32) {
	chain.cnt.Add(cnt)
}

// IsEmtpy indicates whether memory deletes are empty
func (chain *DeleteChain) IsEmpty() bool {
	return chain.mask.IsEmpty()
}
func (chain *DeleteChain) GetDeleteCnt() uint32 {
	return chain.cnt.Load()
}

func (chain *DeleteChain) StringLocked() string {
	msg := "DeleteChain:"
	line := 1
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		msg = fmt.Sprintf("%s\n%d. %s", msg, line, n.StringLocked())
		line++
		return true
	})
	if line == 1 {
		return ""
	}
	return msg
}

func (chain *DeleteChain) EstimateMemSizeLocked() int {
	size := 0
	if chain.mask != nil {
		size += chain.mask.GetBitmap().Size()
	}
	if chain.persistedMask != nil {
		size += chain.persistedMask.GetBitmap().Size()
	}
	size += int(float32(len(chain.links)*(4+8)) * 1.1) /*map size*/
	size += chain.MVCCChain.Depth() * (DeleteNodeApproxSize + 16 /* link node overhead */)

	return size + DeleteChainApproxSize
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
	if (chain.mask == nil || chain.mask.IsEmpty()) &&
		(chain.persistedMask == nil || chain.persistedMask.IsEmpty()) {
		return false
	}
	var yes bool
	for i := start; i < end+1; i++ {
		if chain.mask.Contains(i) || chain.persistedMask.Contains(i) {
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
	node := NewDeleteNode(txn, deleteType, IOET_WALTxnCommand_DeleteNode_V2)
	node.AttachTo(chain)
	return node
}

func (chain *DeleteChain) AddPersistedNodeLocked(txn txnif.AsyncTxn, deltaloc objectio.Location) txnif.DeleteNode {
	node := NewPersistedDeleteNode(txn, deltaloc)
	node.AttachTo(chain)
	node.setPersistedRows()
	mask := node.chain.Load().mask
	it := node.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		mask.Add(uint64(row))
	}
	return node
}

func (chain *DeleteChain) InsertInDeleteView(row uint32, deleteNode *DeleteNode) {
	if chain.links[row] != nil {
		panic(fmt.Sprintf("row %d already in delete view", row))
	}
	chain.links[row] = deleteNode
}
func (chain *DeleteChain) DeleteInDeleteView(deleteNode *DeleteNode) {
	it := deleteNode.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		if chain.links[row] != deleteNode {
			panic(fmt.Sprintf("row %d not in delete view", row))
		}
		delete(chain.links, row)
	}
}

func (chain *DeleteChain) shrinkDeleteChainByTSLocked(flushed types.TS) *DeleteChain {
	new := NewDeleteChain(chain.RWMutex, chain.mvcc)
	new.persistedMask = chain.persistedMask

	chain.LoopChainLocked(func(n *DeleteNode) bool {
		if !n.IsVisibleByTS(flushed) {
			if n.nt == NT_Persisted {
				return false
			}
			n.AttachTo(new)
			it := n.mask.Iterator()
			for it.HasNext() {
				row := it.Next()
				new.InsertInDeleteView(row, n)
				new.mask.Add(uint64(row))
			}
		} else {
			it := n.mask.Iterator()
			for it.HasNext() {
				row := it.Next()
				new.persistedMask.Add(uint64(row))
			}
			n.Close()
		}
		return true
	})

	new.cnt.Store(chain.cnt.Load())

	return new
}

func (chain *DeleteChain) OnReplayNode(deleteNode *DeleteNode) {
	deleteNode.AttachTo(chain)
	switch deleteNode.nt {
	case NT_Persisted:
	case NT_Merge, NT_Normal:
		it := deleteNode.mask.Iterator()
		for it.HasNext() {
			row := it.Next()
			chain.InsertInDeleteView(row, deleteNode)
		}
	}
	chain.AddDeleteCnt(uint32(deleteNode.mask.GetCardinality()))
	chain.insertInMaskByNode(deleteNode)
	chain.mvcc.IncChangeIntentionCnt()
}

func (chain *DeleteChain) AddMergeNode() txnif.DeleteNode {
	var merged *DeleteNode
	chain.mvcc.RLock()
	chain.LoopChainLocked(func(n *DeleteNode) bool {
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
func (chain *DeleteChain) CollectDeletesInRangeWithLock(
	startTs, endTs types.TS,
	rwlocker *sync.RWMutex,
) (mask *nulls.Bitmap, err error) {
	for {
		needWaitFound := false
		mask = nil
		chain.LoopChainLocked(func(n *DeleteNode) bool {
			// Merged node is a loop breaker
			if n.IsMerged() {
				commitTS := n.GetCommitTSLocked()
				if commitTS.Greater(&endTs) {
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
				needWaitFound = true
				return false
			}
			if n.IsVisibleByTS(endTs) && !n.IsVisibleByTS(startTs) {
				if mask == nil {
					mask = nulls.NewWithSize(int(n.mask.Maximum()))
				}
				mergeDelete(mask, n)
			}
			return true
		})
		if !needWaitFound {
			break
		}
	}
	return
}

// any uncommited node, return true
// any committed node with prepare ts within [from, to], return true
func (chain *DeleteChain) HasDeleteIntentsPreparedInLocked(from, to types.TS) (found, isPersisted bool) {
	chain.LoopChainLocked(func(n *DeleteNode) bool {
		if n.IsMerged() {
			found, _ = n.PreparedIn(from, to)
			return false
		}

		if n.IsActive() {
			return true
		}

		if n.nt == NT_Persisted {
			isPersisted = true
		}
		found, _ = n.PreparedIn(from, to)
		if n.IsAborted() {
			found = false
		}
		return !found
	})
	return
}

func (chain *DeleteChain) ResetPersistedMask() { chain.persistedMask = &nulls.Bitmap{} }

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
	for {
		needWaitFound := false
		merged = chain.mask.Clone()
		chain.LoopChainLocked(func(n *DeleteNode) bool {
			needWait, txnToWait := n.NeedWaitCommitting(txn.GetStartTS())
			if needWait {
				rwlocker.RUnlock()
				txnToWait.GetTxnState(true)
				rwlocker.RLock()
				needWaitFound = true
				return false
			}
			if !n.IsVisible(txn) {
				it := n.GetDeleteMaskLocked().Iterator()
				if n.dt != handle.DT_MergeCompact {
					for it.HasNext() {
						row := it.Next()
						merged.Del(uint64(row))
					}
				} else {
					ts := txn.GetStartTS()
					rt := chain.mvcc.meta.GetObjectData().GetRuntime()
					tsMapping := rt.TransferDelsMap.GetDelsForBlk(*objectio.NewBlockidWithObjectID(&chain.mvcc.meta.ID, chain.mvcc.blkID)).Mapping
					if tsMapping == nil {
						logutil.Warnf("flushtabletail check special dels for %s, no tsMapping", chain.mvcc.meta.ID.String())
						return true
					}
					for it.HasNext() {
						row := it.Next()
						committs, ok := tsMapping[int(row)]
						if !ok {
							logutil.Errorf("flushtabletail check Transfer dels for %s row %d not in dels", chain.mvcc.meta.ID.String(), row)
							continue
						}
						// if the ts can't see the del, then remove it from merged
						if committs.Greater(&ts) {
							merged.Del(uint64(row))
						}
					}
				}
			}
			return true
		})

		if !needWaitFound {
			break
		}
	}

	return merged, err
}

func (chain *DeleteChain) GetDeleteNodeByRow(row uint32) (n *DeleteNode) {
	return chain.links[row]
}
